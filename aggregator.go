package producer

import (
	"bytes"
	"crypto/md5"
	"errors"
	"sync"

	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"google.golang.org/protobuf/proto"
)

var (
	magicNumber = []byte{0xF3, 0x89, 0x9A, 0xC2}
)

// Aggregator coordinates multiple Reducers for per-shard aggregation.
//
// Routing Strategy:
//
//	Routes records by target shard ID (computed from partition key MD5 hash).
//	ShardMap is required to determine the target shard for each partition key.
//
// This matches Java KPL behavior where per-shard aggregation is always enabled
// and routes records based on shard boundaries.
//
// Example:
//
//	shardMap, _ := NewShardMap(streamName, client, logger, false, 30*time.Second)
//	aggregator := NewAggregator(shardMap, config)
//	aggregator.Put(data, "random-uuid-123")  // Routes to shard based on hash
type Aggregator struct {
	reducers    map[string]*Reducer
	shardMap    *ShardMap
	config      *ReducerConfig
	flushNotify chan string // channel to receive flush notifications from Reducers
	mutex       sync.RWMutex
}

// NewAggregator creates a new Aggregator with a required ShardMap.
// ShardMap must not be nil as it is required for per-shard aggregation (matching Java KPL behavior).
// Returns an error if ShardMap is nil.
func NewAggregator(shardMap *ShardMap, config *ReducerConfig) (*Aggregator, error) {
	if shardMap == nil {
		return nil, errors.New("aggregator: ShardMap is required and must not be nil")
	}
	if config == nil {
		config = &ReducerConfig{
			MaxSize:  defaultAggregationSize,
			MaxCount: maxAggregationCount,
		}
	}
	return &Aggregator{
		reducers:    make(map[string]*Reducer),
		shardMap:    shardMap,
		config:      config,
		flushNotify: make(chan string, 100), // buffered channel for flush notifications
	}, nil
}

// Size returns the total bytes across all reducers.
func (a *Aggregator) Size() int {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	total := 0
	for _, r := range a.reducers {
		total += r.Size()
	}
	return total
}

// Count returns the total number of records across all reducers.
func (a *Aggregator) Count() int {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	total := 0
	for _, r := range a.reducers {
		total += r.Count()
	}
	return total
}

// Put adds a record to the appropriate reducer.
// Routes by shard ID (computed from partition key hash via ShardMap).
// Returns flushed aggregated record if flush conditions are met, nil otherwise.
// This matches Java KPL behavior where records are immediately returned when ready.
func (a *Aggregator) Put(data []byte, partitionKey string) (*ktypes.PutRecordsRequestEntry, error) {
	// Determine routing key (shard ID)
	shardID, err := a.shardMap.GetShardID(partitionKey)
	if err != nil {
		// Log warning when falling back to partition key
		if a.shardMap.logger != nil {
			a.shardMap.logger.Error("failed to get shard ID, using partition key as fallback", err)
		}
		// Fallback to partition key if shard map lookup fails
		// This should be rare and only happens if ShardMap is in an invalid state
		shardID = partitionKey
	}
	routingKey := shardID

	// Get or create reducer
	// Use double-checked locking to minimize lock contention
	a.mutex.RLock()
	r, exists := a.reducers[routingKey]
	a.mutex.RUnlock()

	if !exists {
		a.mutex.Lock()
		// Re-check after acquiring write lock (another goroutine may have created it)
		r, exists = a.reducers[routingKey]
		if !exists {
			r = newReducerWithNotify(routingKey, a.config, a.flushNotify)
			a.reducers[routingKey] = r
		}
		a.mutex.Unlock()
	}

	// Add record to reducer and get flushed record if any
	return r.Put(data, partitionKey)
}

// Drain returns all records from all reducers, regardless of flush conditions.
// This is useful for flushing on shutdown or periodic flushes.
// After draining, empty reducers are removed.
func (a *Aggregator) Drain() ([]*ktypes.PutRecordsRequestEntry, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Pre-allocate with expected capacity to avoid reallocations
	entries := make([]*ktypes.PutRecordsRequestEntry, 0, len(a.reducers))

	for key, r := range a.reducers {
		if !r.IsEmpty() {
			entry, err := r.Drain()
			if err != nil {
				return entries, err
			}
			if entry != nil {
				entries = append(entries, entry)
			}
			// Remove empty reducer
			if r.IsEmpty() {
				delete(a.reducers, key)
			}
		}
	}

	return entries, nil
}

// GetShardCount returns the number of distinct shards/keys being aggregated.
func (a *Aggregator) GetShardCount() int {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	return len(a.reducers)
}

// DrainShard drains a specific shard's reducer by shardID.
// Returns the aggregated record if the shard exists and has records, nil otherwise.
// This is used for event-driven flushing when FlushInterval expires.
func (a *Aggregator) DrainShard(shardID string) (*ktypes.PutRecordsRequestEntry, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	r, exists := a.reducers[shardID]
	if !exists || r.IsEmpty() {
		return nil, nil
	}

	entry, err := r.Drain()
	if err != nil {
		return nil, err
	}

	// Remove empty reducer after drain
	if r.IsEmpty() {
		delete(a.reducers, shardID)
	}

	return entry, nil
}

// Test if a given entry is aggregated record.
func isAggregated(entry *ktypes.PutRecordsRequestEntry) bool {
	return bytes.HasPrefix(entry.Data, magicNumber)
}

func extractRecords(entry *ktypes.PutRecordsRequestEntry) (out []ktypes.PutRecordsRequestEntry) {
	src := entry.Data[len(magicNumber) : len(entry.Data)-md5.Size]
	dest := new(AggregatedRecord)
	err := proto.Unmarshal(src, dest)
	if err != nil {
		return
	}
	for i := range dest.Records {
		r := dest.Records[i]
		out = append(out, ktypes.PutRecordsRequestEntry{
			Data:         r.GetData(),
			PartitionKey: &dest.PartitionKeyTable[r.GetPartitionKeyIndex()],
		})
	}
	return
}
