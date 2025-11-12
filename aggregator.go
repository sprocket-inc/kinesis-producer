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

// Aggregator coordinates multiple ShardAggregators for per-shard aggregation.
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
	shardAggregators map[string]*ShardAggregator
	shardMap         *ShardMap
	config           *AggregatorConfig
	mutex            sync.RWMutex
}

// NewAggregator creates a new Aggregator with a required ShardMap.
// ShardMap must not be nil as it is required for per-shard aggregation (matching Java KPL behavior).
// Returns an error if ShardMap is nil.
func NewAggregator(shardMap *ShardMap, config *AggregatorConfig) (*Aggregator, error) {
	if shardMap == nil {
		return nil, errors.New("aggregator: ShardMap is required and must not be nil")
	}
	if config == nil {
		config = &AggregatorConfig{
			MaxSize:  defaultAggregationSize,
			MaxCount: maxAggregationCount,
		}
	}
	return &Aggregator{
		shardAggregators: make(map[string]*ShardAggregator),
		shardMap:         shardMap,
		config:           config,
	}, nil
}

// Size returns the total bytes across all shard aggregators.
func (a *Aggregator) Size() int {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	total := 0
	for _, sa := range a.shardAggregators {
		total += sa.Size()
	}
	return total
}

// Count returns the total number of records across all shard aggregators.
func (a *Aggregator) Count() int {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	total := 0
	for _, sa := range a.shardAggregators {
		total += sa.Count()
	}
	return total
}

// Put adds a record to the appropriate shard aggregator.
// Routes by shard ID (computed from partition key hash via ShardMap).
func (a *Aggregator) Put(data []byte, partitionKey string) {
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

	// Get or create shard aggregator
	// Use double-checked locking to minimize lock contention
	a.mutex.RLock()
	sa, exists := a.shardAggregators[routingKey]
	a.mutex.RUnlock()

	if !exists {
		a.mutex.Lock()
		// Re-check after acquiring write lock (another goroutine may have created it)
		sa, exists = a.shardAggregators[routingKey]
		if !exists {
			sa = NewShardAggregator(routingKey, a.config)
			a.shardAggregators[routingKey] = sa
		}
		a.mutex.Unlock()
	}

	// Add record to shard aggregator
	sa.Put(data, partitionKey)
}

// Drain returns all records from all aggregators, regardless of flush conditions.
// This is useful for flushing on shutdown or periodic flushes.
// After draining, empty aggregators are removed.
func (a *Aggregator) Drain() ([]*ktypes.PutRecordsRequestEntry, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Pre-allocate with expected capacity to avoid reallocations
	entries := make([]*ktypes.PutRecordsRequestEntry, 0, len(a.shardAggregators))

	for key, sa := range a.shardAggregators {
		if !sa.IsEmpty() {
			entry, err := sa.Drain()
			if err != nil {
				return entries, err
			}
			if entry != nil {
				entries = append(entries, entry)
			}
			// Remove empty aggregator
			if sa.IsEmpty() {
				delete(a.shardAggregators, key)
			}
		}
	}

	return entries, nil
}

// DrainReady returns all aggregated records that should be flushed based on flush conditions.
// This is the recommended method for periodic flushes when you only want to drain records
// that have reached their flush thresholds, as opposed to Drain() which drains all records.
func (a *Aggregator) DrainReady() ([]*ktypes.PutRecordsRequestEntry, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Pre-allocate with expected capacity to avoid reallocations
	entries := make([]*ktypes.PutRecordsRequestEntry, 0, len(a.shardAggregators))

	for key, sa := range a.shardAggregators {
		if sa.ShouldFlush() {
			entry, err := sa.Drain()
			if err != nil {
				return entries, err
			}
			if entry != nil {
				entries = append(entries, entry)
			}
			// Remove empty aggregator
			if sa.IsEmpty() {
				delete(a.shardAggregators, key)
			}
		}
	}

	return entries, nil
}

// clear removes all aggregators (for compatibility with old code).
func (a *Aggregator) clear() {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.shardAggregators = make(map[string]*ShardAggregator)
}

// HasRecords returns true if any aggregator has records.
func (a *Aggregator) HasRecords() bool {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	for _, sa := range a.shardAggregators {
		if !sa.IsEmpty() {
			return true
		}
	}
	return false
}

// GetShardCount returns the number of distinct shards/keys being aggregated.
func (a *Aggregator) GetShardCount() int {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	return len(a.shardAggregators)
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
