package producer

import (
	"bytes"
	"crypto/md5"
	"math/big"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"google.golang.org/protobuf/proto"
)

// Reducer aggregates records for a single shard.
// This matches the KPL Reducer<UserRecord, KinesisRecord> behavior.
// All records in this reducer will be sent to the same shard.
type Reducer struct {
	shardID      string
	buf          []*Record
	pkeys        []string       // partition key index table
	pkeyMap      map[string]int // partition key -> index for deduplication
	nbytes       int            // total bytes including keys
	nrecords     int            // number of records
	creationTime time.Time      // time when first record was added (for FlushInterval)
	timer        *time.Timer    // timer for FlushInterval (event-driven)
	flushNotify  chan<- string  // channel to notify when flush is needed (sends shardID)
	mutex        sync.Mutex
	config       *ReducerConfig
}

// ReducerConfig holds configuration for reducers (per-shard aggregation).
type ReducerConfig struct {
	MaxSize       int           // max bytes per aggregated record (default 50KB)
	MaxCount      int           // max records per aggregated record (default 1000)
	FlushInterval time.Duration // max time records can stay buffered (default 100ms, matching Java KPL)
}

// NewReducer creates a new shard-specific reducer (for testing without notification).
func NewReducer(shardID string, config *ReducerConfig) *Reducer {
	return newReducerWithNotify(shardID, config, nil)
}

// newReducerWithNotify creates a new shard-specific reducer with flush notification channel.
func newReducerWithNotify(shardID string, config *ReducerConfig, flushNotify chan<- string) *Reducer {
	if config == nil {
		config = &ReducerConfig{
			MaxSize:  defaultAggregationSize,
			MaxCount: maxAggregationCount,
		}
	}
	return &Reducer{
		shardID:     shardID,
		pkeyMap:     make(map[string]int),
		config:      config,
		flushNotify: flushNotify,
	}
}

// Count returns the number of records.
func (r *Reducer) Count() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.nrecords
}

// Drain creates an aggregated PutRecordsRequestEntry and resets the reducer.
// Returns nil if there are no records to drain.
func (r *Reducer) Drain() (*ktypes.PutRecordsRequestEntry, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.drainLocked()
}

// drainLocked is the internal drain implementation without locking.
// Must be called with r.mutex already held.
func (r *Reducer) drainLocked() (*ktypes.PutRecordsRequestEntry, error) {
	if len(r.buf) == 0 {
		return nil, nil
	}

	// Stop timer before draining (will be reset on next Put if needed)
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}

	// Marshal the aggregated record
	data, err := proto.Marshal(&AggregatedRecord{
		PartitionKeyTable: r.pkeys,
		Records:           r.buf,
	})
	if err != nil {
		return nil, err
	}

	// Add magic number and checksum
	h := md5.New()
	h.Write(data)
	checkSum := h.Sum(nil)
	aggData := append(magicNumber, data...)
	aggData = append(aggData, checkSum...)

	// Compute explicit hash key from first partition key
	// This matches Java KPL behavior
	firstKey := r.pkeys[0]
	hashKey := r.computeExplicitHashKey(firstKey)

	// Use a placeholder partition key and explicit hash key for routing
	// The explicit hash key determines the actual shard
	entry := &ktypes.PutRecordsRequestEntry{
		Data:            aggData,
		PartitionKey:    aws.String("a"), // Placeholder like Java KPL
		ExplicitHashKey: aws.String(hashKey),
	}

	// Reset the reducer
	r.reset()

	return entry, nil
}

// IsEmpty returns true if the reducer has no records.
func (r *Reducer) IsEmpty() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return len(r.buf) == 0
}

// Put adds a record to the reducer with partition key deduplication.
// Returns flushed aggregated record if flush conditions are met, nil otherwise.
// This matches Java KPL behavior where Reducer.add() returns flushed container.
func (r *Reducer) Put(data []byte, partitionKey string) (*ktypes.PutRecordsRequestEntry, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Record creation time and start timer on first record (event-driven FlushInterval)
	if len(r.buf) == 0 {
		r.creationTime = time.Now()

		// Start timer if FlushInterval is configured and flushNotify channel exists
		if r.config.FlushInterval > 0 && r.flushNotify != nil {
			r.timer = time.AfterFunc(r.config.FlushInterval, func() {
				// Non-blocking send to avoid deadlock
				select {
				case r.flushNotify <- r.shardID:
					// Successfully notified
				default:
					// Channel full, skip notification (will be flushed eventually)
				}
			})
		}
	}

	// Check if partition key already exists (deduplication)
	keyIndex, exists := r.pkeyMap[partitionKey]
	if !exists {
		// New partition key - add to table
		keyIndex = len(r.pkeys)
		r.pkeys = append(r.pkeys, partitionKey)
		r.pkeyMap[partitionKey] = keyIndex
		r.nbytes += len([]byte(partitionKey))
	}

	// Add record
	partitionKeyIndex := uint64(keyIndex)
	r.buf = append(r.buf, &Record{
		Data:              data,
		PartitionKeyIndex: &partitionKeyIndex,
	})
	r.nbytes += len(data) + partitionKeyIndexSize
	r.nrecords++

	// Check if we need to flush (size or count limits)
	estimatedSize := r.nbytes + len(magicNumber) + md5.Size
	if estimatedSize >= r.config.MaxSize || r.nrecords >= r.config.MaxCount {
		// Drain and return the flushed record
		return r.drainLocked()
	}

	return nil, nil
}

// ShardID returns the shard ID this reducer is for.
func (r *Reducer) ShardID() string {
	return r.shardID
}

// Size returns the current size in bytes.
func (r *Reducer) Size() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.nbytes
}

// computeExplicitHashKey computes the MD5 hash of the partition key
// and returns it as a decimal string for ExplicitHashKey.
func (r *Reducer) computeExplicitHashKey(partitionKey string) string {
	hash := md5.Sum([]byte(partitionKey))
	hashInt := new(big.Int).SetBytes(hash[:])
	return hashInt.String()
}

// reset clears the reducer state while preserving capacity.
func (r *Reducer) reset() {
	// Stop timer if it exists
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}

	r.buf = r.buf[:0]                // Clear slice while preserving capacity
	r.pkeys = r.pkeys[:0]            // Clear slice while preserving capacity
	r.pkeyMap = make(map[string]int) // Maps must be recreated
	r.nbytes = 0
	r.nrecords = 0
	r.creationTime = time.Time{} // Clear creation time (zero value)
}

// extractAggregatedRecords extracts individual records from an aggregated entry (helper function).
func extractAggregatedRecords(entry *ktypes.PutRecordsRequestEntry) (out []ktypes.PutRecordsRequestEntry) {
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

// Test if a given entry is aggregated record (helper function).
func isAggregatedRecord(entry *ktypes.PutRecordsRequestEntry) bool {
	return bytes.HasPrefix(entry.Data, magicNumber)
}
