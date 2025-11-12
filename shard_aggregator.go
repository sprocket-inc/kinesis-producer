package producer

import (
	"bytes"
	"crypto/md5"
	"math/big"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"google.golang.org/protobuf/proto"
)

// ShardAggregator aggregates records for a single shard.
// All records in this aggregator will be sent to the same shard.
type ShardAggregator struct {
	shardID  string
	buf      []*Record
	pkeys    []string       // partition key index table
	pkeyMap  map[string]int // partition key -> index for deduplication
	nbytes   int            // total bytes including keys
	nrecords int            // number of records
	mutex    sync.Mutex
	config   *AggregatorConfig
}

// AggregatorConfig holds configuration for shard aggregators.
type AggregatorConfig struct {
	MaxSize  int // max bytes per aggregated record (default 50KB)
	MaxCount int // max records per aggregated record (default 1000)
}

// NewShardAggregator creates a new shard-specific aggregator.
func NewShardAggregator(shardID string, config *AggregatorConfig) *ShardAggregator {
	if config == nil {
		config = &AggregatorConfig{
			MaxSize:  defaultAggregationSize,
			MaxCount: maxAggregationCount,
		}
	}
	return &ShardAggregator{
		shardID: shardID,
		pkeyMap: make(map[string]int),
		config:  config,
	}
}

// Count returns the number of records.
func (sa *ShardAggregator) Count() int {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()
	return sa.nrecords
}

// Drain creates an aggregated PutRecordsRequestEntry and resets the aggregator.
// Returns nil if there are no records to drain.
func (sa *ShardAggregator) Drain() (*ktypes.PutRecordsRequestEntry, error) {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()

	if len(sa.buf) == 0 {
		return nil, nil
	}

	// Marshal the aggregated record
	data, err := proto.Marshal(&AggregatedRecord{
		PartitionKeyTable: sa.pkeys,
		Records:           sa.buf,
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
	firstKey := sa.pkeys[0]
	hashKey := sa.computeExplicitHashKey(firstKey)

	// Use a placeholder partition key and explicit hash key for routing
	// The explicit hash key determines the actual shard
	entry := &ktypes.PutRecordsRequestEntry{
		Data:            aggData,
		PartitionKey:    aws.String("a"), // Placeholder like Java KPL
		ExplicitHashKey: aws.String(hashKey),
	}

	// Reset the aggregator
	sa.reset()

	return entry, nil
}

// IsEmpty returns true if the aggregator has no records.
func (sa *ShardAggregator) IsEmpty() bool {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()
	return len(sa.buf) == 0
}

// Put adds a record to the aggregator with partition key deduplication.
// Returns true if the aggregator needs to be flushed after this put.
func (sa *ShardAggregator) Put(data []byte, partitionKey string) bool {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()

	// Check if partition key already exists (deduplication)
	keyIndex, exists := sa.pkeyMap[partitionKey]
	if !exists {
		// New partition key - add to table
		keyIndex = len(sa.pkeys)
		sa.pkeys = append(sa.pkeys, partitionKey)
		sa.pkeyMap[partitionKey] = keyIndex
		sa.nbytes += len([]byte(partitionKey))
	}

	// Add record
	partitionKeyIndex := uint64(keyIndex)
	sa.buf = append(sa.buf, &Record{
		Data:              data,
		PartitionKeyIndex: &partitionKeyIndex,
	})
	sa.nbytes += len(data) + partitionKeyIndexSize
	sa.nrecords++

	// Check if we need to flush
	return sa.shouldFlushLocked()
}

// ShardID returns the shard ID this aggregator is for.
func (sa *ShardAggregator) ShardID() string {
	return sa.shardID
}

// ShouldFlush checks if the aggregator should be flushed based on size or count limits.
func (sa *ShardAggregator) ShouldFlush() bool {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()
	return sa.shouldFlushLocked()
}

// Size returns the current size in bytes.
func (sa *ShardAggregator) Size() int {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()
	return sa.nbytes
}

// computeExplicitHashKey computes the MD5 hash of the partition key
// and returns it as a decimal string for ExplicitHashKey.
func (sa *ShardAggregator) computeExplicitHashKey(partitionKey string) string {
	hash := md5.Sum([]byte(partitionKey))
	hashInt := new(big.Int).SetBytes(hash[:])
	return hashInt.String()
}

// reset clears the aggregator state while preserving capacity.
func (sa *ShardAggregator) reset() {
	sa.buf = sa.buf[:0]               // Clear slice while preserving capacity
	sa.pkeys = sa.pkeys[:0]           // Clear slice while preserving capacity
	sa.pkeyMap = make(map[string]int) // Maps must be recreated
	sa.nbytes = 0
	sa.nrecords = 0
}

// shouldFlushLocked checks flush conditions without acquiring lock (internal use).
func (sa *ShardAggregator) shouldFlushLocked() bool {
	if len(sa.buf) == 0 {
		return false
	}

	// Check size limit
	estimatedSize := sa.nbytes + len(magicNumber) + md5.Size
	if estimatedSize >= sa.config.MaxSize {
		return true
	}

	// Check count limit
	if sa.nrecords >= sa.config.MaxCount {
		return true
	}

	return false
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
