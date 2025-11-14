package producer

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	k "github.com/aws/aws-sdk-go-v2/service/kinesis"
)

// Constants and default configuration take from:
// github.com/awslabs/amazon-kinesis-producer/.../KinesisProducerConfiguration.java
const (
	maxRecordSize          = 1 << 20 // 1MiB
	maxRequestSize         = 5 << 20 // 5MiB
	maxRecordsPerRequest   = 500
	maxAggregationSize     = 1048576 // 1MiB
	maxAggregationCount    = 4294967295
	defaultAggregationSize = 51200 // 50k
	defaultMaxConnections  = 24
	defaultFlushInterval   = 100 * time.Millisecond // Matches Java KPL's RecordMaxBufferedTime
	partitionKeyIndexSize  = 8
)

// Putter is the interface that wraps the KinesisAPI.PutRecords method.
type Putter interface {
	PutRecords(context.Context, *k.PutRecordsInput, ...func(*k.Options)) (*k.PutRecordsOutput, error)
}

// Config is the Producer configuration.
type Config struct {
	// StreamName is the Kinesis stream.
	StreamName string

	// FlushInterval is the maximum amount of time a record can stay in the aggregation buffer.
	// After this time, records will be flushed regardless of size or count limits.
	// This matches Java KPL's RecordMaxBufferedTime behavior.
	// Default: 100ms (matching Java KPL)
	FlushInterval time.Duration

	// BatchCount determine the maximum number of items to pack in batch.
	// Must not exceed length. Defaults to 500.
	BatchCount int

	// BatchSize determine the maximum number of bytes to send with a PutRecords request.
	// Must not exceed 5MiB; Default to 5MiB.
	BatchSize int

	// AggregateBatchCount determine the maximum number of items to pack into an aggregated record.
	AggregateBatchCount int

	// AggregationBatchSize determine the maximum number of bytes to pack into an aggregated record. User records larger
	// than this will bypass aggregation.
	AggregateBatchSize int

	// BacklogCount determines the channel capacity before Put() will begin blocking. Default to `BatchCount`.
	BacklogCount int

	// Number of requests to sent concurrently. Default to 24.
	MaxConnections int

	// Logger is the logger used. Default to producer.Logger.
	Logger Logger

	// Enabling verbose logging. Default to false.
	Verbose bool

	// Client is the Putter interface implementation.
	Client Putter

	// RateLimit sets the maximum allowed put rate for a shard, as a percentage of backend limits.
	// Like Java KPL, rate limiting is always enabled and cannot be disabled.
	// The rate limit is implemented using a token bucket algorithm per shard.
	//
	// Backend limits per shard:
	//   - 1000 records per second
	//   - 1 MB per second
	//
	// With RateLimit=150 (default):
	//   - 1500 records per second per shard (1000 × 1.5)
	//   - 1.5 MB per second per shard (1 MB × 1.5)
	//
	// The default of 150% allows a single producer to completely saturate a shard.
	// Set to 100 to match exact shard limits, or lower to reduce throttling risk.
	// Minimum value: 1, Maximum value: 9223372036854775807
	//
	// Default: 150 (matches Java KPL)
	// Note: ShardMap and RateLimit are always enabled (matching Java KPL behavior)
	RateLimit int

	// ShardMap is an optional pre-configured ShardMap for testing purposes.
	// If provided, the producer will use this instead of creating a new one.
	// Leave nil for normal operation where ShardMap will be automatically initialized.
	ShardMap *ShardMap

	// ShardMapRefreshTimeout is the maximum time to wait for a ShardMap refresh operation.
	// This timeout is used when calling DescribeStream API.
	// Default: 30 seconds
	ShardMapRefreshTimeout time.Duration

	// BufferFlushTimeout is the maximum time Producer.buf records wait before flush.
	// This matches Java KPL's putrecords_buffer_duration (min(50ms, RecordMaxBufferedTime * 0.2)).
	// Default: 20ms (matching Java KPL's typical value: 100ms * 0.2 = 20ms)
	BufferFlushTimeout time.Duration
}

// defaults for configuration
func (c *Config) defaults() error {
	if c.Logger == nil {
		c.Logger = &StdLogger{log.New(os.Stdout, "", log.LstdFlags)}
	}
	if c.BatchCount == 0 {
		c.BatchCount = maxRecordsPerRequest
	}
	if c.BatchCount > maxRecordsPerRequest {
		return errors.New("kinesis: BatchCount exceeds 500")
	}
	if c.BatchSize == 0 {
		c.BatchSize = maxRequestSize
	}
	if c.BatchSize > maxRequestSize {
		return errors.New("kinesis: BatchSize exceeds 5MiB")
	}
	if c.BacklogCount == 0 {
		c.BacklogCount = maxRecordsPerRequest
	}
	if c.AggregateBatchCount == 0 {
		c.AggregateBatchCount = maxAggregationCount
	}
	if c.AggregateBatchCount > maxAggregationCount {
		return errors.New("kinesis: AggregateBatchCount exceeds 4294967295")
	}
	if c.AggregateBatchSize == 0 {
		c.AggregateBatchSize = defaultAggregationSize
	}
	if c.AggregateBatchSize > maxAggregationSize {
		return errors.New("kinesis: AggregateBatchSize exceeds 1MiB")
	}
	if c.AggregateBatchSize > c.BatchSize {
		return errors.New("kinesis: AggregateBatchSize must not exceed BatchSize")
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = defaultMaxConnections
	}
	if c.MaxConnections < 1 || c.MaxConnections > 256 {
		return errors.New("kinesis: MaxConnections must be between 1 and 256")
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = defaultFlushInterval
	}
	if c.FlushInterval < 1*time.Millisecond {
		return errors.New("kinesis: FlushInterval must be at least 1ms")
	}

	// RateLimit defaults to 150 (150% of shard limits, matching Java KPL)
	// Note: ShardMap and RateLimit are always enabled (matching Java KPL behavior)
	if c.RateLimit == 0 {
		c.RateLimit = 150
	}
	if c.RateLimit < 1 {
		return errors.New("kinesis: RateLimit must be at least 1")
	}
	if len(c.StreamName) == 0 {
		return errors.New("kinesis: StreamName length must be at least 1")
	}

	// ShardMapRefreshTimeout defaults to 30 seconds
	if c.ShardMapRefreshTimeout == 0 {
		c.ShardMapRefreshTimeout = 30 * time.Second
	}

	// BufferFlushTimeout defaults to 20ms (matching Java KPL's putrecords_buffer_duration)
	if c.BufferFlushTimeout == 0 {
		c.BufferFlushTimeout = 20 * time.Millisecond
	}
	return nil
}
