// Amazon kinesis producer
// A KPL-like batch producer for Amazon Kinesis built on top of the official Go AWS SDK
// and using the same aggregation format that KPL use.
//
// Note: this project start as a fork of `tj/go-kinesis`. if you are not intersting in the
// KPL aggregation logic, you probably want to check it out.
package producer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	k "github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/jpillora/backoff"
)

// Errors
var (
	ErrStoppedProducer     = errors.New("Unable to Put record. Producer is already stopped")
	ErrIllegalPartitionKey = errors.New("Invalid partition key. Length must be at least 1 and at most 256")
	ErrRecordSizeExceeded  = errors.New("Data must be less than or equal to 1MB in size")
)

// Producer batches records.
type Producer struct {
	sync.RWMutex
	*Config
	aggregator *Aggregator
	shardMap   *ShardMap
	limiter    *Limiter
	semaphore  semaphore
	records    chan *ktypes.PutRecordsRequestEntry
	failure    chan *FailureRecord
	done       chan struct{}

	// Context for graceful shutdown and cancellation
	ctx        context.Context
	cancelFunc context.CancelFunc

	// Current state of the Producer
	// notify set to true after calling to `NotifyFailures`
	notify bool
	// stopped set to true after `Stop`ing the Producer.
	// This will prevent from user to `Put` any new data.
	stopped bool

	// Event-driven buffer flush (matching Java KPL's putrecords_buffer_duration)
	bufferTimer      *time.Timer   // timer for Producer.buf flush (event-driven)
	bufferFlushReady chan struct{} // channel to notify when buffer timer expires
	bufferFlushMutex sync.Mutex    // mutex for buffer timer operations
}

// New creates new producer with the given config.
// Returns an error if the configuration is invalid.
func New(config *Config) (*Producer, error) {
	if err := config.defaults(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return &Producer{
		Config:           config,
		done:             make(chan struct{}),
		records:          make(chan *ktypes.PutRecordsRequestEntry, config.BacklogCount),
		semaphore:        make(chan struct{}, config.MaxConnections),
		bufferFlushReady: make(chan struct{}, 1),
		shardMap:         config.ShardMap, // Use provided ShardMap if available (for testing)
		// aggregator will be initialized in Start() once ShardMap is available
	}, nil
}

// Put `data` using `partitionKey` asynchronously. This method is thread-safe.
//
// Under the covers, the Producer will automatically re-attempt puts in case of
// transient errors.
// When unrecoverable error has detected(e.g: trying to put to in a stream that
// doesn't exist), the message will returned by the Producer.
// Add a listener with `Producer.NotifyFailures` to handle undeliverable messages.
func (p *Producer) Put(data []byte, partitionKey string) error {
	p.RLock()
	stopped := p.stopped
	p.RUnlock()
	if stopped {
		return ErrStoppedProducer
	}
	if len(data) > maxRecordSize {
		return ErrRecordSizeExceeded
	}
	if l := len(partitionKey); l < 1 || l > 256 {
		return ErrIllegalPartitionKey
	}
	nbytes := len(data) + len([]byte(partitionKey))
	// if the record size is bigger than aggregation size
	// handle it as a simple kinesis record
	if nbytes > p.AggregateBatchSize {
		p.records <- &ktypes.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: &partitionKey,
		}
	} else {
		p.Lock()
		// Check if aggregator is initialized (Start() has been called)
		if p.aggregator == nil {
			p.Unlock()
			return errors.New("producer not started: call Start() before Put()")
		}
		// Put record to aggregator - it will automatically flush and return record if needed
		// This matches Java KPL behavior where Reducer.add() immediately returns flushed records
		flushedRecord, err := p.aggregator.Put(data, partitionKey)
		p.Unlock()

		// If aggregator returned a flushed record, send it immediately
		if err != nil {
			p.Logger.Error("aggregator put", err)
			return err
		}
		if flushedRecord != nil {
			// Send the flushed record to the records channel
			// Release lock before sending to avoid deadlock
			select {
			case p.records <- flushedRecord:
				// Successfully sent the record
			default:
				// Backlog is full, log the error
				p.Logger.Error("backlog full, dropping aggregated record",
					fmt.Errorf("dropped 1 aggregated record"))
			}
		}
	}
	return nil
}

// Failure record type
type FailureRecord struct {
	error
	Data         []byte
	PartitionKey string
}

// NotifyFailures registers and return listener to handle undeliverable messages.
// The incoming struct has a copy of the Data and the PartitionKey along with some
// error information about why the publishing failed.
func (p *Producer) NotifyFailures() <-chan *FailureRecord {
	p.Lock()
	defer p.Unlock()
	if !p.notify {
		p.notify = true
		p.failure = make(chan *FailureRecord, p.BacklogCount)
	}
	return p.failure
}

// Start the producer
func (p *Producer) Start() error {
	p.Logger.Info("starting producer", LogValue{"stream", p.StreamName})

	// Create context for graceful shutdown
	p.ctx, p.cancelFunc = context.WithCancel(context.Background())

	// Initialize ShardMap and RateLimit (always enabled, matching Java KPL)
	// Check if ShardMap was provided in Config (for testing)
	if p.shardMap == nil {
		client, ok := p.Client.(*k.Client)
		if !ok {
			err := errors.New("shard map requires kinesis.Client")
			p.Logger.Error("failed to initialize producer", err)
			return err
		}

		shardMap, err := NewShardMap(p.StreamName, client, p.Logger, p.Verbose, p.ShardMapRefreshTimeout)
		if err != nil {
			p.Logger.Error("failed to initialize shard map", err)
			return fmt.Errorf("shard map initialization failed: %w", err)
		}
		p.shardMap = shardMap
	}

	// Update aggregator with shard map
	aggConfig := &ReducerConfig{
		MaxSize:       p.AggregateBatchSize,
		MaxCount:      p.AggregateBatchCount,
		FlushInterval: p.FlushInterval,
	}
	aggregator, err := NewAggregator(p.shardMap, aggConfig)
	if err != nil {
		return fmt.Errorf("failed to create aggregator: %w", err)
	}
	p.aggregator = aggregator
	p.Logger.Info("shard map initialized", LogValue{"shards", p.shardMap.GetShardCount()})

	// Initialize Rate Limiter (always enabled, matching Java KPL)
	limiterConfig := &LimiterConfig{
		RateLimit: p.RateLimit,
	}
	p.limiter = NewLimiter(limiterConfig)
	p.Logger.Info("rate limiter initialized", LogValue{"rateLimit", p.RateLimit})

	go p.loop()
	return nil
}

// Stop the producer gracefully. Flushes any in-flight data.
func (p *Producer) Stop() {
	p.Lock()
	p.stopped = true
	p.Unlock()

	p.Logger.Info("stopping producer", LogValue{"backlog", len(p.records)})

	// Cancel context to signal shutdown to all operations
	if p.cancelFunc != nil {
		p.cancelFunc()
	}

	// Drain all aggregators on shutdown
	p.Lock()
	records, err := p.aggregator.Drain()
	p.Unlock()
	if err != nil {
		p.Logger.Error("drain all aggregators on stop", err)
	}
	for _, record := range records {
		p.records <- record
	}
	p.done <- struct{}{}
	close(p.records)

	// wait
	<-p.done
	p.semaphore.wait()

	// close the failures channel if we notify
	p.RLock()
	if p.notify {
		close(p.failure)
	}
	p.RUnlock()

	// Stop ShardMap background refresh
	if p.shardMap != nil {
		p.shardMap.Stop()
	}

	p.Logger.Info("stopped producer")
}

// loop processes records and flushes them when buffer limits are reached or timeouts expire.
func (p *Producer) loop() {
	size := 0
	drain := false
	buf := make([]ktypes.PutRecordsRequestEntry, 0, p.BatchCount)

	flush := func(msg string) {
		// Cancel buffer timer when flushing
		p.bufferFlushMutex.Lock()
		if p.bufferTimer != nil {
			p.bufferTimer.Stop()
			p.bufferTimer = nil
		}
		p.bufferFlushMutex.Unlock()

		p.semaphore.acquire()
		go p.flush(buf, msg)
		buf = nil
		size = 0
	}

	bufAppend := func(record *ktypes.PutRecordsRequestEntry) {
		// the record size limit applies to the total size of the
		// partition key and data blob.
		rsize := len(record.Data) + len([]byte(*record.PartitionKey))
		if size+rsize > p.BatchSize {
			flush("batch size")
		}
		size += rsize
		buf = append(buf, *record)
		if len(buf) >= p.BatchCount {
			flush("batch length")
			return
		}

		// Start timer on first record (event-driven, matching Java KPL's putrecords_buffer_duration)
		if len(buf) == 1 && p.BufferFlushTimeout > 0 {
			p.bufferFlushMutex.Lock()
			if p.bufferTimer != nil {
				p.bufferTimer.Stop()
			}
			p.bufferTimer = time.AfterFunc(p.BufferFlushTimeout, func() {
				select {
				case p.bufferFlushReady <- struct{}{}:
				default:
				}
			})
			p.bufferFlushMutex.Unlock()
		}
	}

	defer close(p.done)

	for {
		select {
		case record, ok := <-p.records:
			if drain && !ok {
				if size > 0 {
					flush("drain")
				}
				p.Logger.Info("backlog drained")
				return
			}
			bufAppend(record)
		case shardID := <-p.aggregator.flushNotify:
			// Event-driven flush: FlushInterval expired for this shard
			p.Lock()
			entry, err := p.aggregator.DrainShard(shardID)
			p.Unlock()
			if err != nil {
				p.Logger.Error("drain shard on timer", err)
			} else if entry != nil {
				bufAppend(entry)
			}
		case <-p.bufferFlushReady:
			// Event-driven flush: BufferFlushTimeout expired (matching Java KPL's putrecords_buffer_duration)
			if size > 0 {
				flush("buffer timeout")
			}
		case <-p.done:
			drain = true
		}
	}
}

// flush records and retry failures if necessary.
// for example: when we get "ProvisionedThroughputExceededException"
func (p *Producer) flush(records []ktypes.PutRecordsRequestEntry, reason string) {
	b := &backoff.Backoff{
		Jitter: true,
	}

	defer p.semaphore.release()

	// Apply rate limiting before sending
	if p.limiter != nil && p.shardMap != nil {
		p.applyRateLimiting(records)
	}

	for {
		p.Logger.Info("flushing records", LogValue{"reason", reason}, LogValue{"records", len(records)})
		out, err := p.Client.PutRecords(p.ctx, &k.PutRecordsInput{
			StreamName: aws.String(p.StreamName),
			Records:    records,
		})

		if err != nil {
			p.Logger.Error("flush", err)
			p.RLock()
			notify := p.notify
			p.RUnlock()
			if notify {
				p.dispatchFailures(records, err)
			}
			return
		}

		// Validate shard mapping for successful records (event-driven invalidation)
		if p.shardMap != nil {
			p.validateShardMapping(records, out.Records)
		}

		if p.Verbose {
			for i, r := range out.Records {
				values := make([]LogValue, 2)
				if r.ErrorCode != nil {
					values[0] = LogValue{"ErrorCode", *r.ErrorCode}
					values[1] = LogValue{"ErrorMessage", *r.ErrorMessage}
				} else {
					values[0] = LogValue{"ShardId", *r.ShardId}
					values[1] = LogValue{"SequenceNumber", *r.SequenceNumber}
				}
				p.Logger.Info(fmt.Sprintf("Result[%d]", i), values...)
			}
		}

		failed := *out.FailedRecordCount
		if failed == 0 {
			return
		}

		duration := b.Duration()

		p.Logger.Info(
			"put failures",
			LogValue{"failures", failed},
			LogValue{"backoff", duration.String()},
		)
		time.Sleep(duration)

		// change the logging state for the next itertion
		reason = "retry"
		records = failures(records, out.Records)
	}
}

// dispatchFailures gets batch of records, extract them, and push them
// into the failure channel
func (p *Producer) dispatchFailures(records []ktypes.PutRecordsRequestEntry, err error) {
	for _, r := range records {
		if isAggregated(&r) {
			p.dispatchFailures(extractRecords(&r), err)
		} else {
			p.failure <- &FailureRecord{err, r.Data, *r.PartitionKey}
		}
	}
}

// failures returns the failed records as indicated in the response.
func failures(records []ktypes.PutRecordsRequestEntry,
	response []ktypes.PutRecordsResultEntry) (out []ktypes.PutRecordsRequestEntry) {
	for i, record := range response {
		if record.ErrorCode != nil {
			out = append(out, records[i])
		}
	}
	return
}

// validateShardMapping checks if records were sent to the predicted shard.
// If a mismatch is detected, it invalidates the shard map to trigger a refresh.
// This implements the event-driven shard map update strategy from Java KPL.
func (p *Producer) validateShardMapping(requests []ktypes.PutRecordsRequestEntry, results []ktypes.PutRecordsResultEntry) {
	for i, result := range results {
		// Only check successful records
		if result.ErrorCode != nil || result.ShardId == nil {
			continue
		}

		request := requests[i]
		actualShardID := *result.ShardId

		// Only validate aggregated records with ExplicitHashKey
		if request.ExplicitHashKey == nil {
			continue
		}

		// Compute predicted shard ID from ExplicitHashKey
		predictedShardID, err := p.shardMap.GetShardIDByHash(*request.ExplicitHashKey)
		if err != nil {
			if p.Verbose {
				p.Logger.Error("failed to compute predicted shard ID", err)
			}
			continue
		}

		// Check if actual shard matches predicted shard
		if actualShardID != predictedShardID {
			// Mismatch detected - invalidate shard map
			now := time.Now()
			if p.Verbose {
				p.Logger.Info(fmt.Sprintf("shard mismatch detected: predicted=%s, actual=%s, invalidating shard map",
					predictedShardID, actualShardID))
			}
			p.shardMap.Invalidate(now, predictedShardID)

			// Only invalidate once per batch to avoid excessive API calls
			return
		}
	}
}

// applyRateLimiting applies per-shard rate limiting before sending records.
// Groups records by predicted shard and waits for rate limit tokens for each shard.
func (p *Producer) applyRateLimiting(records []ktypes.PutRecordsRequestEntry) {
	// Group records by shard
	type shardGroup struct {
		records []ktypes.PutRecordsRequestEntry
		bytes   int
	}
	shardGroups := make(map[string]*shardGroup)

	for _, record := range records {
		var shardID string
		var err error

		// Predict shard ID from ExplicitHashKey or PartitionKey
		if record.ExplicitHashKey != nil {
			shardID, err = p.shardMap.GetShardIDByHash(*record.ExplicitHashKey)
		} else if record.PartitionKey != nil {
			shardID, err = p.shardMap.GetShardID(*record.PartitionKey)
		}

		if err != nil {
			if p.Verbose {
				p.Logger.Error("failed to predict shard ID for rate limiting", err)
			}
			// If we can't predict shard, skip rate limiting for this record
			continue
		}

		// Add to shard group
		if shardGroups[shardID] == nil {
			shardGroups[shardID] = &shardGroup{
				records: make([]ktypes.PutRecordsRequestEntry, 0),
				bytes:   0,
			}
		}

		group := shardGroups[shardID]
		group.records = append(group.records, record)
		// Calculate bytes: data + partition key
		group.bytes += len(record.Data) + len([]byte(*record.PartitionKey))
	}

	// Apply rate limiting for each shard
	for shardID, group := range shardGroups {
		recordCount := len(group.records)
		byteCount := group.bytes

		if p.Verbose {
			p.Logger.Info("applying rate limit",
				LogValue{"shard", shardID},
				LogValue{"records", recordCount},
				LogValue{"bytes", byteCount})
		}

		// Wait blocks until rate limit allows this operation
		p.limiter.Wait(shardID, recordCount, byteCount)
	}
}
