package producer

import (
	"sync"
	"time"
)

const (
	// Kinesis backend limits per shard (from AWS documentation)
	kinesisRecordsPerSecondPerShard = 1000    // 1000 records per second per shard
	kinesisBytesPerSecondPerShard   = 1048576 // 1 MB per second per shard
)

// Limiter implements per-shard rate limiting using token bucket algorithm.
// Each shard has independent rate limits for records per second and bytes per second.
type Limiter struct {
	shardLimiters map[string]*ShardLimiter
	config        *LimiterConfig
	mutex         sync.RWMutex
}

// LimiterConfig holds rate limiting configuration.
type LimiterConfig struct {
	// RateLimit is the maximum allowed put rate as a percentage of backend limits.
	// Backend limits per shard: 1000 records/sec, 1 MB/sec
	// Default: 150 (150% = 1500 records/sec, 1.5 MB/sec)
	RateLimit int
}

// ShardLimiter implements token bucket rate limiting for a single shard.
type ShardLimiter struct {
	recordsTokens   float64
	bytesTokens     float64
	lastRefill      time.Time
	recordsPerSec   float64
	bytesPerSec     float64
	maxRecordTokens float64
	maxByteTokens   float64
	mutex           sync.Mutex
}

// NewLimiter creates a new Limiter with the given configuration.
func NewLimiter(config *LimiterConfig) *Limiter {
	if config == nil {
		config = &LimiterConfig{
			RateLimit: 150, // Default: 150% of backend limits
		}
	}
	if config.RateLimit < 1 {
		config.RateLimit = 150
	}
	return &Limiter{
		shardLimiters: make(map[string]*ShardLimiter),
		config:        config,
	}
}

// Allow checks if the operation is allowed under rate limits without blocking.
// Returns true if allowed, false if rate limited.
func (l *Limiter) Allow(shardID string, records int, bytes int) bool {
	limiter := l.getOrCreateShardLimiter(shardID)
	return limiter.allow(records, bytes)
}

// GetShardCount returns the number of shards being tracked.
func (l *Limiter) GetShardCount() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return len(l.shardLimiters)
}

// Reset clears all shard limiters (useful for testing).
func (l *Limiter) Reset() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.shardLimiters = make(map[string]*ShardLimiter)
}

// Wait blocks until the operation is allowed under rate limits.
// This ensures the operation will proceed without exceeding rate limits.
func (l *Limiter) Wait(shardID string, records int, bytes int) {
	limiter := l.getOrCreateShardLimiter(shardID)
	limiter.wait(records, bytes)
}

// getOrCreateShardLimiter retrieves or creates a shard limiter.
func (l *Limiter) getOrCreateShardLimiter(shardID string) *ShardLimiter {
	l.mutex.RLock()
	limiter, exists := l.shardLimiters[shardID]
	l.mutex.RUnlock()

	if exists {
		return limiter
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Double-check after acquiring write lock
	limiter, exists = l.shardLimiters[shardID]
	if exists {
		return limiter
	}

	// Create new shard limiter
	// Calculate actual rates from RateLimit percentage
	recordsPerSec := float64(kinesisRecordsPerSecondPerShard * l.config.RateLimit / 100)
	bytesPerSec := float64(kinesisBytesPerSecondPerShard * l.config.RateLimit / 100)

	limiter = &ShardLimiter{
		recordsPerSec:   recordsPerSec,
		bytesPerSec:     bytesPerSec,
		recordsTokens:   recordsPerSec,
		bytesTokens:     bytesPerSec,
		maxRecordTokens: recordsPerSec,
		maxByteTokens:   bytesPerSec,
		lastRefill:      time.Now(),
	}
	l.shardLimiters[shardID] = limiter

	return limiter
}

// allow checks if tokens are available without blocking.
func (sl *ShardLimiter) allow(records int, bytes int) bool {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	sl.refillTokens()

	// Check if we have enough tokens
	if sl.recordsTokens >= float64(records) && sl.bytesTokens >= float64(bytes) {
		// Consume tokens
		sl.recordsTokens -= float64(records)
		sl.bytesTokens -= float64(bytes)
		return true
	}

	return false
}

// refillTokens adds tokens based on time elapsed since last refill.
func (sl *ShardLimiter) refillTokens() {
	now := time.Now()
	elapsed := now.Sub(sl.lastRefill).Seconds()

	if elapsed > 0 {
		// Add tokens based on elapsed time
		sl.recordsTokens += elapsed * sl.recordsPerSec
		sl.bytesTokens += elapsed * sl.bytesPerSec

		// Cap at maximum
		if sl.recordsTokens > sl.maxRecordTokens {
			sl.recordsTokens = sl.maxRecordTokens
		}
		if sl.bytesTokens > sl.maxByteTokens {
			sl.bytesTokens = sl.maxByteTokens
		}

		sl.lastRefill = now
	}
}

// wait blocks until tokens are available.
func (sl *ShardLimiter) wait(records int, bytes int) {
	for {
		sl.mutex.Lock()
		sl.refillTokens()

		// Check if we have enough tokens
		if sl.recordsTokens >= float64(records) && sl.bytesTokens >= float64(bytes) {
			// Consume tokens
			sl.recordsTokens -= float64(records)
			sl.bytesTokens -= float64(bytes)
			sl.mutex.Unlock()
			return
		}

		// Calculate how long to wait
		recordsWait := time.Duration(0)
		bytesWait := time.Duration(0)

		if sl.recordsTokens < float64(records) {
			needed := float64(records) - sl.recordsTokens
			recordsWait = time.Duration(needed/sl.recordsPerSec*1000) * time.Millisecond
		}

		if sl.bytesTokens < float64(bytes) {
			needed := float64(bytes) - sl.bytesTokens
			bytesWait = time.Duration(needed/sl.bytesPerSec*1000) * time.Millisecond
		}

		// Wait for the longer of the two
		waitTime := recordsWait
		if bytesWait > waitTime {
			waitTime = bytesWait
		}

		// Add a small buffer to avoid tight loops
		if waitTime < 10*time.Millisecond {
			waitTime = 10 * time.Millisecond
		}

		sl.mutex.Unlock()

		// Sleep and retry
		time.Sleep(waitTime)
	}
}
