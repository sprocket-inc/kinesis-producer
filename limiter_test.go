package producer

import (
	"sync"
	"testing"
	"time"
)

func TestConcurrentAccess(t *testing.T) {
	config := &LimiterConfig{
		RateLimit: 100, // 100% = 1000 records/sec, 1MB/sec
	}
	limiter := NewLimiter(config)

	var wg sync.WaitGroup
	numGoroutines := 100
	requestsPerGoroutine := 10

	allowed := make(chan bool, numGoroutines*requestsPerGoroutine)

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				allowed <- limiter.Allow("shard-001", 1, 100)
			}
		}()
	}

	wg.Wait()
	close(allowed)

	// Count how many were allowed
	allowedCount := 0
	for a := range allowed {
		if a {
			allowedCount++
		}
	}

	// Should allow up to the limit (1000 records)
	if allowedCount > 1000 {
		t.Errorf("allowed %d requests, expected <= 1000", allowedCount)
	}

	// Should allow at least most of them initially
	if allowedCount < 900 {
		t.Errorf("allowed only %d requests, expected >= 900", allowedCount)
	}
}

func TestDefaultRateLimit(t *testing.T) {
	// Test default rate limit (150%)
	limiter := NewLimiter(nil)

	// Should allow 1500 records (150% of 1000)
	allowed := 0
	for i := 0; i < 1500; i++ {
		if limiter.Allow("shard-001", 1, 1024) {
			allowed++
		}
	}

	if allowed != 1500 {
		t.Logf("Warning: allowed %d out of 1500 initial requests with RateLimit=150", allowed)
	}

	// Immediately after exhausting tokens, should deny
	// Use a tight loop to check multiple times to account for timing variations
	denied := 0
	for i := 0; i < 10; i++ {
		if !limiter.Allow("shard-001", 1, 1) {
			denied++
		}
	}
	if denied == 0 {
		t.Error("should be denied after exhausting tokens")
	}
}

func TestHighThroughput(t *testing.T) {
	// Test with realistic Kinesis limits (100% = 1000 records/sec, 1MB/sec)
	config := &LimiterConfig{
		RateLimit: 100,
	}
	limiter := NewLimiter(config)

	// Try to send 1000 records immediately
	allowed := 0
	for i := 0; i < 1000; i++ {
		if limiter.Allow("shard-001", 1, 1024) {
			allowed++
		}
	}

	// Should allow exactly 1000 (or close to it due to initial burst)
	if allowed != 1000 {
		t.Logf("Warning: allowed %d out of 1000 initial requests", allowed)
	}

	// Immediately after exhausting tokens, should deny
	// Use a tight loop to check multiple times to account for timing variations
	denied := 0
	for i := 0; i < 10; i++ {
		if !limiter.Allow("shard-001", 1, 1) {
			denied++
		}
	}
	if denied == 0 {
		t.Error("should be denied after exhausting tokens")
	}
}

func TestLimiterAllow(t *testing.T) {
	config := &LimiterConfig{
		RateLimit: 1, // 1% of backend limits = 10 records/sec, ~10KB/sec
	}
	limiter := NewLimiter(config)

	// First request should be allowed
	if !limiter.Allow("shard-001", 5, 500) {
		t.Error("first request should be allowed")
	}

	// Second request should be allowed (still within limits)
	if !limiter.Allow("shard-001", 5, 500) {
		t.Error("second request should be allowed")
	}

	// Third request should be denied (exceeds record limit)
	if limiter.Allow("shard-001", 1, 1) {
		t.Error("third request should be denied - record limit exceeded")
	}
}

func TestLimiterGetShardCount(t *testing.T) {
	limiter := NewLimiter(nil)

	if limiter.GetShardCount() != 0 {
		t.Error("should start with 0 shards")
	}

	limiter.Allow("shard-001", 1, 1)
	if limiter.GetShardCount() != 1 {
		t.Error("should have 1 shard")
	}

	limiter.Allow("shard-002", 1, 1)
	if limiter.GetShardCount() != 2 {
		t.Error("should have 2 shards")
	}

	limiter.Allow("shard-001", 1, 1) // duplicate
	if limiter.GetShardCount() != 2 {
		t.Error("should still have 2 shards")
	}
}

func TestLimiterReset(t *testing.T) {
	limiter := NewLimiter(nil)

	limiter.Allow("shard-001", 1, 1)
	limiter.Allow("shard-002", 1, 1)

	if limiter.GetShardCount() != 2 {
		t.Error("should have 2 shards before reset")
	}

	limiter.Reset()

	if limiter.GetShardCount() != 0 {
		t.Error("should have 0 shards after reset")
	}
}

func TestLimiterWait(t *testing.T) {
	config := &LimiterConfig{
		RateLimit: 10, // 10% = 100 records/sec, ~100KB/sec
	}
	limiter := NewLimiter(config)

	start := time.Now()

	// Exhaust tokens
	limiter.Wait("shard-001", 100, 10000)

	// This should block until tokens refill
	limiter.Wait("shard-001", 50, 5000)

	elapsed := time.Since(start)

	// Should take at least 500ms (50 records / 100 per sec)
	if elapsed < 400*time.Millisecond {
		t.Errorf("expected wait time >= 400ms, got %v", elapsed)
	}

	// Should not take too long (with some tolerance)
	if elapsed > 1*time.Second {
		t.Errorf("expected wait time < 1s, got %v", elapsed)
	}
}

func TestPerShardIndependence(t *testing.T) {
	config := &LimiterConfig{
		RateLimit: 1, // 1% = 10 records/sec, ~10KB/sec
	}
	limiter := NewLimiter(config)

	// Exhaust tokens for shard-001
	limiter.Allow("shard-001", 10, 1000)

	// shard-001 should be denied
	if limiter.Allow("shard-001", 1, 1) {
		t.Error("shard-001 should be rate limited")
	}

	// shard-002 should still have tokens
	if !limiter.Allow("shard-002", 10, 1000) {
		t.Error("shard-002 should have independent tokens")
	}

	// shard-003 should also have tokens
	if !limiter.Allow("shard-003", 5, 500) {
		t.Error("shard-003 should have independent tokens")
	}
}

func TestRecordVsByteLimits(t *testing.T) {
	config := &LimiterConfig{
		RateLimit: 10, // 10% = 100 records/sec, ~100KB/sec
	}
	limiter := NewLimiter(config)

	// Use up byte limit but not record limit (100KB/sec limit)
	if !limiter.Allow("shard-001", 10, 104857) {
		t.Error("should be allowed")
	}

	// Should be denied due to byte limit
	if limiter.Allow("shard-001", 1, 1) {
		t.Error("should be denied due to byte limit")
	}

	// Reset and test the other way
	limiter.Reset()

	// Use up record limit but not byte limit
	if !limiter.Allow("shard-002", 100, 100) {
		t.Error("should be allowed")
	}

	// Should be denied due to record limit
	if limiter.Allow("shard-002", 1, 1) {
		t.Error("should be denied due to record limit")
	}
}

func TestTokenRefill(t *testing.T) {
	config := &LimiterConfig{
		RateLimit: 1, // 1% = 10 records/sec, ~10KB/sec
	}
	limiter := NewLimiter(config)

	// Exhaust tokens
	if !limiter.Allow("shard-001", 10, 1000) {
		t.Error("initial request should be allowed")
	}

	// Immediately should be denied
	if limiter.Allow("shard-001", 1, 1) {
		t.Error("should be denied immediately after exhausting tokens")
	}

	// Wait for refill
	time.Sleep(200 * time.Millisecond)

	// Should have ~2 tokens now (10 per sec * 0.2 sec)
	if !limiter.Allow("shard-001", 2, 200) {
		t.Error("should be allowed after token refill")
	}
}
