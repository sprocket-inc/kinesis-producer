package producer

import (
	"math/big"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestComputeExplicitHashKey(t *testing.T) {
	r := NewReducer("shard-0001", nil)

	partitionKey := "test-key-123"
	hashKey := r.computeExplicitHashKey(partitionKey)

	// Verify it's a valid decimal string
	hashInt := new(big.Int)
	_, ok := hashInt.SetString(hashKey, 10)
	if !ok {
		t.Errorf("computeExplicitHashKey returned invalid decimal string: %s", hashKey)
	}

	// Verify it's consistent
	hashKey2 := r.computeExplicitHashKey(partitionKey)
	if hashKey != hashKey2 {
		t.Error("computeExplicitHashKey should return consistent results")
	}

	// Verify different keys produce different hashes
	hashKey3 := r.computeExplicitHashKey("different-key")
	if hashKey == hashKey3 {
		t.Error("Different partition keys should produce different hash keys")
	}
}

func TestConcurrentPut(t *testing.T) {
	config := &ReducerConfig{
		MaxSize:  defaultAggregationSize,
		MaxCount: 1000,
	}
	r := NewReducer("shard-001", config)

	var wg sync.WaitGroup
	n := 100
	wg.Add(n)

	// Concurrent puts
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			data := []byte("data-" + strconv.Itoa(idx))
			pkey := "key-" + strconv.Itoa(idx%10)
			_, _ = r.Put(data, pkey)
		}(i)
	}

	wg.Wait()

	// Verify count
	if r.Count() != n {
		t.Errorf("expected count %d, got %d", n, r.Count())
	}

	// Verify partition keys
	if len(r.pkeys) != 10 {
		t.Errorf("expected 10 unique partition keys, got %d", len(r.pkeys))
	}
}

func TestDrain(t *testing.T) {
	config := &ReducerConfig{
		MaxSize:  defaultAggregationSize,
		MaxCount: 100,
	}
	r := NewReducer("shard-001", config)

	// Add records
	n := 50
	for i := 0; i < n; i++ {
		data := []byte("data-" + strconv.Itoa(i))
		pkey := "key-" + strconv.Itoa(i%5) // 5 unique keys
		_, _ = r.Put(data, pkey)
	}

	// Drain
	entry, err := r.Drain()
	if err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	if entry == nil {
		t.Fatal("expected non-nil entry")
	}

	// Verify aggregated record
	if !isAggregatedRecord(entry) {
		t.Error("entry should be aggregated record")
	}

	// Verify partition key
	if entry.PartitionKey == nil {
		t.Error("partition key should not be nil")
	}

	// Extract records
	records := extractAggregatedRecords(entry)
	if len(records) != n {
		t.Errorf("expected %d extracted records, got %d", n, len(records))
	}

	// Verify reducer is reset
	if !r.IsEmpty() {
		t.Error("reducer should be empty after drain")
	}
	if r.Count() != 0 {
		t.Errorf("count should be 0 after drain, got %d", r.Count())
	}
	if r.Size() != 0 {
		t.Errorf("size should be 0 after drain, got %d", r.Size())
	}
}

func TestDrainEmpty(t *testing.T) {
	r := NewReducer("shard-001", nil)

	entry, err := r.Drain()
	if err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	if entry != nil {
		t.Error("expected nil entry from empty reducer")
	}
}

func TestDrainWithExplicitHashKey(t *testing.T) {
	r := NewReducer("shard-0001", nil)

	// Add some records
	_, _ = r.Put([]byte("data1"), "key1")
	_, _ = r.Put([]byte("data2"), "key1") // Same key

	entry, err := r.Drain()
	if err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	if entry == nil {
		t.Fatal("Expected entry, got nil")
	}

	// Check PartitionKey is placeholder
	if entry.PartitionKey == nil || *entry.PartitionKey != "a" {
		t.Errorf("Expected PartitionKey 'a', got %v", entry.PartitionKey)
	}

	// Check ExplicitHashKey is set
	if entry.ExplicitHashKey == nil {
		t.Fatal("Expected ExplicitHashKey to be set")
	}

	// Verify it's a valid decimal string
	hashInt := new(big.Int)
	_, ok := hashInt.SetString(*entry.ExplicitHashKey, 10)
	if !ok {
		t.Errorf("ExplicitHashKey is not a valid decimal string: %s", *entry.ExplicitHashKey)
	}

	t.Logf("ExplicitHashKey: %s", *entry.ExplicitHashKey)
}

func TestFlushOnCount(t *testing.T) {
	config := &ReducerConfig{
		MaxSize:  1 << 20, // 1MB
		MaxCount: 10,
	}
	r := NewReducer("shard-001", config)

	// Add records until count limit
	data := []byte("small")
	flushedCount := 0
	for i := 0; i < 25; i++ {
		flushedRecord, err := r.Put(data, "key")
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if flushedRecord != nil {
			flushedCount++
			// Should flush at exactly multiples of MaxCount (10, 20, etc)
			if (i+1)%10 != 0 {
				t.Errorf("unexpected flush at count %d, expected at multiples of 10", i+1)
			}
		}
	}

	// Should have flushed 2 times (at 10 and 20)
	if flushedCount != 2 {
		t.Errorf("expected 2 flushes, got %d", flushedCount)
	}

	// Should have 5 records remaining
	if r.Count() != 5 {
		t.Errorf("expected 5 records remaining, got %d", r.Count())
	}
}

func TestFlushOnSize(t *testing.T) {
	config := &ReducerConfig{
		MaxSize:  1024, // 1KB
		MaxCount: 10000,
	}
	r := NewReducer("shard-001", config)

	// Add records until flush is needed
	data := make([]byte, 200) // 200 bytes
	flushed := false
	for i := 0; i < 100; i++ {
		flushedRecord, err := r.Put(data, "key-"+strconv.Itoa(i))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if flushedRecord != nil {
			t.Logf("flush triggered after %d records", i+1)
			flushed = true
			break
		}
	}

	if !flushed {
		t.Error("should have flushed after adding many large records")
	}
}

func TestMultipleDrains(t *testing.T) {
	config := &ReducerConfig{
		MaxSize:  1024,
		MaxCount: 100, // Set high enough so Put() doesn't auto-flush
	}
	r := NewReducer("shard-001", config)

	// Add records and drain multiple times
	for round := 0; round < 3; round++ {
		// Add records (less than MaxCount so no auto-flush)
		for i := 0; i < 10; i++ {
			flushedRecord, err := r.Put([]byte("data"), "key")
			if err != nil {
				t.Fatalf("Put failed on round %d: %v", round, err)
			}
			if flushedRecord != nil {
				t.Fatalf("unexpected auto-flush on round %d at record %d", round, i+1)
			}
		}

		// Manually drain
		entry, err := r.Drain()
		if err != nil {
			t.Fatalf("Drain failed on round %d: %v", round, err)
		}

		if entry == nil {
			t.Fatalf("expected non-nil entry on round %d", round)
		}

		// Verify reset
		if !r.IsEmpty() {
			t.Errorf("reducer should be empty after drain on round %d", round)
		}
	}
}

func TestPartitionKeyDeduplication(t *testing.T) {
	config := &ReducerConfig{
		MaxSize:  defaultAggregationSize,
		MaxCount: 100,
	}
	r := NewReducer("shard-001", config)

	// Add multiple records with the same partition key
	pkey := "same-key"
	for i := 0; i < 10; i++ {
		_, _ = r.Put([]byte("data-"+strconv.Itoa(i)), pkey)
	}

	// Check that only one partition key is stored
	if len(r.pkeys) != 1 {
		t.Errorf("expected 1 partition key, got %d", len(r.pkeys))
	}

	if r.pkeys[0] != pkey {
		t.Errorf("expected partition key %s, got %s", pkey, r.pkeys[0])
	}

	// Add records with different partition keys
	_, _ = r.Put([]byte("data"), "key-2")
	_, _ = r.Put([]byte("data"), "key-3")
	_, _ = r.Put([]byte("data"), "key-2") // duplicate

	// Should have 3 unique keys total
	if len(r.pkeys) != 3 {
		t.Errorf("expected 3 unique partition keys, got %d", len(r.pkeys))
	}

	// Verify pkeyMap consistency
	for key, idx := range r.pkeyMap {
		if r.pkeys[idx] != key {
			t.Errorf("pkeyMap inconsistency: key %s -> index %d -> %s", key, idx, r.pkeys[idx])
		}
	}
}

func TestPartitionKeyPreservation(t *testing.T) {
	// Critical test: verify that partition keys are preserved in aggregated records
	config := &ReducerConfig{
		MaxSize:  defaultAggregationSize,
		MaxCount: 100,
	}
	r := NewReducer("shard-001", config)

	// Add records with specific partition keys
	testCases := []struct {
		data string
		pkey string
	}{
		{"data-1", "user-123"},
		{"data-2", "user-456"},
		{"data-3", "user-123"}, // duplicate key
		{"data-4", "user-789"},
		{"data-5", "user-456"}, // duplicate key
	}

	for _, tc := range testCases {
		_, _ = r.Put([]byte(tc.data), tc.pkey)
	}

	// Drain and extract
	entry, err := r.Drain()
	if err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	records := extractAggregatedRecords(entry)
	if len(records) != len(testCases) {
		t.Fatalf("expected %d records, got %d", len(testCases), len(records))
	}

	// Verify each record has correct partition key
	for _, tc := range testCases {
		found := false
		for _, record := range records {
			if string(record.Data) == tc.data {
				if *record.PartitionKey != tc.pkey {
					t.Errorf("record %s has wrong partition key: expected %s, got %s",
						tc.data, tc.pkey, *record.PartitionKey)
				}
				found = true
				break
			}
		}
		if !found {
			t.Errorf("record %s not found in extracted records", tc.data)
		}
	}
}

func TestReducerPut(t *testing.T) {
	config := &ReducerConfig{
		MaxSize:  defaultAggregationSize,
		MaxCount: 100,
	}
	r := NewReducer("shard-001", config)

	data := []byte("hello")
	pkey := "test-key"

	flushedRecord, err := r.Put(data, pkey)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if flushedRecord != nil {
		t.Error("should not flush after single small record")
	}

	if r.Count() != 1 {
		t.Errorf("expected count 1, got %d", r.Count())
	}

	if r.Size() == 0 {
		t.Error("size should be greater than 0")
	}

	if r.ShardID() != "shard-001" {
		t.Errorf("expected shard-001, got %s", r.ShardID())
	}
}

func TestFlushOnFlushInterval(t *testing.T) {
	config := &ReducerConfig{
		MaxSize:       50000,
		MaxCount:      1000,
		FlushInterval: 50 * time.Millisecond, // Short timeout for testing
	}
	r := NewReducer("shard-001", config)

	// Add a single small record
	data := []byte("test data")
	partitionKey := "test-key"

	entry, err := r.Put(data, partitionKey)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Should not flush immediately (size and count limits not reached)
	if entry != nil {
		t.Error("should not flush immediately on first record")
	}

	// Wait for less than FlushInterval
	time.Sleep(25 * time.Millisecond)

	// Should still have records (timer hasn't fired yet)
	if r.IsEmpty() {
		t.Error("should still have records before FlushInterval")
	}

	// Wait until FlushInterval is exceeded
	time.Sleep(30 * time.Millisecond) // Total: 55ms > 50ms

	// Drain should succeed (time limit reached)
	entry, err = r.Drain()
	if err != nil {
		t.Fatalf("Drain failed: %v", err)
	}
	if entry == nil {
		t.Error("Drain should return an entry after FlushInterval")
	}

	// After drain, reducer should be empty
	if !r.IsEmpty() {
		t.Error("reducer should be empty after drain")
	}
}

func TestFlushIntervalDisabled(t *testing.T) {
	// Test with FlushInterval = 0 (disabled)
	config := &ReducerConfig{
		MaxSize:       50000,
		MaxCount:      1000,
		FlushInterval: 0, // Disabled
	}
	r := NewReducer("shard-001", config)

	data := []byte("test data")
	partitionKey := "test-key"

	entry, err := r.Put(data, partitionKey)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if entry != nil {
		t.Error("should not flush immediately")
	}

	// Wait for some time
	time.Sleep(200 * time.Millisecond)

	// Should still have records since FlushInterval timer is disabled
	if r.IsEmpty() {
		t.Error("should still have records when FlushInterval is disabled (no timer)")
	}

	// Manual drain should still work
	entry, err = r.Drain()
	if err != nil {
		t.Fatalf("Drain failed: %v", err)
	}
	if entry == nil {
		t.Error("Drain should return an entry even with FlushInterval disabled")
	}
}

func TestFlushIntervalReset(t *testing.T) {
	config := &ReducerConfig{
		MaxSize:       50000,
		MaxCount:      1000,
		FlushInterval: 50 * time.Millisecond,
	}
	r := NewReducer("shard-001", config)

	// Add and drain
	r.Put([]byte("test1"), "key1")
	time.Sleep(60 * time.Millisecond)
	r.Drain()

	// Add new record - creation time should be reset
	r.Put([]byte("test2"), "key2")

	// Should not be empty immediately after adding new record
	if r.IsEmpty() {
		t.Error("should have record after adding")
	}

	// Wait for less than FlushInterval
	time.Sleep(25 * time.Millisecond)
	if r.IsEmpty() {
		t.Error("should still have record before FlushInterval")
	}

	// Wait for FlushInterval to pass and verify it can be drained
	time.Sleep(30 * time.Millisecond)
	entry, err := r.Drain()
	if err != nil {
		t.Fatalf("Drain failed: %v", err)
	}
	if entry == nil {
		t.Error("should be able to drain after FlushInterval on new records")
	}
}
