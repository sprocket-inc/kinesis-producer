package producer

import (
	"math/big"
	"strconv"
	"sync"
	"testing"
)

func TestComputeExplicitHashKey(t *testing.T) {
	sa := NewShardAggregator("shard-0001", nil)

	partitionKey := "test-key-123"
	hashKey := sa.computeExplicitHashKey(partitionKey)

	// Verify it's a valid decimal string
	hashInt := new(big.Int)
	_, ok := hashInt.SetString(hashKey, 10)
	if !ok {
		t.Errorf("computeExplicitHashKey returned invalid decimal string: %s", hashKey)
	}

	// Verify it's consistent
	hashKey2 := sa.computeExplicitHashKey(partitionKey)
	if hashKey != hashKey2 {
		t.Error("computeExplicitHashKey should return consistent results")
	}

	// Verify different keys produce different hashes
	hashKey3 := sa.computeExplicitHashKey("different-key")
	if hashKey == hashKey3 {
		t.Error("Different partition keys should produce different hash keys")
	}
}

func TestConcurrentPut(t *testing.T) {
	config := &AggregatorConfig{
		MaxSize:  defaultAggregationSize,
		MaxCount: 1000,
	}
	sa := NewShardAggregator("shard-001", config)

	var wg sync.WaitGroup
	n := 100
	wg.Add(n)

	// Concurrent puts
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			data := []byte("data-" + strconv.Itoa(idx))
			pkey := "key-" + strconv.Itoa(idx%10)
			sa.Put(data, pkey)
		}(i)
	}

	wg.Wait()

	// Verify count
	if sa.Count() != n {
		t.Errorf("expected count %d, got %d", n, sa.Count())
	}

	// Verify partition keys
	if len(sa.pkeys) != 10 {
		t.Errorf("expected 10 unique partition keys, got %d", len(sa.pkeys))
	}
}

func TestDrain(t *testing.T) {
	config := &AggregatorConfig{
		MaxSize:  defaultAggregationSize,
		MaxCount: 100,
	}
	sa := NewShardAggregator("shard-001", config)

	// Add records
	n := 50
	for i := 0; i < n; i++ {
		data := []byte("data-" + strconv.Itoa(i))
		pkey := "key-" + strconv.Itoa(i%5) // 5 unique keys
		sa.Put(data, pkey)
	}

	// Drain
	entry, err := sa.Drain()
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

	// Verify aggregator is reset
	if !sa.IsEmpty() {
		t.Error("aggregator should be empty after drain")
	}
	if sa.Count() != 0 {
		t.Errorf("count should be 0 after drain, got %d", sa.Count())
	}
	if sa.Size() != 0 {
		t.Errorf("size should be 0 after drain, got %d", sa.Size())
	}
}

func TestDrainEmpty(t *testing.T) {
	sa := NewShardAggregator("shard-001", nil)

	entry, err := sa.Drain()
	if err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	if entry != nil {
		t.Error("expected nil entry from empty aggregator")
	}
}

func TestDrainWithExplicitHashKey(t *testing.T) {
	sa := NewShardAggregator("shard-0001", nil)

	// Add some records
	sa.Put([]byte("data1"), "key1")
	sa.Put([]byte("data2"), "key1") // Same key

	entry, err := sa.Drain()
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
	config := &AggregatorConfig{
		MaxSize:  1 << 20, // 1MB
		MaxCount: 10,
	}
	sa := NewShardAggregator("shard-001", config)

	// Add records until count limit
	data := []byte("small")
	for i := 0; i < 15; i++ {
		shouldFlush := sa.Put(data, "key")
		if i < 9 && shouldFlush {
			t.Errorf("should not flush before count limit, at count %d", i+1)
		}
		if i >= 9 && !shouldFlush {
			t.Errorf("should flush at or after count limit, at count %d", i+1)
		}
	}
}

func TestFlushOnSize(t *testing.T) {
	config := &AggregatorConfig{
		MaxSize:  1024, // 1KB
		MaxCount: 10000,
	}
	sa := NewShardAggregator("shard-001", config)

	// Add records until flush is needed
	data := make([]byte, 200) // 200 bytes
	for i := 0; i < 100; i++ {
		shouldFlush := sa.Put(data, "key-"+strconv.Itoa(i))
		if shouldFlush {
			t.Logf("flush triggered after %d records, size: %d bytes", sa.Count(), sa.Size())
			break
		}
	}

	if !sa.ShouldFlush() {
		t.Error("should need flush after adding many large records")
	}
}

func TestMultipleDrains(t *testing.T) {
	config := &AggregatorConfig{
		MaxSize:  1024,
		MaxCount: 10,
	}
	sa := NewShardAggregator("shard-001", config)

	// Add records and drain multiple times
	for round := 0; round < 3; round++ {
		// Add records
		for i := 0; i < 10; i++ {
			sa.Put([]byte("data"), "key")
		}

		// Drain
		entry, err := sa.Drain()
		if err != nil {
			t.Fatalf("Drain failed on round %d: %v", round, err)
		}

		if entry == nil {
			t.Fatalf("expected non-nil entry on round %d", round)
		}

		// Verify reset
		if !sa.IsEmpty() {
			t.Errorf("aggregator should be empty after drain on round %d", round)
		}
	}
}

func TestPartitionKeyDeduplication(t *testing.T) {
	config := &AggregatorConfig{
		MaxSize:  defaultAggregationSize,
		MaxCount: 100,
	}
	sa := NewShardAggregator("shard-001", config)

	// Add multiple records with the same partition key
	pkey := "same-key"
	for i := 0; i < 10; i++ {
		sa.Put([]byte("data-"+strconv.Itoa(i)), pkey)
	}

	// Check that only one partition key is stored
	if len(sa.pkeys) != 1 {
		t.Errorf("expected 1 partition key, got %d", len(sa.pkeys))
	}

	if sa.pkeys[0] != pkey {
		t.Errorf("expected partition key %s, got %s", pkey, sa.pkeys[0])
	}

	// Add records with different partition keys
	sa.Put([]byte("data"), "key-2")
	sa.Put([]byte("data"), "key-3")
	sa.Put([]byte("data"), "key-2") // duplicate

	// Should have 3 unique keys total
	if len(sa.pkeys) != 3 {
		t.Errorf("expected 3 unique partition keys, got %d", len(sa.pkeys))
	}

	// Verify pkeyMap consistency
	for key, idx := range sa.pkeyMap {
		if sa.pkeys[idx] != key {
			t.Errorf("pkeyMap inconsistency: key %s -> index %d -> %s", key, idx, sa.pkeys[idx])
		}
	}
}

func TestPartitionKeyPreservation(t *testing.T) {
	// Critical test: verify that partition keys are preserved in aggregated records
	config := &AggregatorConfig{
		MaxSize:  defaultAggregationSize,
		MaxCount: 100,
	}
	sa := NewShardAggregator("shard-001", config)

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
		sa.Put([]byte(tc.data), tc.pkey)
	}

	// Drain and extract
	entry, err := sa.Drain()
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

func TestShardAggregatorPut(t *testing.T) {
	config := &AggregatorConfig{
		MaxSize:  defaultAggregationSize,
		MaxCount: 100,
	}
	sa := NewShardAggregator("shard-001", config)

	data := []byte("hello")
	pkey := "test-key"

	shouldFlush := sa.Put(data, pkey)
	if shouldFlush {
		t.Error("should not need to flush after single small record")
	}

	if sa.Count() != 1 {
		t.Errorf("expected count 1, got %d", sa.Count())
	}

	if sa.Size() == 0 {
		t.Error("size should be greater than 0")
	}

	if sa.ShardID() != "shard-001" {
		t.Errorf("expected shard-001, got %s", sa.ShardID())
	}
}
