package producer

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

func assert(t *testing.T, val bool, msg string) {
	if !val {
		t.Error(msg)
	}
}

func TestSizeAndCount(t *testing.T) {
	shardMap := createMockShardMap()
	a, err := NewAggregator(shardMap, nil)
	assert(t, err == nil, "NewAggregator should not return an error")
	assert(t, a.Size()+a.Count() == 0, "size and count should equal to 0 at the beginning")
	data := []byte("hello")
	pkey := "world"
	n := rand.Intn(100)
	for i := 0; i < n; i++ {
		a.Put(data, pkey)
	}
	assert(t, a.Size() == 5*n+5+8*n, "size should equal to the data and the partition-key")
	assert(t, a.Count() == n, "count should be equal to the number of Put calls")
}

func TestAggregation(t *testing.T) {
	var wg sync.WaitGroup
	shardMap := createMockShardMap()
	a, err := NewAggregator(shardMap, nil)
	assert(t, err == nil, "NewAggregator should not return an error")
	n := 50
	wg.Add(n)
	// Use same partition key so all records go to same shard aggregator
	pkey := "test-key"
	for i := 0; i < n; i++ {
		c := strconv.Itoa(i)
		data := []byte("hello-" + c)
		a.Put(data, pkey)
		wg.Done()
	}
	wg.Wait()
	entries, err := a.Drain()
	if err != nil {
		t.Error(err)
	}
	assert(t, len(entries) > 0, "should return at least one entry")
	// All records should be in the first entry since we used the same partition key
	record := entries[0]
	assert(t, isAggregated(record), "should return an agregated record")
	records := extractRecords(record)
	// Verify all records are present
	assert(t, len(records) == n, "should have all records")
	for i := 0; i < n; i++ {
		c := strconv.Itoa(i)
		found := false
		for _, record := range records {
			if string(record.Data) == "hello-"+c {
				assert(t, string(record.Data) == "hello-"+c, "`Data` field contains invalid value")
				found = true
			}
		}
		assert(t, found, "record not found after extracting: "+c)
	}
}

func TestDrainEmptyAggregator(t *testing.T) {
	shardMap := createMockShardMap()
	a, err := NewAggregator(shardMap, nil)
	assert(t, err == nil, "NewAggregator should not return an error")
	entries, err := a.Drain()
	assert(t, err == nil, "should not return an error")
	assert(t, len(entries) == 0, "should return empty slice")
}
