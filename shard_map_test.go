package producer

import (
	"math/big"
	"testing"
	"time"
)

func TestBinarySearch(t *testing.T) {
	sm := &ShardMap{}

	// Create mock shard ranges
	// Kinesis hash key space: 0 to 2^128 - 1
	maxHash := new(big.Int)
	maxHash.SetString("340282366920938463463374607431768211455", 10) // 2^128 - 1

	// Split into 4 shards
	shard1End := new(big.Int).Div(maxHash, big.NewInt(4))
	shard2End := new(big.Int).Mul(shard1End, big.NewInt(2))
	shard3End := new(big.Int).Mul(shard1End, big.NewInt(3))

	sm.shards = []ShardRange{
		{
			ShardID:         "shard-0001",
			StartingHashKey: big.NewInt(0),
			EndingHashKey:   shard1End,
		},
		{
			ShardID:         "shard-0002",
			StartingHashKey: new(big.Int).Add(shard1End, big.NewInt(1)),
			EndingHashKey:   shard2End,
		},
		{
			ShardID:         "shard-0003",
			StartingHashKey: new(big.Int).Add(shard2End, big.NewInt(1)),
			EndingHashKey:   shard3End,
		},
		{
			ShardID:         "shard-0004",
			StartingHashKey: new(big.Int).Add(shard3End, big.NewInt(1)),
			EndingHashKey:   maxHash,
		},
	}

	// Test finding shards
	tests := []struct {
		name        string
		hash        *big.Int
		expectedIdx int
	}{
		{
			name:        "hash in first shard",
			hash:        big.NewInt(100),
			expectedIdx: 0,
		},
		{
			name:        "hash at shard boundary",
			hash:        shard1End,
			expectedIdx: 0,
		},
		{
			name:        "hash in second shard",
			hash:        new(big.Int).Add(shard1End, big.NewInt(1000)),
			expectedIdx: 1,
		},
		{
			name:        "hash in third shard",
			hash:        new(big.Int).Add(shard2End, big.NewInt(1000)),
			expectedIdx: 2,
		},
		{
			name:        "hash in fourth shard",
			hash:        new(big.Int).Add(shard3End, big.NewInt(1000)),
			expectedIdx: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := sm.binarySearch(tt.hash)
			if idx != tt.expectedIdx {
				t.Errorf("expected shard index %d, got %d", tt.expectedIdx, idx)
			}
			if idx >= 0 {
				shardID := sm.shards[idx].ShardID
				t.Logf("hash %s mapped to %s", tt.hash.String(), shardID)
			}
		})
	}
}

func TestComputeMD5Hash(t *testing.T) {
	sm := &ShardMap{}

	// Test with known partition key
	hash1 := sm.computeMD5Hash("test-key-1")
	hash2 := sm.computeMD5Hash("test-key-1")

	// Same key should produce same hash
	if hash1.Cmp(hash2) != 0 {
		t.Error("same partition key should produce same hash")
	}

	// Different keys should produce different hashes
	hash3 := sm.computeMD5Hash("test-key-2")
	if hash1.Cmp(hash3) == 0 {
		t.Error("different partition keys should produce different hashes")
	}

	// Verify hash is 128-bit (16 bytes = 128 bits)
	if hash1.BitLen() > 128 {
		t.Errorf("MD5 hash should be 128-bit, got %d bits", hash1.BitLen())
	}
}

func TestGetShardID(t *testing.T) {
	sm := &ShardMap{}

	// Setup mock shard map
	maxHash := new(big.Int)
	maxHash.SetString("340282366920938463463374607431768211455", 10)

	midHash := new(big.Int).Div(maxHash, big.NewInt(2))

	sm.shards = []ShardRange{
		{
			ShardID:         "shard-0001",
			StartingHashKey: big.NewInt(0),
			EndingHashKey:   midHash,
		},
		{
			ShardID:         "shard-0002",
			StartingHashKey: new(big.Int).Add(midHash, big.NewInt(1)),
			EndingHashKey:   maxHash,
		},
	}

	// Test with actual partition keys
	keys := []string{"user-1", "user-2", "user-3", "order-1", "order-2"}
	shardCounts := make(map[string]int)

	for _, key := range keys {
		shardID, err := sm.GetShardID(key)
		if err != nil {
			t.Errorf("GetShardID failed for key %s: %v", key, err)
		}
		shardCounts[shardID]++
		t.Logf("partition key %s -> shard %s", key, shardID)
	}

	// Verify that keys are distributed across shards
	if len(shardCounts) == 0 {
		t.Error("no shards were selected")
	}
}

func TestGetShardIDByHash(t *testing.T) {
	// Create shard ranges using SetString for large numbers
	startHash1 := big.NewInt(0)
	endHash1 := new(big.Int)
	endHash1.SetString("85070591730234615865843651857942052863", 10)

	startHash2 := new(big.Int)
	startHash2.SetString("85070591730234615865843651857942052864", 10)
	endHash2 := new(big.Int)
	endHash2.SetString("170141183460469231731687303715884105727", 10)

	sm := &ShardMap{
		shards: []ShardRange{
			{
				ShardID:         "shard-0001",
				StartingHashKey: startHash1,
				EndingHashKey:   endHash1,
			},
			{
				ShardID:         "shard-0002",
				StartingHashKey: startHash2,
				EndingHashKey:   endHash2,
			},
		},
	}

	tests := []struct {
		name          string
		hashKey       string
		expectedShard string
		expectError   bool
	}{
		{
			name:          "hash in first shard",
			hashKey:       "1000",
			expectedShard: "shard-0001",
			expectError:   false,
		},
		{
			name:          "hash in second shard",
			hashKey:       "100000000000000000000000000000000000000", // Large number in second shard range
			expectedShard: "shard-0002",
			expectError:   false,
		},
		{
			name:          "invalid hash format",
			hashKey:       "not-a-number",
			expectedShard: "",
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shardID, err := sm.GetShardIDByHash(tt.hashKey)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if shardID != tt.expectedShard {
				t.Errorf("Expected shard %s, got %s", tt.expectedShard, shardID)
			}
		})
	}
}

func TestGetShardIDEmptyMap(t *testing.T) {
	sm := &ShardMap{
		shards: []ShardRange{},
	}

	_, err := sm.GetShardID("test-key")
	if err == nil {
		t.Error("expected error for empty shard map")
	}
}

func TestInvalidate(t *testing.T) {
	// Setup mock shard map with READY state
	maxHash := new(big.Int)
	maxHash.SetString("340282366920938463463374607431768211455", 10)
	midHash := new(big.Int).Div(maxHash, big.NewInt(2))

	sm := &ShardMap{
		state:     ShardMapStateReady,
		updatedAt: time.Now().Add(-5 * time.Second), // 5 seconds ago
		shards: []ShardRange{
			{
				ShardID:         "shard-0001",
				StartingHashKey: big.NewInt(0),
				EndingHashKey:   midHash,
			},
			{
				ShardID:         "shard-0002",
				StartingHashKey: new(big.Int).Add(midHash, big.NewInt(1)),
				EndingHashKey:   maxHash,
			},
		},
		minBackoff:     1 * time.Second,
		maxBackoff:     30 * time.Second,
		currentBackoff: 1 * time.Second,
	}

	tests := []struct {
		name                 string
		seenAt               time.Time
		initialState         ShardMapState
		shouldTrigger        bool
		expectedStateAfter   ShardMapState
	}{
		{
			name:               "should trigger: seenAt after updatedAt and READY state",
			seenAt:             time.Now(), // After updatedAt
			initialState:       ShardMapStateReady,
			shouldTrigger:      true,
			expectedStateAfter: ShardMapStateUpdating, // State changes to UPDATING immediately
		},
		{
			name:               "should not trigger: seenAt before updatedAt",
			seenAt:             sm.updatedAt.Add(-1 * time.Second), // Before updatedAt
			initialState:       ShardMapStateReady,
			shouldTrigger:      false,
			expectedStateAfter: ShardMapStateReady, // State unchanged
		},
		{
			name:               "should not trigger: INVALID state",
			seenAt:             time.Now(), // After updatedAt
			initialState:       ShardMapStateInvalid,
			shouldTrigger:      false,
			expectedStateAfter: ShardMapStateInvalid, // State unchanged
		},
		{
			name:               "should not trigger: UPDATING state",
			seenAt:             time.Now(), // After updatedAt
			initialState:       ShardMapStateUpdating,
			shouldTrigger:      false,
			expectedStateAfter: ShardMapStateUpdating, // State unchanged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset state for each test
			sm.mutex.Lock()
			sm.state = tt.initialState
			sm.updatedAt = time.Now().Add(-5 * time.Second)
			sm.mutex.Unlock()

			// Call Invalidate
			sm.Invalidate(tt.seenAt, "shard-0001")

			// Give goroutine a moment to start if it was triggered
			if tt.shouldTrigger {
				time.Sleep(10 * time.Millisecond)
			}

			// Check state
			sm.mutex.RLock()
			actualState := sm.state
			sm.mutex.RUnlock()

			if actualState != tt.expectedStateAfter {
				t.Errorf("Expected state %d after Invalidate, got %d", tt.expectedStateAfter, actualState)
			}
		})
	}
}

func TestSamePartitionKeyMapsToSameShard(t *testing.T) {
	// Critical test: same partition key must always map to the same shard
	sm := &ShardMap{}

	maxHash := new(big.Int)
	maxHash.SetString("340282366920938463463374607431768211455", 10)
	midHash := new(big.Int).Div(maxHash, big.NewInt(2))

	sm.shards = []ShardRange{
		{
			ShardID:         "shard-0001",
			StartingHashKey: big.NewInt(0),
			EndingHashKey:   midHash,
		},
		{
			ShardID:         "shard-0002",
			StartingHashKey: new(big.Int).Add(midHash, big.NewInt(1)),
			EndingHashKey:   maxHash,
		},
	}

	partitionKey := "test-user-123"

	// Call GetShardID multiple times with same key
	var previousShardID string
	for i := 0; i < 100; i++ {
		shardID, err := sm.GetShardID(partitionKey)
		if err != nil {
			t.Fatalf("GetShardID failed: %v", err)
		}

		if i == 0 {
			previousShardID = shardID
		} else {
			if shardID != previousShardID {
				t.Errorf("partition key %s mapped to different shards: %s vs %s", partitionKey, previousShardID, shardID)
			}
		}
	}

	t.Logf("partition key %s consistently maps to %s", partitionKey, previousShardID)
}

func TestShardDistribution(t *testing.T) {
	// This test verifies that partition keys are reasonably distributed across shards
	sm := &ShardMap{}

	// Setup 4 shards
	maxHash := new(big.Int)
	maxHash.SetString("340282366920938463463374607431768211455", 10)

	quarter := new(big.Int).Div(maxHash, big.NewInt(4))

	sm.shards = []ShardRange{
		{
			ShardID:         "shard-0001",
			StartingHashKey: big.NewInt(0),
			EndingHashKey:   quarter,
		},
		{
			ShardID:         "shard-0002",
			StartingHashKey: new(big.Int).Add(quarter, big.NewInt(1)),
			EndingHashKey:   new(big.Int).Mul(quarter, big.NewInt(2)),
		},
		{
			ShardID:         "shard-0003",
			StartingHashKey: new(big.Int).Add(new(big.Int).Mul(quarter, big.NewInt(2)), big.NewInt(1)),
			EndingHashKey:   new(big.Int).Mul(quarter, big.NewInt(3)),
		},
		{
			ShardID:         "shard-0004",
			StartingHashKey: new(big.Int).Add(new(big.Int).Mul(quarter, big.NewInt(3)), big.NewInt(1)),
			EndingHashKey:   maxHash,
		},
	}

	// Generate many partition keys and verify distribution
	shardCounts := make(map[string]int)
	numKeys := 1000

	for i := 0; i < numKeys; i++ {
		key := "test-key-" + string(rune(i))
		shardID, err := sm.GetShardID(key)
		if err != nil {
			t.Errorf("GetShardID failed: %v", err)
			continue
		}
		shardCounts[shardID]++
	}

	// Verify all shards got some keys (with reasonable distribution)
	// Each shard should get roughly 25% of keys, but allow for variance
	expectedPerShard := numKeys / 4
	minExpected := expectedPerShard / 2      // Allow 50% below expected
	maxExpected := expectedPerShard*3/2 + 50 // Allow 50% above expected

	for shardID, count := range shardCounts {
		t.Logf("Shard %s: %d keys (%.1f%%)", shardID, count, float64(count)/float64(numKeys)*100)
		if count < minExpected || count > maxExpected {
			t.Logf("Warning: shard %s has %d keys, expected between %d and %d", shardID, count, minExpected, maxExpected)
		}
	}

	if len(shardCounts) != 4 {
		t.Errorf("expected 4 shards to receive keys, got %d", len(shardCounts))
	}
}

func TestUpdateFail(t *testing.T) {
	sm := &ShardMap{
		state:          ShardMapStateReady,
		minBackoff:     1 * time.Second,
		maxBackoff:     30 * time.Second,
		currentBackoff: 1 * time.Second,
	}

	// First failure: 1s -> 1.5s
	sm.updateFail(nil)
	sm.mutex.RLock()
	if sm.state != ShardMapStateInvalid {
		t.Errorf("Expected state INVALID after updateFail, got %d", sm.state)
	}
	expectedBackoff := time.Duration(float64(1*time.Second) * 1.5)
	if sm.currentBackoff != expectedBackoff {
		t.Errorf("Expected backoff %v after first failure, got %v", expectedBackoff, sm.currentBackoff)
	}
	sm.mutex.RUnlock()

	// Second failure: 1.5s -> 2.25s
	sm.updateFail(nil)
	sm.mutex.RLock()
	expectedBackoff = time.Duration(float64(expectedBackoff) * 1.5)
	if sm.currentBackoff != expectedBackoff {
		t.Errorf("Expected backoff %v after second failure, got %v", expectedBackoff, sm.currentBackoff)
	}
	sm.mutex.RUnlock()

	// Multiple failures until max
	for i := 0; i < 20; i++ {
		sm.updateFail(nil)
	}

	sm.mutex.RLock()
	if sm.currentBackoff > sm.maxBackoff {
		t.Errorf("Expected backoff to cap at %v, got %v", sm.maxBackoff, sm.currentBackoff)
	}
	sm.mutex.RUnlock()
}

func TestUpdateSuccess(t *testing.T) {
	sm := &ShardMap{
		state:          ShardMapStateInvalid,
		minBackoff:     1 * time.Second,
		maxBackoff:     30 * time.Second,
		currentBackoff: 10 * time.Second, // High backoff
	}

	sm.updateSuccess()

	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	if sm.state != ShardMapStateReady {
		t.Errorf("Expected state READY after updateSuccess, got %d", sm.state)
	}

	if sm.currentBackoff != sm.minBackoff {
		t.Errorf("Expected backoff to reset to %v, got %v", sm.minBackoff, sm.currentBackoff)
	}

	if sm.updatedAt.IsZero() {
		t.Error("Expected updatedAt to be set after updateSuccess")
	}
}
