package producer

import (
	"context"
	"crypto/md5"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	k "github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// ShardMapState represents the current state of the shard map.
type ShardMapState int

const (
	ShardMapStateReady    ShardMapState = iota // Shard map is ready for use
	ShardMapStateInvalid                       // Shard map is invalid and needs refresh
	ShardMapStateUpdating                      // Shard map is currently being updated
)

// ShardMap maintains a mapping from partition key hashes to shard IDs.
// It uses an event-driven approach to handle shard splits and merges.
//
// Event-Driven Updates (matching Java KPL behavior):
// The shard map is invalidated when a mismatch is detected between the
// predicted shard and the actual shard where a record was sent. This triggers
// an immediate refresh with exponential backoff retry logic (1s -> 30s).
//
// Unlike the periodic refresh approach, this matches Java KPL's reactive
// strategy where updates only occur when needed.
type ShardMap struct {
	streamName string
	shards     []ShardRange
	mutex      sync.RWMutex
	client     *k.Client
	logger     Logger
	verbose    bool

	// Event-driven update state
	state           ShardMapState
	updatedAt       time.Time
	minBackoff      time.Duration
	maxBackoff      time.Duration
	currentBackoff  time.Duration
	scheduledUpdate *time.Timer
	updateMutex     sync.Mutex // Protects update scheduling

	// Configurable timeout for DescribeStream API calls
	refreshTimeout time.Duration
}

// ShardRange represents a shard's hash key range.
type ShardRange struct {
	ShardID         string
	StartingHashKey *big.Int // 128-bit hash key
	EndingHashKey   *big.Int // 128-bit hash key
}

// NewShardMap creates a new ShardMap with event-driven updates.
// The shard map uses an event-driven approach (matching Java KPL) where updates
// only occur when a mismatch is detected.
// refreshTimeout specifies the maximum time to wait for DescribeStream API calls.
func NewShardMap(streamName string, client *k.Client, logger Logger, verbose bool, refreshTimeout time.Duration) (*ShardMap, error) {
	if refreshTimeout == 0 {
		refreshTimeout = 30 * time.Second
	}
	sm := &ShardMap{
		streamName:     streamName,
		client:         client,
		logger:         logger,
		verbose:        verbose,
		state:          ShardMapStateInvalid,
		minBackoff:     1 * time.Second,  // Java KPL: kMinBackoff
		maxBackoff:     30 * time.Second, // Java KPL: kMaxBackoff
		currentBackoff: 1 * time.Second,
		refreshTimeout: refreshTimeout,
	}

	// Initial refresh (required to populate shard information)
	if err := sm.refresh(); err != nil {
		return nil, fmt.Errorf("initial shard map refresh failed: %w", err)
	}

	// Note: Unlike the previous implementation, we do not start a background
	// refresh goroutine. Updates are triggered only when mismatches are detected,
	// matching the Java KPL behavior.

	return sm, nil
}

// GetShardCount returns the current number of open shards.
func (sm *ShardMap) GetShardCount() int {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	return len(sm.shards)
}

// GetShardID returns the shard ID for a given partition key.
// It computes the MD5 hash of the partition key and performs a binary search
// through the sorted shard ranges.
//
// Thread-safety: Uses RLock to allow concurrent reads. The Copy-on-Write pattern
// in refresh() ensures readers never see partial updates and are only briefly
// blocked during the atomic pointer swap.
func (sm *ShardMap) GetShardID(partitionKey string) (string, error) {
	hash := sm.computeMD5Hash(partitionKey)

	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	if len(sm.shards) == 0 {
		return "", fmt.Errorf("shard map is empty")
	}

	// Binary search through sorted shard ranges
	idx := sm.binarySearch(hash)
	if idx < 0 || idx >= len(sm.shards) {
		return "", fmt.Errorf("no shard found for partition key hash")
	}

	return sm.shards[idx].ShardID, nil
}

// GetShardIDByHash returns the shard ID for a given hash key (decimal string).
// This is used to compute the predicted shard ID from ExplicitHashKey.
func (sm *ShardMap) GetShardIDByHash(hashKeyStr string) (string, error) {
	hashKey := new(big.Int)
	if _, ok := hashKey.SetString(hashKeyStr, 10); !ok {
		return "", fmt.Errorf("invalid hash key format: %s", hashKeyStr)
	}

	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	if len(sm.shards) == 0 {
		return "", fmt.Errorf("shard map is empty")
	}

	// Binary search through sorted shard ranges
	idx := sm.binarySearch(hashKey)
	if idx < 0 || idx >= len(sm.shards) {
		return "", fmt.Errorf("no shard found for hash key")
	}

	return sm.shards[idx].ShardID, nil
}

// Invalidate marks the shard map as invalid and triggers an immediate refresh.
// This is called when a mismatch is detected between the predicted shard and
// the actual shard where a record was sent.
//
// seenAt: The time when the mismatch was detected
// predictedShardID: The shard ID that was predicted (optional)
func (sm *ShardMap) Invalidate(seenAt time.Time, predictedShardID string) {
	sm.mutex.Lock()
	currentState := sm.state
	updatedAt := sm.updatedAt

	// Only invalidate if:
	// 1. The mismatch was detected after the last update
	// 2. The shard map is currently in READY state
	if seenAt.After(updatedAt) && currentState == ShardMapStateReady {
		// Atomically transition to UPDATING state to prevent multiple concurrent refreshes
		sm.state = ShardMapStateUpdating
		sm.mutex.Unlock()

		if sm.logger != nil && sm.verbose {
			gap := seenAt.Sub(updatedAt)
			sm.logger.Info(fmt.Sprintf("invalidating shard map: gap=%v, predicted_shard=%s",
				gap, predictedShardID))
		}

		// Trigger immediate update
		go sm.refresh()
	} else {
		sm.mutex.Unlock()
	}
}

// Stop cancels any scheduled updates.
func (sm *ShardMap) Stop() {
	sm.updateMutex.Lock()
	defer sm.updateMutex.Unlock()

	// Cancel any scheduled retry
	if sm.scheduledUpdate != nil {
		sm.scheduledUpdate.Stop()
		sm.scheduledUpdate = nil
	}
}

// binarySearch performs a binary search to find the shard that contains the given hash.
// Returns the index of the shard in the sorted shards slice.
func (sm *ShardMap) binarySearch(hash *big.Int) int {
	// Find the first shard where hash <= EndingHashKey
	idx := sort.Search(len(sm.shards), func(i int) bool {
		return hash.Cmp(sm.shards[i].EndingHashKey) <= 0
	})

	// Verify that hash is also >= StartingHashKey
	if idx < len(sm.shards) {
		if hash.Cmp(sm.shards[idx].StartingHashKey) >= 0 {
			return idx
		}
	}

	return -1
}

// computeMD5Hash computes the MD5 hash of the partition key and converts it
// to a 128-bit integer, matching Kinesis's hash key space.
func (sm *ShardMap) computeMD5Hash(partitionKey string) *big.Int {
	hash := md5.Sum([]byte(partitionKey))
	// Convert 16-byte MD5 hash to big.Int
	hashInt := new(big.Int).SetBytes(hash[:])
	return hashInt
}

// refresh fetches the current shard information from Kinesis and updates the shard map.
func (sm *ShardMap) refresh() error {
	// Skip refresh if client is nil (for testing with mock ShardMap)
	if sm.client == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), sm.refreshTimeout)
	defer cancel()

	var shards []ktypes.Shard
	var exclusiveStartShardID *string

	// Paginate through all shards
	for {
		input := &k.DescribeStreamInput{
			StreamName:            &sm.streamName,
			ExclusiveStartShardId: exclusiveStartShardID,
		}

		output, err := sm.client.DescribeStream(ctx, input)
		if err != nil {
			sm.updateFail(err)
			return fmt.Errorf("failed to describe stream: %w", err)
		}

		shards = append(shards, output.StreamDescription.Shards...)

		if output.StreamDescription.HasMoreShards == nil || !*output.StreamDescription.HasMoreShards {
			break
		}

		// Get the last shard ID for pagination
		if len(output.StreamDescription.Shards) > 0 {
			lastShard := output.StreamDescription.Shards[len(output.StreamDescription.Shards)-1]
			exclusiveStartShardID = lastShard.ShardId
		}
	}

	// Filter only open shards (no EndingSequenceNumber)
	openShards := make([]ktypes.Shard, 0, len(shards))
	for _, shard := range shards {
		if shard.SequenceNumberRange.EndingSequenceNumber == nil {
			openShards = append(openShards, shard)
		}
	}

	if len(openShards) == 0 {
		err := fmt.Errorf("no open shards found in stream")
		sm.updateFail(err)
		return err
	}

	// Copy-on-Write pattern: Build new shard map completely before locking
	// This allows readers (GetShardID) to continue using the old map without blocking
	// during the expensive build and sort operations. Only the final pointer swap requires a lock.
	shardRanges := make([]ShardRange, 0, len(openShards))
	for _, shard := range openShards {
		startHash := new(big.Int)
		endHash := new(big.Int)

		startHash.SetString(*shard.HashKeyRange.StartingHashKey, 10)
		endHash.SetString(*shard.HashKeyRange.EndingHashKey, 10)

		shardRanges = append(shardRanges, ShardRange{
			ShardID:         *shard.ShardId,
			StartingHashKey: startHash,
			EndingHashKey:   endHash,
		})
	}

	// Sort by StartingHashKey for binary search (still unlocked)
	sort.Slice(shardRanges, func(i, j int) bool {
		return shardRanges[i].StartingHashKey.Cmp(shardRanges[j].StartingHashKey) < 0
	})

	// Atomic pointer swap: Only lock for the brief assignment
	// Readers will see either the old or new map, never a partial state
	sm.mutex.Lock()
	sm.shards = shardRanges
	sm.mutex.Unlock()

	// Mark update as successful
	sm.updateSuccess()

	if sm.logger != nil && sm.verbose {
		sm.logger.Info(fmt.Sprintf("shard map refreshed: %d open shards", len(shardRanges)))
	}

	return nil
}

// updateFail is called when a shard map refresh fails.
// It schedules a retry with exponential backoff (1s -> 1.5s -> 2.25s -> ... -> 30s max).
func (sm *ShardMap) updateFail(err error) {
	sm.updateMutex.Lock()
	defer sm.updateMutex.Unlock()

	// Lock order: updateMutex -> mutex (consistent with updateSuccess)
	// Update state and backoff atomically to prevent race conditions
	sm.mutex.Lock()
	sm.state = ShardMapStateInvalid
	backoff := sm.currentBackoff
	// Increase backoff for next failure (multiply by 1.5, max 30s)
	sm.currentBackoff = time.Duration(float64(sm.currentBackoff) * 1.5)
	if sm.currentBackoff > sm.maxBackoff {
		sm.currentBackoff = sm.maxBackoff
	}
	sm.mutex.Unlock()

	if sm.logger != nil {
		sm.logger.Error(fmt.Sprintf("shard map update failed, retrying in %v", backoff), err)
	}

	// Schedule retry if not already scheduled
	if sm.scheduledUpdate == nil {
		sm.scheduledUpdate = time.AfterFunc(backoff, func() {
			sm.updateMutex.Lock()
			sm.scheduledUpdate = nil
			sm.updateMutex.Unlock()

			sm.refresh()
		})
	} else {
		// Stop the existing timer and create a new one
		// Note: We don't use Reset() after Stop() because Reset() should only be used
		// on stopped or expired timers, and we can't be certain of the timer's state
		sm.scheduledUpdate.Stop()
		sm.scheduledUpdate = time.AfterFunc(backoff, func() {
			sm.updateMutex.Lock()
			sm.scheduledUpdate = nil
			sm.updateMutex.Unlock()

			sm.refresh()
		})
	}
}

// updateSuccess is called when a shard map refresh succeeds.
// It resets the backoff timer and marks the map as READY.
func (sm *ShardMap) updateSuccess() {
	sm.updateMutex.Lock()
	defer sm.updateMutex.Unlock()

	// Lock order: updateMutex -> mutex (consistent with updateFail)
	// Update state atomically to prevent race conditions
	sm.mutex.Lock()
	sm.state = ShardMapStateReady
	sm.updatedAt = time.Now()
	sm.currentBackoff = sm.minBackoff // Reset backoff
	sm.mutex.Unlock()

	// Cancel any scheduled retry
	if sm.scheduledUpdate != nil {
		sm.scheduledUpdate.Stop()
		sm.scheduledUpdate = nil
	}
}
