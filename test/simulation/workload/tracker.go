package workload

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/gocql/gocql"
)

// WorkloadStats tracks error classifications from the traffic generator.
type WorkloadStats struct {
	WriteOK        atomic.Int64
	WriteAsync     atomic.Int64 // errors.Is(err, types.ErrWriteAsync)
	WriteDropped   atomic.Int64 // errors.Is(err, types.ErrWriteDropped)
	WriteFailed    atomic.Int64 // all other write errors
	DualClusterErr atomic.Int64 // errors.As(err, *types.DualClusterError)
	ReadOK         atomic.Int64
	ReadFailed     atomic.Int64
}

// NewWorkloadStats creates a new WorkloadStats.
func NewWorkloadStats() *WorkloadStats {
	return &WorkloadStats{}
}

// Reset zeroes all counters.
func (s *WorkloadStats) Reset() {
	s.WriteOK.Store(0)
	s.WriteAsync.Store(0)
	s.WriteDropped.Store(0)
	s.WriteFailed.Store(0)
	s.DualClusterErr.Store(0)
	s.ReadOK.Store(0)
	s.ReadFailed.Store(0)
}

// WriteTracker tracks successful writes for verification.
type WriteTracker struct {
	mu          sync.RWMutex
	writes      map[gocql.UUID]int64 // key -> timestamp (unix nanos)
	totalWrites atomic.Int64         // monotonically increasing write counter
}

// NewWriteTracker creates a new write tracker.
func NewWriteTracker() *WriteTracker {
	return &WriteTracker{
		writes: make(map[gocql.UUID]int64),
	}
}

// TrackWrite records a successful write.
func (t *WriteTracker) TrackWrite(key gocql.UUID, timestampUnixNano int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writes[key] = timestampUnixNano
	t.totalWrites.Add(1)
}

// Verify checks if the tracked writes exist in the database.
// In a real implementation, this would query the database.
// For now, we'll just return the count of tracked writes.
func (t *WriteTracker) Verify() error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.writes) == 0 {
		return errors.New("no writes tracked")
	}

	return nil
}

// VerifyAndPrune verifies writes older than minAge and removes them to save memory.
// This is essential for long-running soak tests.
//
// The lock is held only while collecting and deleting stale keys from the map.
// DB verification queries run after the lock is released so that concurrent
// TrackWrite and Count calls are not blocked for the duration of the queries.
func (t *WriteTracker) VerifyAndPrune(sessionA, sessionB cql.Session, minAge time.Duration) (int, error) {
	// Step 1: collect and delete stale keys under the lock.
	t.mu.Lock()
	now := time.Now().UnixNano()
	cutoffNano := now - minAge.Nanoseconds()
	var toVerify []gocql.UUID
	for key, ts := range t.writes {
		if ts < cutoffNano {
			toVerify = append(toVerify, key)
			delete(t.writes, key)
		}
	}
	t.mu.Unlock()

	if len(toVerify) == 0 {
		return 0, nil
	}

	// Step 2: verify against both clusters without holding the lock.
	var missingA, missingB int
	for _, key := range toVerify {
		if !rowExists(sessionA, key) {
			missingA++
		}
		if !rowExists(sessionB, key) {
			missingB++
		}
	}

	if missingA > 0 || missingB > 0 {
		return len(toVerify), fmt.Errorf("consistency check failed during pruning: missingA=%d, missingB=%d", missingA, missingB)
	}

	return len(toVerify), nil
}

// RandomKey returns a random key from the tracked writes.
// Returns a zero UUID if no writes are tracked.
func (t *WriteTracker) RandomKey() gocql.UUID {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.writes) == 0 {
		return gocql.UUID{}
	}

	// Iterate to a random position — map iteration order is random in Go.
	for k := range t.writes {
		return k
	}

	return gocql.UUID{}
}

// Count returns the number of currently tracked writes.
// This value may decrease when VerifyAndPrune removes old entries.
// For write volume gates that need a monotonically increasing counter,
// use TotalWrites instead.
func (t *WriteTracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(t.writes)
}

// TotalWrites returns the total number of writes tracked since creation.
// Unlike Count, this value never decreases (pruning does not affect it),
// making it safe for write volume gate checks in scenarios.
func (t *WriteTracker) TotalWrites() int64 {
	return t.totalWrites.Load()
}

// VerifyConsistency checks if all tracked writes exist in both clusters.
func (t *WriteTracker) VerifyConsistency(sessionA, sessionB cql.Session) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var missingA, missingB int
	total := len(t.writes)
	checked := 0

	fmt.Printf("Verifying consistency for %d keys...\n", total)

	for key := range t.writes {
		checked++
		if checked%100 == 0 {
			fmt.Printf("Checked %d/%d keys...\r", checked, total)
		}

		// Check Cluster A
		if !rowExists(sessionA, key) {
			missingA++
		}

		// Check Cluster B
		if !rowExists(sessionB, key) {
			missingB++
		}
	}
	fmt.Println() // Newline after progress

	if missingA > 0 || missingB > 0 {
		return fmt.Errorf("consistency check failed: missing in A: %d, missing in B: %d (total: %d)", missingA, missingB, total)
	}

	return nil
}

func rowExists(session cql.Session, key gocql.UUID) bool {
	var id gocql.UUID
	err := session.Query("SELECT id FROM test_data WHERE id = ?", key).Scan(&id)
	// if err != nil {
	// 	fmt.Printf("Check failed for key %s: %v\n", key, err)
	// }

	return err == nil
}
