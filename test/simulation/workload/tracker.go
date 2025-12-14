package workload

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/gocql/gocql"
)

// WriteTracker tracks successful writes for verification.
type WriteTracker struct {
	mu     sync.RWMutex
	writes map[gocql.UUID]int64 // key -> timestamp (unix nanos)
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
func (t *WriteTracker) VerifyAndPrune(sessionA, sessionB cql.Session, minAge time.Duration) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now().UnixNano()
	cutoffNano := now - minAge.Nanoseconds()
	pruned := 0
	var missingA, missingB int

	for key, ts := range t.writes {
		if ts < cutoffNano {
			// Verify existence in both clusters
			existsA := rowExists(sessionA, key)
			existsB := rowExists(sessionB, key)

			if !existsA {
				missingA++
			}
			if !existsB {
				missingB++
			}

			// Remove from map to free memory regardless of result
			// (In a real scenario, we might want to keep failed ones for investigation,
			// but for soak test we want to avoid OOM)
			delete(t.writes, key)
			pruned++
		}
	}

	if missingA > 0 || missingB > 0 {
		return pruned, fmt.Errorf("consistency check failed during pruning: missingA=%d, missingB=%d", missingA, missingB)
	}

	return pruned, nil
}

// Count returns the number of currently tracked writes.
func (t *WriteTracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(t.writes)
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
