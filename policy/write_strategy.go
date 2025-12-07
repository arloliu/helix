package policy

import (
	"context"
	"sync"
)

// ConcurrentDualWrite implements a concurrent dual-write strategy.
//
// Writes are executed concurrently on both clusters. Success is defined
// as at least one cluster succeeding. Failed writes are enqueued for replay.
type ConcurrentDualWrite struct {
	timeout *dualWriteTimeout
}

// dualWriteTimeout holds timeout configuration.
type dualWriteTimeout struct {
	// perCluster timeout is handled at the orchestrator level
}

// ConcurrentDualWriteOption configures a ConcurrentDualWrite strategy.
type ConcurrentDualWriteOption func(*ConcurrentDualWrite)

// NewConcurrentDualWrite creates a new ConcurrentDualWrite strategy.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *ConcurrentDualWrite: A new concurrent dual-write strategy
func NewConcurrentDualWrite(opts ...ConcurrentDualWriteOption) *ConcurrentDualWrite {
	c := &ConcurrentDualWrite{
		timeout: &dualWriteTimeout{},
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Execute performs concurrent writes to both clusters.
//
// Spawns two goroutines to write concurrently and waits for both to complete.
// Returns the errors from both clusters (nil if successful).
//
// Parameters:
//   - ctx: Context for the operation
//   - writeA: Function to write to cluster A
//   - writeB: Function to write to cluster B
//
// Returns:
//   - resultA: Error from cluster A (nil if successful)
//   - resultB: Error from cluster B (nil if successful)
func (c *ConcurrentDualWrite) Execute(
	ctx context.Context,
	writeA func(context.Context) error,
	writeB func(context.Context) error,
) (resultA, resultB error) {
	var wg sync.WaitGroup

	wg.Go(func() {
		resultA = writeA(ctx)
	})

	wg.Go(func() {
		resultB = writeB(ctx)
	})

	wg.Wait()

	return resultA, resultB
}

// SyncDualWrite implements a sequential dual-write strategy.
//
// Writes are executed sequentially: first to cluster A, then to cluster B.
// This is useful for debugging or when strict ordering is required.
type SyncDualWrite struct {
	primaryFirst bool
}

// SyncDualWriteOption configures a SyncDualWrite strategy.
type SyncDualWriteOption func(*SyncDualWrite)

// WithPrimaryFirst configures writes to go to cluster A first.
//
// Returns:
//   - SyncDualWriteOption: Configuration option
func WithPrimaryFirst() SyncDualWriteOption {
	return func(s *SyncDualWrite) {
		s.primaryFirst = true
	}
}

// WithSecondaryFirst configures writes to go to cluster B first.
//
// Returns:
//   - SyncDualWriteOption: Configuration option
func WithSecondaryFirst() SyncDualWriteOption {
	return func(s *SyncDualWrite) {
		s.primaryFirst = false
	}
}

// NewSyncDualWrite creates a new SyncDualWrite strategy.
//
// By default, writes go to cluster A first.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *SyncDualWrite: A new sync dual-write strategy
func NewSyncDualWrite(opts ...SyncDualWriteOption) *SyncDualWrite {
	s := &SyncDualWrite{
		primaryFirst: true,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Execute performs sequential writes to both clusters.
//
// Writes to the first cluster, then to the second, regardless of
// whether the first succeeds or fails.
//
// Parameters:
//   - ctx: Context for the operation
//   - writeA: Function to write to cluster A
//   - writeB: Function to write to cluster B
//
// Returns:
//   - resultA: Error from cluster A (nil if successful)
//   - resultB: Error from cluster B (nil if successful)
func (s *SyncDualWrite) Execute(
	ctx context.Context,
	writeA func(context.Context) error,
	writeB func(context.Context) error,
) (resultA, resultB error) {
	if s.primaryFirst {
		resultA = writeA(ctx)
		resultB = writeB(ctx)
	} else {
		resultB = writeB(ctx)
		resultA = writeA(ctx)
	}

	return resultA, resultB
}
