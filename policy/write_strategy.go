package policy

import (
	"context"
	"fmt"
	"runtime"
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
		resultA = safeWrite(ctx, writeA, "A")
	})

	wg.Go(func() {
		resultB = safeWrite(ctx, writeB, "B")
	})

	wg.Wait()

	return resultA, resultB
}

// ExecuteStrict performs concurrent writes to both clusters with strict semantics.
//
// For ConcurrentDualWrite, ExecuteStrict is identical to Execute: writes are
// already synchronous and never fire-and-forget. The method exists to satisfy
// the [helix.StrictWriter] interface so strict statements can use this strategy.
func (c *ConcurrentDualWrite) ExecuteStrict(
	ctx context.Context,
	writeA func(context.Context) error,
	writeB func(context.Context) error,
) (errA, errB error) {
	return c.Execute(ctx, writeA, writeB)
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
// Writes to the first cluster, then to the second. If the context is already
// canceled or deadline-exceeded after the first write returns, the second write
// is skipped and ctx.Err() is returned for it — avoiding wasted work and an
// immediate predictable error that the caller would have to handle anyway.
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
		resultA = safeWrite(ctx, writeA, "A")
		if ctx.Err() != nil {
			return resultA, ctx.Err()
		}
		resultB = safeWrite(ctx, writeB, "B")
	} else {
		resultB = safeWrite(ctx, writeB, "B")
		if ctx.Err() != nil {
			return ctx.Err(), resultB
		}
		resultA = safeWrite(ctx, writeA, "A")
	}

	return resultA, resultB
}

// ExecuteStrict performs sequential writes to both clusters with strict semantics.
//
// For SyncDualWrite, ExecuteStrict is identical to Execute: writes are already
// synchronous and never fire-and-forget. The method exists to satisfy the
// [helix.StrictWriter] interface so strict statements can use this strategy.
func (s *SyncDualWrite) ExecuteStrict(
	ctx context.Context,
	writeA func(context.Context) error,
	writeB func(context.Context) error,
) (errA, errB error) {
	return s.Execute(ctx, writeA, writeB)
}

// safeWrite calls write and recovers from panics, converting them to errors.
//
// The captured stack is included in the returned error so post-mortem debugging
// is possible. Without the stack, "panic: ..." with no trace is essentially
// useless when the panic originated several frames deep in caller code.
func safeWrite(ctx context.Context, write func(context.Context) error, cluster string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			err = fmt.Errorf("helix: panic in cluster %s write: %v\n%s", cluster, r, buf[:n])
		}
	}()

	return write(ctx)
}
