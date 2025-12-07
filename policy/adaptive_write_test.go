package policy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveDualWrite_Defaults(t *testing.T) {
	a := NewAdaptiveDualWrite()

	assert.Equal(t, 300*time.Millisecond, a.deltaThreshold)
	assert.Equal(t, 2*time.Second, a.absoluteMax)
	assert.Equal(t, 100*time.Millisecond, a.minFloor)
	assert.Equal(t, int32(3), a.strikeThreshold)
	assert.Equal(t, int32(5), a.recoveryThreshold)
	assert.Equal(t, 30*time.Second, a.fireForgetTimeout)
}

func TestAdaptiveDualWrite_CustomOptions(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveDeltaThreshold(200*time.Millisecond),
		WithAdaptiveAbsoluteMax(3*time.Second),
		WithAdaptiveMinFloor(100*time.Millisecond),
		WithAdaptiveStrikeThreshold(5),
		WithAdaptiveRecoveryThreshold(10),
		WithAdaptiveFireForgetTimeout(1*time.Minute),
	)

	assert.Equal(t, 200*time.Millisecond, a.deltaThreshold)
	assert.Equal(t, 3*time.Second, a.absoluteMax)
	assert.Equal(t, 100*time.Millisecond, a.minFloor)
	assert.Equal(t, int32(5), a.strikeThreshold)
	assert.Equal(t, int32(10), a.recoveryThreshold)
	assert.Equal(t, 1*time.Minute, a.fireForgetTimeout)
}

func TestAdaptiveDualWrite_BothHealthy(t *testing.T) {
	a := NewAdaptiveDualWrite()
	ctx := t.Context()

	var callsA, callsB atomic.Int32

	errA, errB := a.Execute(ctx,
		func(ctx context.Context) error {
			callsA.Add(1)
			time.Sleep(10 * time.Millisecond)
			return nil
		},
		func(ctx context.Context) error {
			callsB.Add(1)
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	)

	assert.NoError(t, errA)
	assert.NoError(t, errB)
	assert.Equal(t, int32(1), callsA.Load())
	assert.Equal(t, int32(1), callsB.Load())
	assert.False(t, a.IsDegraded(types.ClusterA))
	assert.False(t, a.IsDegraded(types.ClusterB))
}

func TestAdaptiveDualWrite_AbsoluteCapDegradation(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveAbsoluteMax(50*time.Millisecond),
		WithAdaptiveStrikeThreshold(2),
	)
	ctx := t.Context()

	// Simulate slow cluster A (exceeds absolute max)
	for i := 0; i < 2; i++ {
		_, _ = a.Execute(ctx,
			func(ctx context.Context) error {
				time.Sleep(60 * time.Millisecond) // Exceeds 50ms
				return nil
			},
			func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond) // Fast
				return nil
			},
		)
	}

	assert.True(t, a.IsDegraded(types.ClusterA))
	assert.False(t, a.IsDegraded(types.ClusterB))
}

func TestAdaptiveDualWrite_RelativeDeltaDegradation(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveDeltaThreshold(50*time.Millisecond),
		WithAdaptiveMinFloor(20*time.Millisecond),
		WithAdaptiveAbsoluteMax(1*time.Second), // High so it doesn't trigger
		WithAdaptiveStrikeThreshold(2),
	)
	ctx := t.Context()

	// Simulate cluster B consistently slower by > 50ms
	for i := 0; i < 2; i++ {
		_, _ = a.Execute(ctx,
			func(ctx context.Context) error {
				time.Sleep(30 * time.Millisecond)
				return nil
			},
			func(ctx context.Context) error {
				time.Sleep(100 * time.Millisecond) // 70ms slower
				return nil
			},
		)
	}

	assert.False(t, a.IsDegraded(types.ClusterA))
	assert.True(t, a.IsDegraded(types.ClusterB))
}

func TestAdaptiveDualWrite_MinFloorIgnoresDelta(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveDeltaThreshold(10*time.Millisecond),
		WithAdaptiveMinFloor(50*time.Millisecond),
		WithAdaptiveStrikeThreshold(2),
	)
	ctx := t.Context()

	// Both are fast (< minFloor), even though delta > threshold
	for i := 0; i < 5; i++ {
		_, _ = a.Execute(ctx,
			func(ctx context.Context) error {
				time.Sleep(5 * time.Millisecond)
				return nil
			},
			func(ctx context.Context) error {
				time.Sleep(30 * time.Millisecond) // 25ms difference
				return nil
			},
		)
	}

	// Neither should be degraded because both are under minFloor
	assert.False(t, a.IsDegraded(types.ClusterA))
	assert.False(t, a.IsDegraded(types.ClusterB))
}

func TestAdaptiveDualWrite_FireAndForget(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveFireForgetTimeout(100 * time.Millisecond),
	)
	ctx := t.Context()

	// Force degrade cluster A
	a.ForceDegrade(types.ClusterA)
	require.True(t, a.IsDegraded(types.ClusterA))

	var callsA, callsB atomic.Int32
	var fireForgetCompleted atomic.Bool

	errA, errB := a.Execute(ctx,
		func(ctx context.Context) error {
			callsA.Add(1)
			time.Sleep(10 * time.Millisecond)
			fireForgetCompleted.Store(true)
			return nil
		},
		func(ctx context.Context) error {
			callsB.Add(1)
			return nil
		},
	)

	// A should return ErrWriteAsync immediately
	assert.ErrorIs(t, errA, types.ErrWriteAsync)
	assert.NoError(t, errB)

	// B should have been called synchronously
	assert.Equal(t, int32(1), callsB.Load())

	// Wait for fire-and-forget to complete
	time.Sleep(50 * time.Millisecond)
	assert.True(t, fireForgetCompleted.Load())
	assert.Equal(t, int32(1), callsA.Load())
}

func TestAdaptiveDualWrite_Recovery(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveMinFloor(100*time.Millisecond),
		WithAdaptiveRecoveryThreshold(3),
	)

	// Force degrade cluster A
	a.ForceDegrade(types.ClusterA)
	require.True(t, a.IsDegraded(types.ClusterA))

	// Simulate recovery via external health probe reporting fast writes
	// In production, this would be called by a health check mechanism
	for i := 0; i < 3; i++ {
		a.RecordFastWrite(types.ClusterA)
	}

	// A should have recovered
	assert.False(t, a.IsDegraded(types.ClusterA))
}

func TestAdaptiveDualWrite_RecoveryViaFastWrites(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveMinFloor(20*time.Millisecond),
		WithAdaptiveDeltaThreshold(50*time.Millisecond), // A-B delta must exceed this
		WithAdaptiveRecoveryThreshold(2),
		WithAdaptiveStrikeThreshold(2),
	)
	ctx := t.Context()

	// First degrade cluster A via slow writes (150ms vs 10ms = 140ms delta > 50ms threshold)
	for i := 0; i < 2; i++ {
		_, _ = a.Execute(ctx,
			func(ctx context.Context) error {
				time.Sleep(150 * time.Millisecond) // Slow
				return nil
			},
			func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond) // Fast
				return nil
			},
		)
	}
	require.True(t, a.IsDegraded(types.ClusterA))

	// Force recover for testing the fast write path
	a.ForceRecover(types.ClusterA)
	require.False(t, a.IsDegraded(types.ClusterA))

	// Now execute fast writes - both clusters healthy, both fast
	for i := 0; i < 2; i++ {
		_, _ = a.Execute(ctx,
			func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond) // Fast
				return nil
			},
			func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond) // Fast
				return nil
			},
		)
	}

	// Both should remain healthy
	assert.False(t, a.IsDegraded(types.ClusterA))
	assert.False(t, a.IsDegraded(types.ClusterB))
}

func TestAdaptiveDualWrite_Reset(t *testing.T) {
	a := NewAdaptiveDualWrite()

	// Force degrade both
	a.ForceDegrade(types.ClusterA)
	a.ForceDegrade(types.ClusterB)
	require.True(t, a.IsDegraded(types.ClusterA))
	require.True(t, a.IsDegraded(types.ClusterB))

	// Reset
	a.Reset()

	assert.False(t, a.IsDegraded(types.ClusterA))
	assert.False(t, a.IsDegraded(types.ClusterB))
}

func TestAdaptiveDualWrite_ErrorCountsAsStrike(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveStrikeThreshold(2),
	)
	ctx := t.Context()

	testErr := assert.AnError

	// Simulate cluster A failing
	for i := 0; i < 2; i++ {
		_, _ = a.Execute(ctx,
			func(ctx context.Context) error {
				return testErr
			},
			func(ctx context.Context) error {
				return nil
			},
		)
	}

	assert.True(t, a.IsDegraded(types.ClusterA))
	assert.False(t, a.IsDegraded(types.ClusterB))
}

func TestAdaptiveDualWrite_ImplementsWriteStrategy(t *testing.T) {
	// Verify AdaptiveDualWrite can be used where WriteStrategy is expected
	var _ interface {
		Execute(context.Context, func(context.Context) error, func(context.Context) error) (error, error)
	} = NewAdaptiveDualWrite()
}

// TestAdaptiveDualWrite_AutomaticRecoveryViaDegradedWrites tests that a degraded
// cluster can automatically recover through successful fire-and-forget writes.
// This tests the fix for the recovery limitation where degraded clusters
// previously couldn't recover because fire-and-forget writes didn't track latency.
func TestAdaptiveDualWrite_AutomaticRecoveryViaDegradedWrites(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveAbsoluteMax(100*time.Millisecond),
		WithAdaptiveStrikeThreshold(2),
		WithAdaptiveRecoveryThreshold(3), // Need 3 fast writes to recover
		WithAdaptiveFireForgetTimeout(5*time.Second),
	)
	ctx := t.Context()

	// Step 1: Degrade cluster A via slow writes
	for i := 0; i < 2; i++ {
		_, _ = a.Execute(ctx,
			func(ctx context.Context) error {
				time.Sleep(150 * time.Millisecond) // Exceeds 100ms absoluteMax
				return nil
			},
			func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond) // Fast
				return nil
			},
		)
	}
	require.True(t, a.IsDegraded(types.ClusterA), "Cluster A should be degraded after slow writes")
	require.False(t, a.IsDegraded(types.ClusterB), "Cluster B should remain healthy")

	// Step 2: Now simulate the cluster recovering (fast writes while degraded)
	// These go through fire-and-forget, which now tracks latency for recovery
	asyncCount := 0
	syncCount := 0

	for i := 0; i < 10; i++ { // Execute several writes
		errA, errB := a.Execute(ctx,
			func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond) // Fast - should trigger recovery
				return nil
			},
			func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond)
				return nil
			},
		)

		if errors.Is(errA, types.ErrWriteAsync) {
			asyncCount++
		} else if errA == nil {
			syncCount++
		}

		assert.NoError(t, errB)

		// Give fire-and-forget goroutines time to complete
		time.Sleep(20 * time.Millisecond)
	}

	// Step 3: Verify cluster A has recovered
	assert.False(t, a.IsDegraded(types.ClusterA),
		"Cluster A should have recovered after fast fire-and-forget writes")
	assert.False(t, a.IsDegraded(types.ClusterB))

	// We should have seen some async writes (while degraded) followed by sync writes (after recovery)
	assert.Greater(t, asyncCount, 0, "Should have had some async writes while degraded")
	assert.Greater(t, syncCount, 0, "Should have had some sync writes after recovery")
	t.Logf("Async writes: %d, Sync writes: %d", asyncCount, syncCount)
}

func TestAdaptiveDualWrite_FireForgetLimit(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveFireForgetLimit(2), // Very small limit for testing
		WithAdaptiveFireForgetTimeout(1*time.Second),
	)
	ctx := t.Context()

	// Force degrade cluster A
	a.ForceDegrade(types.ClusterA)
	require.True(t, a.IsDegraded(types.ClusterA))

	var asyncCount, droppedCount atomic.Int32
	var writesStarted sync.WaitGroup
	writesStarted.Add(3) // We'll try 3 concurrent writes

	// Block all writes so they hold the semaphore
	blockCh := make(chan struct{})

	// Execute 3 concurrent writes - only 2 should be accepted (limit=2)
	for range 3 {
		go func() {
			errA, _ := a.Execute(ctx,
				func(ctx context.Context) error {
					writesStarted.Done()
					<-blockCh // Block until released
					return nil
				},
				func(ctx context.Context) error {
					return nil
				},
			)

			if errors.Is(errA, types.ErrWriteAsync) {
				asyncCount.Add(1)
			} else if errors.Is(errA, types.ErrWriteDropped) {
				droppedCount.Add(1)
			}
		}()
	}

	// Wait for all writes to start (or be dropped)
	// Give time for goroutines to attempt semaphore acquisition
	time.Sleep(50 * time.Millisecond)

	// Release blocked writes
	close(blockCh)

	// Wait for completion
	time.Sleep(100 * time.Millisecond)

	// Verify: 2 accepted (async), 1 dropped
	assert.Equal(t, int32(2), asyncCount.Load(), "Should have 2 async writes (semaphore limit)")
	assert.Equal(t, int32(1), droppedCount.Load(), "Should have 1 dropped write (limit exceeded)")
}

func TestAdaptiveDualWrite_RecoveryRequiresDelta(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveAbsoluteMax(2*time.Second),
		WithAdaptiveDeltaThreshold(100*time.Millisecond),
		WithAdaptiveRecoveryThreshold(3),
		WithAdaptiveFireForgetTimeout(5*time.Second),
	)
	ctx := t.Context()

	// Step 1: Establish healthy baseline for cluster B (10ms latency)
	_, errB := a.Execute(ctx,
		func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
		func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	)
	require.NoError(t, errB)

	// Step 2: Force degrade cluster A
	a.ForceDegrade(types.ClusterA)
	require.True(t, a.IsDegraded(types.ClusterA))

	// Step 3: Execute fire-and-forget writes that are fast (< absoluteMax)
	// but significantly slower than cluster B (delta > threshold)
	// These should NOT trigger recovery because 500ms - 10ms = 490ms > 100ms threshold
	for range 5 {
		errA, _ := a.Execute(ctx,
			func(ctx context.Context) error {
				time.Sleep(500 * time.Millisecond) // Fast enough (< 2s) but delta too large
				return nil
			},
			func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond) // Keep cluster B baseline fresh
				return nil
			},
		)
		require.True(t, errors.Is(errA, types.ErrWriteAsync) || errors.Is(errA, types.ErrWriteDropped))

		// Wait for fire-and-forget to complete
		time.Sleep(600 * time.Millisecond)
	}

	// Cluster A should still be degraded because delta is too large
	assert.True(t, a.IsDegraded(types.ClusterA),
		"Cluster A should remain degraded - latency within absoluteMax but delta too large")

	// Step 4: Now execute writes that are both fast AND within delta of sibling
	for range 5 {
		errA, _ := a.Execute(ctx,
			func(ctx context.Context) error {
				time.Sleep(50 * time.Millisecond) // Fast and delta = 40ms < 100ms threshold
				return nil
			},
			func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond)
				return nil
			},
		)
		_ = errA

		// Wait for fire-and-forget to complete
		time.Sleep(100 * time.Millisecond)
	}

	// Cluster A should now have recovered
	assert.False(t, a.IsDegraded(types.ClusterA),
		"Cluster A should have recovered - latency within both absoluteMax and deltaThreshold")
}

func TestAdaptiveDualWrite_FireForgetLimitOption(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveFireForgetLimit(500),
	)

	assert.Equal(t, int32(500), a.fireForgetLimit)
	assert.Equal(t, 500, cap(a.fireForgetSem))
}

func TestAdaptiveDualWrite_RecoveryWhenBothDegraded(t *testing.T) {
	// When both clusters are degraded, recovery should fall back to absoluteMax-only check
	// because there's no healthy sibling baseline to compare against
	a := NewAdaptiveDualWrite(
		WithAdaptiveAbsoluteMax(100*time.Millisecond),
		WithAdaptiveDeltaThreshold(50*time.Millisecond),
		WithAdaptiveRecoveryThreshold(2),
		WithAdaptiveFireForgetTimeout(5*time.Second),
	)
	ctx := t.Context()

	// Force degrade both clusters
	a.ForceDegrade(types.ClusterA)
	a.ForceDegrade(types.ClusterB)
	require.True(t, a.IsDegraded(types.ClusterA))
	require.True(t, a.IsDegraded(types.ClusterB))

	// Execute fast writes from both degraded clusters
	// Since both are degraded, lastLatency will be 0 for both
	// Recovery should use absoluteMax-only check
	for range 4 {
		_, _ = a.Execute(ctx,
			func(ctx context.Context) error {
				time.Sleep(20 * time.Millisecond) // Fast (< 100ms absoluteMax)
				return nil
			},
			func(ctx context.Context) error {
				time.Sleep(20 * time.Millisecond) // Fast (< 100ms absoluteMax)
				return nil
			},
		)

		// Wait for fire-and-forget to complete
		time.Sleep(50 * time.Millisecond)
	}

	// Both clusters should have recovered (using absoluteMax-only check)
	assert.False(t, a.IsDegraded(types.ClusterA),
		"Cluster A should recover when both degraded - using absoluteMax-only check")
	assert.False(t, a.IsDegraded(types.ClusterB),
		"Cluster B should recover when both degraded - using absoluteMax-only check")
}
