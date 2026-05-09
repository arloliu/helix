package policy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/test/testutil"
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

func TestAdaptiveDualWrite_InvalidOptionsPreserveDefaults(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveDeltaThreshold(0),
		WithAdaptiveAbsoluteMax(-time.Second),
		WithAdaptiveMinFloor(-time.Millisecond),
		WithAdaptiveStrikeThreshold(0),
		WithAdaptiveRecoveryThreshold(-1),
		WithAdaptiveFireForgetTimeout(0),
		WithAdaptiveFireForgetLimit(0),
		WithAdaptiveMetrics(nil),
		WithAdaptiveLogger(nil),
		WithAdaptiveClusterNames(types.ClusterNames{A: "", B: "valid"}),
	)

	assert.Equal(t, 300*time.Millisecond, a.deltaThreshold)
	assert.Equal(t, 2*time.Second, a.absoluteMax)
	assert.Equal(t, 100*time.Millisecond, a.minFloor)
	assert.Equal(t, int32(3), a.strikeThreshold)
	assert.Equal(t, int32(5), a.recoveryThreshold)
	assert.Equal(t, 30*time.Second, a.fireForgetTimeout)
	assert.Equal(t, int32(100), a.fireForgetLimit)
	assert.False(t, a.MetricsConfigured())
	assert.False(t, a.LoggerConfigured())
	assert.Equal(t, "A", a.clusterName(types.ClusterA))
}

func TestAdaptiveDualWriteChecked_InvalidOptionsReturnJoinedErrors(t *testing.T) {
	a, err := NewAdaptiveDualWriteChecked(
		WithAdaptiveDeltaThreshold(0),
		WithAdaptiveAbsoluteMax(0),
		WithAdaptiveMinFloor(-time.Millisecond),
		WithAdaptiveStrikeThreshold(maxInt32+1),
		WithAdaptiveRecoveryThreshold(0),
		WithAdaptiveFireForgetTimeout(0),
		WithAdaptiveFireForgetLimit(0),
		WithAdaptiveClusterNames(types.ClusterNames{A: "", B: "valid"}),
	)

	require.Nil(t, a)
	require.Error(t, err)
	assert.True(t, types.IsOptionError(err))

	var optionErr *types.OptionError
	require.True(t, errors.As(err, &optionErr))
	assert.Equal(t, adaptiveDualWriteComponent, optionErr.Component)

	assert.Contains(t, err.Error(), "WithAdaptiveDeltaThreshold")
	assert.Contains(t, err.Error(), "WithAdaptiveAbsoluteMax")
	assert.Contains(t, err.Error(), "WithAdaptiveMinFloor")
	assert.Contains(t, err.Error(), "WithAdaptiveStrikeThreshold")
	assert.Contains(t, err.Error(), "WithAdaptiveRecoveryThreshold")
	assert.Contains(t, err.Error(), "WithAdaptiveFireForgetTimeout")
	assert.Contains(t, err.Error(), "WithAdaptiveFireForgetLimit")
	assert.Contains(t, err.Error(), "WithAdaptiveClusterNames")
}

func TestAdaptiveDualWriteChecked_ValidOptions(t *testing.T) {
	a, err := NewAdaptiveDualWriteChecked(
		WithAdaptiveDeltaThreshold(200*time.Millisecond),
		WithAdaptiveAbsoluteMax(3*time.Second),
		WithAdaptiveMinFloor(0),
		WithAdaptiveStrikeThreshold(4),
		WithAdaptiveRecoveryThreshold(6),
		WithAdaptiveFireForgetTimeout(45*time.Second),
		WithAdaptiveFireForgetLimit(50),
		WithAdaptiveClusterNames(types.ClusterNames{A: "primary", B: "secondary"}),
	)

	require.NoError(t, err)
	require.NotNil(t, a)
	assert.Equal(t, 200*time.Millisecond, a.deltaThreshold)
	assert.Equal(t, 3*time.Second, a.absoluteMax)
	assert.Equal(t, int32(4), a.strikeThreshold)
	assert.Equal(t, int32(6), a.recoveryThreshold)
	assert.Equal(t, 45*time.Second, a.fireForgetTimeout)
	assert.Equal(t, int32(50), a.fireForgetLimit)
	assert.Equal(t, "primary", a.clusterName(types.ClusterA))
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

// TestAdaptiveDualWrite_FireAndForgetSurfacesError verifies that a real
// error against a degraded cluster's background write is observable: the
// metrics collector sees IncWriteError for that cluster. Without this the
// foreground caller only saw ErrWriteAsync — the background error was
// silently swallowed and operators couldn't tell a degraded cluster had
// progressed to permanently broken.
func TestAdaptiveDualWrite_FireAndForgetSurfacesError(t *testing.T) {
	m := testutil.NewTestMetricsCollector()
	a := NewAdaptiveDualWrite(
		WithAdaptiveFireForgetTimeout(100*time.Millisecond),
		WithAdaptiveMetrics(m),
	)
	ctx := t.Context()

	a.ForceDegrade(types.ClusterA)
	require.True(t, a.IsDegraded(types.ClusterA))

	bgErr := errors.New("background failed")

	errA, errB := a.Execute(ctx,
		func(_ context.Context) error { return bgErr },
		func(_ context.Context) error { return nil },
	)
	require.ErrorIs(t, errA, types.ErrWriteAsync,
		"foreground sees ErrWriteAsync regardless of background outcome")
	require.NoError(t, errB)

	// Background goroutine has its own context — give it time to finish.
	require.Eventually(t, func() bool {
		return m.GetWriteErrors(types.ClusterA) == 1
	}, time.Second, 10*time.Millisecond,
		"IncWriteError must be emitted for the degraded cluster's background failure")

	assert.Equal(t, int64(0), m.GetWriteErrors(types.ClusterB),
		"healthy cluster must not see IncWriteError")
}

// TestAdaptiveDualWrite_ExplicitLoggerWinsOverInjection verifies that
// WithAdaptiveLogger marks the strategy's logger as explicitly set, so a
// subsequent SetLogger from the helix client's auto-injection path does
// NOT overwrite it. Without this guard, callers who deliberately route
// background-write logs to a separate sink would silently lose that
// configuration once a client logger is wired up.
func TestAdaptiveDualWrite_ExplicitLoggerWinsOverInjection(t *testing.T) {
	explicit := &recordingLogger{}
	clientLogger := &recordingLogger{}

	a := NewAdaptiveDualWrite(WithAdaptiveLogger(explicit))
	require.True(t, a.LoggerConfigured(), "WithAdaptiveLogger must mark configured")

	// Simulate the helix client auto-injection path.
	a.SetLogger(clientLogger)

	// Force degrade and run a background failure to provoke a Warn log.
	bgErr := errors.New("background failure")
	a.ForceDegrade(types.ClusterA)

	_, _ = a.Execute(t.Context(),
		func(_ context.Context) error { return bgErr },
		func(_ context.Context) error { return nil },
	)

	// Wait for the background goroutine to log.
	require.Eventually(t, func() bool {
		return explicit.warnCount() >= 1
	}, time.Second, 10*time.Millisecond,
		"explicit logger must receive the background-write Warn")

	assert.Zero(t, clientLogger.warnCount(),
		"client-injected logger must not receive logs once an explicit logger is configured")
}

// TestAdaptiveDualWrite_SetLoggerInjectsWhenUnconfigured verifies the
// other side of the contract: when no WithAdaptiveLogger was provided,
// SetLogger does install the client logger.
func TestAdaptiveDualWrite_SetLoggerInjectsWhenUnconfigured(t *testing.T) {
	clientLogger := &recordingLogger{}

	a := NewAdaptiveDualWrite() // no explicit logger
	require.False(t, a.LoggerConfigured())

	a.SetLogger(clientLogger)
	require.True(t, a.LoggerConfigured(),
		"SetLogger must mark configured to prevent further auto-injection")

	bgErr := errors.New("background failure")
	a.ForceDegrade(types.ClusterA)

	_, _ = a.Execute(t.Context(),
		func(_ context.Context) error { return bgErr },
		func(_ context.Context) error { return nil },
	)

	require.Eventually(t, func() bool {
		return clientLogger.warnCount() >= 1
	}, time.Second, 10*time.Millisecond,
		"client-injected logger must receive logs when no explicit logger was set")
}

// recordingLogger is a minimal types.Logger implementation that counts
// Warn calls. Sufficient for verifying which logger received what.
type recordingLogger struct {
	mu    sync.Mutex
	warns int
}

func (l *recordingLogger) Debug(_ string, _ ...any) {}
func (l *recordingLogger) Info(_ string, _ ...any)  {}
func (l *recordingLogger) Warn(_ string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns++
}
func (l *recordingLogger) Error(_ string, _ ...any) {}
func (l *recordingLogger) Fatal(_ string, _ ...any) {}
func (l *recordingLogger) With(_ ...any) types.Logger {
	return l
}

func (l *recordingLogger) warnCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.warns
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

// TestAdaptiveDualWrite_ForceDegrade_ResetsFastStrikes verifies that ForceDegrade
// resets fastStrikes atomically, preventing a cluster with accumulated fast-strike
// credit from recovering immediately after a forced degradation.
func TestAdaptiveDualWrite_ForceDegrade_ResetsFastStrikes(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveStrikeThreshold(3),
		WithAdaptiveRecoveryThreshold(5),
	)

	// Degrade cluster A.
	a.ForceDegrade(types.ClusterA)
	require.True(t, a.IsDegraded(types.ClusterA))

	// Accumulate 4 of 5 fast strikes needed for recovery.
	for range 4 {
		a.recordFast(&a.stateA)
	}
	require.True(t, a.IsDegraded(types.ClusterA), "still degraded after 4/5 fast strikes")
	require.Equal(t, int32(4), a.stateA.fastStrikes)

	// ForceDegrade again — must reset fastStrikes to 0.
	a.ForceDegrade(types.ClusterA)
	require.True(t, a.IsDegraded(types.ClusterA))
	require.Equal(t, int32(0), a.stateA.fastStrikes, "ForceDegrade must zero fastStrikes")

	// 4 more fast strikes must not recover the cluster (needs full 5).
	for range 4 {
		a.recordFast(&a.stateA)
	}
	assert.True(t, a.IsDegraded(types.ClusterA),
		"cluster must stay degraded when fastStrikes < recoveryThreshold after ForceDegrade")

	// 5th fast strike completes the threshold.
	a.recordFast(&a.stateA)
	assert.False(t, a.IsDegraded(types.ClusterA),
		"cluster must recover after a full recoveryThreshold=5 run from zero")
}

func TestAdaptiveDualWrite_Reset(t *testing.T) {
	a := NewAdaptiveDualWrite()

	// Force degrade both
	a.ForceDegrade(types.ClusterA)
	a.ForceDegrade(types.ClusterB)
	a.stateA.lastLatency.Store((10 * time.Millisecond).Nanoseconds())
	a.stateB.lastLatency.Store((20 * time.Millisecond).Nanoseconds())
	require.True(t, a.IsDegraded(types.ClusterA))
	require.True(t, a.IsDegraded(types.ClusterB))

	// Reset
	a.Reset()

	assert.False(t, a.IsDegraded(types.ClusterA))
	assert.False(t, a.IsDegraded(types.ClusterB))
	assert.Equal(t, int64(0), a.stateA.lastLatency.Load())
	assert.Equal(t, int64(0), a.stateB.lastLatency.Load())
}

func TestAdaptiveDualWrite_InvalidClusterNoop(t *testing.T) {
	a := NewAdaptiveDualWrite(WithAdaptiveStrikeThreshold(1))
	invalid := types.ClusterID("C")

	a.ForceDegrade(invalid)
	assert.False(t, a.IsDegraded(types.ClusterA))
	assert.False(t, a.IsDegraded(types.ClusterB))
	assert.False(t, a.IsDegraded(invalid))

	a.ForceDegrade(types.ClusterA)
	a.ForceRecover(invalid)
	assert.True(t, a.IsDegraded(types.ClusterA))

	a.RecordFastWrite(invalid)
	assert.True(t, a.IsDegraded(types.ClusterA))
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

// TestAdaptiveDualWrite_NoDoubleStrike verifies that a cluster which violates BOTH
// the absolute cap AND the delta threshold in a single execution receives exactly
// one strike — not two. This ensures strikeThreshold behaves as documented.
func TestAdaptiveDualWrite_NoDoubleStrike(t *testing.T) {
	// Configure so that strikeThreshold=2 is the only way to degrade.
	// Without the fix, a single execution with cap+delta violation gives 2 strikes,
	// degrading the cluster after just 1 round (2 >= 2).
	// With the fix, it takes 2 rounds (1 strike each).
	a := NewAdaptiveDualWrite(
		WithAdaptiveAbsoluteMax(50*time.Millisecond),    // cap threshold
		WithAdaptiveDeltaThreshold(30*time.Millisecond), // delta threshold
		WithAdaptiveMinFloor(10*time.Millisecond),
		WithAdaptiveStrikeThreshold(2),
	)
	ctx := t.Context()

	// Single execution: A takes 80ms (>50ms cap), B takes 10ms.
	// Delta = 70ms > 30ms threshold → both conditions fire for A.
	// With the fix: A gets exactly 1 strike, NOT 2.
	_, _ = a.Execute(ctx,
		func(ctx context.Context) error {
			time.Sleep(80 * time.Millisecond) // exceeds absoluteMax AND deltaThreshold
			return nil
		},
		func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond) // fast
			return nil
		},
	)

	// After 1 execution, A must NOT be degraded yet (strikeThreshold=2).
	assert.False(t, a.IsDegraded(types.ClusterA),
		"cluster A must not degrade after 1 execution when strikeThreshold=2 (no double-strike)")
	assert.False(t, a.IsDegraded(types.ClusterB))

	// Second execution with same pattern → 2nd strike → now degraded.
	_, _ = a.Execute(ctx,
		func(ctx context.Context) error {
			time.Sleep(80 * time.Millisecond)
			return nil
		},
		func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	)

	assert.True(t, a.IsDegraded(types.ClusterA),
		"cluster A must be degraded after 2 slow executions with strikeThreshold=2")
}

// TestAdaptiveDualWrite_HandleErrors_ExcludesDropped verifies that ErrWriteDropped
// does NOT record a strike. Only genuine write errors should move the strike counter.
func TestAdaptiveDualWrite_HandleErrors_ExcludesDropped(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveStrikeThreshold(2),
	)

	// Force-degrade A so fireAndForget returns ErrWriteDropped when semaphore is full.
	a.ForceDegrade(types.ClusterA)

	// Directly invoke handleErrors with ErrWriteDropped for A.
	// If it were recorded as a strike, the slow counter would advance.
	// We verify this does not happen by checking that B (which gets a nil)
	// also stays unaffected and that A remains at 0 slow strikes.
	a.handleErrors(types.ErrWriteDropped, nil)
	a.handleErrors(types.ErrWriteDropped, nil)

	// A was already degraded; verify it did not accumulate slow strikes from dropped errors.
	// We use ForceRecover to reset degraded status and then check that strikes
	// don't immediately re-degrade it (which would happen if slowStrikes were > 0).
	a.ForceRecover(types.ClusterA)
	require.False(t, a.IsDegraded(types.ClusterA), "after ForceRecover, cluster A must be healthy")

	// One more dropped call — must not degrade with strikeThreshold=2.
	a.handleErrors(types.ErrWriteDropped, nil)
	assert.False(t, a.IsDegraded(types.ClusterA), "ErrWriteDropped must not count as a slow strike")
}

// TestAdaptiveDualWrite_RecordFast_ClearsSlowStrikesOnHealthyCluster is a regression
// test for the bug where recordFast returned early on a healthy cluster without clearing
// slowStrikes. A slow→fast→slow sequence would accumulate stale strikes across the fast
// gap and degrade prematurely — the slow writes were not truly consecutive.
func TestAdaptiveDualWrite_RecordFast_ClearsSlowStrikesOnHealthyCluster(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveStrikeThreshold(3),
		WithAdaptiveRecoveryThreshold(5),
	)

	// Accumulate 2 slow strikes on A — one below the degrade threshold of 3.
	a.recordStrike(&a.stateA)
	a.recordStrike(&a.stateA)
	require.False(t, a.IsDegraded(types.ClusterA), "precondition: 2 strikes should not degrade at threshold=3")

	// A fast write should clear the accumulated slow strikes.
	a.recordFast(&a.stateA)
	require.False(t, a.IsDegraded(types.ClusterA), "still healthy after fast write")

	// Without the fix: the 2 stale strikes survive recordFast, so this 3rd slow
	// strike (the first after a fast gap) would tip the total to 3 and degrade A.
	// With the fix: slowStrikes was reset to 0 by recordFast, so this is strike 1.
	a.recordStrike(&a.stateA)
	assert.False(t, a.IsDegraded(types.ClusterA),
		"first slow strike after a fast gap must not degrade the cluster (stale strikes must have been cleared)")
}

// TestAdaptiveDualWrite_RecordFast_RaceWithForceRecover verifies that a concurrent
// ForceRecover cannot cause a stale fire-and-forget completion to apply recovery
// credit after the cluster has been intentionally reset. The isDegraded check inside
// the lock prevents the TOCTOU window that existed when the check was outside the lock.
func TestAdaptiveDualWrite_RecordFast_RaceWithForceRecover(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveStrikeThreshold(3),
		WithAdaptiveRecoveryThreshold(2),
	)

	// Degrade A.
	a.ForceDegrade(types.ClusterA)
	require.True(t, a.IsDegraded(types.ClusterA))

	// ForceRecover runs first, resetting the cluster to healthy with fastStrikes=0.
	a.ForceRecover(types.ClusterA)
	require.False(t, a.IsDegraded(types.ClusterA))

	// Now a stale fire-and-forget goroutine calls recordFast.
	// Before the fix: isDegraded check was outside the lock; by the time the lock
	// was acquired, isDegraded was already false. The goroutine then incremented
	// fastStrikes (from 0 to 1) on a now-healthy cluster — harmless on its own,
	// but the counter was left dirty, meaning the next genuine recovery would
	// need only 1 more fast write instead of recoveryThreshold.
	// After the fix: isDegraded is checked inside the lock, so the goroutine sees
	// false and exits without touching fastStrikes.
	a.recordFast(&a.stateA)

	// fastStrikes must still be 0 — the stale goroutine must not have incremented it.
	// Re-degrade A and verify it still requires a full recoveryThreshold=2 fast writes.
	a.ForceDegrade(types.ClusterA)
	a.recordFast(&a.stateA) // fast write 1 of 2
	assert.True(t, a.IsDegraded(types.ClusterA),
		"cluster must still need recoveryThreshold=2 fast writes to recover; stale credit must not have been applied")
	a.recordFast(&a.stateA) // fast write 2 of 2
	assert.False(t, a.IsDegraded(types.ClusterA), "cluster must recover after full recoveryThreshold")
}

// TestAdaptiveDualWrite_Concurrent_RecoveryThreshold stress-tests the state machine
// under concurrent writes to verify that mutex-protected compound transitions
// (slowStrikes→degraded, fastStrikes→recovered) are data-race free and produce
// consistent final state.
//
// Scenario: 50 goroutines simultaneously fire recoveries against a pre-degraded
// cluster. With recoveryThreshold=5, the cluster must recover exactly once —
// and the final isDegraded state must be false regardless of interleaving.
func TestAdaptiveDualWrite_Concurrent_RecoveryThreshold(t *testing.T) {
	const goroutines = 50

	a := NewAdaptiveDualWrite(
		WithAdaptiveStrikeThreshold(3),
		WithAdaptiveRecoveryThreshold(5),
	)
	a.ForceDegrade(types.ClusterA)
	require.True(t, a.IsDegraded(types.ClusterA))

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			a.recordFast(&a.stateA)
		}()
	}
	wg.Wait()

	// After 50 concurrent fast recordings against threshold=5,
	// the cluster must have recovered (isDegraded==false).
	assert.False(t, a.IsDegraded(types.ClusterA),
		"cluster A must be recovered after sufficient concurrent fast recordings")
}

// TestAdaptiveDualWrite_Concurrent_StrikeAndRecover stress-tests interleaved
// strikes and recoveries from many goroutines to verify no data race is detected
// by the race detector (-race). The exact final state is non-deterministic, but
// the test must complete without crashing or deadlocking.
func TestAdaptiveDualWrite_Concurrent_StrikeAndRecover(t *testing.T) {
	const goroutines = 100

	a := NewAdaptiveDualWrite(
		WithAdaptiveStrikeThreshold(3),
		WithAdaptiveRecoveryThreshold(5),
	)

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Half the goroutines record strikes on A; half record fast recoveries on B.
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			a.recordStrike(&a.stateA)
		}()
		go func() {
			defer wg.Done()
			a.recordFast(&a.stateB)
		}()
	}
	wg.Wait()

	// State is non-deterministic but must be internally consistent:
	// isDegraded must match whether slowStrikes reached the threshold.
	// We just verify no panic/deadlock occurred (the -race detector catches races).
}

// TestAdaptiveDualWrite_LastLatencyUpdatedInFireForget verifies that a successful
// fire-and-forget write updates state.lastLatency so that subsequent delta comparisons
// use a fresh observation rather than a stale pre-degradation value.
//
// The bug (before the fix): recordFast was called but lastLatency was never stored,
// so the sibling's delta check would always compare against the old latency snapshot
// taken when the cluster was last written synchronously — potentially hours stale.
func TestAdaptiveDualWrite_LastLatencyUpdatedInFireForget(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveStrikeThreshold(1),
		WithAdaptiveRecoveryThreshold(1),
		WithAdaptiveFireForgetTimeout(5*time.Second),
		// Use a wide delta so the write definitely passes the delta check.
		WithAdaptiveDeltaThreshold(10*time.Second),
		// absoluteMax must be large enough to not reject our fast write.
		WithAdaptiveAbsoluteMax(10*time.Second),
	)

	// Pre-seed sibling (B) with a known latency so the delta path is exercised.
	a.stateB.lastLatency.Store((50 * time.Millisecond).Nanoseconds())

	// Degrade cluster A so the next Execute call takes the fireAndForget path.
	a.ForceDegrade(types.ClusterA)
	require.True(t, a.IsDegraded(types.ClusterA))

	// Capture the old latency — should be 0 (never written).
	require.Equal(t, int64(0), a.stateA.lastLatency.Load(),
		"precondition: stateA.lastLatency must be 0 before fire-and-forget")

	ctx := t.Context()
	var writeDone sync.WaitGroup
	writeDone.Add(1)

	errA, _ := a.Execute(ctx,
		func(context.Context) error {
			// Simulate a fast write (~1ms) — well within absoluteMax and delta.
			time.Sleep(1 * time.Millisecond)
			writeDone.Done()
			return nil
		},
		func(context.Context) error { return nil },
	)

	// Execute must return ErrWriteAsync for the degraded cluster.
	require.ErrorIs(t, errA, types.ErrWriteAsync,
		"cluster A (degraded) must return ErrWriteAsync")

	// Wait for the goroutine to complete before checking lastLatency.
	writeDone.Wait()
	// Give the goroutine a brief moment to store latency after writeDone.Done().
	time.Sleep(5 * time.Millisecond)

	latency := a.stateA.lastLatency.Load()
	assert.Greater(t, latency, int64(0),
		"stateA.lastLatency must be updated after a successful fire-and-forget write")
}

// TestAdaptiveDualWrite_HealthyClusterClearsStrikesWhileSiblingDegraded verifies
// that a healthy cluster's stale slowStrikes are cleared while its sibling is
// degraded. Previously, updateHealthState returned early when only one cluster
// had valid latency, leaving the healthy cluster's slowStrikes frozen.
func TestAdaptiveDualWrite_HealthyClusterClearsStrikesWhileSiblingDegraded(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveAbsoluteMax(50*time.Millisecond),
		WithAdaptiveStrikeThreshold(3),
		WithAdaptiveRecoveryThreshold(3),
	)
	ctx := t.Context()

	// Accumulate 2 slow strikes on cluster A (one short of degradation).
	for range 2 {
		_, _ = a.Execute(ctx,
			func(ctx context.Context) error {
				time.Sleep(60 * time.Millisecond) // Exceeds absoluteMax
				return nil
			},
			func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond)
				return nil
			},
		)
	}
	require.False(t, a.IsDegraded(types.ClusterA), "A not yet degraded (2/3 strikes)")
	require.Equal(t, int32(2), a.stateA.slowStrikes, "A should have 2 slow strikes")

	// Now degrade cluster B so that updateHealthState has only one valid latency.
	a.ForceDegrade(types.ClusterB)
	require.True(t, a.IsDegraded(types.ClusterB))

	// Execute a fast write where only A returns latency. With the fix,
	// A's slowStrikes should be cleared by recordFastIfNoViolation.
	_, _ = a.Execute(ctx,
		func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond) // Fast, no cap violation
			return nil
		},
		func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	)

	assert.Equal(t, int32(0), a.stateA.slowStrikes,
		"A's slowStrikes must be cleared while sibling is degraded and A is healthy")
	assert.False(t, a.IsDegraded(types.ClusterA),
		"A must remain healthy after fast writes while sibling is degraded")
}

// TestAdaptiveDualWrite_ExecuteStrict_BothHealthy verifies that ExecuteStrict
// runs both writes synchronously when both clusters are healthy.
func TestAdaptiveDualWrite_ExecuteStrict_BothHealthy(t *testing.T) {
	a := NewAdaptiveDualWrite()
	ctx := t.Context()

	var callsA, callsB int32
	errA, errB := a.ExecuteStrict(ctx,
		func(_ context.Context) error { atomic.AddInt32(&callsA, 1); return nil },
		func(_ context.Context) error { atomic.AddInt32(&callsB, 1); return nil },
	)

	assert.NoError(t, errA)
	assert.NoError(t, errB)
	assert.Equal(t, int32(1), atomic.LoadInt32(&callsA))
	assert.Equal(t, int32(1), atomic.LoadInt32(&callsB))
}

// TestAdaptiveDualWrite_ExecuteStrict_OneDegraded verifies that when one cluster
// is degraded, ExecuteStrict skips it (returns ErrClusterDegraded) and still
// writes to the healthy cluster — no fire-and-forget goroutine is launched.
func TestAdaptiveDualWrite_ExecuteStrict_OneDegraded(t *testing.T) {
	a := NewAdaptiveDualWrite()
	ctx := t.Context()

	a.ForceDegrade(types.ClusterA)
	require.True(t, a.IsDegraded(types.ClusterA))

	var calledA, calledB int32
	errA, errB := a.ExecuteStrict(ctx,
		func(_ context.Context) error { atomic.AddInt32(&calledA, 1); return nil },
		func(_ context.Context) error { atomic.AddInt32(&calledB, 1); return nil },
	)

	require.ErrorIs(t, errA, types.ErrClusterDegraded, "degraded cluster must return ErrClusterDegraded")
	require.NoError(t, errB)
	assert.Equal(t, int32(0), atomic.LoadInt32(&calledA), "write must not be called for degraded cluster")
	assert.Equal(t, int32(1), atomic.LoadInt32(&calledB))
}

// TestAdaptiveDualWrite_ExecuteStrict_BothDegraded verifies that when both clusters
// are degraded, ExecuteStrict returns ErrClusterDegraded for both without calling
// any write function.
func TestAdaptiveDualWrite_ExecuteStrict_BothDegraded(t *testing.T) {
	a := NewAdaptiveDualWrite()
	ctx := t.Context()

	a.ForceDegrade(types.ClusterA)
	a.ForceDegrade(types.ClusterB)

	var called int32
	errA, errB := a.ExecuteStrict(ctx,
		func(_ context.Context) error { atomic.AddInt32(&called, 1); return nil },
		func(_ context.Context) error { atomic.AddInt32(&called, 1); return nil },
	)

	require.ErrorIs(t, errA, types.ErrClusterDegraded)
	require.ErrorIs(t, errB, types.ErrClusterDegraded)
	assert.Equal(t, int32(0), atomic.LoadInt32(&called), "no write must be called when both clusters are degraded")
}

// TestAdaptiveDualWrite_ExecuteStrict_NoFireAndForget verifies that ExecuteStrict
// does not launch fire-and-forget goroutines when a cluster is degraded. The
// write function for the degraded cluster must never be called, even after a delay.
func TestAdaptiveDualWrite_ExecuteStrict_NoFireAndForget(t *testing.T) {
	a := NewAdaptiveDualWrite(WithAdaptiveFireForgetTimeout(100 * time.Millisecond))
	ctx := t.Context()

	a.ForceDegrade(types.ClusterA)

	var calledA int32
	errA, errB := a.ExecuteStrict(ctx,
		func(_ context.Context) error { atomic.AddInt32(&calledA, 1); return nil },
		func(_ context.Context) error { return nil },
	)

	require.ErrorIs(t, errA, types.ErrClusterDegraded)
	require.NoError(t, errB)

	// Wait longer than the fire-and-forget timeout to confirm no goroutine ran
	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&calledA), "write must not be called via fire-and-forget")
}

// TestAdaptiveDualWrite_ExecuteStrict_DegradedDoesNotStrike verifies that
// ErrClusterDegraded returned by ExecuteStrict for a skipped cluster is NOT
// counted as a strike against that cluster.
func TestAdaptiveDualWrite_ExecuteStrict_DegradedDoesNotStrike(t *testing.T) {
	a := NewAdaptiveDualWrite(WithAdaptiveStrikeThreshold(1))
	ctx := t.Context()

	// Degrade A via ForceDegrade (not via strikes)
	a.ForceDegrade(types.ClusterA)
	a.ForceRecover(types.ClusterA)
	require.False(t, a.IsDegraded(types.ClusterA))
	require.Equal(t, int32(0), a.stateA.slowStrikes)

	// Now ForceDegrade again and run ExecuteStrict
	a.ForceDegrade(types.ClusterA)

	_, _ = a.ExecuteStrict(ctx,
		func(_ context.Context) error { return nil },
		func(_ context.Context) error { return nil },
	)

	// Recover A; if strikes were counted, it would re-degrade immediately
	a.ForceRecover(types.ClusterA)
	require.False(t, a.IsDegraded(types.ClusterA))
	assert.Equal(t, int32(0), a.stateA.slowStrikes, "ErrClusterDegraded must not record a strike")
}

// TestAdaptiveDualWrite_RecordProbeSuccess verifies that RecordProbeSuccess feeds
// the existing recovery counter (recordFast), enabling degraded clusters to
// recover via probe signals.
func TestAdaptiveDualWrite_RecordProbeSuccess(t *testing.T) {
	a := NewAdaptiveDualWrite(WithAdaptiveRecoveryThreshold(3))

	a.ForceDegrade(types.ClusterA)
	require.True(t, a.IsDegraded(types.ClusterA))

	a.RecordProbeSuccess(types.ClusterA)
	a.RecordProbeSuccess(types.ClusterA)
	assert.True(t, a.IsDegraded(types.ClusterA), "not yet recovered after 2/3 probe successes")

	a.RecordProbeSuccess(types.ClusterA)
	assert.False(t, a.IsDegraded(types.ClusterA), "must recover after 3 probe successes")
}

// TestAdaptiveDualWrite_HandleErrors_ExcludesStrict verifies that ErrClusterDegraded
// and ErrClusterDraining do NOT count as strikes.
func TestAdaptiveDualWrite_HandleErrors_ExcludesStrict(t *testing.T) {
	a := NewAdaptiveDualWrite(WithAdaptiveStrikeThreshold(1))

	// Two strict-sentinel calls must not record any strikes
	a.handleErrors(types.ErrClusterDegraded, nil)
	a.handleErrors(types.ErrClusterDraining, nil)

	assert.Equal(t, int32(0), a.stateA.slowStrikes)
	assert.False(t, a.IsDegraded(types.ClusterA), "ErrClusterDegraded/ErrClusterDraining must not cause degradation")
}

// TestAdaptiveDualWrite_CapViolationNotClearedWhileSiblingDegraded verifies
// that when a cluster still exceeds absoluteMax while its sibling is degraded,
// recordFastIfNoViolation does NOT reset slowStrikes — the cap-violation
// strike from checkAbsoluteCap must be preserved.
func TestAdaptiveDualWrite_CapViolationNotClearedWhileSiblingDegraded(t *testing.T) {
	a := NewAdaptiveDualWrite(
		WithAdaptiveAbsoluteMax(50*time.Millisecond),
		WithAdaptiveStrikeThreshold(3),
		WithAdaptiveRecoveryThreshold(3),
	)
	ctx := t.Context()

	// Accumulate 2 slow strikes on cluster A (one short of degradation).
	for range 2 {
		_, _ = a.Execute(ctx,
			func(ctx context.Context) error {
				time.Sleep(60 * time.Millisecond) // Exceeds absoluteMax
				return nil
			},
			func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond)
				return nil
			},
		)
	}
	require.False(t, a.IsDegraded(types.ClusterA), "A not yet degraded (2/3 strikes)")
	require.Equal(t, int32(2), a.stateA.slowStrikes, "A should have 2 slow strikes")

	// Degrade cluster B so that updateHealthState enters the early-return path.
	a.ForceDegrade(types.ClusterB)
	require.True(t, a.IsDegraded(types.ClusterB))

	// Execute a SLOW write on A (still exceeding absoluteMax). The
	// cap-violation strike must be preserved — recordFastIfNoViolation
	// must NOT clear it.
	_, _ = a.Execute(ctx,
		func(ctx context.Context) error {
			time.Sleep(60 * time.Millisecond) // Still exceeds absoluteMax
			return nil
		},
		func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	)

	assert.True(t, a.IsDegraded(types.ClusterA),
		"A must degrade: 3rd cap-violation strike should not be cleared by the early-return path")
	assert.GreaterOrEqual(t, a.stateA.slowStrikes, int32(3),
		"A's slowStrikes must reach the threshold when cap violation persists")
}
