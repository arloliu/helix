package policy

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

func TestStickyRead_SwapsDuringCooldownWhenAlternativeIsKnownGood(t *testing.T) {
	s := NewStickyRead(WithPreferredCluster(types.ClusterA), WithStickyReadCooldown(time.Hour))

	// A blips once: preferred moves to B and the cooldown starts.
	alt, ok := s.OnFailure(types.ClusterA, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterB, alt)
	require.Equal(t, types.ClusterB, s.Preferred())

	// B goes hard down.
	// The first failed read retries on A per request without moving the preference, because A is still known bad.
	alt, ok = s.OnFailure(types.ClusterB, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterA, alt)
	require.Equal(t, types.ClusterB, s.Preferred(), "A has not proven itself yet")

	// That retry succeeds on A: A is known good again.
	s.OnSuccess(types.ClusterA)

	// The next failure on B moves the preference during the cooldown.
	alt, ok = s.OnFailure(types.ClusterB, nil)
	require.True(t, ok)
	require.Equal(t, types.ClusterA, alt)
	require.Equal(t, types.ClusterA, s.Preferred(), "a known-good alternative ends the pin on a dead preferred")
}

func TestStickyRead_FlappingClustersDoNotOscillateDuringCooldown(t *testing.T) {
	s := NewStickyRead(WithPreferredCluster(types.ClusterA), WithStickyReadCooldown(time.Hour))

	_, _ = s.OnFailure(types.ClusterA, nil) // A → B
	require.Equal(t, types.ClusterB, s.Preferred())
	for range 5 {
		// B fails, the retry on A fails too: neither cluster is known good,
		// so the preference stays put.
		_, _ = s.OnFailure(types.ClusterB, nil)
		_, _ = s.OnFailure(types.ClusterA, nil)
		require.Equal(t, types.ClusterB, s.Preferred())
	}
}

func TestStickyRead_SetPreferredAndReset(t *testing.T) {
	s := NewStickyRead(WithPreferredCluster(types.ClusterA), WithStickyReadCooldown(time.Hour))
	_, _ = s.OnFailure(types.ClusterA, nil)
	require.Equal(t, types.ClusterB, s.Preferred())

	s.SetPreferred(types.ClusterA)
	require.Equal(t, types.ClusterA, s.Preferred())
	s.SetPreferred("unknown")
	require.Equal(t, types.ClusterA, s.Preferred(), "an unknown cluster is ignored")

	// SetPreferred restarted the cooldown, and A is known bad from its
	// earlier failure while B is known good: a failure on A moves back to B.
	_, _ = s.OnFailure(types.ClusterA, nil)
	require.Equal(t, types.ClusterB, s.Preferred())

	s.Reset()
	require.Equal(t, types.ClusterA, s.Preferred(), "Reset returns to the constructed preference")
	// Reset cleared the cooldown and the known-bad marks: a failure on A
	// swaps normally.
	_, _ = s.OnFailure(types.ClusterA, nil)
	require.Equal(t, types.ClusterB, s.Preferred())
}

// longAgoNs returns a Unix nano timestamp two hours in the past.
func longAgoNs() int64 {
	return time.Now().Add(-2 * time.Hour).UnixNano()
}

// failedOverLongAgo returns a PrimaryOnlyRead that failed over to B and whose
// recovery timeout has already elapsed.
func failedOverLongAgo(t *testing.T) *PrimaryOnlyRead {
	t.Helper()
	p := NewPrimaryOnlyRead(WithPrimaryOnlyRecoveryTimeout(time.Hour))
	_, _ = p.OnFailure(types.ClusterA, nil)
	p.nextProbeAt.Store(longAgoNs())

	return p
}

func TestPrimaryOnlyRead_RecoveryProbeIsSingleFlight(t *testing.T) {
	p := failedOverLongAgo(t)

	// Concurrent callers behind the probe keep reading B.
	const callers = 64
	var wg sync.WaitGroup
	var probes atomic.Int32
	for range callers {
		wg.Go(func() {
			if p.Select(context.Background()) == types.ClusterA {
				probes.Add(1)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, probes.Load(), "exactly one concurrent caller probes A")

	// The probe reports success: everyone returns to A.
	p.OnSuccess(types.ClusterA)
	require.Equal(t, types.ClusterA, p.Select(context.Background()))
}

func TestPrimaryOnlyRead_FailedProbeRestartsTimer(t *testing.T) {
	p := failedOverLongAgo(t)
	require.Equal(t, types.ClusterA, p.Select(context.Background()))
	require.Equal(t, types.ClusterB, p.Select(context.Background()), "the probe is in flight")

	_, _ = p.OnFailure(types.ClusterA, nil)
	require.Greater(t, p.nextProbeAt.Load(), time.Now().UnixNano(), "the failed probe restarted the timer")
	require.Equal(t, types.ClusterB, p.Select(context.Background()))
}

func TestPrimaryOnlyRead_LostProbeExpires(t *testing.T) {
	p := failedOverLongAgo(t)
	require.Equal(t, types.ClusterA, p.Select(context.Background()))
	require.Equal(t, types.ClusterB, p.Select(context.Background()))

	// The caller holding the probe never reported (its context ended).
	// After another recovery timeout the reservation expires.
	p.nextProbeAt.Store(longAgoNs())
	require.Equal(t, types.ClusterA, p.Select(context.Background()), "a lost probe does not block recovery forever")
}
