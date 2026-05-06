//go:build e2e

package cql_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	htypes "github.com/arloliu/helix/types"
)

// outcome captures everything we want to compare between v1 and v2 runs of
// the same scenario. Wall-clock timing is deliberately excluded — drivers
// will diverge there by design (see hypothesis H5 in SPIKE_FINDINGS.md).
//
// The fingerprint covers the *Helix-observable contract*: error type,
// error.Is / errors.As classification, policy state, replay state. If two
// drivers produce different fingerprints for the same scenario, that's an
// adapter-layer normalization gap.
type outcome struct {
	finalErr      error // raw error returned to the caller
	finalErrIsNil bool

	// Helix sentinel matches.
	isWriteAsync   bool
	isWriteDropped bool
	isNotFound     bool

	// DualClusterError unwrap.
	isDualClusterErr bool
	dualHasA         bool
	dualHasB         bool

	// Strategy state at the end of the operation.
	isDegradedA bool
	isDegradedB bool

	// Replay queue size (post-operation, pre-drain).
	replayLen int

	// Cumulative metrics.
	totalFailovers int64
}

func (o outcome) String() string {
	return fmt.Sprintf("err=%v isWriteAsync=%v isWriteDropped=%v isNotFound=%v "+
		"isDualClusterErr=%v dualA=%v dualB=%v "+
		"degradedA=%v degradedB=%v replayLen=%d failovers=%d",
		o.finalErr, o.isWriteAsync, o.isWriteDropped, o.isNotFound,
		o.isDualClusterErr, o.dualHasA, o.dualHasB,
		o.isDegradedA, o.isDegradedB, o.replayLen, o.totalFailovers)
}

func captureOutcomeForErr(err error) outcome {
	o := outcome{finalErr: err, finalErrIsNil: err == nil}
	if err == nil {
		return o
	}
	o.isWriteAsync = errors.Is(err, htypes.ErrWriteAsync)
	o.isWriteDropped = errors.Is(err, htypes.ErrWriteDropped)
	o.isNotFound = htypes.IsNotFound(err)

	var dual *htypes.DualClusterError
	if errors.As(err, &dual) {
		o.isDualClusterErr = true
		o.dualHasA = dual.ErrorA != nil
		o.dualHasB = dual.ErrorB != nil
	}

	return o
}

// TestS6_BothPaused_DualClusterError probes hypothesis H3 (error type
// differences between drivers): when both clusters hang, both drivers
// should surface a DualClusterError to the caller. If one returns the
// raw driver error and the other returns the sentinel, the parity claim
// is broken at the adapter layer.
func TestS6_BothPaused_DualClusterError(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "parity_both_paused")
	seedKV(t, a, b, table, "k", "v")

	outcomes := map[string]outcome{}

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			mc := testutil.NewTestMetricsCollector()
			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(policy.NewStickyRead()),
				helix.WithWriteStrategy(policy.NewSyncDualWrite()),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			// Pause both clusters so every op hangs until per-query Timeout.
			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			require.NoError(t, b.Pause(ctx))

			// Defensive: unpause in scenario cleanup too (withRestoredCluster
			// also handles it but happens after this t.Run finishes).
			defer func() {
				_ = a.Unpause(context.Background())
				_ = b.Unpause(context.Background())
			}()

			// Drive a read with a bounded context — the actual query also
			// has gocql's Timeout set, but the context cap is a belt-and-
			// suspenders bound on the test runtime.
			qCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			var got string
			err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
				ScanContext(qCtx, &got)

			o := captureOutcomeForErr(err)
			o.totalFailovers = mc.GetTotalFailovers()
			outcomes[d.name] = o

			t.Logf("%s outcome: %s", d.name, o)

			// Per-driver assertion: must surface DualClusterError, not the
			// raw driver error. If this fires, the adapter is leaking driver
			// semantics.
			assert.True(t, o.isDualClusterErr,
				"%s: expected DualClusterError when both paused; got %T: %v",
				d.name, err, err)
		})
	}

	// Cross-driver parity: every fingerprint field must match.
	require.Contains(t, outcomes, "v1")
	require.Contains(t, outcomes, "v2")
	v1, v2 := outcomes["v1"], outcomes["v2"]
	assertOutcomeParity(t, "BothPaused", v1, v2)
}

// TestS6_PauseA_StickyReadParity probes the read-side failover path:
// with one cluster paused, both drivers should route reads to the
// surviving cluster and return the same observable success/error fingerprint.
func TestS6_PauseA_StickyReadParity(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "parity_pause_a")
	seedKV(t, a, b, table, "k", "v")

	outcomes := map[string]outcome{}

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			mc := testutil.NewTestMetricsCollector()
			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(htypes.ClusterA),
					policy.WithStickyReadCooldown(1*time.Hour),
				)),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			// Pause A; B remains healthy. Sticky should fail over to B.
			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			defer func() { _ = a.Unpause(context.Background()) }()

			qCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			var got string
			err = client.Query("SELECT value FROM "+table+" WHERE key = ?", "k").
				ScanContext(qCtx, &got)

			o := captureOutcomeForErr(err)
			o.totalFailovers = mc.GetTotalFailovers()
			outcomes[d.name] = o

			t.Logf("%s outcome: %s gotValue=%q", d.name, o, got)

			require.NoError(t, err, "%s: read should succeed via cluster B", d.name)
			assert.Equal(t, "v", got, "%s: cluster B value", d.name)
			assert.GreaterOrEqual(t, o.totalFailovers, int64(1),
				"%s: expected at least one failover event", d.name)
		})
	}

	require.Contains(t, outcomes, "v1")
	require.Contains(t, outcomes, "v2")
	assertOutcomeParity(t, "PauseA_StickyRead", outcomes["v1"], outcomes["v2"])
}

// TestS6_PauseA_WritePathParity drives writes against a paused A. Captures
// the replay-queue state and AdaptiveDualWrite degradation flags at the
// end of the burst — both drivers should produce the same shape: degraded
// flag set on A, replay queue grew, etc.
func TestS6_PauseA_WritePathParity(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)

	table := createKVTableOnBoth(t, "parity_pause_a_write")

	outcomes := map[string]outcome{}

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			adw := policy.NewAdaptiveDualWrite(
				policy.WithAdaptiveStrikeThreshold(2),
				policy.WithAdaptiveDeltaThreshold(100*time.Millisecond),
			)
			memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(1000))
			mc := testutil.NewTestMetricsCollector()

			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(adw),
				helix.WithReadStrategy(policy.NewStickyRead()),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
				helix.WithReplayer(memReplayer),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			// Pause A. Drive 20 writes. Capture the resulting fingerprint.
			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			defer func() { _ = a.Unpause(context.Background()) }()

			var lastErr error
			for i := 0; i < 20; i++ {
				key := fmt.Sprintf("k%d", i)
				lastErr = client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
					key, "v").Exec()
			}

			o := captureOutcomeForErr(lastErr)
			o.replayLen = memReplayer.Len()
			o.totalFailovers = mc.GetTotalFailovers()
			o.isDegradedA = adw.IsDegraded(htypes.ClusterA)
			o.isDegradedB = adw.IsDegraded(htypes.ClusterB)
			outcomes[d.name] = o

			t.Logf("%s outcome: %s", d.name, o)
		})
	}

	require.Contains(t, outcomes, "v1")
	require.Contains(t, outcomes, "v2")
	assertOutcomeParity(t, "PauseA_Write", outcomes["v1"], outcomes["v2"])
}

// assertOutcomeParity compares two outcome fingerprints and reports each
// divergent field individually so failures are diagnostic. Wall-clock
// timing is deliberately not part of the fingerprint and not asserted.
//
// replayLen is asserted as "both zero or both nonzero" rather than exact
// equality, since exact counts depend on driver-specific timing.
func assertOutcomeParity(t *testing.T, label string, v1, v2 outcome) {
	t.Helper()
	assert.Equal(t, v1.finalErrIsNil, v2.finalErrIsNil,
		"[%s] error nil-ness divergence: v1=%v v2=%v", label, v1.finalErr, v2.finalErr)
	assert.Equal(t, v1.isWriteAsync, v2.isWriteAsync,
		"[%s] errors.Is(ErrWriteAsync) divergence: v1=%v v2=%v", label, v1, v2)
	assert.Equal(t, v1.isWriteDropped, v2.isWriteDropped,
		"[%s] errors.Is(ErrWriteDropped) divergence: v1=%v v2=%v", label, v1, v2)
	assert.Equal(t, v1.isNotFound, v2.isNotFound,
		"[%s] IsNotFound divergence: v1=%v v2=%v", label, v1, v2)
	assert.Equal(t, v1.isDualClusterErr, v2.isDualClusterErr,
		"[%s] DualClusterError divergence: v1=%v v2=%v", label, v1, v2)
	assert.Equal(t, v1.dualHasA, v2.dualHasA,
		"[%s] DualClusterError.ErrA presence divergence: v1=%+v v2=%+v", label, v1, v2)
	assert.Equal(t, v1.dualHasB, v2.dualHasB,
		"[%s] DualClusterError.ErrB presence divergence: v1=%+v v2=%+v", label, v1, v2)
	assert.Equal(t, v1.replayLen > 0, v2.replayLen > 0,
		"[%s] replay queue grew for one driver but not the other: v1=%d v2=%d",
		label, v1.replayLen, v2.replayLen)
	assert.Equal(t, v1.isDegradedA, v2.isDegradedA,
		"[%s] isDegraded(A) divergence: v1=%v v2=%v", label, v1, v2)
	assert.Equal(t, v1.isDegradedB, v2.isDegradedB,
		"[%s] isDegraded(B) divergence: v1=%v v2=%v", label, v1, v2)
}

// seedKV writes (key, value) to both clusters' raw sessions so reads can
// verify each side independently. key/value are kept variadic for future
// scenarios that need different seeds.
//
//nolint:unparam // intentional: API kept open for future scenarios
func seedKV(t *testing.T, a, b *testutil.CQLCluster, table, key, value string) {
	t.Helper()
	stmt := "INSERT INTO " + table + " (key, value) VALUES (?, ?)"
	require.NoError(t, a.Session.Query(stmt, key, value).Exec())
	require.NoError(t, b.Session.Query(stmt, key, value).Exec())
}
