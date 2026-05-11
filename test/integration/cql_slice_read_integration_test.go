package integration_test

// Phase 9 integration tests for SliceMap / SliceScan / MaxRows.
//
// These tests exercise the new slice-read surface against real Cassandra /
// ScyllaDB clusters to lock the contracts that are silent in unit tests:
//   - FallbackRead routes to the alt cluster when the primary has 0 rows.
//   - MaxRows overflow returns ErrRowLimitExceeded with real driver pagination.
//   - Drain-aware skip: alt is not contacted when it is draining.
//   - PageState guard: FallbackRead is suppressed for continuation queries.
//
// Conventions:
//   - Do NOT call client.Close() — it would close the shared gocql sessions
//     that TestMain manages.
//   - Use t.Context() for all contexts.
//   - Use require.Eventually for topology-state propagation.

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	cqlv2 "github.com/arloliu/helix/adapter/cql/v2"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/topology"
	"github.com/arloliu/helix/types"
)

// sliceSeqSchema has a clustering column for stable, partition-scoped
// pagination — used by the PageState guard test.
const sliceSeqSchema = `
	CREATE TABLE IF NOT EXISTS %s (
		bucket INT,
		seq    INT,
		val    TEXT,
		PRIMARY KEY ((bucket), seq)
	)
`

// intSliceDriver parameterizes pagination-sensitive tests over the v1 and v2
// gocql adapters. Data seeding always uses v1 (getSharedSessions); only the
// helix client session layer differs between drivers.
type intSliceDriver struct {
	name       string
	makeClient func(t *testing.T, opts ...helix.Option) (*helix.CQLClient, error)
}

var intSliceDrivers = []intSliceDriver{
	{
		name: "v1",
		makeClient: func(t *testing.T, opts ...helix.Option) (*helix.CQLClient, error) {
			t.Helper()
			sA, sB := getSharedSessions(t)
			return helix.NewCQLClient(cqlv1.WrapSession(sA), cqlv1.WrapSession(sB), opts...)
		},
	},
	{
		name: "v2",
		makeClient: func(t *testing.T, opts ...helix.Option) (*helix.CQLClient, error) {
			t.Helper()
			sA, sB := getSharedSessionsV2(t)
			return helix.NewCQLClient(cqlv2.NewSession(sA), cqlv2.NewSession(sB), opts...)
		},
	},
}

// =============================================================================
// FallbackRead: primary empty → alt rows returned
// =============================================================================

func TestFallback_SliceMap_AltHasData_ReturnsAltRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()
	table := createTestTableOnBoth(t, "sr_fb_slicemap", configTableCQLSchema)

	// Seed 3 rows on B only (simulates replication lag: A has none yet).
	for i := range 3 {
		require.NoError(t,
			sessionB.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table),
				fmt.Sprintf("key_%d", i), fmt.Sprintf("val_%d", i)).Exec())
	}

	mc := testutil.NewTestMetricsCollector()
	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	// Do NOT call client.Close() — shared gocql sessions.

	rows, err := client.Query(fmt.Sprintf("SELECT key, value FROM %s", table)).
		FallbackRead().
		SliceMapContext(ctx)

	require.NoError(t, err)
	require.Len(t, rows, 3, "FallbackRead must surface B's 3 rows")

	// Divergence fires on the stale primary (A), not the alt.
	assert.Equal(t, int64(1), mc.GetReadDivergence(types.ClusterA))
	assert.Equal(t, int64(0), mc.GetReadDivergence(types.ClusterB))

	// Primary attempted once; alt attempted exactly once via FallbackRead.
	assert.Equal(t, int64(1), mc.ReadTotal[types.ClusterA])
	assert.Equal(t, int64(1), mc.ReadTotal[types.ClusterB])
}

func TestFallback_SliceScan_AltHasData_ReturnsAltRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()
	table := createTestTableOnBoth(t, "sr_fb_slicescan", configTableCQLSchema)

	for i := range 3 {
		require.NoError(t,
			sessionB.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table),
				fmt.Sprintf("key_%d", i), fmt.Sprintf("val_%d", i)).Exec())
	}

	mc := testutil.NewTestMetricsCollector()
	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	// Do NOT call client.Close() — shared gocql sessions.

	var got []string
	rowCount, err := client.Query(fmt.Sprintf("SELECT value FROM %s", table)).
		FallbackRead().
		SliceScanContext(ctx, func(r helix.RowScanner) error {
			var v string
			if scanErr := r.Scan(&v); scanErr != nil {
				return scanErr
			}
			got = append(got, v)
			return nil
		})

	require.NoError(t, err)
	assert.Equal(t, 3, rowCount)
	assert.Len(t, got, 3)

	assert.Equal(t, int64(1), mc.GetReadDivergence(types.ClusterA))
	assert.Equal(t, int64(1), mc.ReadTotal[types.ClusterB])
}

// =============================================================================
// FallbackRead: both clusters empty → (nil, nil)
// =============================================================================

func TestFallback_SliceMap_BothEmpty_ReturnsNilNil(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()
	table := createTestTableOnBoth(t, "sr_fb_empty", configTableCQLSchema)

	mc := testutil.NewTestMetricsCollector()
	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	// Do NOT call client.Close() — shared gocql sessions.

	rows, err := client.Query(fmt.Sprintf("SELECT key, value FROM %s", table)).
		FallbackRead().
		SliceMapContext(ctx)

	require.NoError(t, err, "both clusters empty: must return nil error, not ErrNotFound")
	assert.Nil(t, rows, "both clusters empty: must return nil slice")

	// Both clusters must have been contacted — proves the fallback leg ran, not
	// just that the primary happened to return empty without trying the alt.
	assert.Equal(t, int64(1), mc.ReadTotal[types.ClusterA], "primary must be attempted once")
	assert.Equal(t, int64(1), mc.ReadTotal[types.ClusterB], "alt must be attempted when primary empty")
}

// =============================================================================
// MaxRows: overflow with real data
// =============================================================================

func TestSliceMap_MaxRows_Overflow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessions(t)
	ctx := t.Context()
	table := createTestTableOnBoth(t, "sr_maxrows_overflow", configTableCQLSchema)

	// Insert 5 rows; MaxRows(3) must trigger ErrRowLimitExceeded.
	for i := range 5 {
		require.NoError(t,
			sessionA.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table),
				fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)).Exec())
	}

	for _, d := range intSliceDrivers {
		t.Run(d.name, func(t *testing.T) {
			client, err := d.makeClient(t,
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(types.ClusterA),
				)),
			)
			require.NoError(t, err)
			// Do NOT call client.Close() — shared gocql sessions.

			rows, err := client.Query(fmt.Sprintf("SELECT key, value FROM %s", table)).
				MaxRows(3).
				SliceMapContext(ctx)

			assert.Nil(t, rows, "on overflow the partial slice must not be exposed")
			require.Error(t, err)
			assert.True(t, helix.IsRowLimitExceeded(err),
				"must return ErrRowLimitExceeded; got: %v", err)
		})
	}
}

// TestSliceMap_MaxRows_LargePageSize_StillEnforced verifies end-to-end MaxRows
// enforcement when a caller sets PageSize larger than MaxRows.  This test does
// not verify the internal page-size clamp mechanism (which would require a
// driver-level spy); it verifies only that the overflow contract holds
// regardless of the PageSize value supplied by the caller.
func TestSliceMap_MaxRows_LargePageSize_StillEnforced(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessions(t)
	ctx := t.Context()
	table := createTestTableOnBoth(t, "sr_maxrows_clamp", configTableCQLSchema)

	for i := range 5 {
		require.NoError(t,
			sessionA.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table),
				fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)).Exec())
	}

	for _, d := range intSliceDrivers {
		t.Run(d.name, func(t *testing.T) {
			client, err := d.makeClient(t,
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(types.ClusterA),
				)),
			)
			require.NoError(t, err)
			// Do NOT call client.Close() — shared gocql sessions.

			rows, err := client.Query(fmt.Sprintf("SELECT key, value FROM %s", table)).
				MaxRows(3).
				PageSize(1000).
				SliceMapContext(ctx)

			assert.Nil(t, rows)
			require.Error(t, err)
			assert.True(t, helix.IsRowLimitExceeded(err),
				"[%s] large PageSize must not bypass MaxRows; got: %v", d.name, err)
		})
	}
}

// =============================================================================
// Drain-aware skip: alt draining → FallbackRead skips alt
// =============================================================================

func TestFallback_SliceMap_AltDraining_SkipsAlt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)
	ctx := t.Context()
	table := createTestTableOnBoth(t, "sr_drain_skip", configTableCQLSchema)

	// B has rows — without drain-aware skip, FallbackRead would return them.
	for i := range 3 {
		require.NoError(t,
			sessionB.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", table),
				fmt.Sprintf("key_%d", i), fmt.Sprintf("val_%d", i)).Exec())
	}

	mc := testutil.NewTestMetricsCollector()
	watcher := topology.NewLocal()
	defer watcher.Close()

	client, err := helix.NewCQLClient(
		cqlv1.WrapSession(sessionA),
		cqlv1.WrapSession(sessionB),
		helix.WithReadStrategy(policy.NewStickyRead(
			policy.WithPreferredCluster(types.ClusterA),
		)),
		helix.WithTopologyWatcher(watcher),
		helix.WithMetrics(mc),
	)
	require.NoError(t, err)
	// Do NOT call client.Close() — shared gocql sessions.

	// Mark B as draining and wait for the state to propagate.
	require.NoError(t, watcher.SetDrain(ctx, types.ClusterB, true, "maintenance"))
	require.Eventually(t, func() bool {
		return client.IsDraining(helix.ClusterB)
	}, time.Second, 10*time.Millisecond, "drain state must propagate")

	rows, err := client.Query(fmt.Sprintf("SELECT key, value FROM %s", table)).
		FallbackRead().
		SliceMapContext(ctx)

	// A is empty, B is draining → FallbackRead skips B → (nil, nil).
	require.NoError(t, err, "drain-skip must return nil, not an error")
	assert.Nil(t, rows, "drain-skip must return nil slice, not B's rows")

	// Alt (B) must not have been contacted at all.
	assert.Equal(t, int64(0), mc.ReadTotal[types.ClusterB],
		"draining alt must not be contacted")
}

// =============================================================================
// PageState guard: FallbackRead suppressed for continuation queries
// =============================================================================

// TestSliceMap_PageState_FallbackNotFired verifies that setting a PageState on
// a SliceMapContext query disables FallbackRead, even when FallbackRead is
// explicitly chained.  This prevents opaque pagination cursors from being sent
// to a different cluster.
//
// Setup:
//   - 10 rows are seeded on A only; B has none.
//   - Page 1 is fetched via helix Iter (PageSize=5) to obtain a real pageState.
//   - The rows that would appear on page 2 are deleted from A (so A returns
//     empty for the continuation), and B is seeded with rows.
//   - Page 2 is fetched via SliceMapContext with pageState + FallbackRead.
//
// With the pageState guard active, FallbackRead is suppressed even though A
// returns empty and B has rows.  The load-bearing assertions are:
//   - rows == nil  (A empty, guard prevents B fallback → (nil, nil))
//   - ReadTotal[B] unchanged  (B was never contacted)
//
// If the guard were absent, FallbackRead would fire and B's rows would be
// returned, causing both assertions to fail.
func TestSliceMap_PageState_FallbackNotFired(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, sessionB := getSharedSessions(t)

	const bucket = 1
	const total = 10
	const pageSize = 5

	for _, d := range intSliceDrivers {
		t.Run(d.name, func(t *testing.T) {
			ctx := t.Context()
			// Each driver gets its own table so per-driver data mutations don't interfere.
			table := createTestTableOnBoth(t, "sr_pagestate_"+d.name, sliceSeqSchema)
			stmt := fmt.Sprintf("SELECT seq, val FROM %s WHERE bucket = ?", table)

			// Seed 10 rows on A only; B has none.
			for i := range total {
				require.NoError(t,
					sessionA.Query(
						fmt.Sprintf("INSERT INTO %s (bucket, seq, val) VALUES (?, ?, ?)", table),
						bucket, i, fmt.Sprintf("v%d", i),
					).Exec())
			}

			mc := testutil.NewTestMetricsCollector()
			client, err := d.makeClient(t,
				helix.WithReadStrategy(policy.NewStickyRead(
					policy.WithPreferredCluster(types.ClusterA),
				)),
				helix.WithMetrics(mc),
			)
			require.NoError(t, err)
			// Do NOT call client.Close() — shared gocql sessions.

			// Page 1: consume exactly one page of rows and capture the continuation token.
			iter := client.Query(stmt, bucket).PageSize(pageSize).IterContext(ctx)
			var seq int
			var val string
			var page1Seqs []int
			for len(page1Seqs) < pageSize && iter.Scan(&seq, &val) {
				page1Seqs = append(page1Seqs, seq)
			}
			pageState := iter.PageState()
			require.NoError(t, iter.Close())
			require.Len(t, page1Seqs, pageSize)
			require.NotEmpty(t, pageState,
				"must obtain a non-nil pageState after page 1 (%d rows, PageSize %d)", total, pageSize)

			// Delete the rows that would appear on page 2 from A, so the continuation
			// query returns empty.  Seed B with rows so that a broken guard would
			// surface them via FallbackRead.
			for i := pageSize; i < total; i++ {
				require.NoError(t,
					sessionA.Query(
						fmt.Sprintf("DELETE FROM %s WHERE bucket = ? AND seq = ?", table),
						bucket, i,
					).Exec())
			}
			for i := range 3 {
				require.NoError(t,
					sessionB.Query(
						fmt.Sprintf("INSERT INTO %s (bucket, seq, val) VALUES (?, ?, ?)", table),
						bucket, 100+i, fmt.Sprintf("b%d", i),
					).Exec())
			}

			prePage2B := mc.ReadTotal[types.ClusterB]

			// Page 2: A returns empty (rows deleted); pageState guard must suppress
			// FallbackRead → (nil, nil).  If the guard were broken, B's rows would
			// be returned and the assertions below would fail.
			rows, err := client.Query(stmt, bucket).
				PageSize(pageSize).
				PageState(pageState).
				FallbackRead().
				SliceMapContext(ctx)

			require.NoError(t, err)
			assert.Nil(t, rows,
				"[%s] pageState guard must suppress FallbackRead; A empty on continuation, B rows must not be returned",
				d.name)
			assert.Equal(t, prePage2B, mc.ReadTotal[types.ClusterB],
				"[%s] pageState guard must prevent alt contact during continuation query", d.name)
		})
	}
}
