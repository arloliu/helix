package helix_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

// TestReplayDrop_QueueOverflow_IsObservable verifies the SPIKE_FINDINGS §6
// hypothesis ("replay queue silently loses writes under sustained load") with
// a deterministic unit reproducer. The claim is invalidated if every dropped
// payload fires OnReplayDropped AND IncReplayDropped — the loss is then
// observable, just opt-in.
//
// Setup: cluster A always fails, cluster B always succeeds, replay queue
// capacity is small. Drive many more writes than the queue can hold.
//
// Assertions:
//  1. Every caller-side Exec returns nil (B succeeded; partial-success is
//     reported as success per the documented contract).
//  2. enqueued > 0 (some writes did make it into the queue — sanity check).
//  3. dropped > 0 (overflow did happen — the test is meaningful).
//  4. droppedCallback == droppedMetric (instrumentation is symmetric).
//  5. enqueued + dropped == totalWrites (full accounting — no silent loss).
//
// If this test passes, F1 is "false alarm — instrumented, opt-in" and the
// fix is documentation, not code. If it fails, the loss is genuinely silent
// and the production code needs fixing.
func TestReplayDrop_QueueOverflow_IsObservable(t *testing.T) {
	const (
		queueCapacity = 10
		totalWrites   = 100
	)

	sessionA := newAlwaysFailSession(errors.New("cluster A down"))
	sessionB := newAlwaysOKSession()

	memReplayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(queueCapacity))
	mc := testutil.NewTestMetricsCollector()

	var dropCallbackCount atomic.Int64
	client, err := helix.NewCQLClient(sessionA, sessionB,
		helix.WithWriteStrategy(policy.NewSyncDualWrite()),
		helix.WithReadStrategy(policy.NewStickyRead()),
		helix.WithFailoverPolicy(policy.NewActiveFailover()),
		helix.WithReplayer(memReplayer),
		helix.WithMetrics(mc),
		helix.WithOnReplayDropped(func(_ types.ReplayPayload, _ error) {
			dropCallbackCount.Add(1)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx := context.Background()
	var execErrs int
	for i := 0; i < totalWrites; i++ {
		if err := client.Query("INSERT INTO t (k, v) VALUES (?, ?)", i, "v").ExecContext(ctx); err != nil {
			execErrs++
		}
	}

	// 1. Caller-side: B succeeded so every Exec returns nil.
	assert.Equal(t, 0, execErrs, "all writes should report success (B succeeded)")

	enqueued := mc.GetReplayEnqueued(types.ClusterA)
	dropped := mc.GetReplayDropped(types.ClusterA)
	t.Logf("enqueued=%d dropped(metric)=%d dropped(callback)=%d capacity=%d writes=%d",
		enqueued, dropped, dropCallbackCount.Load(), queueCapacity, totalWrites)

	// 2. Queue absorbed at least one item (sanity).
	assert.Greater(t, enqueued, int64(0), "some writes should have enqueued")

	// 3. Overflow did happen — without overflow this test proves nothing.
	assert.Greater(t, dropped, int64(0), "drops must occur with capacity<<writes")

	// 4. Metric and callback agree on drop count.
	assert.EqualValues(t, dropCallbackCount.Load(), dropped,
		"OnReplayDropped callback count must equal IncReplayDropped metric")

	// 5. Full accounting: every write is either enqueued or dropped.
	//    This is the core "no silent loss" claim.
	assert.EqualValues(t, totalWrites, enqueued+dropped,
		"every write must be either enqueued or surfaced as dropped — no silent loss")
}

// alwaysFailSession is a cql.Session whose every Query.Exec returns the
// configured error.
type alwaysFailSession struct {
	err error
}

func newAlwaysFailSession(err error) cql.Session { return &alwaysFailSession{err: err} }

func (s *alwaysFailSession) Query(stmt string, values ...any) cql.Query {
	return &fixedErrQuery{stmt: stmt, values: values, err: s.err}
}

func (s *alwaysFailSession) Batch(_ cql.BatchType) cql.Batch {
	return &fixedErrBatch{err: s.err}
}

func (s *alwaysFailSession) Close() {}

// alwaysOKSession returns nil from Exec; everything else is a no-op.
type alwaysOKSession struct{}

func newAlwaysOKSession() cql.Session { return &alwaysOKSession{} }

func (s *alwaysOKSession) Query(stmt string, values ...any) cql.Query {
	return &fixedErrQuery{stmt: stmt, values: values, err: nil}
}

func (s *alwaysOKSession) Batch(_ cql.BatchType) cql.Batch {
	return &fixedErrBatch{err: nil}
}

func (s *alwaysOKSession) Close() {}

// fixedErrQuery and fixedErrBatch are minimal cql.Query/Batch impls that
// return a fixed error. Methods we don't exercise return zero values.
type fixedErrQuery struct {
	stmt   string
	values []any
	err    error
}

func (q *fixedErrQuery) Consistency(cql.Consistency) cql.Query       { return q }
func (q *fixedErrQuery) SerialConsistency(cql.Consistency) cql.Query { return q }
func (q *fixedErrQuery) PageSize(int) cql.Query                      { return q }
func (q *fixedErrQuery) PageState([]byte) cql.Query                  { return q }
func (q *fixedErrQuery) WithTimestamp(int64) cql.Query               { return q }
func (q *fixedErrQuery) Statement() string                           { return q.stmt }
func (q *fixedErrQuery) Values() []any                               { return q.values }
func (q *fixedErrQuery) Release()                                    {}
func (q *fixedErrQuery) Exec() error                                 { return q.err }
func (q *fixedErrQuery) ExecContext(context.Context) error           { return q.err }
func (q *fixedErrQuery) Scan(...any) error                           { return q.err }
func (q *fixedErrQuery) ScanContext(context.Context, ...any) error   { return q.err }
func (q *fixedErrQuery) MapScan(map[string]any) error                { return q.err }
func (q *fixedErrQuery) MapScanContext(context.Context, map[string]any) error {
	return q.err
}
func (q *fixedErrQuery) ScanCAS(...any) (bool, error)                         { return false, q.err }
func (q *fixedErrQuery) ScanCASContext(context.Context, ...any) (bool, error) { return false, q.err }
func (q *fixedErrQuery) MapScanCAS(map[string]any) (bool, error)              { return false, q.err }
func (q *fixedErrQuery) MapScanCASContext(context.Context, map[string]any) (bool, error) {
	return false, q.err
}
func (q *fixedErrQuery) Iter() cql.Iter                       { return &emptyIter{err: q.err} }
func (q *fixedErrQuery) IterContext(context.Context) cql.Iter { return &emptyIter{err: q.err} }

type fixedErrBatch struct {
	err  error
	stmt string
}

func (b *fixedErrBatch) Query(stmt string, _ ...any) cql.Batch {
	b.stmt = stmt
	return b
}
func (b *fixedErrBatch) Consistency(cql.Consistency) cql.Batch       { return b }
func (b *fixedErrBatch) SerialConsistency(cql.Consistency) cql.Batch { return b }
func (b *fixedErrBatch) WithTimestamp(int64) cql.Batch               { return b }
func (b *fixedErrBatch) Exec() error                                 { return b.err }
func (b *fixedErrBatch) ExecContext(context.Context) error           { return b.err }
func (b *fixedErrBatch) IterContext(context.Context) cql.Iter        { return &emptyIter{err: b.err} }
func (b *fixedErrBatch) ExecCAS(...any) (bool, cql.Iter, error)      { return false, nil, b.err }
func (b *fixedErrBatch) ExecCASContext(context.Context, ...any) (bool, cql.Iter, error) {
	return false, nil, b.err
}
func (b *fixedErrBatch) MapExecCAS(map[string]any) (bool, cql.Iter, error) {
	return false, nil, b.err
}
func (b *fixedErrBatch) MapExecCASContext(context.Context, map[string]any) (bool, cql.Iter, error) {
	return false, nil, b.err
}
func (b *fixedErrBatch) Size() int                    { return 0 }
func (b *fixedErrBatch) Statements() []cql.BatchEntry { return nil }

// emptyIter is a cql.Iter that returns no rows.
type emptyIter struct {
	err error
}

func (i *emptyIter) Scan(...any) bool            { return false }
func (i *emptyIter) Close() error                { return i.err }
func (i *emptyIter) MapScan(map[string]any) bool { return false }
func (i *emptyIter) SliceMap() ([]map[string]any, error) {
	return nil, i.err
}
func (i *emptyIter) PageState() []byte         { return nil }
func (i *emptyIter) NumRows() int              { return 0 }
func (i *emptyIter) Columns() []cql.ColumnInfo { return nil }
func (i *emptyIter) Scanner() cql.Scanner      { return nil }
func (i *emptyIter) Warnings() []string        { return nil }
