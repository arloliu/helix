package helix

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/policy"
	"github.com/stretchr/testify/require"
)

// gatedSession is a cql.Session that holds a write inside ExecContext until
// the test releases it, then records the byte arguments exactly as they are
// when the write executes — the moment a real driver marshals them.
type gatedSession struct {
	*recordingSession
	release     chan struct{}
	releaseOnce sync.Once
	done        chan struct{}
	doneOnce    sync.Once
	mu          sync.Mutex
	seen        [][]byte
}

type gatedQuery struct {
	recordingQuery
	gate *gatedSession
}

type gatedBatch struct {
	recordingBatch
	gate *gatedSession
}

func newGatedSession() *gatedSession {
	return &gatedSession{
		recordingSession: newRecordingSession(nil),
		release:          make(chan struct{}),
		done:             make(chan struct{}),
	}
}

func (g *gatedSession) Query(stmt string, values ...any) cql.Query {
	return &gatedQuery{recordingQuery: recordingQuery{session: g.recordingSession, stmt: stmt, values: values}, gate: g}
}

func (g *gatedSession) Batch(cql.BatchType) cql.Batch {
	return &gatedBatch{recordingBatch: recordingBatch{session: g.recordingSession}, gate: g}
}

// unblock lets a held write proceed.
// Safe to call more than once.
func (g *gatedSession) unblock() { g.releaseOnce.Do(func() { close(g.release) }) }

// record stores a nil-preserving copy of every byte argument in args.
func (g *gatedSession) record(args []any) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, a := range args {
		if b, ok := a.([]byte); ok {
			g.seen = append(g.seen, bytes.Clone(b))
		}
	}
}

// execute blocks until the test releases the gate, records args, then reports
// the write as finished.
func (g *gatedSession) execute(args []any) error {
	<-g.release
	g.record(args)
	g.doneOnce.Do(func() { close(g.done) })

	return nil
}

// byteArgs returns the byte arguments the session marshalled.
func (g *gatedSession) byteArgs(t *testing.T) [][]byte {
	t.Helper()
	select {
	case <-g.done:
	case <-time.After(regressionWaitTimeout):
		t.Fatal("timed out waiting for the cluster B write leg to execute")
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.seen
}

func (q *gatedQuery) Consistency(cql.Consistency) cql.Query       { return q }
func (q *gatedQuery) SerialConsistency(cql.Consistency) cql.Query { return q }
func (q *gatedQuery) PageSize(int) cql.Query                      { return q }
func (q *gatedQuery) PageState([]byte) cql.Query                  { return q }
func (q *gatedQuery) WithTimestamp(int64) cql.Query               { return q }
func (q *gatedQuery) Exec() error                                 { return q.gate.execute(q.values) }
func (q *gatedQuery) ExecContext(context.Context) error           { return q.gate.execute(q.values) }

func (b *gatedBatch) Query(stmt string, args ...any) cql.Batch {
	b.entries = append(b.entries, cql.BatchEntry{Statement: stmt, Args: args})
	return b
}
func (b *gatedBatch) Consistency(cql.Consistency) cql.Batch       { return b }
func (b *gatedBatch) SerialConsistency(cql.Consistency) cql.Batch { return b }
func (b *gatedBatch) WithTimestamp(int64) cql.Batch               { return b }
func (b *gatedBatch) Exec() error                                 { return b.ExecContext(context.Background()) }
func (b *gatedBatch) ExecContext(context.Context) error {
	args := make([]any, 0, len(b.entries))
	for _, e := range b.entries {
		args = append(args, e.Args...)
	}

	return b.gate.execute(args)
}

// newBackgroundLegClient returns a dual client whose cluster-B leg is run in
// the background by AdaptiveDualWrite's fire-and-forget path and blocks inside
// the returned session until the test releases it.
func newBackgroundLegClient(t *testing.T) (*CQLClient, *gatedSession) {
	t.Helper()

	sessionB := newGatedSession()
	adaptive := policy.NewAdaptiveDualWrite()
	adaptive.ForceDegrade(ClusterB)

	client, err := NewCQLClient(newRecordingSession(nil), sessionB, WithWriteStrategy(adaptive))
	require.NoError(t, err)
	t.Cleanup(func() {
		sessionB.unblock()
		client.Close()
	})

	return client, sessionB
}

// TestQueryExec_BackgroundLegKeepsTheByteArgumentsTheCallerPassed pins that a
// write leg still running after Exec returned marshals the bytes the caller
// passed, not whatever the caller wrote into its buffer afterwards.
func TestQueryExec_BackgroundLegKeepsTheByteArgumentsTheCallerPassed(t *testing.T) {
	client, sessionB := newBackgroundLegClient(t)

	blob := []byte("original")
	err := client.Query("INSERT INTO t (k, v) VALUES (?, ?)", "k", blob).ExecContext(t.Context())
	require.NoError(t, err)

	// The caller is free to reuse its buffer the moment Exec returns.
	copy(blob, "REWRITE!")
	sessionB.unblock()

	require.Equal(t, [][]byte{[]byte("original")}, sessionB.byteArgs(t))
}

// TestBatchExec_BackgroundLegKeepsTheByteArgumentsTheCallerPassed is the batch
// twin of TestQueryExec_BackgroundLegKeepsTheByteArgumentsTheCallerPassed.
func TestBatchExec_BackgroundLegKeepsTheByteArgumentsTheCallerPassed(t *testing.T) {
	client, sessionB := newBackgroundLegClient(t)

	blob := []byte("original")
	err := client.Batch(LoggedBatch).
		Query("INSERT INTO t (k, v) VALUES (?, ?)", "k", blob).
		ExecContext(t.Context())
	require.NoError(t, err)

	copy(blob, "REWRITE!")
	sessionB.unblock()

	require.Equal(t, [][]byte{[]byte("original")}, sessionB.byteArgs(t))
}

// TestWrite_NilByteArgumentStaysNil pins that copying the caller's byte
// arguments keeps a nil []byte nil, so a NULL value is never turned into an
// empty one on its way to the cluster.
func TestWrite_NilByteArgumentStaysNil(t *testing.T) {
	client, sessionB := newBackgroundLegClient(t)

	// The second byte argument holds content, so the nil one travels through
	// the copy the write path makes rather than past it.
	err := client.Query("INSERT INTO t (k, a, b) VALUES (?, ?, ?)", "k", []byte(nil), []byte("x")).
		ExecContext(t.Context())
	require.NoError(t, err)

	sessionB.unblock()

	args := sessionB.byteArgs(t)
	require.Len(t, args, 2)
	require.Nil(t, args[0], "a nil byte argument must not become an empty one")
	require.Equal(t, []byte("x"), args[1])
}
