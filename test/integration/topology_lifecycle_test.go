package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/adapter/cql"
)

// mockWatcher implements helix.TopologyWatcher and allows verifying context cancellation.
type mockWatcher struct {
	updates      chan helix.TopologyUpdate
	ctxCancelled chan struct{}
}

func newMockWatcher() *mockWatcher {
	return &mockWatcher{
		updates:      make(chan helix.TopologyUpdate),
		ctxCancelled: make(chan struct{}),
	}
}

func (m *mockWatcher) Watch(ctx context.Context) <-chan helix.TopologyUpdate {
	go func() {
		<-ctx.Done()
		close(m.ctxCancelled)
	}()
	return m.updates
}

// Minimal mock implementation of cql.Session
type mockSession struct{}

func (m *mockSession) Query(stmt string, values ...any) cql.Query { return &mockQuery{} }
func (m *mockSession) Batch(kind cql.BatchType) cql.Batch       { return &mockBatch{} }
func (m *mockSession) NewBatch(kind cql.BatchType) cql.Batch    { return &mockBatch{} }
func (m *mockSession) ExecuteBatch(batch cql.Batch) error       { return nil }
func (m *mockSession) ExecuteBatchCAS(batch cql.Batch, dest ...any) (bool, cql.Iter, error) {
	return true, nil, nil
}
func (m *mockSession) MapExecuteBatchCAS(batch cql.Batch, dest map[string]any) (bool, cql.Iter, error) {
	return true, nil, nil
}
func (m *mockSession) Close() {}

type mockQuery struct{}

func (q *mockQuery) Exec() error                                                    { return nil }
func (q *mockQuery) ExecContext(ctx context.Context) error                          { return nil }
func (q *mockQuery) Scan(dest ...any) error                                         { return nil }
func (q *mockQuery) ScanContext(ctx context.Context, dest ...any) error             { return nil }
func (q *mockQuery) Iter() cql.Iter                                                 { return &mockIter{} }
func (q *mockQuery) IterContext(ctx context.Context) cql.Iter                       { return &mockIter{} }
func (q *mockQuery) MapScan(m map[string]any) error                                 { return nil }
func (q *mockQuery) MapScanContext(ctx context.Context, m map[string]any) error     { return nil }
func (q *mockQuery) ScanCAS(dest ...any) (bool, error)                              { return true, nil }
func (q *mockQuery) ScanCASContext(ctx context.Context, dest ...any) (bool, error)  { return true, nil }
func (q *mockQuery) MapScanCAS(dest map[string]any) (bool, error)                   { return true, nil }
func (q *mockQuery) MapScanCASContext(ctx context.Context, dest map[string]any) (bool, error) {
	return true, nil
}
func (q *mockQuery) WithContext(ctx context.Context) cql.Query         { return q }
func (q *mockQuery) Consistency(c cql.Consistency) cql.Query           { return q }
func (q *mockQuery) SetConsistency(c cql.Consistency)                  {}
func (q *mockQuery) SerialConsistency(c cql.Consistency) cql.Query     { return q }
func (q *mockQuery) PageSize(n int) cql.Query                          { return q }
func (q *mockQuery) PageState(state []byte) cql.Query                  { return q }
func (q *mockQuery) WithTimestamp(ts int64) cql.Query                  { return q }
func (q *mockQuery) Statement() string                                 { return "" }
func (q *mockQuery) Values() []any                                     { return nil }
func (q *mockQuery) Release()                                          {}

type mockBatch struct{}

func (b *mockBatch) Query(stmt string, args ...any) cql.Batch          { return b }
func (b *mockBatch) WithContext(ctx context.Context) cql.Batch         { return b }
func (b *mockBatch) WithTimestamp(ts int64) cql.Batch                  { return b }
func (b *mockBatch) Consistency(c cql.Consistency) cql.Batch           { return b }
func (b *mockBatch) SetConsistency(c cql.Consistency)                  {}
func (b *mockBatch) SerialConsistency(c cql.Consistency) cql.Batch     { return b }
func (b *mockBatch) Exec() error                                       { return nil }
func (b *mockBatch) ExecContext(ctx context.Context) error             { return nil }
func (b *mockBatch) ExecCAS(dest ...any) (bool, cql.Iter, error)       { return true, nil, nil }
func (b *mockBatch) ExecCASContext(ctx context.Context, dest ...any) (bool, cql.Iter, error) {
	return true, nil, nil
}
func (b *mockBatch) MapExecCAS(dest map[string]any) (bool, cql.Iter, error) { return true, nil, nil }
func (b *mockBatch) MapExecCASContext(ctx context.Context, dest map[string]any) (bool, cql.Iter, error) {
	return true, nil, nil
}
func (b *mockBatch) IterContext(ctx context.Context) cql.Iter { return &mockIter{} }
func (b *mockBatch) Size() int                                { return 0 }
func (b *mockBatch) Statements() []cql.BatchEntry             { return nil }

type mockIter struct{}

func (i *mockIter) Scan(dest ...any) bool                  { return false }
func (i *mockIter) Close() error                           { return nil }
func (i *mockIter) MapScan(m map[string]any) bool          { return false }
func (i *mockIter) SliceMap() ([]map[string]any, error)    { return nil, nil }
func (i *mockIter) PageState() []byte                      { return nil }
func (i *mockIter) NumRows() int                           { return 0 }
func (i *mockIter) Columns() []cql.ColumnInfo              { return nil }
func (i *mockIter) Scanner() cql.Scanner                   { return &mockScanner{} }
func (i *mockIter) Warnings() []string                     { return nil }

type mockScanner struct{}

func (s *mockScanner) Next() bool             { return false }
func (s *mockScanner) Scan(dest ...any) error { return nil }
func (s *mockScanner) Err() error             { return nil }

func TestTopologyContextLifecycle(t *testing.T) {
	t.Run("With TopologyWatcher", func(t *testing.T) {
		watcher := newMockWatcher()
		sessionA := &mockSession{}
		sessionB := &mockSession{}

		client, err := helix.NewCQLClient(sessionA, sessionB,
			helix.WithTopologyWatcher(watcher),
		)
		require.NoError(t, err)

		// Verify watcher is active (context not cancelled yet)
		select {
		case <-watcher.ctxCancelled:
			t.Fatal("Topology context cancelled prematurely")
		default:
			// OK
		}

		// Close client
		client.Close()

		// Verify context is cancelled
		select {
		case <-watcher.ctxCancelled:
			// OK
		case <-time.After(1 * time.Second):
			t.Fatal("Topology context not cancelled after Close()")
		}
	})

	t.Run("Without TopologyWatcher", func(t *testing.T) {
		sessionA := &mockSession{}
		sessionB := &mockSession{}

		client, err := helix.NewCQLClient(sessionA, sessionB)
		require.NoError(t, err)

		// Just ensure it doesn't crash on Close
		client.Close()
	})
}
