package chaos_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/test/simulation/chaos"
	"github.com/arloliu/helix/types"
)

// ---------------------------------------------------------------------------
// Minimal stubs — just enough to satisfy the cql.Session/Query/Batch/Iter
// interfaces without pulling in a real database.
// ---------------------------------------------------------------------------

type stubSession struct {
	closed  bool
	execErr error
	scanErr error
}

func (s *stubSession) Query(_ string, _ ...any) cql.Query {
	return &stubQuery{execErr: s.execErr, scanErr: s.scanErr}
}
func (s *stubSession) Batch(_ cql.BatchType) cql.Batch { return &stubBatch{execErr: s.execErr} }
func (s *stubSession) Close()                          { s.closed = true }

type stubQuery struct {
	execErr error
	scanErr error
}

func (q *stubQuery) Consistency(c cql.Consistency) cql.Query              { return q }
func (q *stubQuery) PageSize(int) cql.Query                               { return q }
func (q *stubQuery) PageState([]byte) cql.Query                           { return q }
func (q *stubQuery) WithTimestamp(int64) cql.Query                        { return q }
func (q *stubQuery) SerialConsistency(cql.Consistency) cql.Query          { return q }
func (q *stubQuery) Statement() string                                    { return "" }
func (q *stubQuery) Values() []any                                        { return nil }
func (q *stubQuery) Release()                                             {}
func (q *stubQuery) Exec() error                                          { return q.execErr }
func (q *stubQuery) ExecContext(context.Context) error                    { return q.execErr }
func (q *stubQuery) Scan(...any) error                                    { return q.scanErr }
func (q *stubQuery) ScanContext(context.Context, ...any) error            { return q.scanErr }
func (q *stubQuery) MapScan(map[string]any) error                         { return q.execErr }
func (q *stubQuery) MapScanContext(context.Context, map[string]any) error { return q.execErr }
func (q *stubQuery) Iter() cql.Iter                                       { return &stubIter{err: q.execErr} }
func (q *stubQuery) IterContext(context.Context) cql.Iter                 { return &stubIter{err: q.execErr} }
func (q *stubQuery) ScanCAS(...any) (bool, error)                         { return true, q.execErr }
func (q *stubQuery) ScanCASContext(context.Context, ...any) (bool, error) {
	return true, q.execErr
}
func (q *stubQuery) MapScanCAS(map[string]any) (bool, error) { return true, q.execErr }
func (q *stubQuery) MapScanCASContext(context.Context, map[string]any) (bool, error) {
	return true, q.execErr
}

type stubBatch struct{ execErr error }

func (b *stubBatch) Query(string, ...any) cql.Batch              { return b }
func (b *stubBatch) Consistency(cql.Consistency) cql.Batch       { return b }
func (b *stubBatch) WithTimestamp(int64) cql.Batch               { return b }
func (b *stubBatch) SerialConsistency(cql.Consistency) cql.Batch { return b }
func (b *stubBatch) Exec() error                                 { return b.execErr }
func (b *stubBatch) ExecContext(context.Context) error           { return b.execErr }
func (b *stubBatch) IterContext(context.Context) cql.Iter        { return &stubIter{} }
func (b *stubBatch) ExecCAS(...any) (bool, cql.Iter, error)      { return true, &stubIter{}, b.execErr }
func (b *stubBatch) ExecCASContext(context.Context, ...any) (bool, cql.Iter, error) {
	return true, &stubIter{}, b.execErr
}
func (b *stubBatch) MapExecCAS(map[string]any) (bool, cql.Iter, error) {
	return true, &stubIter{}, b.execErr
}
func (b *stubBatch) MapExecCASContext(context.Context, map[string]any) (bool, cql.Iter, error) {
	return true, &stubIter{}, b.execErr
}
func (b *stubBatch) Size() int                    { return 0 }
func (b *stubBatch) Statements() []cql.BatchEntry { return nil }

type stubIter struct{ err error }

func (i *stubIter) Scan(...any) bool                    { return false }
func (i *stubIter) MapScan(map[string]any) bool         { return false }
func (i *stubIter) Close() error                        { return i.err }
func (i *stubIter) SliceMap() ([]map[string]any, error) { return nil, i.err }
func (i *stubIter) PageState() []byte                   { return nil }
func (i *stubIter) NumRows() int                        { return 0 }
func (i *stubIter) Columns() []cql.ColumnInfo           { return nil }
func (i *stubIter) Scanner() cql.Scanner                { return nil }
func (i *stubIter) Warnings() []string                  { return nil }

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Close must not propagate to the underlying session.
// The underlying gocql session is owned by testutil.CQLCluster; chaos sessions
// are reused across multiple helix client instances during strategy groups.
func TestSession_Close_IsNoOp(t *testing.T) {
	stub := &stubSession{}
	s := chaos.NewSession(stub)
	s.Close()
	if stub.closed {
		t.Fatal("Close() must not propagate to the underlying session")
	}
}

// Exec increments execCount on success and leaves scanCount/dropCount at zero.
func TestSession_Counters_ExecIncrementOnSuccess(t *testing.T) {
	s := chaos.NewSession(&stubSession{})

	if err := s.Query("INSERT INTO t (id) VALUES (?)", 1).Exec(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	exec, scan, drop := s.Counters()
	if exec != 1 {
		t.Errorf("exec = %d, want 1", exec)
	}
	if scan != 0 || drop != 0 {
		t.Errorf("scan=%d drop=%d, want 0 for both", scan, drop)
	}
}

// Exec must NOT increment execCount when the underlying query returns an error.
func TestSession_Counters_ExecNoIncrementOnError(t *testing.T) {
	stub := &stubSession{execErr: errors.New("db error")}
	s := chaos.NewSession(stub)

	_ = s.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()

	exec, _, _ := s.Counters()
	if exec != 0 {
		t.Errorf("exec = %d, want 0 (should not count on underlying error)", exec)
	}
}

// Scan increments scanCount on success.
func TestSession_Counters_ScanIncrementOnSuccess(t *testing.T) {
	s := chaos.NewSession(&stubSession{})

	var id string
	_ = s.Query("SELECT id FROM t WHERE id = ?", "x").Scan(&id)

	_, scan, _ := s.Counters()
	if scan != 1 {
		t.Errorf("scan = %d, want 1", scan)
	}
}

// Scan must NOT increment scanCount when it returns an error.
func TestSession_Counters_ScanNoIncrementOnError(t *testing.T) {
	stub := &stubSession{scanErr: errors.New("not found")}
	s := chaos.NewSession(stub)

	var id string
	_ = s.Query("SELECT id FROM t WHERE id = ?", "x").Scan(&id)

	_, scan, _ := s.Counters()
	if scan != 0 {
		t.Errorf("scan = %d, want 0 on error", scan)
	}
}

// ResetCounters zeroes all three counters.
func TestSession_ResetCounters(t *testing.T) {
	s := chaos.NewSession(&stubSession{})

	_ = s.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	var id string
	_ = s.Query("SELECT id FROM t WHERE id = ?", "x").Scan(&id)

	s.SetErrorRate(1.0)
	_ = s.Query("INSERT INTO t (id) VALUES (?)", 2).Exec() // drop
	s.SetConfig(chaos.SessionConfig{})

	s.ResetCounters()

	exec, scan, drop := s.Counters()
	if exec != 0 || scan != 0 || drop != 0 {
		t.Errorf("after ResetCounters: exec=%d scan=%d drop=%d, all want 0", exec, scan, drop)
	}
}

// SetErrorRate(1.0) must always return ErrWriteDropped and increment dropCount.
func TestSession_DropRate_100Percent(t *testing.T) {
	s := chaos.NewSession(&stubSession{})
	s.SetErrorRate(1.0)

	err := s.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	if err == nil {
		t.Fatal("expected error from 100% drop rate, got nil")
	}
	if !errors.Is(err, types.ErrWriteDropped) {
		t.Errorf("expected ErrWriteDropped, got %v", err)
	}

	exec, _, drop := s.Counters()
	if drop != 1 {
		t.Errorf("drop = %d, want 1", drop)
	}
	if exec != 0 {
		t.Errorf("exec = %d, want 0 (drop must not count as exec)", exec)
	}
}

// SetErrorRate(0.0) must never inject an error.
func TestSession_DropRate_Zero(t *testing.T) {
	s := chaos.NewSession(&stubSession{})
	s.SetErrorRate(0.0)

	for i := range 100 {
		if err := s.Query("INSERT INTO t (id) VALUES (?)", i).Exec(); err != nil {
			t.Fatalf("expected nil error at 0%% drop rate, got %v", err)
		}
	}
}

// SetErrorRate(0.5) should produce a drop rate within ±10 percentage points of 50%.
func TestSession_DropRate_Statistical(t *testing.T) {
	s := chaos.NewSession(&stubSession{})
	s.SetErrorRate(0.5)

	const n = 2000
	var dropped int
	for i := range n {
		if err := s.Query("INSERT INTO t (id) VALUES (?)", i).Exec(); err != nil {
			dropped++
		}
	}

	ratio := float64(dropped) / float64(n)
	if ratio < 0.40 || ratio > 0.60 {
		t.Errorf("drop ratio = %.3f, want 0.40–0.60 (50%% ±10%%)", ratio)
	}
}

// SetLatency must delay operations by approximately the configured duration.
func TestSession_SetLatency(t *testing.T) {
	s := chaos.NewSession(&stubSession{})
	s.SetLatency(50 * time.Millisecond)

	start := time.Now()
	_ = s.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	elapsed := time.Since(start)

	if elapsed < 40*time.Millisecond {
		t.Errorf("elapsed = %v, want >= 40ms (latency injection of 50ms)", elapsed)
	}
}

// A custom ErrorFunc must be returned as the query error, and dropCount must increase.
func TestSession_ErrorFunc(t *testing.T) {
	sentinel := errors.New("custom chaos error")
	s := chaos.NewSession(&stubSession{})
	s.SetConfig(chaos.SessionConfig{
		ErrorFunc: func() error { return sentinel },
	})

	err := s.Query("INSERT INTO t (id) VALUES (?)", 1).Exec()
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}

	_, _, drop := s.Counters()
	if drop != 1 {
		t.Errorf("drop = %d, want 1 (ErrorFunc result counts as a drop)", drop)
	}
}

// Clearing the config via SetConfig({}) must remove all injected chaos.
func TestSession_SetConfig_ClearRestoresNormalBehavior(t *testing.T) {
	s := chaos.NewSession(&stubSession{})

	// Enable full drop rate.
	s.SetErrorRate(1.0)
	if err := s.Query("INSERT INTO t (id) VALUES (?)", 1).Exec(); err == nil {
		t.Fatal("expected error after SetErrorRate(1.0)")
	}

	// Clear chaos — subsequent calls must succeed.
	s.SetConfig(chaos.SessionConfig{})
	if err := s.Query("INSERT INTO t (id) VALUES (?)", 2).Exec(); err != nil {
		t.Fatalf("unexpected error after clearing chaos: %v", err)
	}
}

// Iter() with active drop rate must return an ErrorIter whose Close() returns an error.
func TestSession_Iter_ReturnsErrorIterOnDrop(t *testing.T) {
	s := chaos.NewSession(&stubSession{})
	s.SetErrorRate(1.0)

	iter := s.Query("SELECT id FROM t").Iter()
	if iter.Scan() {
		t.Fatal("ErrorIter.Scan() should return false")
	}
	if err := iter.Close(); err == nil {
		t.Fatal("ErrorIter.Close() should return error")
	}
}

// Batch.Exec with full drop rate must return an error and increment dropCount.
func TestSession_Batch_DropRateReturnsError(t *testing.T) {
	s := chaos.NewSession(&stubSession{})
	s.SetErrorRate(1.0)

	b := s.Batch(cql.LoggedBatch)
	b = b.Query("INSERT INTO t (id) VALUES (?)", 1)
	if err := b.Exec(); err == nil {
		t.Fatal("expected error from 100% drop rate on batch Exec()")
	}

	_, _, drop := s.Counters()
	if drop != 1 {
		t.Errorf("drop = %d, want 1", drop)
	}
}

// Batch.Exec must increment execCount on success.
func TestSession_Batch_ExecIncrementOnSuccess(t *testing.T) {
	s := chaos.NewSession(&stubSession{})

	b := s.Batch(cql.LoggedBatch)
	b = b.Query("INSERT INTO t (id) VALUES (?)", 1)
	if err := b.Exec(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	exec, _, _ := s.Counters()
	if exec != 1 {
		t.Errorf("exec = %d, want 1", exec)
	}
}

// Multiple Exec calls must accumulate counters correctly.
func TestSession_Counters_AccumulateAcrossMultipleCalls(t *testing.T) {
	s := chaos.NewSession(&stubSession{})

	for i := range 5 {
		_ = s.Query("INSERT INTO t (id) VALUES (?)", i).Exec()
	}

	exec, _, _ := s.Counters()
	if exec != 5 {
		t.Errorf("exec = %d, want 5", exec)
	}
}
