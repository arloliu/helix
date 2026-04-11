package workload_test

import (
	"context"
	"errors"
	"testing"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/test/simulation/workload"
	"github.com/gocql/gocql"
)

// ---------------------------------------------------------------------------
// Minimal stubs for cql.Session / cql.Query / cql.Iter
// ---------------------------------------------------------------------------

// scanSession returns scanErr on every Scan call.
type scanSession struct{ scanErr error }

func (s *scanSession) Query(_ string, _ ...any) cql.Query {
	return &scanQuery{scanErr: s.scanErr}
}
func (s *scanSession) Batch(_ cql.BatchType) cql.Batch { return &nopBatch{} }
func (s *scanSession) Close()                          {}

type scanQuery struct{ scanErr error }

func (q *scanQuery) Consistency(cql.Consistency) cql.Query                { return q }
func (q *scanQuery) PageSize(int) cql.Query                               { return q }
func (q *scanQuery) PageState([]byte) cql.Query                           { return q }
func (q *scanQuery) WithTimestamp(int64) cql.Query                        { return q }
func (q *scanQuery) SerialConsistency(cql.Consistency) cql.Query          { return q }
func (q *scanQuery) Statement() string                                    { return "" }
func (q *scanQuery) Values() []any                                        { return nil }
func (q *scanQuery) Release()                                             {}
func (q *scanQuery) Exec() error                                          { return nil }
func (q *scanQuery) ExecContext(context.Context) error                    { return nil }
func (q *scanQuery) Scan(...any) error                                    { return q.scanErr }
func (q *scanQuery) ScanContext(context.Context, ...any) error            { return q.scanErr }
func (q *scanQuery) MapScan(map[string]any) error                         { return nil }
func (q *scanQuery) MapScanContext(context.Context, map[string]any) error { return nil }
func (q *scanQuery) Iter() cql.Iter                                       { return &nopIter{} }
func (q *scanQuery) IterContext(context.Context) cql.Iter                 { return &nopIter{} }
func (q *scanQuery) ScanCAS(...any) (bool, error)                         { return true, nil }
func (q *scanQuery) ScanCASContext(context.Context, ...any) (bool, error) { return true, nil }
func (q *scanQuery) MapScanCAS(map[string]any) (bool, error)              { return true, nil }
func (q *scanQuery) MapScanCASContext(context.Context, map[string]any) (bool, error) {
	return true, nil
}

type nopBatch struct{}

func (b *nopBatch) Query(string, ...any) cql.Batch              { return b }
func (b *nopBatch) Consistency(cql.Consistency) cql.Batch       { return b }
func (b *nopBatch) WithTimestamp(int64) cql.Batch               { return b }
func (b *nopBatch) SerialConsistency(cql.Consistency) cql.Batch { return b }
func (b *nopBatch) Exec() error                                 { return nil }
func (b *nopBatch) ExecContext(context.Context) error           { return nil }
func (b *nopBatch) IterContext(context.Context) cql.Iter        { return &nopIter{} }
func (b *nopBatch) ExecCAS(...any) (bool, cql.Iter, error)      { return true, &nopIter{}, nil }
func (b *nopBatch) ExecCASContext(context.Context, ...any) (bool, cql.Iter, error) {
	return true, &nopIter{}, nil
}
func (b *nopBatch) MapExecCAS(map[string]any) (bool, cql.Iter, error) {
	return true, &nopIter{}, nil
}
func (b *nopBatch) MapExecCASContext(context.Context, map[string]any) (bool, cql.Iter, error) {
	return true, &nopIter{}, nil
}
func (b *nopBatch) Size() int                    { return 0 }
func (b *nopBatch) Statements() []cql.BatchEntry { return nil }

type nopIter struct{}

func (i *nopIter) Scan(...any) bool                    { return false }
func (i *nopIter) MapScan(map[string]any) bool         { return false }
func (i *nopIter) Close() error                        { return nil }
func (i *nopIter) SliceMap() ([]map[string]any, error) { return nil, nil }
func (i *nopIter) PageState() []byte                   { return nil }
func (i *nopIter) NumRows() int                        { return 0 }
func (i *nopIter) Columns() []cql.ColumnInfo           { return nil }
func (i *nopIter) Scanner() cql.Scanner                { return nil }
func (i *nopIter) Warnings() []string                  { return nil }

// ---------------------------------------------------------------------------
// WorkloadStats tests
// ---------------------------------------------------------------------------

// Reset must zero all seven atomic counters.
func TestWorkloadStats_Reset_ZerosAllCounters(t *testing.T) {
	s := workload.NewWorkloadStats()
	s.WriteOK.Add(10)
	s.WriteAsync.Add(3)
	s.WriteDropped.Add(2)
	s.WriteFailed.Add(1)
	s.DualClusterErr.Add(4)
	s.ReadOK.Add(20)
	s.ReadFailed.Add(5)

	s.Reset()

	fields := map[string]int64{
		"WriteOK":        s.WriteOK.Load(),
		"WriteAsync":     s.WriteAsync.Load(),
		"WriteDropped":   s.WriteDropped.Load(),
		"WriteFailed":    s.WriteFailed.Load(),
		"DualClusterErr": s.DualClusterErr.Load(),
		"ReadOK":         s.ReadOK.Load(),
		"ReadFailed":     s.ReadFailed.Load(),
	}
	for name, v := range fields {
		if v != 0 {
			t.Errorf("%s = %d after Reset(), want 0", name, v)
		}
	}
}

// ---------------------------------------------------------------------------
// WriteTracker tests
// ---------------------------------------------------------------------------

// TrackWrite must record the key; Count must reflect it.
func TestWriteTracker_TrackWrite_Count(t *testing.T) {
	tr := workload.NewWriteTracker()

	if tr.Count() != 0 {
		t.Fatalf("Count() = %d before any writes, want 0", tr.Count())
	}

	key := gocql.TimeUUID()
	tr.TrackWrite(key, 1234567890)

	if tr.Count() != 1 {
		t.Errorf("Count() = %d after one TrackWrite, want 1", tr.Count())
	}
}

// Multiple TrackWrite calls must accumulate without duplicates for distinct keys.
func TestWriteTracker_TrackWrite_MultipleKeys(t *testing.T) {
	tr := workload.NewWriteTracker()

	for range 10 {
		tr.TrackWrite(gocql.TimeUUID(), 0)
	}
	if tr.Count() != 10 {
		t.Errorf("Count() = %d, want 10", tr.Count())
	}
}

// TrackWrite for the same key twice must not increase Count.
func TestWriteTracker_TrackWrite_Idempotent(t *testing.T) {
	tr := workload.NewWriteTracker()
	key := gocql.TimeUUID()

	tr.TrackWrite(key, 1)
	tr.TrackWrite(key, 2) // overwrite same key

	if tr.Count() != 1 {
		t.Errorf("Count() = %d after two writes for same key, want 1", tr.Count())
	}
}

// RandomKey on an empty tracker must return the zero UUID.
func TestWriteTracker_RandomKey_EmptyReturnsZeroUUID(t *testing.T) {
	tr := workload.NewWriteTracker()
	zero := gocql.UUID{}

	if tr.RandomKey() != zero {
		t.Error("RandomKey() on empty tracker returned non-zero UUID")
	}
}

// RandomKey on a non-empty tracker must return one of the tracked keys.
func TestWriteTracker_RandomKey_ReturnsTrackedKey(t *testing.T) {
	tr := workload.NewWriteTracker()
	key := gocql.TimeUUID()
	tr.TrackWrite(key, 0)

	got := tr.RandomKey()
	if got != key {
		t.Errorf("RandomKey() = %v, want %v", got, key)
	}
}

// VerifyConsistency must return nil when both sessions find every key.
func TestWriteTracker_VerifyConsistency_AllPresent(t *testing.T) {
	tr := workload.NewWriteTracker()
	for range 5 {
		tr.TrackWrite(gocql.TimeUUID(), 0)
	}

	// Scan succeeds (nil error) → rowExists returns true for every key.
	good := &scanSession{scanErr: nil}
	if err := tr.VerifyConsistency(good, good); err != nil {
		t.Errorf("VerifyConsistency() error = %v, want nil", err)
	}
}

// VerifyConsistency must return an error when session A reports missing rows.
func TestWriteTracker_VerifyConsistency_MissingInA(t *testing.T) {
	tr := workload.NewWriteTracker()
	tr.TrackWrite(gocql.TimeUUID(), 0)

	// Session A scans fail → rowExists returns false.
	bad := &scanSession{scanErr: errors.New("not found")}
	good := &scanSession{scanErr: nil}

	err := tr.VerifyConsistency(bad, good)
	if err == nil {
		t.Fatal("VerifyConsistency() expected error for missing keys in A, got nil")
	}
}

// VerifyConsistency must return an error when session B reports missing rows.
func TestWriteTracker_VerifyConsistency_MissingInB(t *testing.T) {
	tr := workload.NewWriteTracker()
	tr.TrackWrite(gocql.TimeUUID(), 0)

	good := &scanSession{scanErr: nil}
	bad := &scanSession{scanErr: errors.New("not found")}

	err := tr.VerifyConsistency(good, bad)
	if err == nil {
		t.Fatal("VerifyConsistency() expected error for missing keys in B, got nil")
	}
}

// VerifyConsistency on an empty tracker must return nil (nothing to verify).
func TestWriteTracker_VerifyConsistency_Empty(t *testing.T) {
	tr := workload.NewWriteTracker()
	good := &scanSession{}
	if err := tr.VerifyConsistency(good, good); err != nil {
		t.Errorf("VerifyConsistency() on empty tracker = %v, want nil", err)
	}
}
