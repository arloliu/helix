package v2_test

import (
	"errors"
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/adapter/cql"
	v2 "github.com/arloliu/helix/adapter/cql/v2" //nolint:revive // required for v2_test package
	"github.com/arloliu/helix/types"
)

// TestSessionImplementsInterface verifies that v2.Session implements cql.Session.
func TestSessionImplementsInterface(t *testing.T) {
	// This is a compile-time check
	var _ cql.Session = (*v2.Session)(nil)
}

// TestQueryImplementsInterface verifies that v2.Query implements cql.Query.
func TestQueryImplementsInterface(t *testing.T) {
	// This is a compile-time check
	var _ cql.Query = (*v2.Query)(nil)
}

// TestBatchImplementsInterface verifies that v2.Batch implements cql.Batch.
func TestBatchImplementsInterface(t *testing.T) {
	// This is a compile-time check
	var _ cql.Batch = (*v2.Batch)(nil)
}

// TestIterImplementsInterface verifies that v2.Iter implements cql.Iter.
func TestIterImplementsInterface(t *testing.T) {
	// This is a compile-time check
	var _ cql.Iter = (*v2.Iter)(nil)
}

// TestNewSessionNilPanics verifies that NewSession panics on nil input.
func TestNewSessionNilPanics(t *testing.T) {
	require.PanicsWithValue(t, "cql/v2: NewSession called with nil gocql.Session", func() {
		v2.NewSession(nil)
	})
}

// TestWrapSessionNilPanics verifies that WrapSession panics on nil input.
func TestWrapSessionNilPanics(t *testing.T) {
	require.PanicsWithValue(t, "cql/v2: NewSession called with nil gocql.Session", func() {
		v2.WrapSession(nil)
	})
}

// The following tests require a real gocql.Session and are run as integration tests.
// See test/integration/cql_v2_adapter_test.go for those tests.

// TestBatchTypeConstants verifies batch type constants match gocql v2.
func TestBatchTypeConstants(t *testing.T) {
	require.Equal(t, cql.BatchType(gocql.LoggedBatch), cql.LoggedBatch)
	require.Equal(t, cql.BatchType(gocql.UnloggedBatch), cql.UnloggedBatch)
	require.Equal(t, cql.BatchType(gocql.CounterBatch), cql.CounterBatch)
}

// TestConsistencyConstants verifies consistency constants match gocql v2.
func TestConsistencyConstants(t *testing.T) {
	require.Equal(t, cql.Consistency(gocql.Any), cql.Any)
	require.Equal(t, cql.Consistency(gocql.One), cql.One)
	require.Equal(t, cql.Consistency(gocql.Two), cql.Two)
	require.Equal(t, cql.Consistency(gocql.Three), cql.Three)
	require.Equal(t, cql.Consistency(gocql.Quorum), cql.Quorum)
	require.Equal(t, cql.Consistency(gocql.All), cql.All)
	require.Equal(t, cql.Consistency(gocql.LocalQuorum), cql.LocalQuorum)
	require.Equal(t, cql.Consistency(gocql.EachQuorum), cql.EachQuorum)
	require.Equal(t, cql.Consistency(gocql.LocalOne), cql.LocalOne)
}

// TestQueryMethodsExist verifies all Query interface methods exist on v2.Query.
// This is a compile-time verification that v2.Query has all the new context methods.
func TestQueryMethodsExist(t *testing.T) {
	// Create a typed nil to verify method signatures exist
	var q *v2.Query

	// Verify context methods exist with correct signatures
	// These are compile-time checks - if any method is missing, this won't compile
	_ = func() {
		_ = q.ExecContext
		_ = q.ScanContext
		_ = q.IterContext
		_ = q.MapScanContext
		_ = q.ScanCAS
		_ = q.ScanCASContext
		_ = q.MapScanCAS
		_ = q.MapScanCASContext
	}
}

// TestNotFoundMapping verifies the error mapping intent: gocql.ErrNotFound from
// Scan/MapScan/ScanContext/MapScanContext is surfaced as types.ErrNotFound.
//
// Full round-trip testing (gocql query returning ErrNotFound) requires a live
// Cassandra session — see test/integration/ for those tests.
func TestNotFoundMapping(t *testing.T) {
	// gocql.ErrNotFound is a real sentinel error
	require.Error(t, gocql.ErrNotFound)

	// types.ErrNotFound is the Helix sentinel we map to
	require.True(t, types.IsNotFound(types.ErrNotFound))

	// They must be distinct errors (not the same object)
	require.False(t, errors.Is(types.ErrNotFound, gocql.ErrNotFound))

	// types.ErrNotFound must be detectable via errors.Is
	wrapped := errors.Join(types.ErrNotFound)
	require.True(t, types.IsNotFound(wrapped))
}

// TestBatchMethodsExist verifies all Batch interface methods exist on v2.Batch.
// This is a compile-time verification that v2.Batch has all the new context methods.
func TestBatchMethodsExist(t *testing.T) {
	// Create a typed nil to verify method signatures exist
	var b *v2.Batch

	// Verify context methods exist with correct signatures
	// These are compile-time checks - if any method is missing, this won't compile
	_ = func() {
		_ = b.ExecContext
		_ = b.IterContext
		_ = b.ExecCAS
		_ = b.ExecCASContext
		_ = b.MapExecCAS
		_ = b.MapExecCASContext
	}
}
