package v1_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/adapter/cql"
	v1 "github.com/arloliu/helix/adapter/cql/v1" //nolint:revive // required for v1_test package
)

// TestSessionImplementsInterface verifies that v1.Session implements cql.Session.
func TestSessionImplementsInterface(t *testing.T) {
	// This is a compile-time check
	var _ cql.Session = (*v1.Session)(nil)
}

// TestQueryImplementsInterface verifies that v1.Query implements cql.Query.
func TestQueryImplementsInterface(t *testing.T) {
	// This is a compile-time check
	var _ cql.Query = (*v1.Query)(nil)
}

// TestBatchImplementsInterface verifies that v1.Batch implements cql.Batch.
func TestBatchImplementsInterface(t *testing.T) {
	// This is a compile-time check
	var _ cql.Batch = (*v1.Batch)(nil)
}

// TestIterImplementsInterface verifies that v1.Iter implements cql.Iter.
func TestIterImplementsInterface(t *testing.T) {
	// This is a compile-time check
	var _ cql.Iter = (*v1.Iter)(nil)
}

// TestNewSessionNil tests that NewSession handles nil gracefully.
func TestNewSessionNil(t *testing.T) {
	// Note: This will create a Session with nil underlying gocql.Session.
	// In production, this would panic on use, but we verify the constructor works.
	session := v1.NewSession(nil)
	require.NotNil(t, session)
}

// TestWrapSessionNil tests that WrapSession handles nil gracefully.
func TestWrapSessionNil(t *testing.T) {
	session := v1.WrapSession(nil)
	require.NotNil(t, session)
}

// The following tests require a real gocql.Session and are run as integration tests.
// See test/integration/cql_v1_adapter_test.go for those tests.

// TestBatchTypeConstants verifies batch type constants match gocql.
func TestBatchTypeConstants(t *testing.T) {
	require.Equal(t, cql.BatchType(gocql.LoggedBatch), cql.LoggedBatch)
	require.Equal(t, cql.BatchType(gocql.UnloggedBatch), cql.UnloggedBatch)
	require.Equal(t, cql.BatchType(gocql.CounterBatch), cql.CounterBatch)
}

// TestConsistencyConstants verifies consistency constants match gocql.
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

// TestQueryMethodsExist verifies all Query interface methods exist on v1.Query.
// This is a compile-time verification that v1.Query has all the new context methods.
func TestQueryMethodsExist(t *testing.T) {
	// Create a typed nil to verify method signatures exist
	var q *v1.Query

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

// TestBatchMethodsExist verifies all Batch interface methods exist on v1.Batch.
// This is a compile-time verification that v1.Batch has all the new context methods.
func TestBatchMethodsExist(t *testing.T) {
	// Create a typed nil to verify method signatures exist
	var b *v1.Batch

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
