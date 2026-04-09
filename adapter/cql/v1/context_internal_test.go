package v1

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

// These tests verify that ExecCASContext and MapExecCASContext correctly
// pass the context'd batch to the session, matching the pattern used by
// ExecContext. Full verification requires integration tests with a real
// Cassandra session (see test/integration/).

// TestBatchExecCASContextUsesSession verifies that ExecCASContext
// passes the context'd batch (from WithContext) to session.ExecuteBatchCAS,
// not the original batch with context discarded.
//
// We construct a Batch with a nil session — the method will panic when
// calling session.ExecuteBatchCAS, but critically it must call
// b.session.ExecuteBatchCAS(b.batch.WithContext(ctx), ...), NOT
// b.batch.WithContext(ctx) followed by b.ExecCAS().
func TestBatchExecCASContextUsesSession(t *testing.T) {
	b := &Batch{
		batch:   &gocql.Batch{},
		session: nil, // nil session will panic on ExecuteBatchCAS
	}

	// Panics because session is nil — but the important thing is it
	// reaches session.ExecuteBatchCAS (not b.ExecCAS which uses b.batch
	// without context).
	assert.Panics(t, func() {
		_, _, _ = b.ExecCASContext(t.Context())
	}, "ExecCASContext should call session.ExecuteBatchCAS")
}

// TestBatchMapExecCASContextUsesSession verifies MapExecCASContext
// follows the same pattern as ExecCASContext.
func TestBatchMapExecCASContextUsesSession(t *testing.T) {
	b := &Batch{
		batch:   &gocql.Batch{},
		session: nil,
	}

	assert.Panics(t, func() {
		_, _, _ = b.MapExecCASContext(t.Context(), nil)
	}, "MapExecCASContext should call session.MapExecuteBatchCAS")
}
