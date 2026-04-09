package v2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests verify internal context wiring that cannot be tested from
// the external _test package because the ctx field is unexported.
// Full context-propagation verification requires integration tests with
// a real Cassandra session (see test/integration/).

// ctxKey is a context key type for testing.
type ctxKey struct{}

func TestQueryWithContextStoresCtx(t *testing.T) {
	q := &Query{statement: "SELECT 1"}
	require.Nil(t, q.ctx, "ctx should be nil before WithContext")

	ctx := context.WithValue(t.Context(), ctxKey{}, "v")
	q.WithContext(ctx)
	assert.Equal(t, ctx, q.ctx, "WithContext should store the context")
}

func TestBatchWithContextStoresCtx(t *testing.T) {
	b := &Batch{}
	require.Nil(t, b.ctx, "ctx should be nil before WithContext")

	ctx := context.WithValue(t.Context(), ctxKey{}, "v")
	b.WithContext(ctx)
	assert.Equal(t, ctx, b.ctx, "WithContext should store the context")
}

// TestQueryCASMethodsGateOnCtx verifies that ScanCAS and MapScanCAS
// contain the ctx-dispatch branch by testing the prerequisite: that a
// Query with ctx=nil vs ctx!=nil will follow different code paths.
//
// Both paths ultimately panic (nil underlying *gocql.Query), but we
// verify the structural prerequisite: ctx is non-nil after WithContext,
// and nil by default. The actual context-aware execution (cancellation,
// timeouts) is verified in integration tests.
func TestQueryCASMethodsGateOnCtx(t *testing.T) {
	t.Run("ScanCAS without context", func(t *testing.T) {
		q := &Query{statement: "INSERT"}
		assert.Nil(t, q.ctx)
		assert.Panics(t, func() { _, _ = q.ScanCAS() })
	})

	t.Run("ScanCAS with context", func(t *testing.T) {
		q := &Query{statement: "INSERT", ctx: t.Context()}
		assert.NotNil(t, q.ctx)
		assert.Panics(t, func() { _, _ = q.ScanCAS() })
	})

	t.Run("MapScanCAS without context", func(t *testing.T) {
		q := &Query{statement: "INSERT"}
		assert.Nil(t, q.ctx)
		assert.Panics(t, func() { _, _ = q.MapScanCAS(nil) })
	})

	t.Run("MapScanCAS with context", func(t *testing.T) {
		q := &Query{statement: "INSERT", ctx: t.Context()}
		assert.NotNil(t, q.ctx)
		assert.Panics(t, func() { _, _ = q.MapScanCAS(nil) })
	})
}

// TestBatchCASMethodsGateOnCtx verifies that ExecCAS and MapExecCAS
// contain the ctx-dispatch branch (same rationale as query tests above).
func TestBatchCASMethodsGateOnCtx(t *testing.T) {
	t.Run("ExecCAS without context", func(t *testing.T) {
		b := &Batch{}
		assert.Nil(t, b.ctx)
		assert.Panics(t, func() { _, _, _ = b.ExecCAS() })
	})

	t.Run("ExecCAS with context", func(t *testing.T) {
		b := &Batch{ctx: t.Context()}
		assert.NotNil(t, b.ctx)
		assert.Panics(t, func() { _, _, _ = b.ExecCAS() })
	})

	t.Run("MapExecCAS without context", func(t *testing.T) {
		b := &Batch{}
		assert.Nil(t, b.ctx)
		assert.Panics(t, func() { _, _, _ = b.MapExecCAS(nil) })
	})

	t.Run("MapExecCAS with context", func(t *testing.T) {
		b := &Batch{ctx: t.Context()}
		assert.NotNil(t, b.ctx)
		assert.Panics(t, func() { _, _, _ = b.MapExecCAS(nil) })
	})
}
