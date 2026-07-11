package v2

import (
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatchSizeAndStatementsDeriveFromGocql verifies that Size and Statements derive
// from the underlying gocql batch: the count tracks the statements added, Statements
// reflects them in order, and the per-statement Args are shared with (not copied
// from) the underlying batch.
func TestBatchSizeAndStatementsDeriveFromGocql(t *testing.T) {
	b := &Batch{batch: &gocql.Batch{}}

	assert.Equal(t, 0, b.Size())
	assert.Empty(t, b.Statements())

	args0 := []any{1, "a"}
	args1 := []any{2, "b"}
	b.Query("INSERT INTO t (id, v) VALUES (?, ?)", args0...)
	b.Query("UPDATE t SET v = ? WHERE id = ?", args1...)

	assert.Equal(t, 2, b.Size())

	stmts := b.Statements()
	require.Len(t, stmts, 2)
	assert.Equal(t, "INSERT INTO t (id, v) VALUES (?, ?)", stmts[0].Statement)
	assert.Equal(t, "UPDATE t SET v = ? WHERE id = ?", stmts[1].Statement)
	require.Len(t, stmts[0].Args, 2)
	assert.Equal(t, "b", stmts[1].Args[1])

	// Args slices are shared, not deep-copied: the entry references the same backing
	// array the caller passed through to the underlying batch.
	require.Same(t, &args0[0], &stmts[0].Args[0], "Args should be shared with the underlying batch entry")
}
