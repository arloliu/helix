package integration_test

import (
	"context"
	"testing"

	"github.com/arloliu/helix/adapter/cql"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/stretchr/testify/require"
)

func TestBatchContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	sessionA, _ := getSharedSessions(t)
	helixSession := cqlv1.NewSession(sessionA)

	// Create a table
	table := createTestTableOnBoth(t, "batch_context_test", "CREATE TABLE %s (id uuid PRIMARY KEY, val text)")

	t.Run("ExecContext returns error on canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		batch := helixSession.Batch(cql.LoggedBatch)
		batch.Query("INSERT INTO " + table + " (id, val) VALUES (uuid(), 'test')")

		err := batch.ExecContext(ctx)
		require.Error(t, err)
		// gocql returns context.Canceled when context is canceled
		require.Equal(t, context.Canceled, err)
	})

	t.Run("IterContext handles canceled context gracefully", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		batch := helixSession.Batch(cql.LoggedBatch)
		batch.Query("INSERT INTO " + table + " (id, val) VALUES (uuid(), 'test')")

		// IterContext in v1 adapter returns an iterator and swallows execution errors (returning a nil-safe iterator).
		// We verify that it doesn't panic and returns a non-nil iterator wrapper.
		iter := batch.IterContext(ctx)
		require.NotNil(t, iter)

		// The iterator should be empty/closed effectively
		require.NoError(t, iter.Close())
	})
}
