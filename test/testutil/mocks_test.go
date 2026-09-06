package testutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
)

// A scanFn that maps rows by column name must be able to see metadata from
// MockQuery, the way it does from a real drain.
func TestMockQuery_SliceScanColumns(t *testing.T) {
	q := NewMockQuery("SELECT id, name FROM users").
		SetSliceScanData([][]any{{1, "alice"}, {2, "bob"}}).
		SetSliceScanColumns([]helix.ColumnInfo{
			{Keyspace: "ks", Table: "users", Name: "id"},
			{Keyspace: "ks", Table: "users", Name: "name"},
		})

	names := make([][]string, 0, 2)
	rowCount, err := q.SliceScan(func(r helix.RowScanner) error {
		row := make([]string, 0, len(r.Columns()))
		for _, col := range r.Columns() {
			row = append(row, col.Name)
		}
		names = append(names, row)

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, rowCount)
	assert.Equal(t, [][]string{{"id", "name"}, {"id", "name"}}, names,
		"every row sees the configured column metadata")
}

// Columns configured for a positional callback are optional: an unset mock
// still drives a scanFn that only scans by position.
func TestMockQuery_SliceScanColumns_UnsetIsEmpty(t *testing.T) {
	q := NewMockQuery("SELECT id, name FROM users").SetSliceScanData([][]any{{1}})

	rowCount, err := q.SliceScan(func(r helix.RowScanner) error {
		assert.Empty(t, r.Columns())

		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, rowCount)
}
