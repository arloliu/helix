package types

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClusterNamesValidate covers every rejection branch of
// [ClusterNames.Validate].
// The names become metric label values,
// so a name that slips through here turns up as a malformed series
// long after the client was constructed.
func TestClusterNamesValidate(t *testing.T) {
	tests := []struct {
		name    string
		names   ClusterNames
		wantErr string
	}{
		{"default", DefaultClusterNames(), ""},
		{"custom valid", ClusterNames{A: "east_1", B: "_west2"}, ""},
		{"max length", ClusterNames{A: strings.Repeat("a", 32), B: "B"}, ""},
		{"A empty", ClusterNames{A: "", B: "B"}, "cluster A name cannot be empty"},
		{"B empty", ClusterNames{A: "A", B: ""}, "cluster B name cannot be empty"},
		{"A too long", ClusterNames{A: strings.Repeat("a", 33), B: "B"}, "cluster A name cannot exceed 32 characters"},
		{"B too long", ClusterNames{A: "A", B: strings.Repeat("b", 33)}, "cluster B name cannot exceed 32 characters"},
		{"A leading digit", ClusterNames{A: "1east", B: "B"}, "cluster A name must be alphanumeric"},
		{"B hyphen", ClusterNames{A: "A", B: "west-2"}, "cluster B name must be alphanumeric"},
		{"A dot", ClusterNames{A: "east.1", B: "B"}, "cluster A name must be alphanumeric"},
		{"B space", ClusterNames{A: "A", B: "west 2"}, "cluster B name must be alphanumeric"},
		{"identical", ClusterNames{A: "same", B: "same"}, "cluster names must be different"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.names.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestClusterNamesValidateAFirst verifies that a pair invalid on both sides
// reports A, so the message names the first field the caller should fix
// rather than an arbitrary one.
func TestClusterNamesValidateAFirst(t *testing.T) {
	err := ClusterNames{A: "", B: ""}.Validate()
	require.ErrorContains(t, err, "cluster A name cannot be empty")
}

// TestClusterNamesName verifies Name maps each cluster ID to its display name.
func TestClusterNamesName(t *testing.T) {
	names := ClusterNames{A: "east", B: "west"}
	require.Equal(t, "east", names.Name(ClusterA))
	require.Equal(t, "west", names.Name(ClusterB))
}
