package v1

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

func TestBindDurations(t *testing.T) {
	plain := []any{1, "a"}
	require.Equal(t, &plain[0], &bindDurations(plain)[0], "no duration: the slice is returned as-is")

	values := []any{1, types.Duration{Months: 1, Days: 2, Nanoseconds: 3}}
	bound := bindDurations(values)
	require.Equal(t, []any{1, gocql.Duration{Months: 1, Days: 2, Nanoseconds: 3}}, bound)
	require.IsType(t, types.Duration{}, values[1], "the caller's slice is left untouched")

	nested := []any{
		[]any{types.Duration{Months: 1}},
		map[string]any{"d": types.Duration{Days: 2}},
		[]types.Duration{{Nanoseconds: 3}},
	}
	require.Equal(t, []any{
		[]any{gocql.Duration{Months: 1}},
		map[string]any{"d": gocql.Duration{Days: 2}},
		[]gocql.Duration{{Nanoseconds: 3}},
	}, bindDurations(nested))
	inner, ok := nested[0].([]any)
	require.True(t, ok)
	require.IsType(t, types.Duration{}, inner[0], "nested collections are copied, not mutated")
}
