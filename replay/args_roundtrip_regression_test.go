package replay

import (
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"

	"github.com/arloliu/helix/types"
)

// TestArgRoundtripRegression is the full matrix of CQL argument types that a
// replay payload must carry through the NATS encode/decode path without loss.
// Each row states the Go value a caller passes to the driver and the value the
// replay worker must hand back to the driver after a round-trip.
//
// Rows marked pending describe the desired behavior, not the current one:
// today the NATS path rejects or mangles those types (varint, decimal, inet,
// CQL duration, empty blob). They are skipped so the suite stays green until
// the explicit encoders land; remove the skip line in the loop to see them fail.
func TestArgRoundtripRegression(t *testing.T) {
	bigValue, ok := new(big.Int).SetString("123456789012345678901234567890", 10)
	require.True(t, ok)

	tests := []struct {
		name    string
		input   any
		want    any
		pending string
	}{
		// Already correct: msgp widens all signed integers to int64, which gocql
		// marshals back into int/bigint/smallint/tinyint columns.
		{
			name:  "int widens to int64",
			input: int(42),
			want:  int64(42),
		},
		// Already correct: the instant survives, the zone is normalized to UTC.
		{
			name:  "time.Time keeps the instant",
			input: time.Date(2026, 9, 2, 10, 30, 0, 0, time.FixedZone("CST", 8*3600)),
			want:  time.Date(2026, 9, 2, 2, 30, 0, 0, time.UTC),
		},
		// Already correct: UUID-shaped arrays travel as a msgp extension and come
		// back as a 16-byte slice, which gocql accepts for uuid/timeuuid/blob.
		{
			name:  "[16]byte becomes 16-byte slice",
			input: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			want:  []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		},
		{
			name:  "gocql.UUID becomes 16-byte slice",
			input: gocql.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			want:  []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		},

		// Assumption: a varint argument decodes back to an equal *big.Int so the worker can pass it to the driver unchanged.
		// Any other representation is acceptable only if gocql marshals it to the same varint bytes.
		{
			name:    "*big.Int round-trips as equal *big.Int",
			input:   bigValue,
			want:    bigValue,
			pending: "NATS replay rejects *big.Int (varint) at encode time, so the write is dropped instead of replayed",
		},
		{
			name:    "*inf.Dec round-trips as equal *inf.Dec",
			input:   inf.NewDec(12345, 2),
			want:    inf.NewDec(12345, 2),
			pending: "NATS replay rejects *inf.Dec (decimal) at encode time, so the write is dropped instead of replayed",
		},
		{
			name:    "net.IP v4 round-trips as equal net.IP",
			input:   net.ParseIP("10.0.0.1"),
			want:    net.ParseIP("10.0.0.1"),
			pending: "NATS replay turns net.IP (inet) into a list of integers, so every replay attempt fails in the driver",
		},
		{
			name:    "net.IP v6 round-trips as equal net.IP",
			input:   net.ParseIP("2001:db8::1"),
			want:    net.ParseIP("2001:db8::1"),
			pending: "NATS replay turns net.IP (inet) into a list of integers, so every replay attempt fails in the driver",
		},
		{
			name:    "gocql.Duration round-trips as equal gocql.Duration",
			input:   gocql.Duration{Months: 1, Days: 2, Nanoseconds: 3},
			want:    gocql.Duration{Months: 1, Days: 2, Nanoseconds: 3},
			pending: "NATS replay rejects gocql.Duration (CQL duration) at encode time, so the write is dropped instead of replayed",
		},
		// An empty blob must stay an empty blob: decoding it as nil makes the
		// replay write NULL (a tombstone) where the original wrote an empty value.
		{
			name:    "empty []byte stays empty and non-nil",
			input:   []byte{},
			want:    []byte{},
			pending: "NATS replay decodes an empty []byte as nil, so the replayed write stores NULL instead of an empty blob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.pending != "" {
				t.Skip("pending: " + tt.pending)
			}

			encoded, err := encodeArgs([]any{tt.input})
			require.NoError(t, err)

			decoded, err := decodeArgs(encoded)
			require.NoError(t, err)
			require.Len(t, decoded, 1)

			requireArgEqual(t, tt.want, decoded[0])
		})
	}
}

// TestMemoryReplayerEnqueueRejectsUnsupportedArgType asserts that the memory backend refuses argument types
// no backend can carry, at enqueue time, instead of accepting them silently.
// The NATS backend already fails for these types when it encodes;
// both backends must apply the same check so callers get the same answer regardless of which replayer is configured.
func TestMemoryReplayerEnqueueRejectsUnsupportedArgType(t *testing.T) {
	t.Skip("pending: MemoryReplayer.Enqueue accepts any argument type; the agreed design rejects unsupported types at enqueue on both backends")

	type udt struct {
		Name string
		Age  int
	}

	tests := []struct {
		name string
		arg  any
	}{
		{name: "map with non-string keys", arg: map[int]string{1: "a"}},
		{name: "struct value", arg: udt{Name: "x", Age: 1}},
		{name: "struct pointer", arg: &udt{Name: "x", Age: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replayer := NewMemoryReplayer()
			t.Cleanup(replayer.Close)

			err := replayer.Enqueue(t.Context(), types.ReplayPayload{
				TargetCluster: "A",
				Query:         "INSERT INTO t (k, v) VALUES (?, ?)",
				Args:          []any{"k", tt.arg},
			})
			require.Error(t, err, "enqueue must reject %T", tt.arg)
			require.Equal(t, 0, replayer.Len(), "rejected payload must not be queued")
		})
	}
}

// requireArgEqual compares a decoded argument against the desired value using
// the equality each type defines, since plain require.Equal treats a nil and an
// empty byte slice as the same and compares big numbers structurally.
func requireArgEqual(t *testing.T, want, got any) {
	t.Helper()

	switch w := want.(type) {
	case *big.Int:
		g, ok := got.(*big.Int)
		require.True(t, ok, "want *big.Int, got %T", got)
		require.Zero(t, w.Cmp(g), "want %s, got %s", w, g)
	case *inf.Dec:
		g, ok := got.(*inf.Dec)
		require.True(t, ok, "want *inf.Dec, got %T", got)
		require.Zero(t, w.Cmp(g), "want %s, got %s", w, g)
	case net.IP:
		g, ok := got.(net.IP)
		require.True(t, ok, "want net.IP, got %T", got)
		require.True(t, w.Equal(g), "want %s, got %s", w, g)
	case time.Time:
		g, ok := got.(time.Time)
		require.True(t, ok, "want time.Time, got %T", got)
		require.True(t, w.Equal(g), "want %s, got %s", w, g)
	case []byte:
		g, ok := got.([]byte)
		require.True(t, ok, "want []byte, got %T", got)
		require.NotNil(t, g, "want non-nil []byte, got nil")
		require.Equal(t, w, g)
	default:
		require.Equal(t, want, got)
	}
}
