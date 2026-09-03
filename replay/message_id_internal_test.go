package replay

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

func identityOf(t *testing.T, payload types.ReplayPayload) string {
	t.Helper()
	msg, err := newNATSReplayMessage(payload)
	require.NoError(t, err)

	return msg.messageID()
}

func TestMessageID_IsStableAndCanonical(t *testing.T) {
	base := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO t (k, v) VALUES (?, ?)",
		Args:          []any{1, map[string]any{"b": 2, "a": map[string]any{"y": 1, "x": 2}}},
		Timestamp:     1_700_000_000_000_000,
		Priority:      types.PriorityHigh,
	}
	first := identityOf(t, base)
	require.Equal(t, first, identityOf(t, base), "the same payload always gets the same id")

	reordered := base
	reordered.Args = []any{1, map[string]any{"a": map[string]any{"x": 2, "y": 1}, "b": 2}}
	require.Equal(t, first, identityOf(t, reordered), "map key order does not change the id, even nested")
}

func TestMessageID_DistinguishesEveryIdentityField(t *testing.T) {
	quorum := types.Quorum
	base := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO t (k, v) VALUES (?, ?)",
		Args:          []any{1, "v"},
		Timestamp:     1_700_000_000_000_000,
	}
	first := identityOf(t, base)

	variants := map[string]func(p *types.ReplayPayload){
		"arguments":            func(p *types.ReplayPayload) { p.Args = []any{1, "w"} },
		"cluster":              func(p *types.ReplayPayload) { p.TargetCluster = types.ClusterB },
		"timestamp":            func(p *types.ReplayPayload) { p.Timestamp++ },
		"priority":             func(p *types.ReplayPayload) { p.Priority = types.PriorityLow },
		"consistency presence": func(p *types.ReplayPayload) { c := types.Consistency(0); p.Consistency = &c },
		"consistency value":    func(p *types.ReplayPayload) { p.Consistency = &quorum },
		"serial consistency":   func(p *types.ReplayPayload) { p.SerialConsistency = &quorum },
		"variant": func(p *types.ReplayPayload) {
			p.IsBatch = true
			p.BatchStatements = []types.BatchStatement{{Query: p.Query, Args: p.Args}}
			p.Query, p.Args = "", nil
		},
	}
	for name, change := range variants {
		p := base
		change(&p)
		require.NotEqual(t, first, identityOf(t, p), "%s is part of the identity", name)
	}
}

func TestMessageID_BatchOrderMatters(t *testing.T) {
	batch := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		IsBatch:       true,
		BatchType:     types.LoggedBatch,
		BatchStatements: []types.BatchStatement{
			{Query: "INSERT INTO t (k) VALUES (?)", Args: []any{1}},
			{Query: "INSERT INTO t (k) VALUES (?)", Args: []any{2}},
		},
		Timestamp: 1_700_000_000_000_000,
	}
	first := identityOf(t, batch)
	swapped := batch
	swapped.BatchStatements = []types.BatchStatement{batch.BatchStatements[1], batch.BatchStatements[0]}
	require.NotEqual(t, first, identityOf(t, swapped))
}

func TestMessageID_AbsentForNonIdempotentPayloads(t *testing.T) {
	counter := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "UPDATE counters SET hits = hits + 1 WHERE id = ?",
		Args:          []any{1},
		Timestamp:     1_700_000_000_000_000,
		NonIdempotent: true,
	}
	require.Empty(t, identityOf(t, counter), "two distinct counter updates may share every field")
}

// The identity layout is pinned by a golden value: a change to the fields
// or their order must bump identitySchema on purpose.
func TestMessageID_GoldenValue(t *testing.T) {
	msg := makeSimpleNATSMsg(t, "INSERT INTO t (k) VALUES (?)", 1)
	require.Equal(t, "bdbd96d86c608e9623084a995e0e9ec445ace8cc25146fcd57497c81060dece4", msg.messageID())
}
