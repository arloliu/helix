package helix

import (
	"testing"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

func TestPageState_RoundTripCarriesIssuingCluster(t *testing.T) {
	raw := []byte("driver-cursor")

	token := encodePageState(ClusterB, raw)
	cluster, got := decodePageState(token)
	require.Equal(t, ClusterB, cluster)
	require.Equal(t, raw, got)

	require.Nil(t, encodePageState(ClusterA, nil), "no more pages stays nil")
	require.Empty(t, encodePageState(ClusterA, []byte{}), "no more pages stays empty")

	cluster, got = decodePageState(raw)
	require.Empty(t, cluster, "a driver token has no issuing cluster")
	require.Equal(t, raw, got, "a driver token is passed through unchanged")

	cluster, got = decodePageState([]byte("hx1Zrest"))
	require.Empty(t, cluster, "an unknown cluster letter is not a Helix token")
	require.Equal(t, []byte("hx1Zrest"), got)

	colliding := []byte("hx1A-a-driver-token-that-starts-with-the-magic")
	cluster, got = decodePageState(colliding)
	require.Empty(t, cluster, "a driver token starting with the magic fails the checksum and passes through")
	require.Equal(t, colliding, got)
	cluster, got = decodePageState(encodePageState(ClusterA, colliding))
	require.Equal(t, ClusterA, cluster)
	require.Equal(t, colliding, got)
}

// TestIter_PageStateIsStrippedBeforeTheDriver asserts that the routing
// header never reaches the driver and that the driver's own token is what
// the iterator wraps.
func TestIter_PageStateIsStrippedBeforeTheDriver(t *testing.T) {
	sessionA, sessionB := newMockSession(), newMockSession()
	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	token := encodePageState(ClusterB, []byte("cursor-from-b"))
	it := client.Query("SELECT v FROM t").PageState(token).IterContext(t.Context())
	require.NoError(t, it.Close())

	require.Empty(t, sessionA.queries, "the token names cluster B, so cluster A is not asked")
	require.Len(t, sessionB.queries, 1)
	require.Equal(t, []byte("cursor-from-b"), sessionB.lastQuery.pageState,
		"the driver receives its own cursor without the routing header")
}

// TestIter_PageStateHonoursOverrideFence asserts that a token issued by a
// cluster the operator has excluded fails closed instead of being sent to
// the other cluster.
func TestIter_PageStateHonoursOverrideFence(t *testing.T) {
	client, err := NewCQLClient(newMockSession(), newMockSession(),
		WithAllowedClusters(func() []ClusterID { return []ClusterID{ClusterA} }),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	token := encodePageState(ClusterB, []byte("cursor-from-b"))
	it := client.Query("SELECT v FROM t").PageState(token).IterContext(t.Context())
	require.ErrorIs(t, it.Close(), types.ErrNoValidClusters)
}

// TestSliceMap_PageStatePinsIssuingCluster asserts that a paged slice read
// follows its token to the issuing cluster even when the strategy now
// prefers the other one.
func TestSliceMap_PageStatePinsIssuingCluster(t *testing.T) {
	specA := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{1})}
	specB := &sliceSpec{cols: []string{"id"}, rows: rowsOf([]any{2})}
	client, sa, sb := newSliceClient(t, specA, specB,
		WithReadStrategy(&trackingReadStrategy{preferred: ClusterA}),
	)

	token := encodePageState(ClusterB, []byte("cursor-from-b"))
	rows, err := client.Query("SELECT id FROM t").PageState(token).SliceMapContext(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, rows[0]["id"])
	require.Zero(t, sa.iterCtxCalls.Load())
	require.Equal(t, int32(1), sb.iterCtxCalls.Load())
}
