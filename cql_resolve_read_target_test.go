package helix

import (
	"context"
	"testing"
	"time"

	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// waitDraining blocks until client.IsDraining(cluster) reports true (or t
// fails the test). The mock topology watcher only updates the client's
// cached drain state via its update channel, so tests that need cached
// drain to be true must trigger a SetDrain after NewCQLClient and wait
// for the goroutine to apply the update.
func waitDraining(t *testing.T, client *CQLClient, cluster ClusterID) {
	t.Helper()
	require.Eventually(t, func() bool {
		return client.IsDraining(cluster)
	}, time.Second, 10*time.Millisecond, "drain state did not propagate to client cache")
}

func TestResolveReadTarget_PreserveSelectedCluster_FirstOverrideEntryDraining_ReturnsErrNoValidClusters(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	watcher := newMockTopologyWatcher()

	client, err := NewCQLClient(sessionA, sessionB,
		WithTopologyWatcher(watcher),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterA, ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	watcher.SetDrain(ClusterA, true)
	waitDraining(t, client, ClusterA)

	rt := client.resolveReadTarget(context.Background(), readOptions{preserveSelectedCluster: true})
	require.ErrorIs(t, rt.err, types.ErrNoValidClusters,
		"preserveSelectedCluster + draining first entry must fail closed; silently shipping the cursor to ClusterB would be unsound")
}

func TestResolveReadTarget_PreserveSelectedCluster_FirstEntryHealthy_ReturnsAsIs(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	watcher := newMockTopologyWatcher()

	client, err := NewCQLClient(sessionA, sessionB,
		WithTopologyWatcher(watcher),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterA, ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	// Drain the second entry — preserve mode must not consult it.
	watcher.SetDrain(ClusterB, true)
	waitDraining(t, client, ClusterB)

	rt := client.resolveReadTarget(context.Background(), readOptions{preserveSelectedCluster: true})
	require.NoError(t, rt.err)
	require.Equal(t, ClusterA, rt.cluster,
		"preserveSelectedCluster mode returns the first known override entry as-is")
	require.True(t, rt.snap.active, "override snapshot must mark itself active")
	require.Equal(t, ClusterA, rt.snap.primary)
	require.Equal(t, ClusterID(""), rt.snap.fallback,
		"preserveSelectedCluster intentionally leaves snap.fallback empty — no cross-cluster movement allowed")
}

// TestResolveReadTarget_DefaultPath_DrainFilterFallsOverToSecond is the
// regression assertion that the default (preserveSelectedCluster=false)
// path keeps today's drain-filter behavior: a draining ClusterA followed
// by ClusterB falls over silently to ClusterB.
func TestResolveReadTarget_DefaultPath_DrainFilterFallsOverToSecond(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()
	watcher := newMockTopologyWatcher()

	client, err := NewCQLClient(sessionA, sessionB,
		WithTopologyWatcher(watcher),
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterA, ClusterB}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	watcher.SetDrain(ClusterA, true)
	waitDraining(t, client, ClusterA)

	rt := client.resolveReadTarget(context.Background(), readOptions{})
	require.NoError(t, rt.err)
	require.Equal(t, ClusterB, rt.cluster,
		"default-path drain-filter must fall over to ClusterB — pre-v1.5.0 behavior preserved")
}

func TestResolveReadTarget_PreserveSelectedCluster_AllUnknown_ReturnsErrInvalidClusterOverride(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	client, err := NewCQLClient(sessionA, sessionB,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterID("Z")} // not A or B
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	rt := client.resolveReadTarget(context.Background(), readOptions{preserveSelectedCluster: true})
	require.ErrorIs(t, rt.err, types.ErrInvalidClusterOverride)
}

func TestResolveReadTarget_PreserveSelectedCluster_NoOverride_UsesNormalSelect(t *testing.T) {
	sessionA := newMockSession()
	sessionB := newMockSession()

	// No AllowedClusters — preserveSelectedCluster has no effect.
	client, err := NewCQLClient(sessionA, sessionB)
	require.NoError(t, err)
	defer client.Close()

	rt := client.resolveReadTarget(context.Background(), readOptions{preserveSelectedCluster: true})
	require.NoError(t, rt.err)
	require.False(t, rt.snap.active,
		"without override, preserveSelectedCluster must NOT synthesize one — normalSelect path is taken")
}

func TestResolveReadTarget_PreserveSelectedCluster_SingleClusterMode_Unchanged(t *testing.T) {
	sessionA := newMockSession()

	client, err := NewCQLClient(sessionA, nil,
		WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterA}
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	rt := client.resolveReadTarget(context.Background(), readOptions{preserveSelectedCluster: true})
	require.NoError(t, rt.err)
	require.Equal(t, ClusterA, rt.cluster,
		"single-cluster mode must remain unchanged regardless of preserveSelectedCluster")
	require.True(t, rt.snap.active)
}
