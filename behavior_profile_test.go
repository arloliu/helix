package helix

import (
	"errors"
	"testing"

	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

func TestBehaviorProfile_SafeIsPureOptionExpansion(t *testing.T) {
	var safe, explicit ClientConfig
	WithBehaviorProfile(Safe)(&safe)
	WithRouteVeto(true)(&explicit)
	require.Equal(t, explicit.RouteVeto, safe.RouteVeto, "Safe sets exactly the root-owned knob WithRouteVeto(true) sets")

	var overridden ClientConfig
	WithBehaviorProfile(Safe)(&overridden)
	WithRouteVeto(false)(&overridden)
	require.False(t, overridden.RouteVeto, "a later option wins, as with any other option")

	var legacy ClientConfig
	WithBehaviorProfile(Legacy)(&legacy)
	require.False(t, legacy.RouteVeto)
}

func TestBehaviorProfile_SafeEnablesRouteVetoOnTheClient(t *testing.T) {
	sa, sb := newReadProbeSession(), newReadProbeSession()
	client := newReadProbeClient(t, sa, sb,
		WithReadStrategy(policy.NewStickyRead(policy.WithPreferredCluster(ClusterA))),
		WithFailoverPolicy(newVetoPolicy(ClusterA)),
		WithBehaviorProfile(Safe),
	)

	var v string
	require.NoError(t, client.Query("SELECT v FROM t").ScanContext(t.Context(), &v))
	require.Equal(t, int64(1), sb.scans.Load(), "the vetoed cluster is avoided under the Safe profile")
}

func TestBehaviorProfile_UnknownRejected(t *testing.T) {
	_, err := NewCQLClient(newMockSession(), newMockSession(), WithBehaviorProfile(BehaviorProfile(42)))
	var optErr *types.OptionError
	require.True(t, errors.As(err, &optErr))
	require.Equal(t, "WithBehaviorProfile", optErr.Option)
}
