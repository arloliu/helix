package helix

import (
	"testing"

	"github.com/arloliu/helix/policy"
	"github.com/stretchr/testify/require"
)

// TestIter_CloseReportsOnce asserts that closing an iterator twice records
// its outcome once and returns the same error both times.
func TestIter_CloseReportsOnce(t *testing.T) {
	sa, sb := newReadProbeSession(), newReadProbeSession()
	cb := policy.NewCircuitBreaker(policy.WithThreshold(2))
	client := newReadProbeClient(t, sa, sb, WithFailoverPolicy(cb))

	sa.setIterCloseErr(errReadProbeCluster)
	it := client.Query("SELECT v FROM t").IterContext(t.Context())
	require.ErrorIs(t, it.Close(), errReadProbeCluster)
	require.ErrorIs(t, it.Close(), errReadProbeCluster, "a repeated Close returns the same error")
	require.EqualValues(t, 1, cb.Failures(ClusterA), "one iterator error is recorded once")
}
