//go:build e2e

package cql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
	htypes "github.com/arloliu/helix/types"
)

// TestS4_BothPaused_WriteReturnsDualClusterError verifies the write path
// returns a DualClusterError when both clusters hang. The read path is
// covered by TestS6_BothPaused_DualClusterError in parity_test.go.
//
// Probes hypothesis H3 (error type differences across drivers): Helix's
// adapter must wrap the underlying driver error such that errors.As to
// *DualClusterError works on both v1 and v2.
func TestS4_BothPaused_WriteReturnsDualClusterError(t *testing.T) {
	a, b := sharedClusters(t)
	withRestoredCluster(t, a)
	withRestoredCluster(t, b)

	table := createKVTableOnBoth(t, "s4_both_paused_write")

	for _, d := range allDrivers {
		t.Run(d.name, func(t *testing.T) {
			client, err := helix.NewCQLClient(d.wrap(a), d.wrap(b),
				helix.WithWriteStrategy(policy.NewSyncDualWrite()),
				helix.WithReadStrategy(policy.NewStickyRead()),
				helix.WithFailoverPolicy(policy.NewActiveFailover()),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ctx := context.Background()
			require.NoError(t, a.Pause(ctx))
			require.NoError(t, b.Pause(ctx))
			defer func() {
				_ = a.Unpause(context.Background())
				_ = b.Unpause(context.Background())
			}()

			qCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			err = client.Query("INSERT INTO "+table+" (key, value) VALUES (?, ?)",
				"k", "v").ExecContext(qCtx)

			require.Error(t, err, "[%s] expected error when both clusters paused", d.name)

			var dual *htypes.DualClusterError
			assert.True(t, errors.As(err, &dual),
				"[%s] expected DualClusterError, got %T: %v", d.name, err, err)

			if dual != nil {
				assert.NotNil(t, dual.ErrorA, "[%s] DualClusterError.ErrorA should be set", d.name)
				assert.NotNil(t, dual.ErrorB, "[%s] DualClusterError.ErrorB should be set", d.name)
			}
		})
	}
}
