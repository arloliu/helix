package helix

import (
	"context"
	"testing"
	"time"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

type replayLegPayload struct {
	name    string
	payload types.ReplayPayload
}

// replayLegPayloads returns the two payload shapes DefaultExecuteFunc
// executes, both targeting cluster A and both carrying the non-zero
// timestamp the executor demands.
func replayLegPayloads() []replayLegPayload {
	return []replayLegPayload{
		{
			name: "single query",
			payload: types.ReplayPayload{
				TargetCluster: ClusterA,
				Query:         "INSERT INTO t (k, v) VALUES (?, ?)",
				Args:          []any{1, "v"},
				Timestamp:     7,
			},
		},
		{
			name: "batch",
			payload: types.ReplayPayload{
				TargetCluster:   ClusterA,
				IsBatch:         true,
				BatchType:       LoggedBatch,
				BatchStatements: []types.BatchStatement{{Query: "INSERT INTO t (k, v) VALUES (?, ?)", Args: []any{1, "v"}}},
				Timestamp:       7,
			},
		},
	}
}

// A replay attempt is a cluster leg, so ClusterWriteTimeout bounds it: a
// stalled cluster ends the attempt at the leg deadline instead of holding
// it for the worker's whole ExecuteTimeout, and the expiry is reported as
// the cluster's own timeout because the deadline is Helix's.
func TestDefaultExecuteFunc_ClusterWriteTimeout_ExpiredAttemptIsClusterTimeout(t *testing.T) {
	for _, tt := range replayLegPayloads() {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewCQLClient(newBlockingSession(), newMockSession(),
				WithClusterWriteTimeout(20*time.Millisecond))
			require.NoError(t, err)
			t.Cleanup(client.Close)

			start := time.Now()
			err = client.DefaultExecuteFunc()(t.Context(), tt.payload)
			require.Less(t, time.Since(start), time.Second,
				"the attempt must end on the leg deadline, not the worker's ExecuteTimeout")
			require.ErrorIs(t, err, types.ErrClusterTimeout)
			require.Equal(t, replay.DispositionRetry, replay.DefaultReplayClassifier(err),
				"a timed-out attempt stays retryable, so the payload is not dead-lettered")
			require.Equal(t, int32(0), client.statsForCluster(ClusterA).consecutiveFailures.Load(),
				"a replay attempt reports to the worker's classifier, not to the client's health accounting")
		})
	}
}

// A deadline the caller brought is the caller's: the worker's own context
// ending must not be reported as the cluster timing out, or a shutting-down
// worker would record a health failure against a healthy cluster.
// The leg timeout is longer than the parent's remaining budget in both cases,
// so the parent is what ends the attempt whether it expired before the
// attempt started or while it was in flight.
func TestDefaultExecuteFunc_ClusterWriteTimeout_ExpiredParentIsNotClusterTimeout(t *testing.T) {
	parents := []struct {
		name string
		// deadline is relative to the moment the attempt is about to run.
		deadline time.Duration
	}{
		{name: "expired before the attempt", deadline: -time.Millisecond},
		{name: "expires during the attempt", deadline: 20 * time.Millisecond},
	}

	for _, parent := range parents {
		for _, tt := range replayLegPayloads() {
			t.Run(parent.name+"/"+tt.name, func(t *testing.T) {
				client, err := NewCQLClient(newBlockingSession(), newMockSession(),
					WithClusterWriteTimeout(time.Minute))
				require.NoError(t, err)
				t.Cleanup(client.Close)

				parentCtx, cancel := context.WithDeadline(t.Context(), time.Now().Add(parent.deadline))
				defer cancel()

				err = client.DefaultExecuteFunc()(parentCtx, tt.payload)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.NotErrorIs(t, err, types.ErrClusterTimeout,
					"the parent's deadline expired, so the failure is the caller's")
			})
		}
	}
}

// Without the option the attempt runs on the worker's context untouched:
// no deadline is imposed and nothing is wrapped, exactly as before the
// option was honoured here.
func TestDefaultExecuteFunc_ClusterWriteTimeout_DeadlineOnlyWhenConfigured(t *testing.T) {
	tests := []struct {
		name         string
		timeout      time.Duration
		wantDeadline bool
	}{
		{name: "disabled", timeout: 0, wantDeadline: false},
		{name: "configured", timeout: time.Minute, wantDeadline: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sessionA := newMockSession()
			var opts []Option
			if tt.timeout > 0 {
				opts = append(opts, WithClusterWriteTimeout(tt.timeout))
			}
			client, err := NewCQLClient(sessionA, newMockSession(), opts...)
			require.NoError(t, err)
			t.Cleanup(client.Close)
			execute := client.DefaultExecuteFunc()

			for _, tt := range replayLegPayloads() {
				require.NoError(t, execute(t.Context(), tt.payload))
			}

			_, queryHasDeadline := sessionA.lastQuery.ctx.Deadline()
			require.Equal(t, tt.wantDeadline, queryHasDeadline, "single-query attempt context")
			_, batchHasDeadline := sessionA.lastBatch.ctx.Deadline()
			require.Equal(t, tt.wantDeadline, batchHasDeadline, "batch attempt context")
		})
	}
}
