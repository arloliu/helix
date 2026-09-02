package v2

import (
	"errors"
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// Driver errors that mean the cluster could not be reached are wrapped in
// the typed sentinel while staying reachable through errors.Is / errors.As.
func TestMapErr(t *testing.T) {
	unavailable := &gocql.RequestErrUnavailable{}
	other := errors.New("syntax error")

	tests := []struct {
		name        string
		in          error
		unreachable bool
	}{
		{name: "nil", in: nil},
		{name: "no connections", in: gocql.ErrNoConnections, unreachable: true},
		{name: "no connections started", in: gocql.ErrNoConnectionsStarted, unreachable: true},
		{name: "connection closed", in: gocql.ErrConnectionClosed, unreachable: true},
		{name: "session closed", in: gocql.ErrSessionClosed, unreachable: true},
		{name: "unavailable", in: unavailable, unreachable: true},
		{name: "statement error", in: other},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapErr(tt.in)
			if tt.in == nil {
				require.NoError(t, got)

				return
			}
			require.ErrorIs(t, got, tt.in, "the driver error must stay in the chain")
			require.Equal(t, tt.unreachable, errors.Is(got, types.ErrClusterUnreachable))
		})
	}

	require.ErrorIs(t, mapErr(gocql.ErrNotFound), types.ErrNotFound)
	var target *gocql.RequestErrUnavailable
	require.ErrorAs(t, mapErr(unavailable), &target)
}
