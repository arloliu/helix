package v1

import (
	"errors"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// requestErr is a coordinator error frame with a chosen CQL error code.
// The driver keeps the concrete frame types for the rejection codes
// unexported (v1) or unconstructable from outside the package (v2, whose
// typed frames carry an unexported code field), so a test builds the
// exported gocql.RequestError interface itself.
type requestFrameError struct{ code int }

func (e *requestFrameError) Code() int       { return e.code }
func (e *requestFrameError) Message() string { return "rejected" }
func (e *requestFrameError) Error() string   { return e.Message() }

// Driver errors that mean the cluster could not be reached are wrapped in
// the typed sentinel while staying reachable through errors.Is / errors.As.
// A coordinator response that rejected the statement itself is wrapped the
// same way, in types.ErrStatementRejected, and only for the four rejection
// codes.
func TestMapErr(t *testing.T) {
	unavailable := &gocql.RequestErrUnavailable{}
	other := errors.New("syntax error")

	tests := []struct {
		name        string
		in          error
		unreachable bool
		rejected    bool
	}{
		{name: "nil", in: nil},
		{name: "no connections", in: gocql.ErrNoConnections, unreachable: true},
		{name: "no connections started", in: gocql.ErrNoConnectionsStarted, unreachable: true},
		{name: "connection closed", in: gocql.ErrConnectionClosed, unreachable: true},
		{name: "session closed", in: gocql.ErrSessionClosed, unreachable: true},
		{name: "unavailable", in: unavailable, unreachable: true},
		{name: "plain error", in: other},
		{name: "syntax", in: &requestFrameError{code: gocql.ErrCodeSyntax}, rejected: true},
		{name: "unauthorized", in: &requestFrameError{code: gocql.ErrCodeUnauthorized}, rejected: true},
		{name: "invalid", in: &requestFrameError{code: gocql.ErrCodeInvalid}, rejected: true},
		{name: "config", in: &requestFrameError{code: gocql.ErrCodeConfig}, rejected: true},
		// The driver re-prepares an unprepared statement, and neither an
		// already-existing keyspace nor a read timeout describes a
		// statement the caller must fix.
		{name: "already exists", in: &requestFrameError{code: gocql.ErrCodeAlreadyExists}},
		{name: "unprepared", in: &requestFrameError{code: gocql.ErrCodeUnprepared}},
		{name: "read timeout", in: &requestFrameError{code: gocql.ErrCodeReadTimeout}},
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
			require.Equal(t, tt.rejected, errors.Is(got, types.ErrStatementRejected))

			var reqErr gocql.RequestError
			if tt.rejected {
				require.ErrorAs(t, got, &reqErr,
					"a custom replay classifier must still reach the driver error's code")
				require.Equal(t, tt.in, error(reqErr),
					"the code a classifier discriminates on must survive the wrap")
			}
		})
	}

	require.ErrorIs(t, mapErr(gocql.ErrNotFound), types.ErrNotFound)
	var target *gocql.RequestErrUnavailable
	require.ErrorAs(t, mapErr(unavailable), &target)
}
