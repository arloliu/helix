package replay

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// rejectedFrameError is a coordinator error frame with a chosen CQL error code.
// gocql keeps the concrete frame types for the rejection codes unexported,
// so a test builds the exported gocql.RequestError interface itself.
type rejectedFrameError struct{ code int }

func (e *rejectedFrameError) Code() int       { return e.code }
func (e *rejectedFrameError) Message() string { return "rejected" }
func (e *rejectedFrameError) Error() string   { return e.Message() }

// The adapters now wrap a rejected statement in types.ErrStatementRejected
// instead of passing the driver error through untouched.
// The wrap keeps the driver error in the chain, so the custom classifier
// documented in docs/replay-system.md — match gocql.RequestError,
// discriminate on Code() — still reaches the coordinator's error code and
// can still dead-letter a syntax error.
//
// The default classifier is unchanged by the wrap: a rejected statement is
// retried, because a schema migration that has not reached this cluster yet
// reports the same codes.
func TestReplayClassifierThroughStatementRejectedWrap(t *testing.T) {
	syntax := &rejectedFrameError{code: gocql.ErrCodeSyntax}
	// The shape the adapters produce; adapter/cql/v{1,2}/errors_test.go
	// pins it against the real driver errors.
	wrapped := fmt.Errorf("%w: %w", types.ErrStatementRejected, syntax)

	var reqErr gocql.RequestError
	require.ErrorAs(t, wrapped, &reqErr, "the driver error must stay in the chain")
	require.Equal(t, gocql.ErrCodeSyntax, reqErr.Code())

	classifier := func(err error) ReplayDisposition {
		var reqErr gocql.RequestError
		if errors.As(err, &reqErr) && reqErr.Code() == gocql.ErrCodeSyntax {
			return DispositionDeadLetter
		}

		return DefaultReplayClassifier(err)
	}
	require.Equal(t, DispositionDeadLetter, classifier(wrapped))

	invalid := fmt.Errorf("%w: %w", types.ErrStatementRejected, &rejectedFrameError{code: gocql.ErrCodeInvalid})
	require.Equal(t, DispositionRetry, classifier(invalid),
		"the documented classifier must stay narrow: 0x2200 is also a schema not yet replicated")
	require.Equal(t, DispositionRetry, DefaultReplayClassifier(wrapped),
		"the wrap must not change the default disposition")
}
