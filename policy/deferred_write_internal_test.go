package policy

import (
	"errors"
	"testing"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

var _ helix.DeferredWriteResult = (*deferredWriteError)(nil)

func TestDeferredWrite_ReportsFinalErrorOnce(t *testing.T) {
	d := &deferredWriteError{}
	require.ErrorIs(t, d, types.ErrWriteAsync)
	require.Equal(t, types.ErrWriteAsync.Error(), d.Error())

	// Registered before completion: runs on complete.
	var got []error
	d.OnComplete(func(err error) { got = append(got, err) })
	legErr := errors.New("background leg failed")
	d.complete(legErr)
	require.Equal(t, []error{legErr}, got)

	// Registered after completion: runs immediately with the recorded error.
	late := &deferredWriteError{}
	late.complete(nil)
	called := false
	late.OnComplete(func(err error) { called = true; require.NoError(t, err) })
	require.True(t, called)
}
