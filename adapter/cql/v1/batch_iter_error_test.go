package v1

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// An iterator that stands in for a request the driver already failed reports
// that failure from every method that can return one, so an empty iterator is
// never read as a successful empty result.
func TestIterReportsStandInError(t *testing.T) {
	failed := errors.New("batch execution failed")
	iter := &Iter{err: failed}

	require.ErrorIs(t, iter.Close(), failed)

	rows, err := iter.SliceMap()
	require.Nil(t, rows)
	require.ErrorIs(t, err, failed)

	sc := iter.Scanner()
	require.False(t, sc.Next(), "a row-less iterator yields no rows")
	require.ErrorIs(t, sc.Err(), failed)
	require.ErrorIs(t, sc.Scan(), failed)
}

// A row-less iterator that stands in for nothing — the shape the CAS methods
// return alongside their own error — still reports success.
func TestIterWithoutStandInErrorStaysClean(t *testing.T) {
	iter := &Iter{}

	require.NoError(t, iter.Close())

	rows, err := iter.SliceMap()
	require.Nil(t, rows)
	require.NoError(t, err)

	sc := iter.Scanner()
	require.False(t, sc.Next())
	require.NoError(t, sc.Err())
	require.NoError(t, sc.Scan())
}
