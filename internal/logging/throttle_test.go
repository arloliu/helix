package logging

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEscalate(t *testing.T) {
	escalated := make([]uint64, 0, 6)
	for n := uint64(0); n <= 20; n++ {
		if Escalate(n) {
			escalated = append(escalated, n)
		}
	}
	require.Equal(t, []uint64{1, 2, 4, 8, 16}, escalated)
}
