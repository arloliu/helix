package replay

import (
	"math/big"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"

	"github.com/arloliu/helix/types"
)

// TestArgRoundtrip_NestedExtensionTypes asserts that the extension types
// travel inside slices and string-keyed maps as they do at the top level,
// and that a nil collection stays nil.
func TestArgRoundtrip_NestedExtensionTypes(t *testing.T) {
	encoded, err := encodeArgs([]any{
		[]*big.Int{big.NewInt(-42)},
		map[string]*inf.Dec{"d": inf.NewDec(125, 2)},
		[]net.IP{net.ParseIP("10.0.0.1")},
		[]types.Duration{{Months: 1, Days: 2, Nanoseconds: 3}},
		[]int(nil),
		[][]byte{{}},
	})
	require.NoError(t, err)

	decoded, err := decodeArgs(encoded)
	require.NoError(t, err)
	require.Len(t, decoded, 6)

	ints, ok := decoded[0].([]any)
	require.True(t, ok, "got %T", decoded[0])
	gotInt, ok := ints[0].(*big.Int)
	require.True(t, ok, "got %T", ints[0])
	require.Zero(t, big.NewInt(-42).Cmp(gotInt))

	decs, ok := decoded[1].(map[string]any)
	require.True(t, ok, "got %T", decoded[1])
	gotDec, ok := decs["d"].(*inf.Dec)
	require.True(t, ok, "got %T", decs["d"])
	require.Zero(t, inf.NewDec(125, 2).Cmp(gotDec))

	ips, ok := decoded[2].([]any)
	require.True(t, ok, "got %T", decoded[2])
	gotIP, ok := ips[0].(net.IP)
	require.True(t, ok, "got %T", ips[0])
	require.True(t, net.ParseIP("10.0.0.1").Equal(gotIP))

	durs, ok := decoded[3].([]any)
	require.True(t, ok, "got %T", decoded[3])
	require.Equal(t, types.Duration{Months: 1, Days: 2, Nanoseconds: 3}, durs[0])

	require.Nil(t, decoded[4], "a nil slice stays NULL")

	blobs, ok := decoded[5].([]any)
	require.True(t, ok, "got %T", decoded[5])
	require.Equal(t, []byte{}, blobs[0], "an empty blob inside a collection stays empty")
}

// TestEncodeArgs_RejectsInvalidInet asserts that an IP that is neither 4
// nor 16 bytes is refused at enqueue instead of failing on every replay.
func TestEncodeArgs_RejectsInvalidInet(t *testing.T) {
	_, err := encodeArgs([]any{net.IP{1, 2, 3}})
	require.ErrorIs(t, err, types.ErrUnsupportedReplayArg)

	_, err = encodeArgs([]any{[]net.IP{{1, 2, 3}}})
	require.ErrorIs(t, err, types.ErrUnsupportedReplayArg)
}

// TestDefaultReplayClassifier_DeadLettersInvalidTimestamp asserts that a
// payload the executor rejects for a zero timestamp is not retried.
func TestDefaultReplayClassifier_DeadLettersInvalidTimestamp(t *testing.T) {
	require.Equal(t, DispositionDeadLetter, DefaultReplayClassifier(types.ErrInvalidTimestamp))
}
