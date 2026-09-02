package replay

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// v1 envelopes captured from the previous release: no version field and
// no consistency levels. A worker must keep reading them.
const (
	v1QueryHex = "88ae7461726765745f636c7573746572a142a57175657279d922494e5345525420494e544f207420286b2c2076292056414c55455320283f2c203f29a46172677392a16b07a974696d657374616d70d300060a24181e4000a87072696f7269747901a869735f6261746368c2aa62617463685f7479706500b062617463685f73746174656d656e747390"
	v1BatchHex = "88ae7461726765745f636c7573746572a141a57175657279a0a461726773c0a974696d657374616d7005a87072696f7269747900a869735f6261746368c3aa62617463685f7479706501b062617463685f73746174656d656e74739182a57175657279be5550444154452074205345542076203d203f205748455245206b203d2031a46172677391a178"
)

func TestEnvelope_ReadsVersionOneMessages(t *testing.T) {
	data, err := hex.DecodeString(v1QueryHex)
	require.NoError(t, err)

	var msg natsReplayMessage
	_, err = msg.UnmarshalMsg(data)
	require.NoError(t, err)
	require.Equal(t, "B", msg.TargetCluster)
	require.Equal(t, "INSERT INTO t (k, v) VALUES (?, ?)", msg.Query)
	require.Equal(t, int64(1700000000000000), msg.Timestamp)
	require.Zero(t, msg.Version, "a version 1 message carries no version")
	require.False(t, msg.HasConsistency, "a version 1 message has no consistency; it must not decode as level zero")
	require.False(t, msg.HasSerialConsistency)

	args, err := decodeArgs(msg.Args)
	require.NoError(t, err)
	require.Equal(t, []any{"k", int64(7)}, args)

	data, err = hex.DecodeString(v1BatchHex)
	require.NoError(t, err)
	var batch natsReplayMessage
	_, err = batch.UnmarshalMsg(data)
	require.NoError(t, err)
	require.True(t, batch.IsBatch)
	require.Len(t, batch.BatchStatements, 1)
	require.False(t, batch.HasConsistency)
}

func TestEnvelope_CarriesConsistencyWhenSet(t *testing.T) {
	quorum, serial := types.Quorum, types.LocalSerial
	msg := natsReplayMessage{
		TargetCluster:        "A",
		Query:                "UPDATE t SET v = 1",
		Version:              replayEnvelopeVersion,
		HasConsistency:       true,
		Consistency:          uint16(quorum),
		HasSerialConsistency: true,
		SerialConsistency:    uint16(serial),
	}
	data, err := msg.MarshalMsg(nil)
	require.NoError(t, err)

	var decoded natsReplayMessage
	_, err = decoded.UnmarshalMsg(data)
	require.NoError(t, err)
	require.Equal(t, msg, decoded)

	// Absent levels stay absent through a version 2 round trip too.
	plain := natsReplayMessage{TargetCluster: "A", Query: "q", Version: replayEnvelopeVersion}
	data, err = plain.MarshalMsg(nil)
	require.NoError(t, err)
	var decodedPlain natsReplayMessage
	_, err = decodedPlain.UnmarshalMsg(data)
	require.NoError(t, err)
	require.False(t, decodedPlain.HasConsistency)
	require.False(t, decodedPlain.HasSerialConsistency)
}
