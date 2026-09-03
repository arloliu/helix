package replay

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/arloliu/helix/types"
	"github.com/tinylib/msgp/msgp"
)

//go:generate go run -modfile=../linter.go.mod github.com/tinylib/msgp -unexported -file $GOFILE

// replayEnvelopeVersion is the envelope version this publisher emits.
// Version 1 messages carry no version field and no consistency levels.
// Decoding is tolerant in both directions: a worker ignores fields it does
// not know and treats a missing consistency as "session default", so
// workers are upgraded before publishers and old messages stay readable.
const replayEnvelopeVersion uint8 = 2

// natsReplayMessage is the MessagePack-serializable message format for NATS.
// This struct is used for efficient serialization of replay payloads.
//
// Fields added in envelope version 2 are paired with a Has* flag so an
// absent value (a version 1 message, or a write that used the session
// default) decodes as absent rather than as consistency level zero.
type natsReplayMessage struct {
	TargetCluster        string           `msg:"target_cluster"`
	Query                string           `msg:"query"`
	Args                 msgp.Raw         `msg:"args"`
	Timestamp            int64            `msg:"timestamp"`
	Priority             int              `msg:"priority"`
	IsBatch              bool             `msg:"is_batch"`
	BatchType            uint8            `msg:"batch_type"`
	BatchStatements      []batchStatement `msg:"batch_statements"`
	Version              uint8            `msg:"version"`
	HasConsistency       bool             `msg:"has_consistency"`
	Consistency          uint16           `msg:"consistency"`
	HasSerialConsistency bool             `msg:"has_serial_consistency"`
	SerialConsistency    uint16           `msg:"serial_consistency"`
	NonIdempotent        bool             `msg:"non_idempotent"`
}

// identitySchema tags the layout of the bytes hashed into a message id,
// separately from the wire version: a field added to the identity bumps
// it, so ids from before and after the change never collide by accident.
const identitySchema uint8 = 1

// messageID returns the Nats-Msg-Id of an idempotent envelope: the hex
// SHA-256 of its identity fields (target cluster, timestamp, priority,
// consistency levels, and the statement with its encoded arguments, or the
// batch type and every statement in order), each written with the msgp
// encoder so fields are length-prefixed and map arguments are already in
// sorted key order. The wire version is not part of it.
//
// A non-idempotent envelope returns "": two distinct counter updates can
// share every identity field within one microsecond, and JetStream would
// keep only one.
func (m *natsReplayMessage) messageID() string {
	if m.NonIdempotent {
		return ""
	}
	buf := make([]byte, 0, 64+len(m.Query)+len(m.Args))
	buf = msgp.AppendUint8(buf, identitySchema)
	buf = msgp.AppendString(buf, m.TargetCluster)
	buf = msgp.AppendInt64(buf, m.Timestamp)
	buf = msgp.AppendInt(buf, m.Priority)
	buf = msgp.AppendBool(buf, m.HasConsistency)
	buf = msgp.AppendUint16(buf, m.Consistency)
	buf = msgp.AppendBool(buf, m.HasSerialConsistency)
	buf = msgp.AppendUint16(buf, m.SerialConsistency)
	buf = msgp.AppendBool(buf, m.IsBatch)
	if m.IsBatch {
		buf = msgp.AppendUint8(buf, m.BatchType)
		buf = msgp.AppendArrayHeader(buf, uint32(len(m.BatchStatements))) //nolint:gosec // a batch never approaches the header's range
		for _, stmt := range m.BatchStatements {
			buf = msgp.AppendString(buf, stmt.Query)
			buf = msgp.AppendBytes(buf, stmt.Args)
		}
	} else {
		buf = msgp.AppendString(buf, m.Query)
		buf = msgp.AppendBytes(buf, m.Args)
	}
	sum := sha256.Sum256(buf)

	return hex.EncodeToString(sum[:])
}

// consistencyToWire splits an optional consistency level into the envelope's
// presence flag and value.
func consistencyToWire(c *types.Consistency) (present bool, value uint16) {
	if c == nil {
		return false, 0
	}

	return true, uint16(*c)
}

// consistencyFromWire rebuilds an optional consistency level. A version 1
// message, or a write that used the session default, has no level.
func consistencyFromWire(present bool, value uint16) *types.Consistency {
	if !present {
		return nil
	}
	c := types.Consistency(value)

	return &c
}

// batchStatement represents a single statement in a batch for msgp serialization.
type batchStatement struct {
	Query string   `msg:"query"`
	Args  msgp.Raw `msg:"args"`
}
