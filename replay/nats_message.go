package replay

import "github.com/tinylib/msgp/msgp"

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
}

// batchStatement represents a single statement in a batch for msgp serialization.
type batchStatement struct {
	Query string   `msg:"query"`
	Args  msgp.Raw `msg:"args"`
}
