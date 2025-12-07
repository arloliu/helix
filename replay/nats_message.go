package replay

import "github.com/tinylib/msgp/msgp"

//go:generate msgp -unexported

// natsReplayMessage is the MessagePack-serializable message format for NATS.
// This struct is used for efficient serialization of replay payloads.
type natsReplayMessage struct {
	TargetCluster   string           `msg:"target_cluster"`
	Query           string           `msg:"query"`
	Args            msgp.Raw         `msg:"args"`
	Timestamp       int64            `msg:"timestamp"`
	Priority        int              `msg:"priority"`
	IsBatch         bool             `msg:"is_batch"`
	BatchType       uint8            `msg:"batch_type"`
	BatchStatements []batchStatement `msg:"batch_statements"`
}

// batchStatement represents a single statement in a batch for msgp serialization.
type batchStatement struct {
	Query string   `msg:"query"`
	Args  msgp.Raw `msg:"args"`
}
