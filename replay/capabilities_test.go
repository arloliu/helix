package replay_test

import (
	"github.com/arloliu/helix"
	"github.com/arloliu/helix/replay"
)

// Compile-time assertions that the bundled workers and replayers satisfy the
// root interfaces the client wires them through.
var (
	_ helix.Replayer     = (*replay.MemoryReplayer)(nil)
	_ helix.Replayer     = (*replay.NATSReplayer)(nil)
	_ helix.ReplayWorker = (*replay.Worker)(nil)

	_ helix.Instrumentable = (*replay.Worker)(nil)
)
