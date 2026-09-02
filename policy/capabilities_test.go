package policy_test

import (
	"github.com/arloliu/helix"
	"github.com/arloliu/helix/policy"
)

// Compile-time assertions that every built-in strategy and policy satisfies
// the root interfaces it is documented to implement, including the optional
// capability interfaces the client discovers by type assertion.
var (
	_ helix.ReadStrategy = (*policy.StickyRead)(nil)
	_ helix.ReadStrategy = (*policy.PrimaryOnlyRead)(nil)
	_ helix.ReadStrategy = (*policy.RoundRobinRead)(nil)

	_ helix.WriteStrategy = (*policy.ConcurrentDualWrite)(nil)
	_ helix.WriteStrategy = (*policy.SyncDualWrite)(nil)
	_ helix.WriteStrategy = (*policy.AdaptiveDualWrite)(nil)
	_ helix.StrictWriter  = (*policy.ConcurrentDualWrite)(nil)
	_ helix.StrictWriter  = (*policy.SyncDualWrite)(nil)
	_ helix.StrictWriter  = (*policy.AdaptiveDualWrite)(nil)

	_ helix.FailoverPolicy  = (*policy.ActiveFailover)(nil)
	_ helix.FailoverPolicy  = (*policy.CircuitBreaker)(nil)
	_ helix.FailoverPolicy  = (*policy.LatencyCircuitBreaker)(nil)
	_ helix.LatencyRecorder = (*policy.LatencyCircuitBreaker)(nil)

	_ helix.ProbeReporter = (*policy.AdaptiveDualWrite)(nil)

	_ helix.EventEmitterSetter = (*policy.AdaptiveDualWrite)(nil)
	_ helix.EventEmitterSetter = (*policy.CircuitBreaker)(nil)
	_ helix.EventEmitterSetter = (*policy.LatencyCircuitBreaker)(nil)

	_ helix.Instrumentable = (*policy.AdaptiveDualWrite)(nil)
	_ helix.Instrumentable = (*policy.CircuitBreaker)(nil)
	_ helix.Instrumentable = (*policy.LatencyCircuitBreaker)(nil)

	_ helix.LoggerSetter = (*policy.AdaptiveDualWrite)(nil)
	_ helix.LoggerSetter = (*policy.CircuitBreaker)(nil)
	_ helix.LoggerSetter = (*policy.LatencyCircuitBreaker)(nil)
)
