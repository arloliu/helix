package helix

import (
	"github.com/arloliu/helix/mirror"
	"github.com/arloliu/helix/types"
)

// Mirror returns the runtime control surface for the async mirror engine,
// or nil if [WithMirror] was not configured on this client.
//
// Use the returned engine to enable / disable mirroring at runtime and to
// inspect counters:
//
//	if e := client.Mirror(); e != nil {
//	    e.Disable()
//	    stats := e.Stats()
//	}
//
// The engine's lifecycle (start / stop) is managed by the helix client; do
// not call Start or Stop on the returned value.
func (c *CQLClient) Mirror() *mirror.Engine {
	return c.config.MirrorEngine
}

// cloneArgs returns a copy of args that is safe for the mirror engine to
// retain after the caller's Exec returns. The outer slice is always copied;
// each element that is a []byte is also copied so caller-side buffer reuse
// (pooling, in-place mutation) does not corrupt mirror payloads.
//
// Other types (primitives, strings, time.Time, gocql.UUID, value structs)
// are passed through. Callers that mutate non-byte-slice arg values after
// Exec returns get the standard "do not retain mutable args after a
// fire-and-forget call" semantics already implicit in the existing replay
// path.
func cloneArgs(args []any) []any {
	if len(args) == 0 {
		return nil
	}
	out := make([]any, len(args))
	for i, a := range args {
		if b, ok := a.([]byte); ok {
			cp := make([]byte, len(b))
			copy(cp, b)
			out[i] = cp
			continue
		}
		out[i] = a
	}

	return out
}

// cloneBatchEntries deep-copies batch entries so the mirror engine retains a
// snapshot independent of the caller's buffers. See [cloneArgs] for argument
// copy semantics.
func cloneBatchEntries(entries []batchEntry) []types.BatchStatement {
	if len(entries) == 0 {
		return nil
	}
	out := make([]types.BatchStatement, len(entries))
	for i, e := range entries {
		out[i] = types.BatchStatement{
			Query: e.statement,
			Args:  cloneArgs(e.args),
		}
	}

	return out
}

// dispatchMirrorQuery enqueues a captured single-statement write to the
// mirror engine. Safe to call when the engine is nil or disabled.
func (c *CQLClient) dispatchMirrorQuery(stmt string, values []any, ts int64, priority PriorityLevel) {
	if c.config.MirrorEngine == nil {
		return
	}
	c.config.MirrorEngine.TryEnqueue(types.ReplayPayload{
		Query:     stmt,
		Args:      cloneArgs(values),
		Timestamp: ts,
		Priority:  priority,
	})
}

// dispatchMirrorBatch enqueues a captured batch write to the mirror engine.
// Safe to call when the engine is nil or disabled.
func (c *CQLClient) dispatchMirrorBatch(kind BatchType, entries []batchEntry, ts int64, priority PriorityLevel) {
	if c.config.MirrorEngine == nil {
		return
	}
	c.config.MirrorEngine.TryEnqueue(types.ReplayPayload{
		IsBatch:         true,
		BatchType:       kind,
		BatchStatements: cloneBatchEntries(entries),
		Timestamp:       ts,
		Priority:        priority,
	})
}
