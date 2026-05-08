package helix

import (
	"context"
	"fmt"

	"github.com/arloliu/helix/mirror"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
)

// mirrorExecuteFunc returns a [replay.ExecuteFunc] that dispatches a
// captured mirror payload through target's full Exec path. The same
// function is used by the mirror engine for the initial dispatch and by
// the auto-built mirror replay worker for retries — so timestamps,
// dual-write strategy, and per-cluster routing on the mirror destination
// are preserved across both paths.
func mirrorExecuteFunc(target *CQLClient) replay.ExecuteFunc {
	return func(ctx context.Context, payload types.ReplayPayload) error {
		if payload.IsBatch {
			batch := target.Batch(payload.BatchType)
			for _, stmt := range payload.BatchStatements {
				batch = batch.Query(stmt.Query, stmt.Args...)
			}

			return batch.WithTimestamp(payload.Timestamp).ExecContext(ctx)
		}

		return target.Query(payload.Query, payload.Args...).
			WithTimestamp(payload.Timestamp).
			ExecContext(ctx)
	}
}

// setupMirror constructs the mirror engine (and optionally an auto-built
// mirror replay worker) according to MirrorTarget / MirrorReplayer config.
// Returns an error only when worker startup fails; topology rollback is
// the caller's responsibility.
func setupMirror(config *ClientConfig) error {
	if config.MirrorTarget == nil {
		return nil
	}

	execute := mirrorExecuteFunc(config.MirrorTarget)

	opts := []mirror.Option{mirror.WithLogger(config.Logger)}
	if config.MirrorReplayer != nil {
		opts = append(opts, mirror.WithOnError(mirrorReplayOnError(config)))
	}
	opts = append(opts, config.MirrorOptions...)

	config.MirrorEngine = mirror.NewEngine(execute, opts...)
	config.MirrorEngine.Start()

	if config.MirrorReplayer == nil {
		return nil
	}

	worker := buildMirrorReplayWorker(
		config.MirrorReplayer,
		execute,
		config.MirrorReplayWorkerOpts,
		config.Logger,
		config.Metrics,
	)
	if worker == nil {
		return nil
	}
	if err := worker.Start(); err != nil {
		config.MirrorEngine.Stop()

		return fmt.Errorf("start mirror replay worker: %w", err)
	}
	config.MirrorReplayWorker = worker

	return nil
}

// mirrorReplayOnError returns a mirror.ErrorHandler that pushes failed
// captures onto config.MirrorReplayer; if Enqueue itself fails, the
// existing OnReplayDropped callback fires so mirror and primary replay
// share one alerting path.
func mirrorReplayOnError(config *ClientConfig) mirror.ErrorHandler {
	replayer := config.MirrorReplayer
	logger := config.Logger
	onDropped := config.OnReplayDropped

	return func(p types.ReplayPayload, writeErr error) {
		enqErr := replayer.Enqueue(context.Background(), p)
		if enqErr == nil {
			return
		}
		logger.Error("mirror failure not enqueued for replay",
			"writeError", writeErr.Error(),
			"enqueueError", enqErr.Error(),
		)
		if onDropped != nil {
			onDropped(p, enqErr)
		}
	}
}

// buildMirrorReplayWorker constructs a [ReplayWorker] for the configured
// mirror replayer. Returns nil for unrecognized replayer types — the
// engine still pushes failures to the replayer in that case, but the
// application is expected to run its own worker.
func buildMirrorReplayWorker(
	replayer Replayer,
	exec replay.ExecuteFunc,
	opts []replay.WorkerOption,
	logger types.Logger,
	metrics MetricsCollector,
) ReplayWorker {
	// Auto-inject metrics first so caller-supplied options win on conflict.
	full := append([]replay.WorkerOption{replay.WithWorkerMetrics(metrics)}, opts...)

	switch r := replayer.(type) {
	case *replay.MemoryReplayer:
		return replay.NewMemoryWorker(r, exec, full...)
	case *replay.NATSReplayer:
		return replay.NewNATSWorker(r, exec, full...)
	default:
		logger.Warn("unrecognized mirror replayer type; mirror failures are still enqueued but no replay worker is auto-built — run your own worker",
			"type", fmt.Sprintf("%T", replayer),
		)

		return nil
	}
}

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
