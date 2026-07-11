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

// stopMirrorComponents stops the mirror engine and mirror replay worker
// that setupMirror may have started, nil-guarding each so it is safe to call
// regardless of which mirror mode (if any) was configured. Callers use this
// to unwind mirror startup when a later NewCQLClient initialization step
// fails after setupMirror has already succeeded.
func stopMirrorComponents(config *ClientConfig) {
	if config.MirrorEngine != nil {
		config.MirrorEngine.Stop()
	}
	if config.MirrorReplayWorker != nil {
		config.MirrorReplayWorker.Stop()
	}
}

// setupMirror constructs the mirror engine and (in target mode) an
// optional auto-built replay worker. Two mutually exclusive deployment
// modes are recognized:
//
//   - Target mode (set via WithMirror): the engine writes directly to a
//     second helix CQLClient in this process. Optionally pairs with
//     WithMirrorReplayer for in-process retry of failed mirror writes.
//   - Publisher mode (set via WithMirrorPublisher): the engine publishes
//     captures to a Replayer; a separate consumer binary built via
//     NewMirrorWorker performs the actual writes.
//
// Returns an error if both modes are configured or when worker startup
// fails. Topology rollback is the caller's responsibility.
func setupMirror(config *ClientConfig) error {
	if config.mirrorTargetSet && config.mirrorPublisherSet {
		return types.ErrMirrorModeConflict
	}

	switch {
	case config.mirrorTargetSet:
		return setupMirrorTargetMode(config)
	case config.mirrorPublisherSet:
		return setupMirrorPublisherMode(config)
	default:
		return nil
	}
}

func setupMirrorTargetMode(config *ClientConfig) error {
	if config.MirrorTarget == nil {
		return types.ErrNilMirrorTarget
	}

	execute := mirrorExecuteFunc(config.MirrorTarget)

	opts := defaultMirrorOptions(config)
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

func setupMirrorPublisherMode(config *ClientConfig) error {
	if config.MirrorPublisher == nil {
		return types.ErrNilMirrorPublisher
	}

	opts := append(defaultMirrorOptions(config), config.MirrorOptions...)
	config.MirrorEngine = mirror.NewEngine(config.MirrorPublisher.Enqueue, opts...)
	config.MirrorEngine.Start()

	return nil
}

// defaultMirrorOptions returns the mirror engine options that helix injects
// before any caller-supplied [mirror.Option] values: the client logger, and
// — when the configured [MetricsCollector] also satisfies the optional
// [types.MirrorMetrics] interface — the mirror metrics collector. These
// come first so caller options win on conflict.
func defaultMirrorOptions(config *ClientConfig) []mirror.Option {
	opts := []mirror.Option{mirror.WithLogger(config.Logger)}
	if mm, ok := config.Metrics.(types.MirrorMetrics); ok {
		opts = append(opts, mirror.WithMetrics(mm))
	}

	return opts
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

// NewMirrorWorker constructs a [ReplayWorker] for the consumer side of
// publisher-mode mirroring. It binds the worker to the same execute path
// the in-process mirror engine would use, so client-generated timestamps,
// dual-write strategy, and per-cluster routing on the mirror destination
// are preserved on every retry.
//
// The replayer's concrete type selects the worker implementation:
//   - [*replay.MemoryReplayer] uses [replay.NewMemoryWorker]
//   - [*replay.NATSReplayer]   uses [replay.NewNATSWorker]
//
// Returns an error for any other replayer type. The caller owns the
// returned worker's lifecycle and must call Start / Stop. The mirror
// target's lifecycle is also caller-owned.
//
// Workers in the consumer binary typically run their own observability
// stack — pass [replay.WithWorkerMetrics] / [replay.WithWorkerLogger] (or
// equivalent) via opts. No metrics are auto-injected.
//
// Example:
//
//	mirrorTarget, _ := helix.NewCQLClient(newA, newB)
//	worker, err := helix.NewMirrorWorker(natsReplayer, mirrorTarget,
//	    replay.WithMaxAttempts(5),
//	    replay.WithRetryDelay(100*time.Millisecond),
//	)
//	if err != nil { return err }
//	_ = worker.Start()
//	defer worker.Stop()
//	defer mirrorTarget.Close()
//
// Parameters:
//   - replayer: The transport that holds captured mirror writes.
//   - target:   The destination helix CQLClient.
//   - opts:     [replay.WorkerOption] values applied to the constructed worker.
//
// Returns:
//   - ReplayWorker: The constructed worker.
//   - error:        Non-nil if replayer's concrete type is not supported.
func NewMirrorWorker(replayer Replayer, target *CQLClient, opts ...replay.WorkerOption) (ReplayWorker, error) {
	if target == nil {
		return nil, types.ErrNilMirrorTarget
	}

	return newReplayWorkerFor(replayer, mirrorExecuteFunc(target), opts)
}

// newReplayWorkerFor type-switches a [Replayer] to its matching worker
// constructor. Shared by [NewMirrorWorker] and [buildMirrorReplayWorker].
func newReplayWorkerFor(replayer Replayer, exec replay.ExecuteFunc, opts []replay.WorkerOption) (ReplayWorker, error) {
	switch r := replayer.(type) {
	case *replay.MemoryReplayer:
		return replay.NewMemoryWorker(r, exec, opts...), nil
	case *replay.NATSReplayer:
		return replay.NewNATSWorker(r, exec, opts...), nil
	default:
		return nil, fmt.Errorf("unsupported replayer type %T (supported: *replay.MemoryReplayer, *replay.NATSReplayer)", replayer)
	}
}

// buildMirrorReplayWorker constructs the auto-wired [ReplayWorker] used in
// target-mode mirroring with [WithMirrorReplayer]. Unrecognized replayer
// types yield a nil worker plus a warning log — the engine still pushes
// failures to the replayer, but the application must run its own worker.
func buildMirrorReplayWorker(
	replayer Replayer,
	exec replay.ExecuteFunc,
	opts []replay.WorkerOption,
	logger types.Logger,
	metrics MetricsCollector,
) ReplayWorker {
	// Auto-inject metrics first so caller-supplied options win on conflict.
	full := append([]replay.WorkerOption{replay.WithWorkerMetrics(metrics)}, opts...)

	worker, err := newReplayWorkerFor(replayer, exec, full)
	if err != nil {
		logger.Warn("unrecognized mirror replayer type; mirror failures are still enqueued but no replay worker is auto-built — run your own worker",
			"type", fmt.Sprintf("%T", replayer),
		)

		return nil
	}

	return worker
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

// mirrorTargetCluster is the conventional TargetCluster value attached to
// every mirror payload. Mirror writes target the mirror destination as a
// single logical sink — the destination's own write strategy (dual-write,
// per-cluster routing) handles cluster fan-out internally — so no real
// per-cluster routing happens at the source. The value is required by
// transports that route by cluster (e.g., NATSReplayer subjects):
// without it those transports drop the message.
const mirrorTargetCluster = ClusterA

// dispatchMirrorQuery enqueues a captured single-statement write to the
// mirror engine. Safe to call when the engine is nil or disabled.
func (c *CQLClient) dispatchMirrorQuery(stmt string, values []any, ts int64, priority PriorityLevel) {
	if c.config.MirrorEngine == nil {
		return
	}
	c.config.MirrorEngine.TryEnqueue(types.ReplayPayload{
		TargetCluster: mirrorTargetCluster,
		Query:         stmt,
		Args:          cloneArgs(values),
		Timestamp:     ts,
		Priority:      priority,
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
		TargetCluster:   mirrorTargetCluster,
		BatchType:       kind,
		BatchStatements: cloneBatchEntries(entries),
		Timestamp:       ts,
		Priority:        priority,
	})
}
