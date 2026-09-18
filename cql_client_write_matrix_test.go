package helix

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/policy"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// This file pins the dual-write pipeline's classification as one matrix:
// every write strategy, crossed with what each cluster's leg returns,
// crossed with whether the write is replayed,
// dropped for want of a replayer, or strict.
// The expectations describe the behaviour the client has today,
// so a refactor of the write path must keep this test green unchanged
// and a deliberate behaviour change must edit the rule it changes.
//
// The grid is pruned where two cells would run the same code:
//
//   - The full grid issues Query.ExecContext. Batch.ExecContext reaches the
//     same executeWriteWithReplay with a different leg closure, so
//     TestWriteClassificationBatchEntry runs it on the two failure shapes
//     only. NonIdempotent sets the same strict flag Strict does, so
//     TestWriteClassificationNonIdempotentIsStrict checks one shape.
//     CAS runs on one cluster through selectClusterForCAS, and Mirror is a
//     separate dispatch after the write; neither is a dual write, and
//     neither is here.
//   - Strict never reads the replayer, so strict cells run with one
//     configured only: a strict path that started to enqueue shows up there,
//     and the replayer-less strict cell would observe the same thing.
//   - Only cluster B is ever degraded, draining, cancelled on, timed out
//     or rejecting; the mirror images run the same per-leg code.
//     Cancellation also runs on cluster A, because SyncDualWrite writes A
//     first and a cancellation there changes what happens to B.
//   - Both clusters draining returns before the strategy runs, so it is
//     one test (TestWriteClassificationBothDraining), not a column.

// writeStrategyKind names the write strategy a cell runs under.
type writeStrategyKind string

const (
	strategyDefault    writeStrategyKind = "default" // no WithWriteStrategy: the client's inline concurrent write
	strategyConcurrent writeStrategyKind = "concurrent"
	strategySync       writeStrategyKind = "sync"
	strategyAdaptive   writeStrategyKind = "adaptive"
	// strategyDegraded is AdaptiveDualWrite with cluster B degraded by
	// ForceDegrade, so B's leg is fire-and-forget (or skipped, if strict).
	strategyDegraded writeStrategyKind = "adaptive-degraded-B"
)

// legResult names what one cluster does with its write leg.
type legResult string

const (
	legResOK       legResult = "ok"
	legResFail     legResult = "fail"     // the cluster is unreachable
	legResCancel   legResult = "cancel"   // the caller cancels its context while the leg is in flight; the leg returns its own context's error
	legResTimeout  legResult = "timeout"  // the cluster never answers; WithClusterWriteTimeout ends the leg
	legResDraining legResult = "draining" // the cluster is draining, so its leg is skipped before the session is touched
	legResRejected legResult = "rejected" // the coordinator rejected the statement itself
)

// writeOutcome is what clusters A and B each do with one write.
type writeOutcome struct {
	a, b legResult
}

func (o writeOutcome) String() string { return string(o.a) + "-" + string(o.b) }

// writeMode names how the write is issued and what backs it.
type writeMode string

const (
	writeModeReplayer writeMode = "replayer"    // plain write, replayer configured
	writeModeNone     writeMode = "no-replayer" // plain write, no replayer
	writeModeStrict   writeMode = "strict"      // Strict() write, replayer configured
)

// writeEntry names the public call that issues the write.
type writeEntry string

const (
	writeEntryQuery         writeEntry = "Query"
	writeEntryBatch         writeEntry = "Batch"
	writeEntryNonIdempotent writeEntry = "NonIdempotent" // Query.NonIdempotent, never Strict
)

// legErrClass classifies what the caller's error reports for one leg.
type legErrClass string

const (
	lecNone     legErrClass = "nil"
	lecCluster  legErrClass = "cluster"
	lecTimeout  legErrClass = "cluster-timeout" // types.ErrClusterTimeout
	lecCtx      legErrClass = "ctx"
	lecRejected legErrClass = "rejected" // types.ErrStatementRejected, driver error still in the chain
	lecAsync    legErrClass = "async"    // types.ErrWriteAsync
	lecDraining legErrClass = "draining" // types.ErrClusterDraining
	lecDegraded legErrClass = "degraded" // types.ErrClusterDegraded
)

// Shapes of the caller's error.
const (
	shapeNil       = "nil"
	shapeDual      = "dual"        // *types.DualClusterError matching types.ErrBothClustersFailed
	shapePartial   = "partial"     // *types.PartialWriteError
	shapeNoSyncAck = "no-sync-ack" // *types.NoSynchronousAckError matching types.ErrNoSynchronousAck
)

// Replay results a NoSynchronousAckError can carry.
const (
	replayAdmitted = "nil"
	replayNone     = "no-replayer" // types.ErrNoReplayer
)

// The write matrix starts every cluster from auto-refresh stats that are neither zero nor fresh,
// so a leg that reset them is as visible as one that advanced them.
const (
	writeSeedFailures    int32 = 2
	writeSeedLastSuccess int64 = 1_000
)

var writeStrategies = []writeStrategyKind{
	strategyDefault, strategyConcurrent, strategySync, strategyAdaptive, strategyDegraded,
}

var writeOutcomes = []writeOutcome{
	{legResOK, legResOK},
	{legResOK, legResFail},
	{legResFail, legResOK},
	{legResFail, legResFail},
	{legResOK, legResCancel},
	{legResCancel, legResOK},
	{legResOK, legResTimeout},
	{legResOK, legResDraining},
	{legResOK, legResRejected},
}

var writeModes = []writeMode{writeModeReplayer, writeModeNone, writeModeStrict}

// writeErrShape is the caller's error, taken apart.
type writeErrShape struct {
	kind    string
	a, b    legErrClass // per-leg results; for a partial, only the unacknowledged leg's cause
	unacked ClusterID   // partial only
	replay  string      // no-sync-ack only
}

// writeObservation is everything the matrix records about one write.
// Cluster lists hold one entry per call, A's before B's.
type writeObservation struct {
	err            writeErrShape
	contacted      []ClusterID // sessions the write reached
	writeTotal     []ClusterID // IncWriteTotal
	writeErrors    []ClusterID // IncWriteError, from the client or the strategy's background leg
	writeAsync     []ClusterID // IncWriteAsync
	writeDropped   []ClusterID // IncWriteDropped
	writeSkipped   []ClusterID // StrictMetrics.IncWriteSkipped
	callerExpired  []ClusterID // CallerContextMetrics.IncWriteCallerExpired
	replayed       []ClusterID // payload targets the replayer received
	replayEnqueued []ClusterID // IncReplayEnqueued
	replayDropped  []ClusterID // IncReplayDropped
	healthOK       []ClusterID // clusters whose auto-refresh lastSuccess moved
	healthFail     []ClusterID // clusters whose auto-refresh failure counter advanced
	degraded       []ClusterID // clusters AdaptiveDualWrite holds degraded afterwards
	policyCalls    []string    // every FailoverPolicy / LatencyRecorder call
	readStrategy   []string    // every ReadStrategy.OnSuccess / OnFailure call
}

// writeMatrixSession is a cql.Session whose every write does what result says.
// It reuses the read matrix's query and batch for everything but the write itself,
// overriding the builder methods so the write path keeps holding this type.
type writeMatrixSession struct {
	result legResult
	cancel context.CancelFunc // the caller's; legResCancel calls it
	calls  atomic.Int32
}

var _ cql.Session = (*writeMatrixSession)(nil)

type writeMatrixQuery struct {
	*matrixQuery
	ws *writeMatrixSession
}

type writeMatrixBatch struct {
	*matrixBatch
	ws *writeMatrixSession
}

func (s *writeMatrixSession) Query(_ string, _ ...any) cql.Query {
	s.calls.Add(1)
	return &writeMatrixQuery{matrixQuery: &matrixQuery{}, ws: s}
}

func (s *writeMatrixSession) Batch(_ cql.BatchType) cql.Batch {
	s.calls.Add(1)
	return &writeMatrixBatch{matrixBatch: &matrixBatch{}, ws: s}
}

func (s *writeMatrixSession) Close() {}

// exec is one write leg.
// ctx is the leg's own context: the caller's, a leg deadline's child of it,
// or a fire-and-forget leg's background one.
func (s *writeMatrixSession) exec(ctx context.Context) error {
	switch s.result {
	case legResFail:
		return errMatrixCluster
	case legResRejected:
		return errMatrixStatement
	case legResCancel:
		s.cancel()
		return ctx.Err()
	case legResTimeout:
		<-ctx.Done()
		return ctx.Err()
	case legResOK, legResDraining:
	}

	return nil
}

func (q *writeMatrixQuery) Consistency(_ cql.Consistency) cql.Query       { return q }
func (q *writeMatrixQuery) SerialConsistency(_ cql.Consistency) cql.Query { return q }
func (q *writeMatrixQuery) PageSize(_ int) cql.Query                      { return q }
func (q *writeMatrixQuery) PageState(_ []byte) cql.Query                  { return q }
func (q *writeMatrixQuery) WithTimestamp(_ int64) cql.Query               { return q }
func (q *writeMatrixQuery) Exec() error                                   { return q.ws.exec(context.Background()) }
func (q *writeMatrixQuery) ExecContext(ctx context.Context) error         { return q.ws.exec(ctx) }

func (b *writeMatrixBatch) Query(_ string, _ ...any) cql.Batch            { return b }
func (b *writeMatrixBatch) Consistency(_ cql.Consistency) cql.Batch       { return b }
func (b *writeMatrixBatch) SerialConsistency(_ cql.Consistency) cql.Batch { return b }
func (b *writeMatrixBatch) WithTimestamp(_ int64) cql.Batch               { return b }
func (b *writeMatrixBatch) Exec() error                                   { return b.ws.exec(context.Background()) }
func (b *writeMatrixBatch) ExecContext(ctx context.Context) error         { return b.ws.exec(ctx) }

// writeMatrixMetrics records the write and replay counters per cluster.
// It embeds NopMetrics,
// so it also implements every optional metrics interface the client type-asserts for.
type writeMatrixMetrics struct {
	metrics.NopMetrics
	mu    sync.Mutex
	calls map[string][]ClusterID
}

var (
	_ types.MetricsCollector     = (*writeMatrixMetrics)(nil)
	_ types.StrictMetrics        = (*writeMatrixMetrics)(nil)
	_ types.CallerContextMetrics = (*writeMatrixMetrics)(nil)
)

func (m *writeMatrixMetrics) record(name string, c ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calls == nil {
		m.calls = make(map[string][]ClusterID)
	}
	m.calls[name] = append(m.calls[name], c)
}

// get returns the calls to name, A's before B's.
func (m *writeMatrixMetrics) get(name string) []ClusterID {
	m.mu.Lock()
	defer m.mu.Unlock()

	return sortClusters(m.calls[name])
}

func (m *writeMatrixMetrics) IncWriteTotal(c ClusterID)         { m.record("write_total", c) }
func (m *writeMatrixMetrics) IncWriteError(c ClusterID)         { m.record("write_error", c) }
func (m *writeMatrixMetrics) IncWriteAsync(c ClusterID)         { m.record("write_async", c) }
func (m *writeMatrixMetrics) IncWriteDropped(c ClusterID)       { m.record("write_dropped", c) }
func (m *writeMatrixMetrics) IncWriteSkipped(c ClusterID)       { m.record("write_skipped", c) }
func (m *writeMatrixMetrics) IncWriteCallerExpired(c ClusterID) { m.record("write_caller_expired", c) }
func (m *writeMatrixMetrics) IncReplayEnqueued(c ClusterID)     { m.record("replay_enqueued", c) }
func (m *writeMatrixMetrics) IncReplayDropped(c ClusterID)      { m.record("replay_dropped", c) }

// writeMatrixPolicy is a failover policy, and a latency recorder,
// that records every call it gets.
// A write must reach none of them.
type writeMatrixPolicy struct {
	mu    sync.Mutex
	calls []string
}

var (
	_ FailoverPolicy  = (*writeMatrixPolicy)(nil)
	_ LatencyRecorder = (*writeMatrixPolicy)(nil)
)

func (p *writeMatrixPolicy) record(call string, c ClusterID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, call+"/"+string(c))
}

func (p *writeMatrixPolicy) ShouldFailover(c ClusterID, _ error) bool {
	p.record("ShouldFailover", c)
	return false
}
func (p *writeMatrixPolicy) RecordFailure(c ClusterID)                  { p.record("RecordFailure", c) }
func (p *writeMatrixPolicy) RecordSuccess(c ClusterID)                  { p.record("RecordSuccess", c) }
func (p *writeMatrixPolicy) RecordLatency(c ClusterID, _ time.Duration) { p.record("RecordLatency", c) }

// writeMatrixReplayer records every payload it is handed
// and whether the context it was handed had already ended:
// an enqueue must survive the caller's cancellation.
type writeMatrixReplayer struct {
	mu       sync.Mutex
	payloads []types.ReplayPayload
	ctxDone  int
}

func (r *writeMatrixReplayer) Enqueue(ctx context.Context, p types.ReplayPayload) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.payloads = append(r.payloads, p)
	if ctx.Err() != nil {
		r.ctxDone++
	}

	return nil
}

func sortClusters(cs []ClusterID) []ClusterID {
	out := slices.Clone(cs)
	slices.Sort(out)

	return out
}

func classifyLegErr(err error) legErrClass {
	switch {
	case err == nil:
		return lecNone
	case errors.Is(err, types.ErrWriteAsync):
		return lecAsync
	case errors.Is(err, types.ErrClusterDraining):
		return lecDraining
	case errors.Is(err, types.ErrClusterDegraded):
		return lecDegraded
	case errors.Is(err, types.ErrClusterTimeout):
		return lecTimeout
	case errors.Is(err, types.ErrStatementRejected):
		if !errors.Is(err, errStatementDriver) {
			return legErrClass("statement error lost its driver error")
		}

		return lecRejected
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return lecCtx
	case errors.Is(err, errMatrixCluster):
		return lecCluster
	}

	return legErrClass("unexpected: " + err.Error())
}

// classifyWriteResult takes the caller's error apart.
func classifyWriteResult(err error) writeErrShape {
	var (
		noAck   *types.NoSynchronousAckError
		partial *types.PartialWriteError
		dual    *types.DualClusterError
	)
	switch {
	case err == nil:
		return writeErrShape{kind: shapeNil}
	case errors.As(err, &noAck):
		shape := writeErrShape{
			kind: shapeNoSyncAck, a: classifyLegErr(noAck.ResultA), b: classifyLegErr(noAck.ResultB),
		}
		switch {
		case !errors.Is(err, types.ErrNoSynchronousAck):
			shape.kind = "no-sync-ack without its sentinel"
		case noAck.Replay == nil:
			shape.replay = replayAdmitted
		case errors.Is(noAck.Replay, types.ErrNoReplayer):
			shape.replay = replayNone
		default:
			shape.replay = "unexpected: " + noAck.Replay.Error()
		}

		return shape
	case errors.As(err, &partial):
		shape := writeErrShape{kind: shapePartial, unacked: partial.Unacknowledged}
		if partial.Acknowledged == partial.Unacknowledged {
			shape.kind = "partial acknowledged by the cluster it did not acknowledge"
		}
		if partial.Unacknowledged == ClusterA {
			shape.a = classifyLegErr(partial.Cause)
		} else {
			shape.b = classifyLegErr(partial.Cause)
		}

		return shape
	case errors.As(err, &dual):
		shape := writeErrShape{kind: shapeDual, a: classifyLegErr(dual.ErrorA), b: classifyLegErr(dual.ErrorB)}
		if !errors.Is(err, types.ErrBothClustersFailed) {
			shape.kind = "dual without its sentinel"
		}

		return shape
	}

	return writeErrShape{kind: "unexpected: " + err.Error()}
}

// writeStrategyFor builds the strategy a cell runs under,
// and the adaptive strategy itself when there is one so its state can be read.
// The adaptive strategy degrades on the first strike,
// so every strike a write records is visible through IsDegraded;
// the latency floor and cap are raised far above anything a mock leg takes,
// so only errors can strike.
func writeStrategyFor(kind writeStrategyKind) (WriteStrategy, *policy.AdaptiveDualWrite) {
	switch kind {
	case strategyConcurrent:
		return policy.NewConcurrentDualWrite(), nil
	case strategySync:
		return policy.NewSyncDualWrite(), nil
	case strategyAdaptive, strategyDegraded:
		a := policy.NewAdaptiveDualWrite(
			policy.WithAdaptiveStrikeThreshold(1),
			policy.WithAdaptiveMinFloor(time.Minute),
			policy.WithAdaptiveAbsoluteMax(time.Hour),
		)

		return a, a
	case strategyDefault:
	}

	return nil, nil
}

// observeWrite builds a client for the cell,
// issues one write through entry,
// waits for every leg to finish,
// and records what the write pipeline did.
// It also returns the payloads the replayer received.
func observeWrite(
	t *testing.T,
	entry writeEntry,
	strategy writeStrategyKind,
	outcome writeOutcome,
	mode writeMode,
) (writeObservation, []types.ReplayPayload) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sessionA := &writeMatrixSession{result: outcome.a, cancel: cancel}
	sessionB := &writeMatrixSession{result: outcome.b, cancel: cancel}
	m := &writeMatrixMetrics{}
	fp := &writeMatrixPolicy{}
	rs := &trackingReadStrategy{preferred: ClusterA}
	replayer := &writeMatrixReplayer{}

	opts := []Option{WithMetrics(m), WithFailoverPolicy(fp), WithReadStrategy(rs)}
	ws, adaptive := writeStrategyFor(strategy)
	if ws != nil {
		opts = append(opts, WithWriteStrategy(ws))
	}
	if mode != writeModeNone {
		opts = append(opts, WithReplayer(replayer))
	}
	if outcome.a == legResTimeout || outcome.b == legResTimeout {
		opts = append(opts, WithClusterWriteTimeout(matrixLegTimeout))
	}

	client, err := NewCQLClient(sessionA, sessionB, opts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	if strategy == strategyDegraded {
		adaptive.ForceDegrade(ClusterB)
	}
	client.drainA.Store(outcome.a == legResDraining)
	client.drainB.Store(outcome.b == legResDraining)
	for _, c := range []ClusterID{ClusterA, ClusterB} {
		stats := client.statsForCluster(c)
		stats.consecutiveFailures.Store(writeSeedFailures)
		stats.lastSuccessNanos.Store(writeSeedLastSuccess)
	}

	writeErr := runWriteEntry(ctx, client, entry, mode == writeModeStrict)
	// Close waits for every write in progress and every fire-and-forget leg's completion callback,
	// so nothing below can still move.
	client.Close()

	obs := writeObservation{
		err:            classifyWriteResult(writeErr),
		writeTotal:     m.get("write_total"),
		writeErrors:    m.get("write_error"),
		writeAsync:     m.get("write_async"),
		writeDropped:   m.get("write_dropped"),
		writeSkipped:   m.get("write_skipped"),
		callerExpired:  m.get("write_caller_expired"),
		replayEnqueued: m.get("replay_enqueued"),
		replayDropped:  m.get("replay_dropped"),
		policyCalls:    fp.calls,
	}
	sessions := map[ClusterID]*writeMatrixSession{ClusterA: sessionA, ClusterB: sessionB}
	for _, c := range []ClusterID{ClusterA, ClusterB} {
		for range sessions[c].calls.Load() {
			obs.contacted = append(obs.contacted, c)
		}
		stats := client.statsForCluster(c)
		if stats.consecutiveFailures.Load() > writeSeedFailures {
			obs.healthFail = append(obs.healthFail, c)
		}
		if stats.lastSuccessNanos.Load() != writeSeedLastSuccess {
			obs.healthOK = append(obs.healthOK, c)
		}
		if adaptive != nil && adaptive.IsDegraded(c) {
			obs.degraded = append(obs.degraded, c)
		}
	}
	for _, c := range rs.OnSuccessCalls {
		obs.readStrategy = append(obs.readStrategy, "OnSuccess/"+string(c))
	}
	for _, c := range rs.OnFailureCalls {
		obs.readStrategy = append(obs.readStrategy, "OnFailure/"+string(c))
	}
	for _, p := range replayer.payloads {
		obs.replayed = append(obs.replayed, p.TargetCluster)
	}
	obs.replayed = sortClusters(obs.replayed)
	require.Zero(t, replayer.ctxDone, "a replay enqueue was handed a context that had already ended")

	return obs, replayer.payloads
}

// runWriteEntry issues one write through entry and returns the caller-visible error.
func runWriteEntry(ctx context.Context, client *CQLClient, entry writeEntry, strict bool) error {
	switch entry {
	case writeEntryBatch:
		b := client.Batch(LoggedBatch).Query("UPDATE t SET v = ? WHERE k = ?", 1, 1)
		if strict {
			b = b.Strict()
		}

		return b.ExecContext(ctx)
	case writeEntryNonIdempotent:
		return client.Query("UPDATE c SET n = n + 1 WHERE k = ?", 1).NonIdempotent().ExecContext(ctx)
	case writeEntryQuery:
	}
	q := client.Query("INSERT INTO t (k, v) VALUES (?, ?)", 1, 1)
	if strict {
		q = q.Strict()
	}

	return q.ExecContext(ctx)
}

// legExpect is what one leg contributes to a cell.
type legExpect struct {
	class         legErrClass // the leg's result as the strategy hands it back
	contacted     bool
	writeError    bool
	async         bool
	skipped       bool
	callerExpired bool
	replay        bool // needs replay, unless the write fails as a whole
	replayLater   bool // the replay need is known only when a background leg completes
	healthOK      bool
	healthFail    bool
	strike        bool // AdaptiveDualWrite records a strike for it
}

// failed reports whether the leg's result counts toward a dual failure.
func (l legExpect) failed() bool {
	switch l.class {
	case lecCluster, lecTimeout, lecCtx, lecRejected:
		return true
	case lecNone, lecAsync, lecDraining, lecDegraded:
	}

	return false
}

// expectLeg states what a leg that ran synchronously does for result.
func expectLeg(result legResult) legExpect {
	switch result {
	case legResOK:
		return legExpect{class: lecNone, contacted: true, healthOK: true}
	case legResFail:
		// Unreachable is a connectivity error, so it also counts toward auto-refresh.
		return legExpect{class: lecCluster, contacted: true, writeError: true, replay: true, healthFail: true, strike: true}
	case legResCancel:
		// The caller's doing: replayed, counted as caller-expired,
		// never a write error, a health signal, or a strike.
		return legExpect{class: lecCtx, contacted: true, callerExpired: true, replay: true}
	case legResTimeout:
		// The leg deadline makes it ErrClusterTimeout, a connectivity error,
		// so it counts exactly like an unreachable cluster.
		// It never reaches the failover policy:
		// see TestWriteClusterTimeoutNeverReachesFailoverPolicy.
		return legExpect{class: lecTimeout, contacted: true, writeError: true, replay: true, healthFail: true, strike: true}
	case legResDraining:
		// Skipped before the session is touched, and replayed once the drain lifts.
		return legExpect{class: lecDraining, skipped: true, replay: true}
	case legResRejected:
		// A rejected statement is still a failed write leg:
		// counted, replayed, and an AdaptiveDualWrite strike.
		// The strike is current behaviour, not a settled contract:
		// the read path stopped treating a rejection as a health signal,
		// but AdaptiveDualWrite's isSkippedErr does not exclude it,
		// so malformed CQL can degrade a healthy write leg.
		// It is not an auto-refresh failure:
		// the classifier counts only connectivity errors.
		return legExpect{class: lecRejected, contacted: true, writeError: true, replay: true, strike: true}
	}

	return legExpect{class: legErrClass("unknown result " + string(result))}
}

// expectBackgroundLeg states what a fire-and-forget leg does for result.
// The foreground sees ErrWriteAsync whatever happens;
// the background leg runs on its own context,
// reports its outcome to the auto-refresh stats,
// counts its own write error through the strategy,
// and is replayed only if it fails.
func expectBackgroundLeg(result legResult) legExpect {
	leg := legExpect{class: lecAsync, contacted: true, async: true}
	switch result {
	case legResOK, legResCancel:
		// A cancelled caller changes nothing here:
		// the background leg runs on a context the caller's cancellation cannot reach,
		// so it succeeds.
		leg.healthOK = true
	case legResFail, legResTimeout:
		// The leg deadline still bounds a background leg.
		leg.writeError, leg.replayLater, leg.healthFail = true, true, true
	case legResRejected:
		leg.writeError, leg.replayLater = true, true
	case legResDraining:
		// The strategy still hands back ErrWriteAsync,
		// but the leg never left the client: the draining check ran first.
		// It is counted as skipped, never as async,
		// and replayed at once rather than when a background result arrives.
		return legExpect{class: lecAsync, skipped: true, replay: true}
	}

	return leg
}

// currentWriteBehaviour states, rule by rule,
// what the write pipeline does today for one cell of the matrix.
func currentWriteBehaviour(strategy writeStrategyKind, outcome writeOutcome, mode writeMode) writeObservation {
	strict := mode == writeModeStrict
	a := expectLeg(outcome.a)
	var b legExpect
	switch {
	case strategy == strategyDegraded && strict:
		// Strict never fire-and-forgets:
		// the degraded cluster is skipped without being contacted,
		// whatever it would have done.
		b = legExpect{class: lecDegraded, skipped: true}
	case strategy == strategyDegraded:
		b = expectBackgroundLeg(outcome.b)
	case strategy == strategySync && outcome.a == legResCancel:
		// SyncDualWrite writes A first and, finding the caller's context done,
		// hands back its error for B without dispatching B.
		// A leg that was never sent is not caller-expired either.
		// Current behaviour, flagged: B was never attempted,
		// yet the write is reported as a dual failure and nothing is replayed,
		// where the concurrent strategies replay A and return nil.
		b = legExpect{class: lecCtx, replay: true}
	default:
		b = expectLeg(outcome.b)
	}

	obs := writeObservation{writeTotal: []ClusterID{ClusterA, ClusterB}}
	if strict {
		obs.err = strictWriteShape(a, b)
	} else {
		obs.err = plainWriteShape(a, b, mode)
		if obs.err.kind != shapeDual {
			applyReplay(&obs, a, b, mode)
		}
	}
	for _, leg := range []struct {
		c ClusterID
		legExpect
	}{{ClusterA, a}, {ClusterB, b}} {
		appendIf(&obs.contacted, leg.contacted, leg.c)
		appendIf(&obs.writeErrors, leg.writeError, leg.c)
		appendIf(&obs.writeAsync, leg.async, leg.c)
		appendIf(&obs.writeSkipped, leg.skipped, leg.c)
		appendIf(&obs.callerExpired, leg.callerExpired, leg.c)
		appendIf(&obs.healthOK, leg.healthOK, leg.c)
		appendIf(&obs.healthFail, leg.healthFail, leg.c)
		// Strikes exist only on AdaptiveDualWrite; ForceDegrade latches B.
		adaptive := strategy == strategyAdaptive || strategy == strategyDegraded
		appendIf(&obs.degraded, adaptive && leg.strike || strategy == strategyDegraded && leg.c == ClusterB, leg.c)
	}
	// No write, of any shape, reaches the failover policy or the read strategy;
	// see the observation hub's writeLeg.

	return obs
}

// plainWriteShape states the caller's result for a replaying write:
// success when either cluster acknowledged,
// a dual failure when both legs genuinely failed,
// and otherwise no synchronous acknowledgement.
func plainWriteShape(a, b legExpect, mode writeMode) writeErrShape {
	switch {
	case a.class == lecNone && b.class == lecNone:
		return writeErrShape{kind: shapeNil}
	case a.failed() && b.failed():
		// Both legs failed: the caller retries, so nothing is replayed.
		return writeErrShape{kind: shapeDual, a: a.class, b: b.class}
	case a.class == lecNone || b.class == lecNone:
		return writeErrShape{kind: shapeNil}
	}
	// No cluster acknowledged.
	// The replay error is the first leg's that was enqueued now;
	// only A is ever enqueued now in these cells.
	replay := replayAdmitted
	if mode == writeModeNone {
		replay = replayNone
	}

	return writeErrShape{kind: shapeNoSyncAck, a: a.class, b: b.class, replay: replay}
}

// strictWriteShape states the caller's result for a strict write: any leg
// that did not acknowledge is reported, and nothing is replayed.
func strictWriteShape(a, b legExpect) writeErrShape {
	switch {
	case a.class != lecNone && b.class != lecNone:
		return writeErrShape{kind: shapeDual, a: a.class, b: b.class}
	case a.class != lecNone:
		return writeErrShape{kind: shapePartial, a: a.class, unacked: ClusterA}
	case b.class != lecNone:
		return writeErrShape{kind: shapePartial, b: b.class, unacked: ClusterB}
	}

	return writeErrShape{kind: shapeNil}
}

// applyReplay records the replay each leg gets:
// one enqueue per leg that needs it,
// or one dropped replay per such leg when there is no replayer.
func applyReplay(obs *writeObservation, a, b legExpect, mode writeMode) {
	for _, leg := range []struct {
		c ClusterID
		legExpect
	}{{ClusterA, a}, {ClusterB, b}} {
		if !leg.replay && !leg.replayLater {
			continue
		}
		if mode == writeModeNone {
			obs.replayDropped = append(obs.replayDropped, leg.c)
			continue
		}
		obs.replayed = append(obs.replayed, leg.c)
		obs.replayEnqueued = append(obs.replayEnqueued, leg.c)
	}
}

func appendIf(list *[]ClusterID, cond bool, c ClusterID) {
	if cond {
		*list = append(*list, c)
	}
}

func TestWriteClassificationMatrix(t *testing.T) {
	for _, strategy := range writeStrategies {
		for _, outcome := range writeOutcomes {
			for _, mode := range writeModes {
				t.Run(string(strategy)+"/"+outcome.String()+"/"+string(mode), func(t *testing.T) {
					want := currentWriteBehaviour(strategy, outcome, mode)
					got, _ := observeWrite(t, writeEntryQuery, strategy, outcome, mode)
					require.Equal(t, want, got)
				})
			}
		}
	}
}

// A batch reaches the same dual-write path as a query, so it follows the same rules;
// what it adds is the batch shape of its replay payload.
func TestWriteClassificationBatchEntry(t *testing.T) {
	outcomes := []writeOutcome{{legResOK, legResFail}, {legResFail, legResFail}, {legResFail, legResOK}}
	for _, strategy := range writeStrategies {
		for _, outcome := range outcomes {
			for _, mode := range writeModes {
				t.Run(string(strategy)+"/"+outcome.String()+"/"+string(mode), func(t *testing.T) {
					want := currentWriteBehaviour(strategy, outcome, mode)
					got, payloads := observeWrite(t, writeEntryBatch, strategy, outcome, mode)
					require.Equal(t, want, got)
					for _, p := range payloads {
						require.True(t, p.IsBatch, "a batch is replayed as a batch")
						require.Len(t, p.BatchStatements, 1)
					}
				})
			}
		}
	}
}

// NonIdempotent takes the strict path: the same result and accounting as
// Strict, and no replay.
func TestWriteClassificationNonIdempotentIsStrict(t *testing.T) {
	outcome := writeOutcome{legResOK, legResFail}
	for _, strategy := range writeStrategies {
		t.Run(string(strategy), func(t *testing.T) {
			want := currentWriteBehaviour(strategy, outcome, writeModeStrict)
			got, _ := observeWrite(t, writeEntryNonIdempotent, strategy, outcome, writeModeStrict)
			require.Equal(t, want, got)
		})
	}
}

// A leg ended by WithClusterWriteTimeout counts as the cluster's write error and auto-refresh failure,
// and strikes AdaptiveDualWrite,
// but it never reaches the failover policy:
// a write outcome of any kind calls no FailoverPolicy or LatencyRecorder method.
// This is the current contract, not an oversight the matrix tolerates:
// the observation hub reports a write leg to the session stats alone,
// so a cluster that only times out writes never trips a circuit breaker.
func TestWriteClusterTimeoutNeverReachesFailoverPolicy(t *testing.T) {
	outcome := writeOutcome{legResOK, legResTimeout}
	for _, strategy := range writeStrategies {
		for _, mode := range writeModes {
			t.Run(string(strategy)+"/"+string(mode), func(t *testing.T) {
				got, _ := observeWrite(t, writeEntryQuery, strategy, outcome, mode)
				// A strict write skips a degraded cluster, so that one cell never times out.
				if strategy != strategyDegraded || mode != writeModeStrict {
					require.Equal(t, []ClusterID{ClusterB}, got.writeErrors, "the timed-out leg is the cluster's write error")
					require.Equal(t, []ClusterID{ClusterB}, got.healthFail, "the timed-out leg is an auto-refresh failure")
				}
				require.Empty(t, got.policyCalls)
				require.Empty(t, got.readStrategy)
			})
		}
	}
}

// With both clusters draining the write fails before any strategy runs.
// A plain write returns ErrBothClustersDraining,
// a strict write returns a dual failure,
// and both count each leg in write_total and write_skipped.
// Nothing is replayed and no session is contacted.
func TestWriteClassificationBothDraining(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "strict"}[strict], func(t *testing.T) {
			sessionA, sessionB := &writeMatrixSession{}, &writeMatrixSession{}
			m := &writeMatrixMetrics{}
			replayer := &writeMatrixReplayer{}
			client, err := NewCQLClient(sessionA, sessionB, WithMetrics(m), WithReplayer(replayer))
			require.NoError(t, err)
			t.Cleanup(client.Close)
			client.drainA.Store(true)
			client.drainB.Store(true)

			writeErr := runWriteEntry(t.Context(), client, writeEntryQuery, strict)
			client.Close()

			if strict {
				require.Equal(t, writeErrShape{kind: shapeDual, a: lecDraining, b: lecDraining}, classifyWriteResult(writeErr))
			} else {
				require.ErrorIs(t, writeErr, types.ErrBothClustersDraining)
			}
			require.Equal(t, []ClusterID{ClusterA, ClusterB}, m.get("write_total"))
			require.Equal(t, []ClusterID{ClusterA, ClusterB}, m.get("write_skipped"))
			require.Empty(t, m.get("write_error"))
			require.Empty(t, m.get("write_async"))
			require.Empty(t, replayer.payloads)
			require.Zero(t, sessionA.calls.Load()+sessionB.calls.Load())
		})
	}
}
