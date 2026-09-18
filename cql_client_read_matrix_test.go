package helix

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
	"github.com/stretchr/testify/require"
)

// This file pins the read pipeline's error classification as one matrix:
// every read entry point, crossed with every result class a cluster can
// return, crossed with every routing mode.
// The expectations describe the behaviour the client has today,
// so a refactor of the read path must keep this test green unchanged
// and a deliberate behaviour change must edit the rule it changes.

// readEntry names one public read entry point.
type readEntry string

const (
	entryScan      readEntry = "Scan"
	entryMapScan   readEntry = "MapScan"
	entryIter      readEntry = "Iter"
	entrySliceMap  readEntry = "SliceMap"
	entrySliceScan readEntry = "SliceScan"
	entryBatchIter readEntry = "BatchIter"
)

// readOutcome names what the contacted cluster returns.
type readOutcome string

const (
	outcomeOK            readOutcome = "ok"
	outcomeNotFound      readOutcome = "not-found"
	outcomeRowLimit      readOutcome = "row-limit"
	outcomeCtxErr        readOutcome = "ctx-error"       // the caller's context ended before the cluster answered
	outcomeDriverTimeout readOutcome = "driver-timeout"  // the driver reports a context error while the caller's context is live
	outcomeClusterErr    readOutcome = "cluster-error"   // under modeLegTimeout: the cluster never answers inside the leg deadline
	outcomeStatementErr  readOutcome = "statement-error" // the coordinator rejected the statement itself
)

// readMode names the routing mode the client is in, or the leg deadline it runs under.
type readMode string

const (
	modePlain      readMode = "plain"
	modeOverride   readMode = "override"
	modeDrain      readMode = "drain"
	modeFallback   readMode = "fallback"
	modeLegTimeout readMode = "leg-timeout" // WithClusterReadTimeout bounds every read leg
)

// errClass classifies the error the caller receives.
type errClass string

const (
	errNone           errClass = "nil"
	errNotFound       errClass = "not-found"
	errRowLimit       errClass = "row-limit"
	errCtx            errClass = "ctx-error"
	errCluster        errClass = "cluster-error"
	errClusterTimeout errClass = "cluster-timeout" // types.ErrClusterTimeout: a leg deadline ended the read
	errStatement      errClass = "statement-error" // types.ErrStatementRejected, driver error still in the chain
	errDual           errClass = "dual-cluster"
)

var errMatrixCluster = errUnreachableForTest

// errStatementDriver stands in for the driver error a coordinator returns
// when it rejects the statement, and errStatement for what the adapters
// make of it.
// It is deliberately distinct from errMatrixCluster: a rejected statement
// must not be mistakable for a cluster fault.
// The wrap's shape is the adapters' (adapter/cql/v{1,2}/errors_test.go pins
// it against the real driver types); the root only classifies the sentinel.
var (
	errStatementDriver = errors.New("coordinator rejected the statement")
	errMatrixStatement = fmt.Errorf("%w: %w", types.ErrStatementRejected, errStatementDriver)
)

// matrixLegTimeout is the leg deadline modeLegTimeout runs under.
// A stalled session never answers before its context ends,
// so the value only decides how long an expiring cell takes, never what it observes.
const matrixLegTimeout = 10 * time.Millisecond

// A statement-rejection read starts from auto-refresh stats that are
// neither zero nor fresh, so that a read which reset them — as a success
// would — is as visible as one which advanced them.
const (
	matrixSeedFailures    int32 = 2
	matrixSeedLastSuccess int64 = 1_000
)

var readEntries = []readEntry{
	entryScan, entryMapScan, entryIter, entrySliceMap, entrySliceScan, entryBatchIter,
}

var readOutcomes = []readOutcome{
	outcomeOK, outcomeNotFound, outcomeRowLimit, outcomeCtxErr, outcomeDriverTimeout, outcomeClusterErr,
	outcomeStatementErr,
}

var readModes = []readMode{modePlain, modeOverride, modeDrain, modeFallback, modeLegTimeout}

// readObservation is everything the matrix records about one read.
type readObservation struct {
	err          errClass
	served       ClusterID   // cluster that received the primary attempt
	altContacted bool        // the other cluster received a request
	readTotal    []ClusterID // IncReadTotal calls, in order
	readErrors   []ClusterID // IncReadError calls, in order
	failures     []ClusterID // FailoverPolicy.RecordFailure calls, in order
	successes    []ClusterID // FailoverPolicy.RecordSuccess calls, in order
	onFailure    []ClusterID // ReadStrategy.OnFailure calls, in order
	onSuccess    []ClusterID // ReadStrategy.OnSuccess calls, in order
	healthFail   []ClusterID // clusters whose auto-refresh failure counter advanced
}

// matrixSession is a cql.Session whose every read returns one scripted
// result: scanErr for Scan / MapScan, and an iterator that yields rows
// rows and then reports iterErr from Close and Scanner.Err.
// A stalled session answers only once the context it was handed ends,
// as a frozen cluster does under a leg deadline,
// and Scan / MapScan then report that context's error.
type matrixSession struct {
	scanErr error
	iterErr error
	rows    int
	stall   bool
	clock   *atomic.Int32 // shared between both sessions of one client
	first   atomic.Int32  // clock value at first contact, 0 if never contacted
}

var _ cql.Session = (*matrixSession)(nil)

type matrixQuery struct {
	session *matrixSession
}

type matrixBatch struct {
	session *matrixSession
}

type matrixIter struct {
	session *matrixSession
	pos     int
}

type matrixScanner struct {
	iter *matrixIter
}

func (s *matrixSession) touch() {
	s.first.CompareAndSwap(0, s.clock.Add(1))
}

func (s *matrixSession) Query(_ string, _ ...any) cql.Query {
	s.touch()
	return &matrixQuery{session: s}
}

func (s *matrixSession) Batch(_ cql.BatchType) cql.Batch {
	s.touch()
	return &matrixBatch{session: s}
}

func (s *matrixSession) Close() {}

func (s *matrixSession) newIter() cql.Iter { return &matrixIter{session: s} }

// scan is the single-row read: the scripted error, or, for a stalled
// session, the error of the context it was handed once that context ends.
func (s *matrixSession) scan(ctx context.Context) error {
	if !s.stall {
		return s.scanErr
	}
	<-ctx.Done()

	return ctx.Err()
}

func (q *matrixQuery) Consistency(_ cql.Consistency) cql.Query         { return q }
func (q *matrixQuery) SerialConsistency(_ cql.Consistency) cql.Query   { return q }
func (q *matrixQuery) PageSize(_ int) cql.Query                        { return q }
func (q *matrixQuery) PageState(_ []byte) cql.Query                    { return q }
func (q *matrixQuery) WithTimestamp(_ int64) cql.Query                 { return q }
func (q *matrixQuery) Statement() string                               { return "" }
func (q *matrixQuery) Values() []any                                   { return nil }
func (q *matrixQuery) Release()                                        {}
func (q *matrixQuery) Exec() error                                     { return nil }
func (q *matrixQuery) ExecContext(_ context.Context) error             { return nil }
func (q *matrixQuery) Scan(_ ...any) error                             { return q.session.scanErr }
func (q *matrixQuery) ScanContext(ctx context.Context, _ ...any) error { return q.session.scan(ctx) }
func (q *matrixQuery) MapScan(_ map[string]any) error                  { return q.session.scanErr }
func (q *matrixQuery) MapScanContext(ctx context.Context, _ map[string]any) error {
	return q.session.scan(ctx)
}
func (q *matrixQuery) Iter() cql.Iter { return q.session.newIter() }
func (q *matrixQuery) IterContext(ctx context.Context) cql.Iter {
	if q.session.stall {
		<-ctx.Done()
	}

	return q.session.newIter()
}
func (q *matrixQuery) ScanCAS(_ ...any) (bool, error) { return true, nil }
func (q *matrixQuery) ScanCASContext(_ context.Context, _ ...any) (bool, error) {
	return true, nil
}
func (q *matrixQuery) MapScanCAS(_ map[string]any) (bool, error) { return true, nil }
func (q *matrixQuery) MapScanCASContext(_ context.Context, _ map[string]any) (bool, error) {
	return true, nil
}

func (b *matrixBatch) Query(_ string, _ ...any) cql.Batch            { return b }
func (b *matrixBatch) Consistency(_ cql.Consistency) cql.Batch       { return b }
func (b *matrixBatch) SerialConsistency(_ cql.Consistency) cql.Batch { return b }
func (b *matrixBatch) WithTimestamp(_ int64) cql.Batch               { return b }
func (b *matrixBatch) Size() int                                     { return 1 }
func (b *matrixBatch) Statements() []cql.BatchEntry                  { return nil }
func (b *matrixBatch) Exec() error                                   { return nil }
func (b *matrixBatch) ExecContext(_ context.Context) error           { return nil }
func (b *matrixBatch) IterContext(_ context.Context) cql.Iter        { return b.session.newIter() }
func (b *matrixBatch) ExecCAS(_ ...any) (bool, cql.Iter, error)      { return true, nil, nil }
func (b *matrixBatch) ExecCASContext(_ context.Context, _ ...any) (bool, cql.Iter, error) {
	return true, nil, nil
}
func (b *matrixBatch) MapExecCAS(_ map[string]any) (bool, cql.Iter, error) { return true, nil, nil }
func (b *matrixBatch) MapExecCASContext(_ context.Context, _ map[string]any) (bool, cql.Iter, error) {
	return true, nil, nil
}

func (i *matrixIter) next() bool {
	if i.pos >= i.session.rows {
		return false
	}
	i.pos++

	return true
}

func (i *matrixIter) Scan(_ ...any) bool { return i.next() }
func (i *matrixIter) MapScan(m map[string]any) bool {
	if !i.next() {
		return false
	}
	m["row"] = i.pos

	return true
}
func (i *matrixIter) Close() error                        { return i.session.iterErr }
func (i *matrixIter) SliceMap() ([]map[string]any, error) { return nil, i.session.iterErr }
func (i *matrixIter) PageState() []byte                   { return nil }
func (i *matrixIter) NumRows() int                        { return i.session.rows }
func (i *matrixIter) Columns() []cql.ColumnInfo           { return nil }
func (i *matrixIter) Scanner() cql.Scanner                { return &matrixScanner{iter: i} }
func (i *matrixIter) Warnings() []string                  { return nil }

func (s *matrixScanner) Next() bool          { return s.iter.next() }
func (s *matrixScanner) Scan(_ ...any) error { return nil }
func (s *matrixScanner) Err() error          { return s.iter.session.iterErr }

// legExpires reports whether the cell reads from a cluster that never
// answers inside the leg deadline.
// The batch iterator is excluded: no leg bounds it,
// so a stalled batch would wait on the caller's own context.
func legExpires(entry readEntry, outcome readOutcome, mode readMode) bool {
	return mode == modeLegTimeout && outcome == outcomeClusterErr && entry != entryBatchIter
}

// scriptSession configures a session so that entry observes outcome in mode.
func scriptSession(entry readEntry, outcome readOutcome, mode readMode) *matrixSession {
	s := &matrixSession{stall: legExpires(entry, outcome, mode)}
	switch outcome {
	case outcomeOK:
		s.rows = 1
	case outcomeNotFound:
		s.scanErr = types.ErrNotFound
		// Slice reads derive not-found from an empty drain;
		// iterator reads only see what Close returns.
		if entry == entryIter || entry == entryBatchIter {
			s.iterErr = types.ErrNotFound
		}
	case outcomeRowLimit:
		s.scanErr = types.ErrRowLimitExceeded
		// Slice reads hit the row limit by yielding more rows than MaxRows;
		// iterator reads only see what Close returns.
		s.rows = 2
		if entry == entryIter || entry == entryBatchIter {
			s.iterErr = types.ErrRowLimitExceeded
		}
	case outcomeCtxErr:
		s.scanErr = context.Canceled
		s.iterErr = context.Canceled
	case outcomeDriverTimeout:
		s.scanErr = context.DeadlineExceeded
		s.iterErr = context.DeadlineExceeded
	case outcomeClusterErr:
		s.scanErr = errMatrixCluster
		s.iterErr = errMatrixCluster
	case outcomeStatementErr:
		s.scanErr = errMatrixStatement
		s.iterErr = errMatrixStatement
	}

	return s
}

// runReadEntry issues one read through entry and returns the caller-visible error.
func runReadEntry(t *testing.T, client *CQLClient, entry readEntry, outcome readOutcome) error {
	t.Helper()
	ctx := t.Context()
	if outcome == outcomeCtxErr {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
	}
	q := client.Query("SELECT v FROM t WHERE k = ?", 1)
	if outcome == outcomeRowLimit {
		q = q.MaxRows(1)
	}

	switch entry {
	case entryScan:
		var v int
		return q.ScanContext(ctx, &v)
	case entryMapScan:
		return q.MapScanContext(ctx, map[string]any{})
	case entryIter:
		return q.IterContext(ctx).Close()
	case entrySliceMap:
		_, err := q.SliceMapContext(ctx)
		return err
	case entrySliceScan:
		_, err := q.SliceScanContext(ctx, func(RowScanner) error { return nil })
		return err
	case entryBatchIter:
		b := client.Batch(LoggedBatch).Query("UPDATE t SET v = ? WHERE k = ?", 1, 1)
		return b.IterContext(ctx).Close()
	}

	t.Fatalf("unknown entry %q", entry)

	return nil
}

func classifyMatrixErr(err error) errClass {
	var dual *types.DualClusterError
	switch {
	case err == nil:
		return errNone
	case errors.As(err, &dual):
		return errDual
	case errors.Is(err, types.ErrClusterTimeout):
		return errClusterTimeout
	case errors.Is(err, types.ErrNotFound):
		return errNotFound
	case errors.Is(err, types.ErrRowLimitExceeded):
		return errRowLimit
	case errors.Is(err, types.ErrStatementRejected):
		// The class names the wrap only while the driver's own error is
		// still reachable through it, which is what lets a caller match on
		// the coordinator's error code.
		if !errors.Is(err, errStatementDriver) {
			return errClass("statement error lost its driver error")
		}

		return errStatement
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return errCtx
	case errors.Is(err, errMatrixCluster):
		return errCluster
	}

	return errClass("unexpected: " + err.Error())
}

// observeRead builds a client in mode, runs entry against clusters that
// both return outcome, and records what the read pipeline did.
func observeRead(t *testing.T, entry readEntry, outcome readOutcome, mode readMode) readObservation {
	t.Helper()
	obs, _ := observeSessions(t, entry, outcome, mode,
		scriptSession(entry, outcome, mode), scriptSession(entry, outcome, mode))

	return obs
}

// observeSessions is observeRead with the two clusters scripted separately,
// for the asymmetric results the matrix cannot express — one cluster
// answers, the other rejects the statement.
// mode still decides the client's options and outcome still decides how the
// read is issued; the sessions decide what each cluster returns.
// It also returns the caller's error itself, for the claims that are about
// the error chain rather than its class.
func observeSessions(
	t *testing.T,
	entry readEntry,
	outcome readOutcome,
	mode readMode,
	sessionA, sessionB *matrixSession,
) (readObservation, error) {
	t.Helper()

	clock := &atomic.Int32{}
	sessionA.clock, sessionB.clock = clock, clock
	metrics := newReadTestMetrics()
	policy := &trackingFailoverPolicy{ShouldFailoverAllow: true}
	strategy := &trackingReadStrategy{preferred: ClusterA}

	opts := []Option{
		WithMetrics(metrics),
		WithFailoverPolicy(policy),
		WithReadStrategy(strategy),
	}
	switch mode {
	case modeOverride:
		opts = append(opts, WithAllowedClusters(func() []ClusterID {
			return []ClusterID{ClusterA, ClusterB}
		}))
	case modeFallback:
		opts = append(opts, WithDefaultFallbackRead(true))
	case modeLegTimeout:
		opts = append(opts, WithClusterReadTimeout(matrixLegTimeout))
	case modePlain, modeDrain:
	}

	client, err := NewCQLClient(sessionA, sessionB, opts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	if mode == modeDrain {
		client.drainA.Store(true)
	}
	// Only a rejected statement is seeded: it is the outcome whose claim is
	// that the stats stay exactly where they were.
	var seedFailures int32
	if outcome == outcomeStatementErr {
		seedFailures = matrixSeedFailures
		for _, c := range []ClusterID{ClusterA, ClusterB} {
			stats := client.statsForCluster(c)
			stats.consecutiveFailures.Store(seedFailures)
			stats.lastSuccessNanos.Store(matrixSeedLastSuccess)
		}
	}

	readErr := runReadEntry(t, client, entry, outcome)

	obs := readObservation{err: classifyMatrixErr(readErr), served: ClusterA}
	firstA, firstB := sessionA.first.Load(), sessionB.first.Load()
	require.NotZero(t, firstA+firstB, "no cluster was contacted")
	if firstB != 0 && (firstA == 0 || firstB < firstA) {
		obs.served = ClusterB
	}
	obs.altContacted = firstA != 0 && firstB != 0
	for _, c := range []ClusterID{ClusterA, ClusterB} {
		for range metrics.get(metrics.ReadTotal, c) {
			obs.readTotal = append(obs.readTotal, c)
		}
		for range metrics.get(metrics.ReadErrors, c) {
			obs.readErrors = append(obs.readErrors, c)
		}
		stats := client.statsForCluster(c)
		failures := stats.consecutiveFailures.Load()
		if failures > seedFailures {
			obs.healthFail = append(obs.healthFail, c)
		}
		if outcome == outcomeStatementErr {
			// No cluster succeeds in a statement-rejection read, so nothing
			// may reset the seeded stats; an advance is reported through
			// healthFail and checked against the expected observation.
			require.GreaterOrEqual(t, failures, seedFailures,
				"cluster %s: consecutiveFailures went below its seeded value", c)
			require.Equal(t, matrixSeedLastSuccess, stats.lastSuccessNanos.Load(),
				"cluster %s: lastSuccess moved", c)
		}
	}
	obs.readTotal = orderFrom(obs.served, obs.readTotal)
	obs.readErrors = orderFrom(obs.served, obs.readErrors)
	obs.healthFail = orderFrom(obs.served, obs.healthFail)
	obs.failures = policy.RecordFailureCalls
	obs.successes = policy.RecordSuccessCalls
	obs.onFailure = strategy.OnFailureCalls
	obs.onSuccess = strategy.OnSuccessCalls

	return obs, readErr
}

// orderFrom sorts clusters so that first comes before the other cluster,
// matching the order the pipeline contacts them.
func orderFrom(first ClusterID, clusters []ClusterID) []ClusterID {
	if len(clusters) < 2 || clusters[0] == first {
		return clusters
	}

	return []ClusterID{clusters[1], clusters[0]}
}

// iterMovesStrategy reports whether an iterator's close moves the read
// strategy in mode: an override freezes the strategy, and the failover gate
// the close now shares with a failing Scan refuses a draining alternative.
func iterMovesStrategy(mode readMode) bool {
	return mode != modeOverride && mode != modeDrain
}

// currentReadBehaviour states, rule by rule, what the read pipeline does
// today for one cell of the matrix.
func currentReadBehaviour(entry readEntry, outcome readOutcome, mode readMode) readObservation {
	isIter := entry == entryIter || entry == entryBatchIter
	isSlice := entry == entrySliceMap || entry == entrySliceScan
	expires := legExpires(entry, outcome, mode)

	// Every entry point moves the primary attempt away from a draining cluster.
	served := ClusterA
	if mode == modeDrain {
		served = ClusterB
	}
	alt := ClusterB
	if served == ClusterB {
		alt = ClusterA
	}
	obs := readObservation{err: errNone, served: served}

	switch outcome {
	case outcomeOK:
		// Success reports to the read strategy unless an override froze it,
		// and to the failover policy either way.
		if mode != modeOverride {
			obs.onSuccess = []ClusterID{served}
		}
		obs.successes = []ClusterID{served}
	case outcomeNotFound:
		// Not-found is data, never health: nothing is recorded anywhere.
		// Slice reads translate the empty drain to a nil error.
		obs.err = errNotFound
		if isSlice {
			obs.err = errNone
		}
		// FallbackRead probes the other cluster once, except for iterators.
		obs.altContacted = mode == modeFallback && !isIter
	case outcomeRowLimit:
		// The row cap is an application limit: no health, no second cluster.
		obs.err = errRowLimit
	case outcomeCtxErr:
		// The caller gave up: its context error comes back verbatim and
		// nothing is recorded against either cluster on any entry point.
		obs.err = errCtx
	case outcomeDriverTimeout, outcomeClusterErr:
		// A driver-side timeout with a live caller context is a cluster
		// fault for the failover policy, exactly like any other cluster
		// error; only a connectivity error also counts toward auto-refresh.
		// A leg its deadline ends is ErrClusterTimeout, a connectivity error.
		obs.err = errCtx
		if outcome == outcomeClusterErr {
			obs.err = errCluster
			obs.healthFail = []ClusterID{served}
		}
		if expires {
			obs.err = errClusterTimeout
		}
		obs.failures = []ClusterID{served}
		obs.readErrors = []ClusterID{served}
		if isIter && !expires {
			// Iterator Close counts the read error and reports the failure to
			// the policy, but cannot retry.
			// It moves the strategy only where a failing Scan would be
			// allowed to fail over, so a draining alternative freezes the
			// preference here exactly as it does below.
			// A first page the leg deadline ends never reaches Close:
			// it is reported and retried like a Scan, so it follows the rules below.
			if iterMovesStrategy(mode) {
				obs.onFailure = []ClusterID{served}
			}
			break
		}
		applyFailoverRules(&obs, entry, outcome, mode, alt)
	case outcomeStatementErr:
		// The coordinator rejected the statement itself.
		// The caller gets that error verbatim, the cluster that answered
		// counts one read error, and nothing else moves: no auto-refresh
		// failure, no policy call, no strategy call, and no second cluster —
		// which also means no failover metric, log or event, since
		// announceFailover emits those only on its way to the alternative.
		obs.err = errStatement
		obs.readErrors = []ClusterID{served}
	}
	obs.readTotal = readTotalFor(obs, alt)

	return obs
}

// applyFailoverRules states what a read that failed with a health signal,
// and is still allowed to try the other cluster, leaves in the observation.
// It is the tail of the failure rules, split out of currentReadBehaviour so
// that each half stays inside the cyclomatic budget.
func applyFailoverRules(obs *readObservation, entry readEntry, outcome readOutcome, mode readMode, alt ClusterID) {
	if entry == entrySliceScan {
		// SliceScan never fails over: the caller's callback already ran.
		return
	}
	if mode == modeDrain {
		// The only alternative is draining, so the primary error stands
		// and the strategy is never asked for a failover it would not get.
		return
	}
	if mode != modeOverride {
		obs.onFailure = []ClusterID{obs.served}
	}
	// Failover contacts the other cluster, which fails the same way,
	// and the caller sees both errors.
	obs.altContacted = true
	obs.err = errDual
	obs.readErrors = append(obs.readErrors, alt)
	obs.failures = append(obs.failures, alt)
	if outcome == outcomeClusterErr {
		obs.healthFail = append(obs.healthFail, alt)
	}
}

// readTotalFor states the read_total rule for one cell: every read that
// reaches a cluster counts one, whatever that cluster returns and whether
// the read is a Scan, a slice read or an iterator, so the counter names the
// clusters the read contacted in contact order.
func readTotalFor(obs readObservation, alt ClusterID) []ClusterID {
	if !obs.altContacted {
		return []ClusterID{obs.served}
	}

	return []ClusterID{obs.served, alt}
}

func TestReadClassificationMatrix(t *testing.T) {
	for _, entry := range readEntries {
		for _, outcome := range readOutcomes {
			for _, mode := range readModes {
				t.Run(string(entry)+"/"+string(outcome)+"/"+string(mode), func(t *testing.T) {
					want := currentReadBehaviour(entry, outcome, mode)
					got := observeRead(t, entry, outcome, mode)
					require.Equal(t, want, got)
				})
			}
		}
	}
}

// The matrix scripts both clusters alike, so the cases below are the
// asymmetric ones: one cluster answers and the other rejects the statement.
// Each states what the alternative's rejection does to the read's
// accounting — one read error on the cluster that rejected it, and nothing
// else anywhere.

// A FallbackRead probe whose alternative rejects the statement keeps the
// primary's healthy not-found: the probe exists to improve availability,
// so a rejection on the alternative must not turn an absent row into an
// error.
// The alternative still counts the read error it returned.
func TestFallbackReadAlternativeRejectsStatement(t *testing.T) {
	primary := scriptSession(entryScan, outcomeNotFound, modeFallback)
	alternative := scriptSession(entryScan, outcomeStatementErr, modeFallback)

	obs, err := observeSessions(t, entryScan, outcomeNotFound, modeFallback, primary, alternative)

	require.ErrorIs(t, err, types.ErrNotFound)
	require.NotErrorIs(t, err, types.ErrStatementRejected, "the probe's rejection stays folded")
	require.Equal(t, readObservation{
		err:          errNotFound,
		served:       ClusterA,
		altContacted: true,
		readTotal:    []ClusterID{ClusterA, ClusterB},
		readErrors:   []ClusterID{ClusterB},
	}, obs)
}

// SliceScan's strict FallbackRead propagates the alternative's error once
// the caller's callback has run there, a rejected statement included.
// The accounting is the same either way: the folding decides what the
// caller sees, not what the clusters are charged with.
func TestFallbackReadStrictAlternativeRejectsStatement(t *testing.T) {
	primary := scriptSession(entrySliceScan, outcomeNotFound, modeFallback)
	// The alternative yields a row, which arms the propagation predicate by
	// invoking the caller's callback, and only then reports the rejection.
	alternative := &matrixSession{rows: 1, iterErr: errMatrixStatement}

	obs, err := observeSessions(t, entrySliceScan, outcomeNotFound, modeFallback, primary, alternative)

	require.ErrorIs(t, err, types.ErrStatementRejected)
	require.ErrorIs(t, err, errStatementDriver, "the driver error must stay in the chain")
	require.Equal(t, readObservation{
		err:          errStatement,
		served:       ClusterA,
		altContacted: true,
		readTotal:    []ClusterID{ClusterA, ClusterB},
		readErrors:   []ClusterID{ClusterB},
	}, obs)
}

// A failover whose alternative rejects the statement keeps today's
// two-cluster shape: the caller sees both legs, and the rejection is
// reachable through [types.DualClusterError]'s Unwrap.
// Only the primary's cluster fault reaches the policy, the strategy and
// the auto-refresh stats; the alternative contributes its read error alone.
func TestFailoverAlternativeRejectsStatement(t *testing.T) {
	primary := scriptSession(entryScan, outcomeClusterErr, modePlain)
	alternative := scriptSession(entryScan, outcomeStatementErr, modePlain)

	obs, err := observeSessions(t, entryScan, outcomeClusterErr, modePlain, primary, alternative)

	var dual *types.DualClusterError
	require.ErrorAs(t, err, &dual)
	require.ErrorIs(t, err, errMatrixCluster, "the primary's fault stays reachable")
	require.ErrorIs(t, err, types.ErrStatementRejected)
	require.ErrorIs(t, err, errStatementDriver, "the driver error must stay in the chain")
	require.Equal(t, readObservation{
		err:          errDual,
		served:       ClusterA,
		altContacted: true,
		readTotal:    []ClusterID{ClusterA, ClusterB},
		readErrors:   []ClusterID{ClusterA, ClusterB},
		failures:     []ClusterID{ClusterA},
		onFailure:    []ClusterID{ClusterA},
		healthFail:   []ClusterID{ClusterA},
	}, obs)
}

// An iterator whose first page fails over reaches the alternative's
// rejection at Close, not on the leg: the driver hands back an iterator
// rather than an error, so the alternative's first page wins and the
// rejection is the caller's whole error.
// It is counted once on the alternative and nowhere else, exactly as it is
// on a Scan.
func TestIterFailoverAlternativeRejectsStatement(t *testing.T) {
	// The primary never answers inside the leg deadline, so the iterator's
	// first page fails over.
	primary := scriptSession(entryIter, outcomeClusterErr, modeLegTimeout)
	alternative := scriptSession(entryIter, outcomeStatementErr, modeLegTimeout)

	obs, err := observeSessions(t, entryIter, outcomeStatementErr, modeLegTimeout, primary, alternative)

	require.ErrorIs(t, err, types.ErrStatementRejected)
	require.ErrorIs(t, err, errStatementDriver, "the driver error must stay in the chain")
	require.Equal(t, readObservation{
		err:          errStatement,
		served:       ClusterA,
		altContacted: true,
		readTotal:    []ClusterID{ClusterA, ClusterB},
		readErrors:   []ClusterID{ClusterA, ClusterB},
		failures:     []ClusterID{ClusterA},
		onFailure:    []ClusterID{ClusterA},
		healthFail:   []ClusterID{ClusterA},
	}, obs)
}
