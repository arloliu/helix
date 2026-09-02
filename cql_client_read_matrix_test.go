package helix

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

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
	outcomeCtxErr        readOutcome = "ctx-error"      // the caller's context ended before the cluster answered
	outcomeDriverTimeout readOutcome = "driver-timeout" // the driver reports a context error while the caller's context is live
	outcomeClusterErr    readOutcome = "cluster-error"
)

// readMode names the routing mode the client is in.
type readMode string

const (
	modePlain    readMode = "plain"
	modeOverride readMode = "override"
	modeDrain    readMode = "drain"
	modeFallback readMode = "fallback"
)

// errClass classifies the error the caller receives.
type errClass string

const (
	errNone     errClass = "nil"
	errNotFound errClass = "not-found"
	errRowLimit errClass = "row-limit"
	errCtx      errClass = "ctx-error"
	errCluster  errClass = "cluster-error"
	errDual     errClass = "dual-cluster"
)

var errMatrixCluster = errors.New("matrix: cluster error")

var readEntries = []readEntry{
	entryScan, entryMapScan, entryIter, entrySliceMap, entrySliceScan, entryBatchIter,
}

var readOutcomes = []readOutcome{
	outcomeOK, outcomeNotFound, outcomeRowLimit, outcomeCtxErr, outcomeDriverTimeout, outcomeClusterErr,
}

var readModes = []readMode{modePlain, modeOverride, modeDrain, modeFallback}

// readObservation is everything the matrix records about one read.
type readObservation struct {
	err          errClass
	served       ClusterID   // cluster that received the primary attempt
	altContacted bool        // the other cluster received a request
	readErrors   []ClusterID // IncReadError calls, in order
	failures     []ClusterID // FailoverPolicy.RecordFailure calls, in order
	onFailure    []ClusterID // ReadStrategy.OnFailure calls, in order
	onSuccess    []ClusterID // ReadStrategy.OnSuccess calls, in order
	healthFail   []ClusterID // clusters whose auto-refresh failure counter advanced
}

// matrixSession is a cql.Session whose every read returns one scripted
// result: scanErr for Scan / MapScan, and an iterator that yields rows
// rows and then reports iterErr from Close and Scanner.Err.
type matrixSession struct {
	scanErr error
	iterErr error
	rows    int
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

func (q *matrixQuery) Consistency(_ cql.Consistency) cql.Query       { return q }
func (q *matrixQuery) SerialConsistency(_ cql.Consistency) cql.Query { return q }
func (q *matrixQuery) PageSize(_ int) cql.Query                      { return q }
func (q *matrixQuery) PageState(_ []byte) cql.Query                  { return q }
func (q *matrixQuery) WithTimestamp(_ int64) cql.Query               { return q }
func (q *matrixQuery) Statement() string                             { return "" }
func (q *matrixQuery) Values() []any                                 { return nil }
func (q *matrixQuery) Release()                                      {}
func (q *matrixQuery) Exec() error                                   { return nil }
func (q *matrixQuery) ExecContext(_ context.Context) error           { return nil }
func (q *matrixQuery) Scan(_ ...any) error                           { return q.session.scanErr }
func (q *matrixQuery) ScanContext(_ context.Context, _ ...any) error { return q.session.scanErr }
func (q *matrixQuery) MapScan(_ map[string]any) error                { return q.session.scanErr }
func (q *matrixQuery) MapScanContext(_ context.Context, _ map[string]any) error {
	return q.session.scanErr
}
func (q *matrixQuery) Iter() cql.Iter                         { return q.session.newIter() }
func (q *matrixQuery) IterContext(_ context.Context) cql.Iter { return q.session.newIter() }
func (q *matrixQuery) ScanCAS(_ ...any) (bool, error)         { return true, nil }
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

// scriptSession configures a session so that entry observes outcome.
func scriptSession(entry readEntry, outcome readOutcome) *matrixSession {
	s := &matrixSession{}
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
	case errors.Is(err, types.ErrNotFound):
		return errNotFound
	case errors.Is(err, types.ErrRowLimitExceeded):
		return errRowLimit
	case isCtxErr(err):
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

	clock := &atomic.Int32{}
	sessionA := scriptSession(entry, outcome)
	sessionB := scriptSession(entry, outcome)
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
	case modePlain, modeDrain:
	}

	client, err := NewCQLClient(sessionA, sessionB, opts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	if mode == modeDrain {
		client.drainA.Store(true)
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
		for range metrics.get(metrics.ReadErrors, c) {
			obs.readErrors = append(obs.readErrors, c)
		}
		if client.statsForCluster(c).consecutiveFailures.Load() > 0 {
			obs.healthFail = append(obs.healthFail, c)
		}
	}
	obs.readErrors = orderFrom(obs.served, obs.readErrors)
	obs.healthFail = orderFrom(obs.served, obs.healthFail)
	obs.failures = policy.RecordFailureCalls
	obs.onFailure = strategy.OnFailureCalls
	obs.onSuccess = strategy.OnSuccessCalls

	return obs
}

// orderFrom sorts clusters so that first comes before the other cluster,
// matching the order the pipeline contacts them.
func orderFrom(first ClusterID, clusters []ClusterID) []ClusterID {
	if len(clusters) < 2 || clusters[0] == first {
		return clusters
	}

	return []ClusterID{clusters[1], clusters[0]}
}

// currentReadBehaviour states, rule by rule, what the read pipeline does
// today for one cell of the matrix.
func currentReadBehaviour(entry readEntry, outcome readOutcome, mode readMode) readObservation {
	isIter := entry == entryIter || entry == entryBatchIter
	isSlice := entry == entrySliceMap || entry == entrySliceScan

	// Iterator reads never re-select away from a draining cluster;
	// every other entry point moves the primary attempt to the other cluster.
	served := ClusterA
	if mode == modeDrain && !isIter {
		served = ClusterB
	}
	alt := ClusterB
	if served == ClusterB {
		alt = ClusterA
	}
	obs := readObservation{err: errNone, served: served}

	switch outcome {
	case outcomeOK:
		// Success reports to the read strategy unless an override froze it.
		if mode != modeOverride {
			obs.onSuccess = []ClusterID{served}
		}
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
		// fault and is classified exactly like any other cluster error.
		obs.err = errCtx
		if outcome == outcomeClusterErr {
			obs.err = errCluster
		}
		obs.healthFail = []ClusterID{served}
		if isIter {
			// Iterator Close feeds only the auto-refresh counter.
			break
		}
		obs.readErrors = []ClusterID{served}
		obs.failures = []ClusterID{served}
		if entry == entrySliceScan {
			// SliceScan never fails over: the caller's callback already ran.
			break
		}
		if mode != modeOverride {
			obs.onFailure = []ClusterID{served}
		}
		if mode == modeDrain {
			// The only alternative is draining, so the primary error stands.
			break
		}
		// Failover contacts the other cluster, which fails the same way,
		// and the caller sees both errors.
		obs.altContacted = true
		obs.err = errDual
		obs.readErrors = append(obs.readErrors, alt)
		obs.failures = append(obs.failures, alt)
		obs.healthFail = append(obs.healthFail, alt)
	}

	return obs
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
