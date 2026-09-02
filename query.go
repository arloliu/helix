package helix

import (
	"context"
	"fmt"
	"math"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
)

// Query creates a new Query for the given statement.
//
// The method called on the returned Query determines the strategy:
//   - Exec/ExecContext → Write Strategy (Dual Write)
//   - Scan/Iter/MapScan → Read Strategy (Sticky Read)
//
// Parameters:
//   - stmt: CQL statement with ? placeholders
//   - values: Values to bind to placeholders
//
// Returns:
//   - Query: A query builder for further configuration
func (c *CQLClient) Query(stmt string, values ...any) Query {
	return &cqlQuery{
		client:    c,
		statement: stmt,
		values:    values,
	}
}

// pagedReadOptions returns the routing options for a read that carries a
// PageState: the issuing cluster is pinned when the token names one, and
// drain-aware re-selection is suppressed either way. Without a PageState
// the zero options apply.
func (q *cqlQuery) pagedReadOptions() readOptions {
	if q.pageState == nil {
		return readOptions{}
	}
	cluster, _ := decodePageState(q.pageState)

	return readOptions{preserveSelectedCluster: true, pinnedCluster: cluster}
}

// cqlQuery implements the Query interface for CQLClient.
type cqlQuery struct {
	client            *CQLClient
	statement         string
	values            []any
	ctx               context.Context
	consistency       *Consistency
	serialConsistency *Consistency
	pageSize          *int
	pageState         []byte
	timestamp         *int64
	priority          *PriorityLevel
	maxRows           *int
	fallbackRead      bool
	mirror            bool
	strict            bool
}

func (q *cqlQuery) WithContext(ctx context.Context) Query {
	q.ctx = ctx
	return q
}

func (q *cqlQuery) Consistency(c Consistency) Query {
	q.consistency = &c
	return q
}

// SetConsistency sets the consistency level for this query.
//
// Deprecated: Use Consistency() instead for the modern fluent API.
// This method is provided for compatibility with gocql v1 users.
func (q *cqlQuery) SetConsistency(c Consistency) {
	q.Consistency(c)
}

func (q *cqlQuery) SerialConsistency(c Consistency) Query {
	q.serialConsistency = &c
	return q
}

func (q *cqlQuery) PageSize(n int) Query {
	q.pageSize = &n
	return q
}

func (q *cqlQuery) PageState(state []byte) Query {
	q.pageState = state
	return q
}

func (q *cqlQuery) WithTimestamp(ts int64) Query {
	q.timestamp = &ts
	return q
}

func (q *cqlQuery) WithPriority(p PriorityLevel) Query {
	q.priority = &p
	return q
}

func (q *cqlQuery) FallbackRead() Query {
	q.fallbackRead = true
	return q
}

func (q *cqlQuery) Mirror() Query {
	q.mirror = true
	return q
}

func (q *cqlQuery) Strict() Query {
	q.strict = true
	return q
}

func (q *cqlQuery) MaxRows(n int) Query {
	if n == 0 {
		q.maxRows = nil
		return q
	}
	if n < 0 || n >= math.MaxInt32 {
		panic(fmt.Sprintf("helix: MaxRows(%d) out of range [0, math.MaxInt32-1]", n))
	}
	q.maxRows = &n

	return q
}

func (q *cqlQuery) getContext() context.Context {
	if q.ctx != nil {
		return q.ctx
	}
	return context.Background()
}

// writeTimestamp returns the client-side timestamp for this write.
// Zero is rejected: the drivers treat it as "use the current time", which
// would let a replayed write outrank data written after the original.
func (q *cqlQuery) writeTimestamp() (int64, error) {
	ts := q.client.config.TimestampProvider()
	if q.timestamp != nil {
		ts = *q.timestamp
	}
	if ts == 0 {
		return 0, types.ErrInvalidTimestamp
	}

	return ts, nil
}

func (q *cqlQuery) getPriority() PriorityLevel {
	if q.priority != nil {
		return *q.priority
	}
	return PriorityHigh
}

func (q *cqlQuery) applyConfig(query cql.Query) cql.Query {
	if q.consistency != nil {
		query = query.Consistency(*q.consistency)
	}
	if q.serialConsistency != nil {
		query = query.SerialConsistency(*q.serialConsistency)
	}
	if q.pageSize != nil {
		query = query.PageSize(*q.pageSize)
	}
	if q.pageState != nil {
		_, raw := decodePageState(q.pageState)
		query = query.PageState(raw)
	}

	return query
}

func (q *cqlQuery) Exec() error {
	return q.ExecContext(q.getContext())
}

func (q *cqlQuery) ExecContext(ctx context.Context) (err error) {
	ts, err := q.writeTimestamp()
	if err != nil {
		return err
	}
	priority := q.getPriority()

	if q.strict && q.mirror {
		return types.ErrStrictMirrorUnsupported
	}

	if q.mirror {
		defer func() {
			if err == nil {
				q.client.dispatchMirrorQuery(q.statement, q.values, ts, priority)
			}
		}()
	}

	// Fast path for single-cluster mode to avoid allocations
	if q.client.IsSingleCluster() {
		if q.client.closed.Load() {
			return types.ErrSessionClosed
		}

		query := q.client.loadSessionA().Query(q.statement, q.values...)
		query = q.applyConfig(query)
		// Important for writes to generate the timestamp on the client side
		// to ensure consistency across clusters
		query = query.WithTimestamp(ts)

		err = query.ExecContext(ctx)
		q.client.recordWriteOutcome(ctx, ClusterA, err)

		return err
	}

	wc := writeContext{
		statement: q.statement,
		args:      q.values,
		timestamp: ts,
		priority:  priority,
		strict:    q.strict,
	}

	err = q.client.executeWriteWithReplay(ctx, wc, func(ctx context.Context, session cql.Session) error {
		query := session.Query(q.statement, q.values...)
		query = q.applyConfig(query)
		// Important for writes to generate the timestamp on the client side
		// to ensure consistency across clusters
		query = query.WithTimestamp(ts)

		return query.ExecContext(ctx)
	})

	return err
}

func (q *cqlQuery) Scan(dest ...any) error {
	return q.ScanContext(q.getContext(), dest...)
}

func (q *cqlQuery) ScanContext(ctx context.Context, dest ...any) error {
	opts := q.client.resolveReadOptions(ctx, q)
	return q.client.executeRead(ctx, opts, func(ctx context.Context, session cql.Session) error {
		query := session.Query(q.statement, q.values...)
		query = q.applyConfig(query)

		return query.ScanContext(ctx, dest...)
	})
}

func (q *cqlQuery) Iter() Iter {
	return q.IterContext(q.getContext())
}

// IterContext executes the query and returns an iterator.
//
// NOTE: Iterators do NOT support automatic failover. If the selected cluster
// fails during iteration, the error is returned to the caller. Close still
// reports the outcome: a clean Close is a success for the read strategy
// (unless an AllowedClusters override is active) and the failover policy,
// and a cluster error is a failure for both, so an iterator-heavy workload
// trips the breaker and moves the sticky preference like Scan traffic does.
// Only the retry itself is skipped. Auto-refresh accounting is updated on
// Close for every outcome except a caller-context error.
//
// A query that carries a PageState is sent to the cluster that issued the
// token, whatever the read strategy or drain state say now: a paging cursor
// is only meaningful on the cluster that produced it. A token without the
// Helix routing header keeps whatever cluster resolves first and skips the
// drain-aware re-selection.
//
// If resolveReadTarget returns an error (fail-closed), an errorIter is returned
// that defers the error to Close(). Always call Close() and check its error.
func (q *cqlQuery) IterContext(ctx context.Context) Iter {
	if q.client.closed.Load() {
		return &errorIter{err: types.ErrSessionClosed}
	}

	rt := q.client.resolveReadTarget(ctx, q.pagedReadOptions())
	if rt.err != nil {
		return &errorIter{err: rt.err}
	}

	session := q.client.getSession(rt.cluster)
	query := session.Query(q.statement, q.values...)
	query = q.applyConfig(query)

	return &cqlIter{
		iter:           query.IterContext(ctx),
		client:         q.client,
		cluster:        rt.cluster,
		ctx:            ctx,
		overrideActive: rt.snap.active,
	}
}

func (q *cqlQuery) MapScan(m map[string]any) error {
	return q.MapScanContext(q.getContext(), m)
}

func (q *cqlQuery) MapScanContext(ctx context.Context, m map[string]any) error {
	opts := q.client.resolveReadOptions(ctx, q)
	return q.client.executeRead(ctx, opts, func(ctx context.Context, session cql.Session) error {
		query := session.Query(q.statement, q.values...)
		query = q.applyConfig(query)

		return query.MapScanContext(ctx, m)
	})
}

// ScanCAS executes a lightweight transaction (IF clause) and scans the result.
// CAS operations are executed on a single cluster and are NOT replicated.
func (q *cqlQuery) ScanCAS(dest ...any) (applied bool, err error) {
	return q.ScanCASContext(q.getContext(), dest...)
}

// ScanCASContext executes a lightweight transaction with context and scans the result.
// CAS operations are executed on a single cluster and are NOT replicated.
func (q *cqlQuery) ScanCASContext(ctx context.Context, dest ...any) (applied bool, err error) {
	selectedCluster := q.client.selectClusterForCAS(ctx)

	session := q.client.getSession(selectedCluster)
	query := session.Query(q.statement, q.values...)
	query = q.applyConfig(query)

	return query.ScanCASContext(ctx, dest...)
}

// MapScanCAS executes a lightweight transaction and scans the result into a map.
// CAS operations are executed on a single cluster and are NOT replicated.
func (q *cqlQuery) MapScanCAS(dest map[string]any) (applied bool, err error) {
	return q.MapScanCASContext(q.getContext(), dest)
}

// MapScanCASContext executes a lightweight transaction with context and scans into a map.
// CAS operations are executed on a single cluster and are NOT replicated.
func (q *cqlQuery) MapScanCASContext(ctx context.Context, dest map[string]any) (applied bool, err error) {
	selectedCluster := q.client.selectClusterForCAS(ctx)

	session := q.client.getSession(selectedCluster)
	query := session.Query(q.statement, q.values...)
	query = q.applyConfig(query)

	return query.MapScanCASContext(ctx, dest)
}
