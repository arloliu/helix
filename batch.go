package helix

import (
	"context"
	"errors"

	"github.com/arloliu/helix/adapter/cql"
	"github.com/arloliu/helix/types"
)

// Batch creates a new Batch for grouping multiple mutations.
//
// WARNING: CounterBatch operations are NOT idempotent. If a counter update
// partially fails (succeeds on one cluster, fails on another), or if
// AdaptiveDualWrite returns ErrWriteAsync and the client eagerly enqueues a replay
// safety net while the background write also succeeds, the Replay System can
// apply the same increment twice. Avoid using CounterBatch with dual-cluster mode
// if you require exactly-once semantics.
//
// Parameters:
//   - kind: Type of batch (Logged, Unlogged, or Counter)
//
// Returns:
//   - Batch: A batch builder for adding statements
func (c *CQLClient) Batch(kind BatchType) Batch {
	return &cqlBatch{
		client:  c,
		kind:    kind,
		entries: make([]batchEntry, 0),
	}
}

// NewBatch creates a new Batch for grouping multiple mutations.
//
// Deprecated: Use Batch() instead for the modern fluent API.
// This method is provided for compatibility with gocql v1 users.
//
// Parameters:
//   - kind: Type of batch (Logged, Unlogged, or Counter)
//
// Returns:
//   - Batch: A batch builder for adding statements
func (c *CQLClient) NewBatch(kind BatchType) Batch {
	return c.Batch(kind)
}

// errNilBatch is returned by the deprecated ExecuteBatch / ExecuteBatchCAS /
// MapExecuteBatchCAS compat shims when the caller passes a nil Batch. These
// methods invoke a method directly on the caller-supplied Batch interface,
// so without this guard a nil argument would panic on a nil-interface method
// call instead of returning a diagnosable error. types.ErrNilSession is not
// reused here since its message ("session cannot be nil") would misdescribe
// the failure; this sentinel is local to the file, matching errNilSliceScanFn.
var errNilBatch = errors.New("helix: batch must not be nil")

// ExecuteBatch executes a batch operation.
//
// Deprecated: Use batch.Exec() instead for the modern fluent API.
// This method is provided for compatibility with gocql v1 users.
//
// Parameters:
//   - batch: The batch to execute. Must not be nil.
//
// Returns:
//   - error: errNilBatch if batch is nil; otherwise any execution error
func (c *CQLClient) ExecuteBatch(batch Batch) error {
	if batch == nil {
		return errNilBatch
	}

	return batch.Exec()
}

// ExecuteBatchCAS executes a batch with lightweight transaction semantics.
//
// Deprecated: Use batch.ExecCAS() instead for the modern fluent API.
// This method is provided for compatibility with gocql v1 users.
//
// Parameters:
//   - batch: The batch to execute. Must not be nil.
//   - dest: Optional destination for result columns
//
// Returns:
//   - applied: true if the transaction was applied
//   - iter: Iterator for result rows if not applied
//   - err: errNilBatch if batch is nil; otherwise any execution error
func (c *CQLClient) ExecuteBatchCAS(batch Batch, dest ...any) (applied bool, iter Iter, err error) {
	if batch == nil {
		return false, nil, errNilBatch
	}

	return batch.ExecCAS(dest...)
}

// MapExecuteBatchCAS executes a batch CAS operation and maps results to a map.
//
// Deprecated: Use batch.MapExecCAS() instead for the modern fluent API.
// This method is provided for compatibility with gocql v1 users.
//
// Parameters:
//   - batch: The batch to execute. Must not be nil.
//   - dest: Map to receive result columns
//
// Returns:
//   - applied: true if the transaction was applied
//   - iter: Iterator for result rows if not applied
//   - err: errNilBatch if batch is nil; otherwise any execution error
func (c *CQLClient) MapExecuteBatchCAS(batch Batch, dest map[string]any) (applied bool, iter Iter, err error) {
	if batch == nil {
		return false, nil, errNilBatch
	}

	return batch.MapExecCAS(dest)
}

// batchEntry holds a single statement in a batch.
type batchEntry struct {
	statement string
	args      []any
}

// cqlBatch implements the Batch interface for CQLClient.
type cqlBatch struct {
	client            *CQLClient
	kind              BatchType
	entries           []batchEntry
	ctx               context.Context
	consistency       *Consistency
	serialConsistency *Consistency
	timestamp         *int64
	priority          *PriorityLevel
	mirror            bool
	strict            bool
}

func (b *cqlBatch) Query(stmt string, args ...any) Batch {
	b.entries = append(b.entries, batchEntry{
		statement: stmt,
		args:      args,
	})
	return b
}

func (b *cqlBatch) Consistency(c Consistency) Batch {
	b.consistency = &c
	return b
}

// SetConsistency sets the consistency level for this batch.
//
// Deprecated: Use Consistency() instead for the modern fluent API.
// This method is provided for compatibility with gocql v1 users.
func (b *cqlBatch) SetConsistency(c Consistency) {
	b.Consistency(c)
}

func (b *cqlBatch) SerialConsistency(c Consistency) Batch {
	b.serialConsistency = &c
	return b
}

func (b *cqlBatch) Size() int {
	return len(b.entries)
}

func (b *cqlBatch) WithContext(ctx context.Context) Batch {
	b.ctx = ctx
	return b
}

func (b *cqlBatch) WithTimestamp(ts int64) Batch {
	b.timestamp = &ts
	return b
}

func (b *cqlBatch) WithPriority(p PriorityLevel) Batch {
	b.priority = &p
	return b
}

func (b *cqlBatch) Mirror() Batch {
	b.mirror = true
	return b
}

func (b *cqlBatch) Strict() Batch {
	b.strict = true
	return b
}

func (b *cqlBatch) getContext() context.Context {
	if b.ctx != nil {
		return b.ctx
	}
	return context.Background()
}

func (b *cqlBatch) getTimestamp() int64 {
	if b.timestamp != nil {
		return *b.timestamp
	}
	return b.client.config.TimestampProvider()
}

func (b *cqlBatch) getPriority() PriorityLevel {
	if b.priority != nil {
		return *b.priority
	}
	return PriorityHigh
}

func (b *cqlBatch) Exec() error {
	return b.ExecContext(b.getContext())
}

func (b *cqlBatch) ExecContext(ctx context.Context) (err error) {
	ts := b.getTimestamp()
	priority := b.getPriority()

	if b.strict && b.mirror {
		return types.ErrStrictMirrorUnsupported
	}

	if b.mirror {
		defer func() {
			if err == nil {
				b.client.dispatchMirrorBatch(b.kind, b.entries, ts, priority)
			}
		}()
	}

	// Fast path for single-cluster mode to avoid allocations
	if b.client.IsSingleCluster() {
		if b.client.closed.Load() {
			return types.ErrSessionClosed
		}

		batch := b.client.loadSessionA().Batch(b.kind)
		for _, entry := range b.entries {
			batch = batch.Query(entry.statement, entry.args...)
		}
		if b.consistency != nil {
			batch = batch.Consistency(*b.consistency)
		}
		if b.serialConsistency != nil {
			batch = batch.SerialConsistency(*b.serialConsistency)
		}
		batch = batch.WithTimestamp(ts)

		err = batch.ExecContext(ctx)
		b.client.recordOpOutcome(ClusterA, err)

		return err
	}

	wc := writeContext{
		statement:    "", // Empty for batch
		args:         nil,
		timestamp:    ts,
		priority:     priority,
		isBatch:      true,
		batchType:    b.kind,
		batchEntries: b.entries, // Pass directly, convert lazily if needed for replay
		strict:       b.strict,
	}

	err = b.client.executeWriteWithReplay(ctx, wc, func(ctx context.Context, session cql.Session) error {
		batch := session.Batch(b.kind)
		for _, entry := range b.entries {
			batch = batch.Query(entry.statement, entry.args...)
		}
		if b.consistency != nil {
			batch = batch.Consistency(*b.consistency)
		}
		if b.serialConsistency != nil {
			batch = batch.SerialConsistency(*b.serialConsistency)
		}
		batch = batch.WithTimestamp(ts)

		return batch.ExecContext(ctx)
	})

	return err
}

// IterContext executes the batch and returns an iterator for the results.
//
// NOTE: Iterators do NOT support automatic failover. If the selected cluster
// fails during iteration, the error is returned to the caller.
// If resolveReadTarget returns an error, an errorIter is returned.
func (b *cqlBatch) IterContext(ctx context.Context) Iter {
	if b.client.closed.Load() {
		return &errorIter{err: types.ErrSessionClosed}
	}

	ts := b.getTimestamp()

	rt := b.client.resolveReadTarget(ctx, readOptions{})
	if rt.err != nil {
		return &errorIter{err: rt.err}
	}

	session := b.client.getSession(rt.cluster)
	batch := session.Batch(b.kind)
	for _, entry := range b.entries {
		batch = batch.Query(entry.statement, entry.args...)
	}
	if b.consistency != nil {
		batch = batch.Consistency(*b.consistency)
	}
	if b.serialConsistency != nil {
		batch = batch.SerialConsistency(*b.serialConsistency)
	}
	batch = batch.WithTimestamp(ts)

	return &cqlIter{
		iter:           batch.IterContext(ctx),
		client:         b.client,
		cluster:        rt.cluster,
		overrideActive: rt.snap.active,
	}
}

// ExecCAS executes a batch lightweight transaction.
// CAS operations are executed on a single cluster and are NOT replicated.
func (b *cqlBatch) ExecCAS(dest ...any) (applied bool, iter Iter, err error) {
	return b.ExecCASContext(b.getContext(), dest...)
}

// ExecCASContext executes a batch lightweight transaction with context.
// CAS operations are executed on a single cluster and are NOT replicated.
func (b *cqlBatch) ExecCASContext(ctx context.Context, dest ...any) (applied bool, iter Iter, err error) {
	ts := b.getTimestamp()

	selectedCluster := b.client.selectClusterForCAS(ctx)

	session := b.client.getSession(selectedCluster)
	batch := session.Batch(b.kind)
	for _, entry := range b.entries {
		batch = batch.Query(entry.statement, entry.args...)
	}
	if b.consistency != nil {
		batch = batch.Consistency(*b.consistency)
	}
	if b.serialConsistency != nil {
		batch = batch.SerialConsistency(*b.serialConsistency)
	}
	batch = batch.WithTimestamp(ts)

	applied, cqlItr, err := batch.ExecCASContext(ctx, dest...)
	if cqlItr == nil {
		return applied, nil, err
	}

	return applied, &cqlIter{
		iter:    cqlItr,
		client:  b.client,
		cluster: selectedCluster,
	}, err
}

// MapExecCAS executes a batch lightweight transaction and scans into a map.
// CAS operations are executed on a single cluster and are NOT replicated.
func (b *cqlBatch) MapExecCAS(dest map[string]any) (applied bool, iter Iter, err error) {
	return b.MapExecCASContext(b.getContext(), dest)
}

// MapExecCASContext executes a batch lightweight transaction with context and scans into a map.
// CAS operations are executed on a single cluster and are NOT replicated.
func (b *cqlBatch) MapExecCASContext(ctx context.Context, dest map[string]any) (applied bool, iter Iter, err error) {
	ts := b.getTimestamp()

	selectedCluster := b.client.selectClusterForCAS(ctx)

	session := b.client.getSession(selectedCluster)
	batch := session.Batch(b.kind)
	for _, entry := range b.entries {
		batch = batch.Query(entry.statement, entry.args...)
	}
	if b.consistency != nil {
		batch = batch.Consistency(*b.consistency)
	}
	if b.serialConsistency != nil {
		batch = batch.SerialConsistency(*b.serialConsistency)
	}
	batch = batch.WithTimestamp(ts)

	applied, cqlItr, err := batch.MapExecCASContext(ctx, dest)
	if cqlItr == nil {
		return applied, nil, err
	}

	return applied, &cqlIter{
		iter:    cqlItr,
		client:  b.client,
		cluster: selectedCluster,
	}, err
}
