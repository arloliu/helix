package testutil

import (
	"context"
	"sync"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/types"
)

// MockCQLSession is a mock implementation of helix.CQLSession for testing.
type MockCQLSession struct {
	mu      sync.RWMutex
	closed  bool
	queries map[string]*MockQuery
	batches []*MockBatch

	// Hooks for custom behavior
	OnQuery func(stmt string, values ...any) helix.Query
	OnBatch func(kind helix.BatchType) helix.Batch
	OnClose func()
}

// Compile-time assertion that MockCQLSession implements helix.CQLSession.
var _ helix.CQLSession = (*MockCQLSession)(nil)

// NewMockCQLSession creates a new mock CQL session.
func NewMockCQLSession() *MockCQLSession {
	return &MockCQLSession{
		queries: make(map[string]*MockQuery),
	}
}

// Query returns a mock query for the given statement.
func (m *MockCQLSession) Query(stmt string, values ...any) helix.Query {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.OnQuery != nil {
		return m.OnQuery(stmt, values...)
	}

	// Return existing mock or create new one
	if q, ok := m.queries[stmt]; ok {
		q.values = values
		q.ctx = context.Background()

		return q
	}

	q := NewMockQuery(stmt, values...)
	m.queries[stmt] = q

	return q
}

// Batch returns a mock batch of the given type.
func (m *MockCQLSession) Batch(kind helix.BatchType) helix.Batch {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.OnBatch != nil {
		return m.OnBatch(kind)
	}

	b := NewMockBatch(kind)
	m.batches = append(m.batches, b)

	return b
}

// NewBatch creates a new Batch.
//
// Deprecated: Use Batch() instead for the modern fluent API.
func (m *MockCQLSession) NewBatch(kind helix.BatchType) helix.Batch {
	return m.Batch(kind)
}

// ExecuteBatch executes a batch.
//
// Deprecated: Use batch.Exec() instead for the modern fluent API.
func (m *MockCQLSession) ExecuteBatch(batch helix.Batch) error {
	return batch.Exec()
}

// ExecuteBatchCAS executes a batch with CAS semantics.
//
// Deprecated: Use batch.ExecCAS() instead for the modern fluent API.
func (m *MockCQLSession) ExecuteBatchCAS(batch helix.Batch, dest ...any) (applied bool, iter helix.Iter, err error) {
	return batch.ExecCAS(dest...)
}

// MapExecuteBatchCAS executes a batch CAS operation with map result.
//
// Deprecated: Use batch.MapExecCAS() instead for the modern fluent API.
func (m *MockCQLSession) MapExecuteBatchCAS(batch helix.Batch, dest map[string]any) (applied bool, iter helix.Iter, err error) {
	return batch.MapExecCAS(dest)
}

// Close marks the session as closed.
func (m *MockCQLSession) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true

	if m.OnClose != nil {
		m.OnClose()
	}
}

// IsClosed returns whether the session has been closed.
func (m *MockCQLSession) IsClosed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.closed
}

// SetQueryError configures a query to return an error.
func (m *MockCQLSession) SetQueryError(stmt string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if q, ok := m.queries[stmt]; ok {
		q.execErr = err
	} else {
		q := NewMockQuery(stmt)
		q.execErr = err
		m.queries[stmt] = q
	}
}

// MockQuery is a mock implementation of helix.Query for testing.
type MockQuery struct {
	mu     sync.RWMutex
	stmt   string
	values []any
	ctx    context.Context

	// Configuration
	consistency helix.Consistency
	pageSize    int
	pageState   []byte
	timestamp   int64

	// Return values
	execErr  error
	scanErr  error
	scanData []any
	iter     helix.Iter
	mapData  map[string]any
}

// Compile-time assertion that MockQuery implements helix.Query.
var _ helix.Query = (*MockQuery)(nil)

// NewMockQuery creates a new mock query.
func NewMockQuery(stmt string, values ...any) *MockQuery {
	return &MockQuery{
		stmt:   stmt,
		values: values,
		ctx:    context.Background(),
	}
}

// Statement returns the query statement.
func (m *MockQuery) Statement() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.stmt
}

// Values returns the query values.
func (m *MockQuery) Values() []any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.values
}

// WithContext sets the context.
func (m *MockQuery) WithContext(ctx context.Context) helix.Query {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ctx = ctx

	return m
}

// Consistency sets the consistency level.
func (m *MockQuery) Consistency(c helix.Consistency) helix.Query {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.consistency = c

	return m
}

// SetConsistency sets the consistency level.
//
// Deprecated: Use Consistency() instead for the modern fluent API.
func (m *MockQuery) SetConsistency(c helix.Consistency) {
	m.Consistency(c)
}

// PageSize sets the page size.
func (m *MockQuery) PageSize(n int) helix.Query {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pageSize = n

	return m
}

// PageState sets the page state.
func (m *MockQuery) PageState(state []byte) helix.Query {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pageState = state

	return m
}

// WithTimestamp sets the timestamp.
func (m *MockQuery) WithTimestamp(ts int64) helix.Query {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.timestamp = ts

	return m
}

// SerialConsistency sets the serial consistency level for CAS operations.
func (m *MockQuery) SerialConsistency(_ helix.Consistency) helix.Query {
	return m
}

// WithPriority sets the replay priority level for this query.
func (m *MockQuery) WithPriority(_ helix.PriorityLevel) helix.Query {
	return m
}

// Exec executes the query.
func (m *MockQuery) Exec() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.execErr
}

// ExecContext executes the query with context.
func (m *MockQuery) ExecContext(_ context.Context) error {
	return m.Exec()
}

// Scan scans a single row.
func (m *MockQuery) Scan(dest ...any) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.scanErr != nil {
		return m.scanErr
	}

	// Copy scan data to destinations
	for i := 0; i < len(dest) && i < len(m.scanData); i++ {
		copyValue(dest[i], m.scanData[i])
	}

	return nil
}

// ScanContext scans with context.
func (m *MockQuery) ScanContext(_ context.Context, dest ...any) error {
	return m.Scan(dest...)
}

// Iter returns an iterator.
func (m *MockQuery) Iter() helix.Iter {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.iter != nil {
		return m.iter
	}

	return NewMockIter()
}

// IterContext returns an iterator with context.
func (m *MockQuery) IterContext(_ context.Context) helix.Iter {
	return m.Iter()
}

// MapScan scans into a map.
func (m *MockQuery) MapScan(dest map[string]any) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.scanErr != nil {
		return m.scanErr
	}

	for k, v := range m.mapData {
		dest[k] = v
	}

	return nil
}

// MapScanContext scans into a map with context.
func (m *MockQuery) MapScanContext(_ context.Context, dest map[string]any) error {
	return m.MapScan(dest)
}

// ScanCAS executes a lightweight transaction and scans the result.
func (m *MockQuery) ScanCAS(_ ...any) (applied bool, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return true, m.scanErr
}

// ScanCASContext executes a lightweight transaction with context.
func (m *MockQuery) ScanCASContext(_ context.Context, dest ...any) (applied bool, err error) {
	return m.ScanCAS(dest...)
}

// MapScanCAS executes a lightweight transaction and scans into a map.
func (m *MockQuery) MapScanCAS(_ map[string]any) (applied bool, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return true, m.scanErr
}

// MapScanCASContext executes a lightweight transaction with context.
func (m *MockQuery) MapScanCASContext(_ context.Context, dest map[string]any) (applied bool, err error) {
	return m.MapScanCAS(dest)
}

// SetExecError configures the exec error.
func (m *MockQuery) SetExecError(err error) *MockQuery {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.execErr = err

	return m
}

// SetScanData configures the scan data.
func (m *MockQuery) SetScanData(data ...any) *MockQuery {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.scanData = data

	return m
}

// SetScanError configures the scan error.
func (m *MockQuery) SetScanError(err error) *MockQuery {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.scanErr = err

	return m
}

// SetIter configures the iterator.
func (m *MockQuery) SetIter(iter helix.Iter) *MockQuery {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.iter = iter

	return m
}

// SetMapData configures the map scan data.
func (m *MockQuery) SetMapData(data map[string]any) *MockQuery {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mapData = data

	return m
}

// MockBatch is a mock implementation of helix.Batch for testing.
type MockBatch struct {
	mu       sync.RWMutex
	kind     helix.BatchType
	ctx      context.Context
	stmts    []batchStmt
	execErr  error
	execHook func() error

	// Configuration
	consistency helix.Consistency
	timestamp   int64
}

type batchStmt struct {
	stmt string
	args []any
}

// Compile-time assertion that MockBatch implements helix.Batch.
var _ helix.Batch = (*MockBatch)(nil)

// NewMockBatch creates a new mock batch.
func NewMockBatch(kind helix.BatchType) *MockBatch {
	return &MockBatch{
		kind: kind,
		ctx:  context.Background(),
	}
}

// Query adds a statement to the batch.
func (m *MockBatch) Query(stmt string, args ...any) helix.Batch {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stmts = append(m.stmts, batchStmt{stmt: stmt, args: args})

	return m
}

// Consistency sets the consistency level.
func (m *MockBatch) Consistency(c helix.Consistency) helix.Batch {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.consistency = c

	return m
}

// SetConsistency sets the consistency level.
//
// Deprecated: Use Consistency() instead for the modern fluent API.
func (m *MockBatch) SetConsistency(c helix.Consistency) {
	m.Consistency(c)
}

// WithContext sets the context.
func (m *MockBatch) WithContext(ctx context.Context) helix.Batch {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ctx = ctx

	return m
}

// WithTimestamp sets the timestamp.
func (m *MockBatch) WithTimestamp(ts int64) helix.Batch {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.timestamp = ts

	return m
}

// SerialConsistency sets the serial consistency level for CAS operations.
func (m *MockBatch) SerialConsistency(_ helix.Consistency) helix.Batch {
	return m
}

// WithPriority sets the replay priority level for this batch.
func (m *MockBatch) WithPriority(_ helix.PriorityLevel) helix.Batch {
	return m
}

// Size returns the number of statements in the batch.
func (m *MockBatch) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.stmts)
}

// Exec executes the batch.
func (m *MockBatch) Exec() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.execHook != nil {
		return m.execHook()
	}

	return m.execErr
}

// ExecContext executes the batch with context.
func (m *MockBatch) ExecContext(_ context.Context) error {
	return m.Exec()
}

// IterContext executes the batch with context and returns an iterator.
func (m *MockBatch) IterContext(_ context.Context) helix.Iter {
	return NewMockIter()
}

// ExecCAS executes a batch lightweight transaction.
func (m *MockBatch) ExecCAS(_ ...any) (applied bool, iter helix.Iter, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return true, NewMockIter(), m.execErr
}

// ExecCASContext executes a batch lightweight transaction with context.
func (m *MockBatch) ExecCASContext(_ context.Context, dest ...any) (applied bool, iter helix.Iter, err error) {
	return m.ExecCAS(dest...)
}

// MapExecCAS executes a batch lightweight transaction and scans into a map.
func (m *MockBatch) MapExecCAS(_ map[string]any) (applied bool, iter helix.Iter, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return true, NewMockIter(), m.execErr
}

// MapExecCASContext executes a batch lightweight transaction with context.
func (m *MockBatch) MapExecCASContext(_ context.Context, dest map[string]any) (applied bool, iter helix.Iter, err error) {
	return m.MapExecCAS(dest)
}

// Statements returns the statements in the batch.
func (m *MockBatch) Statements() []struct {
	Stmt string
	Args []any
} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]struct {
		Stmt string
		Args []any
	}, len(m.stmts))

	for i, s := range m.stmts {
		result[i] = struct {
			Stmt string
			Args []any
		}{Stmt: s.stmt, Args: s.args}
	}

	return result
}

// SetExecError configures the exec error.
func (m *MockBatch) SetExecError(err error) *MockBatch {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.execErr = err

	return m
}

// SetExecHook configures a hook to be called on exec.
func (m *MockBatch) SetExecHook(hook func() error) *MockBatch {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.execHook = hook

	return m
}

// MockIter is a mock implementation of helix.Iter for testing.
type MockIter struct {
	mu        sync.RWMutex
	rows      [][]any
	mapRows   []map[string]any
	index     int
	closeErr  error
	pageState []byte
}

// Compile-time assertion that MockIter implements helix.Iter.
var _ helix.Iter = (*MockIter)(nil)

// NewMockIter creates a new mock iterator.
func NewMockIter() *MockIter {
	return &MockIter{
		index: 0,
	}
}

// Scan reads the next row.
func (m *MockIter) Scan(dest ...any) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.index >= len(m.rows) {
		return false
	}

	row := m.rows[m.index]
	for i := 0; i < len(dest) && i < len(row); i++ {
		copyValue(dest[i], row[i])
	}
	m.index++

	return true
}

// Close closes the iterator.
func (m *MockIter) Close() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.closeErr
}

// MapScan reads the next row into a map.
func (m *MockIter) MapScan(dest map[string]any) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.index >= len(m.mapRows) {
		return false
	}

	row := m.mapRows[m.index]
	for k, v := range row {
		dest[k] = v
	}
	m.index++

	return true
}

// SliceMap returns all remaining rows.
func (m *MockIter) SliceMap() ([]map[string]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closeErr != nil {
		return nil, m.closeErr
	}

	remaining := m.mapRows[m.index:]
	m.index = len(m.mapRows)

	return remaining, nil
}

// PageState returns the pagination state.
func (m *MockIter) PageState() []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.pageState
}

// NumRows returns the number of rows in the current page.
func (m *MockIter) NumRows() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.rows)
}

// Columns returns metadata about the columns in the result set.
func (m *MockIter) Columns() []helix.ColumnInfo {
	return nil
}

// Scanner returns a database/sql-style scanner for the iterator.
func (m *MockIter) Scanner() helix.Scanner {
	return &mockScanner{iter: m}
}

// Warnings returns any warnings from the Cassandra server.
func (m *MockIter) Warnings() []string {
	return nil
}

// AddRow adds a row to the iterator.
func (m *MockIter) AddRow(values ...any) *MockIter {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.rows = append(m.rows, values)

	return m
}

// AddMapRow adds a map row to the iterator.
func (m *MockIter) AddMapRow(row map[string]any) *MockIter {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mapRows = append(m.mapRows, row)

	return m
}

// SetCloseError configures the close error.
func (m *MockIter) SetCloseError(err error) *MockIter {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closeErr = err

	return m
}

// SetPageState sets the page state.
func (m *MockIter) SetPageState(state []byte) *MockIter {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pageState = state

	return m
}

// MockReplayer is a mock implementation of helix.Replayer for testing.
type MockReplayer struct {
	mu       sync.Mutex
	payloads []types.ReplayPayload

	// Hooks
	OnEnqueue func(ctx context.Context, payload types.ReplayPayload) error
}

// Compile-time assertion that MockReplayer implements helix.Replayer.
var _ helix.Replayer = (*MockReplayer)(nil)

// NewMockReplayer creates a new mock replayer.
func NewMockReplayer() *MockReplayer {
	return &MockReplayer{}
}

// Enqueue adds a payload to the replayer.
func (m *MockReplayer) Enqueue(ctx context.Context, payload types.ReplayPayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.OnEnqueue != nil {
		return m.OnEnqueue(ctx, payload)
	}

	m.payloads = append(m.payloads, payload)

	return nil
}

// Payloads returns all enqueued payloads.
func (m *MockReplayer) Payloads() []types.ReplayPayload {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]types.ReplayPayload, len(m.payloads))
	copy(result, m.payloads)

	return result
}

// Clear removes all payloads.
func (m *MockReplayer) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.payloads = nil
}

// copyValue copies a value to a destination pointer.
func copyValue(dest, src any) {
	switch d := dest.(type) {
	case *string:
		if s, ok := src.(string); ok {
			*d = s
		}
	case *int:
		if s, ok := src.(int); ok {
			*d = s
		}
	case *int64:
		if s, ok := src.(int64); ok {
			*d = s
		}
	case *float64:
		if s, ok := src.(float64); ok {
			*d = s
		}
	case *bool:
		if s, ok := src.(bool); ok {
			*d = s
		}
	case *[]byte:
		if s, ok := src.([]byte); ok {
			*d = s
		}
	}
}

// mockScanner implements helix.Scanner for testing.
type mockScanner struct {
	iter    *MockIter
	current []any
	err     error
}

func (s *mockScanner) Next() bool {
	s.iter.mu.Lock()
	defer s.iter.mu.Unlock()

	if s.iter.index >= len(s.iter.rows) {
		return false
	}

	s.current = s.iter.rows[s.iter.index]
	s.iter.index++

	return true
}

func (s *mockScanner) Scan(dest ...any) error {
	if s.err != nil {
		return s.err
	}

	for i := 0; i < len(dest) && i < len(s.current); i++ {
		copyValue(dest[i], s.current[i])
	}

	return nil
}

func (s *mockScanner) Err() error {
	return s.err
}
