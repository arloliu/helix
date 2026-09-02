package helix

import (
	"context"

	"github.com/arloliu/helix/adapter/cql"
)

// cqlIter implements the Iter interface for CQLClient.
type cqlIter struct {
	iter           cql.Iter
	client         *CQLClient
	cluster        ClusterID
	ctx            context.Context // the caller's context, for error provenance on Close
	overrideActive bool            // captured from readTarget at creation, not re-evaluated
}

func (i *cqlIter) Scan(dest ...any) bool {
	return i.iter.Scan(dest...)
}

// Close closes the iterator and reports its outcome like any other read:
// a clean close is a success for the read strategy and the failover
// policy, a cluster error is a failure for both (the strategy's suggested
// alternative is ignored because an iterator cannot be retried), and data
// sentinels or a caller-context error are neither. Auto-refresh accounting
// sees every outcome except a caller-context error.
func (i *cqlIter) Close() error {
	err := i.iter.Close()
	kind := classifyReadErr(i.ctx, err)
	if kind != readCtxErr {
		i.client.recordOpOutcome(i.cluster, err)
	}

	c := i.client
	switch kind {
	case readOK:
		if !i.overrideActive && c.config.ReadStrategy != nil {
			c.config.ReadStrategy.OnSuccess(i.cluster)
		}
		if !c.IsSingleCluster() && c.config.FailoverPolicy != nil {
			c.config.FailoverPolicy.RecordSuccess(i.cluster)
		}
	case readClusterErr:
		if c.IsSingleCluster() {
			break
		}
		if c.config.FailoverPolicy != nil {
			c.config.FailoverPolicy.RecordFailure(i.cluster)
		}
		if !i.overrideActive && c.config.ReadStrategy != nil {
			c.config.ReadStrategy.OnFailure(i.cluster, err)
		}
	case readNotFound, readRowLimit, readCallerNotFound, readCtxErr:
	}

	return err
}

// errorIter is returned when resolveReadTarget fails. It defers the error
// to Close() since IterContext's signature cannot return an error directly.
// Scan()/MapScan() always return false. Close()/SliceMap() return the error.
// Metadata methods return zero values.
type errorIter struct {
	err error
}

func (e *errorIter) Scan(...any) bool                    { return false }
func (e *errorIter) Close() error                        { return e.err }
func (e *errorIter) MapScan(map[string]any) bool         { return false }
func (e *errorIter) SliceMap() ([]map[string]any, error) { return nil, e.err }
func (e *errorIter) PageState() []byte                   { return nil }
func (e *errorIter) NumRows() int                        { return 0 }
func (e *errorIter) Columns() []ColumnInfo               { return nil }
func (e *errorIter) Scanner() Scanner                    { return &errorScanner{err: e.err} }
func (e *errorIter) Warnings() []string                  { return nil }

// errorScanner is returned by errorIter.Scanner() so callers using the
// Scanner API get a graceful error instead of a nil-pointer panic.
type errorScanner struct{ err error }

func (s *errorScanner) Next() bool        { return false }
func (s *errorScanner) Scan(...any) error { return s.err }
func (s *errorScanner) Err() error        { return s.err }

func (i *cqlIter) MapScan(m map[string]any) bool {
	return i.iter.MapScan(m)
}

func (i *cqlIter) SliceMap() ([]map[string]any, error) {
	return i.iter.SliceMap()
}

func (i *cqlIter) PageState() []byte {
	return i.iter.PageState()
}

func (i *cqlIter) NumRows() int {
	return i.iter.NumRows()
}

func (i *cqlIter) Columns() []ColumnInfo {
	cqlCols := i.iter.Columns()
	result := make([]ColumnInfo, len(cqlCols))
	for idx, col := range cqlCols {
		result[idx] = ColumnInfo{
			Keyspace: col.Keyspace,
			Table:    col.Table,
			Name:     col.Name,
			TypeInfo: col.TypeInfo,
		}
	}

	return result
}

func (i *cqlIter) Scanner() Scanner {
	return &cqlScanner{scanner: i.iter.Scanner()}
}

func (i *cqlIter) Warnings() []string {
	return i.iter.Warnings()
}

// cqlScanner wraps cql.Scanner to implement helix.Scanner.
type cqlScanner struct {
	scanner cql.Scanner
}

func (s *cqlScanner) Next() bool {
	return s.scanner.Next()
}

func (s *cqlScanner) Scan(dest ...any) error {
	return s.scanner.Scan(dest...)
}

func (s *cqlScanner) Err() error {
	return s.scanner.Err()
}
