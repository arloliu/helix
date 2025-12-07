// Package sql provides SQL-specific adapter interfaces for database/sql.
//
// This package defines interfaces that wrap the standard library's database/sql
// types to enable dual-cluster operations with Helix.
//
// Limitations:
//   - Single-statement non-transactional operations only
//   - No transaction support across clusters
//   - Best-effort semantics (no strong consistency guarantees)
package sql

import (
	"context"
	"database/sql"
)

// DB represents a database connection that can be used with Helix.
//
// This interface wraps *sql.DB to allow for dual-cluster operations.
type DB interface {
	// ExecContext executes a query without returning any rows.
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)

	// QueryContext executes a query that returns rows.
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)

	// QueryRowContext executes a query that returns at most one row.
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row

	// PingContext verifies the connection is alive.
	PingContext(ctx context.Context) error

	// Close closes the database connection.
	Close() error
}

// dbAdapter wraps *sql.DB to implement the DB interface.
type dbAdapter struct {
	db *sql.DB
}

// NewDBAdapter creates a new DB adapter wrapping a *sql.DB.
//
// Parameters:
//   - db: The underlying sql.DB to wrap
//
// Returns:
//   - DB: An adapter implementing the DB interface
func NewDBAdapter(db *sql.DB) DB {
	return &dbAdapter{db: db}
}

// WrapDB is an alias for NewDBAdapter that wraps a *sql.DB.
//
// This is useful for migrating existing database/sql code to use Helix.
//
// Example:
//
//	db, _ := sql.Open("postgres", dsn)
//	helix.NewSQLClient(sqladapter.WrapDB(db), nil) // single-database mode
//
// Parameters:
//   - db: The underlying sql.DB to wrap
//
// Returns:
//   - DB: An adapter implementing the DB interface
func WrapDB(db *sql.DB) DB {
	return NewDBAdapter(db)
}

// ExecContext executes a query without returning any rows.
func (a *dbAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.db.ExecContext(ctx, query, args...)
}

// QueryContext executes a query that returns rows.
func (a *dbAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.db.QueryContext(ctx, query, args...)
}

// QueryRowContext executes a query that returns at most one row.
func (a *dbAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return a.db.QueryRowContext(ctx, query, args...)
}

// PingContext verifies the connection is alive.
func (a *dbAdapter) PingContext(ctx context.Context) error {
	return a.db.PingContext(ctx)
}

// Close closes the database connection.
func (a *dbAdapter) Close() error {
	return a.db.Close()
}
