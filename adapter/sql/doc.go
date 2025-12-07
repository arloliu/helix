// Package sql provides adapter interfaces for database/sql drivers to work with helix.
//
// This package defines interfaces that wrap the standard library's database/sql types,
// allowing helix to provide dual-database support for SQL databases like PostgreSQL,
// MySQL, SQLite, and others.
//
// # Interfaces
//
// The package defines interfaces that mirror database/sql:
//
//   - [DB]: Wraps *sql.DB for database connections
//   - [Tx]: Wraps *sql.Tx for transactions
//   - [Row]: Wraps *sql.Row for single-row results
//   - [Rows]: Wraps *sql.Rows for multi-row results
//   - [Result]: Wraps sql.Result for exec results
//   - [Stmt]: Wraps *sql.Stmt for prepared statements
//
// # Usage with SQLClient
//
// Use these interfaces with helix.SQLClient for dual-database SQL operations:
//
//	import (
//	    "database/sql"
//	    "github.com/arloliu/helix"
//	    _ "github.com/lib/pq"
//	)
//
//	// Create database connections
//	dbA, _ := sql.Open("postgres", "postgres://localhost:5432/db_a")
//	dbB, _ := sql.Open("postgres", "postgres://localhost:5433/db_b")
//
//	// Create helix SQL client
//	client, _ := helix.NewSQLClient(dbA, dbB,
//	    helix.WithSQLReadStrategy(policy.NewStickyRead()),
//	)
//
//	// Execute queries with dual-write support
//	result, err := client.ExecContext(ctx,
//	    "INSERT INTO users (id, name) VALUES ($1, $2)", id, name)
//
// # Transactions
//
// The SQLClient supports transactions that span both clusters:
//
//	tx, err := client.BeginTx(ctx, nil)
//	if err != nil {
//	    return err
//	}
//	defer tx.Rollback()
//
//	_, err = tx.ExecContext(ctx, "INSERT INTO users ...", args...)
//	if err != nil {
//	    return err
//	}
//
//	return tx.Commit()
//
// Note: Distributed transactions do NOT provide ACID guarantees across clusters.
// Each cluster's transaction is independent.
package sql
