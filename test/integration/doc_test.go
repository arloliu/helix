// Package integration_test provides end-to-end integration tests for the helix library.
//
// These tests verify dual-database behavior with real database connections.
//
// # Running Integration Tests
//
// Integration tests are skipped by default when using -short flag:
//
//	go test -short ./...           # Skips integration tests
//	go test ./test/integration/... # Runs integration tests
//
// # SQL Tests
//
// SQL integration tests use SQLite in-memory databases, which require
// the go-sqlite3 driver:
//
//	go get github.com/mattn/go-sqlite3
//
// # CQL Tests
//
// CQL integration tests require Docker and use testcontainers to spin up
// Cassandra instances. They are more resource-intensive and slower.
package integration_test
