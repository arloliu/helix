//go:build e2e

// Package cql_test contains end-to-end tests that exercise Helix against
// REAL Cassandra/ScyllaDB containers driven through container-lifecycle
// chaos (Stop, Start, Pause, Unpause).
//
// Why this package exists separately from test/integration:
//   - test/integration shares one container pair across all tests via TestMain.
//     A test that stops a container would either break peers or have to
//     restore state perfectly — too fragile.
//   - These tests deliberately drive the gocql driver through its native
//     reconnect / timeout / error-emission paths, which the chaos-session
//     wrapper used elsewhere cannot reach (chaos injects errors above the
//     driver).
//   - Tests here exercise BOTH adapter/cql/v1 (gocql) and adapter/cql/v2
//     (apache/cassandra-gocql-driver) against the same containers, asserting
//     parity of Helix-observable outcomes.
//
// Conventions for tests in this package:
//   - Tests run sequentially. DO NOT call t.Parallel() — these tests stop
//     the shared container pair.
//   - Every test that calls cluster.Stop / cluster.Pause MUST register a
//     restore via t.Cleanup BEFORE the destructive call, so that a panic
//     between Stop and Cleanup-registration does not leave the cluster
//     dead for subsequent tests.
//   - Use unique table-name suffixes per test (mirrors test/integration
//     pattern); TRUNCATE in cleanup, do not DROP.
//   - Build-tagged with `e2e` and excluded from `go test ./...` and
//     `make test`. Run via `make test-e2e`.
package cql_test
