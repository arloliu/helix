// Package testutil provides test utilities and mock implementations for helix testing.
//
// This package provides mock implementations of helix interfaces for unit testing,
// as well as helper functions for integration tests.
//
// # Mock Implementations
//
// The package provides mock implementations for testing:
//
//   - [MockCQLSession]: Mock implementation of helix.CQLSession
//   - [MockQuery]: Mock implementation of helix.Query
//   - [MockIter]: Mock implementation of helix.Iter
//   - [MockBatch]: Mock implementation of helix.Batch
//   - [MockReplayer]: Mock implementation of helix.Replayer
//
// # Usage
//
// Create mock sessions for testing helix clients:
//
//	sessionA := testutil.NewMockCQLSession()
//	sessionB := testutil.NewMockCQLSession()
//
//	// Configure mock behavior
//	sessionA.OnQuery("SELECT * FROM users", nil).Returns(mockData)
//
//	// Create client with mocks
//	client, _ := helix.NewCQLClient(sessionA, sessionB)
//
// # Integration Test Helpers
//
// For integration tests, helper functions are provided:
//
//   - StartEmbeddedNATS: Starts an embedded NATS server for replay testing
//   - StartCassandraContainer: Starts a Cassandra test container (requires Docker)
package testutil
