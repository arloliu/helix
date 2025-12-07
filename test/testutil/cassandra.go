// Package testutil provides testing utilities for the helix project.
package testutil

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/cassandra"
)

// CassandraContainer wraps a Cassandra test container.
type CassandraContainer struct {
	Container *cassandra.CassandraContainer
	Host      string
	Port      string
	Session   *gocql.Session
}

// CassandraOptions configures the Cassandra container.
type CassandraOptions struct {
	// Image is the Cassandra image to use. Defaults to "cassandra:4.1".
	Image string
	// Keyspace is the keyspace to create. Defaults to "test_keyspace".
	Keyspace string
}

// DefaultCassandraOptions returns default options for Cassandra container.
func DefaultCassandraOptions() CassandraOptions {
	return CassandraOptions{
		Image:    "cassandra:4.1",
		Keyspace: "test_keyspace",
	}
}

// StartCassandra starts a Cassandra container for testing.
//
// The container is automatically terminated when the test completes.
// This is preferred over ScyllaDB for environments with limited AIO resources.
//
// Parameters:
//   - ctx: Context for container operations
//   - t: Testing context for cleanup registration
//   - opts: Optional configuration (nil uses defaults)
//
// Returns:
//   - *CassandraContainer: Container with connection details and session
//   - error: Error if container fails to start
func StartCassandra(ctx context.Context, t *testing.T, opts *CassandraOptions) (*CassandraContainer, error) {
	t.Helper()

	if opts == nil {
		defaultOpts := DefaultCassandraOptions()
		opts = &defaultOpts
	}

	// Start Cassandra container
	container, err := cassandra.Run(ctx, opts.Image,
		testcontainers.WithEnv(map[string]string{
			"HEAP_NEWSIZE":     "128M",
			"MAX_HEAP_SIZE":    "512M",
			"CASSANDRA_SNITCH": "SimpleSnitch",
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start Cassandra container: %w", err)
	}

	// Register cleanup
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate Cassandra container: %v", err)
		}
	})

	// Get connection host
	host, err := container.ConnectionHost(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection host: %w", err)
	}

	// Create cluster config
	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 60 * time.Second
	cluster.ConnectTimeout = 60 * time.Second

	// Wait for Cassandra to be ready and connect to system keyspace
	cluster.Keyspace = "system"
	var session *gocql.Session
	for i := 0; i < 10; i++ {
		session, err = cluster.CreateSession()
		if err == nil {
			break
		}
		t.Logf("waiting for Cassandra to be ready (attempt %d/10): %v", i+1, err)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create session after retries: %w", err)
	}

	// Create test keyspace
	createKeyspaceQuery := fmt.Sprintf(`
		CREATE KEYSPACE IF NOT EXISTS %s
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
	`, opts.Keyspace)

	if err := session.Query(createKeyspaceQuery).Exec(); err != nil {
		session.Close()
		return nil, fmt.Errorf("failed to create keyspace: %w", err)
	}

	session.Close()

	// Reconnect to the test keyspace
	cluster.Keyspace = opts.Keyspace
	session, err = cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create session for keyspace %s: %w", opts.Keyspace, err)
	}

	t.Cleanup(func() {
		session.Close()
	})

	return &CassandraContainer{
		Container: container,
		Host:      host,
		Session:   session,
	}, nil
}

// StartTwoCassandraContainers starts two independent Cassandra containers for dual-cluster testing.
//
// This is useful for testing Helix's dual-write and failover functionality.
//
// Parameters:
//   - ctx: Context for container operations
//   - t: Testing context for cleanup registration
//
// Returns:
//   - containerA: First Cassandra container
//   - containerB: Second Cassandra container
//   - error: Error if any container fails to start
func StartTwoCassandraContainers(ctx context.Context, t *testing.T) (containerA, containerB *CassandraContainer, err error) {
	t.Helper()

	optsA := DefaultCassandraOptions()
	optsA.Keyspace = "helix_cluster_a"

	optsB := DefaultCassandraOptions()
	optsB.Keyspace = "helix_cluster_b"

	containerA, err = StartCassandra(ctx, t, &optsA)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start cluster A: %w", err)
	}

	containerB, err = StartCassandra(ctx, t, &optsB)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start cluster B: %w", err)
	}

	return containerA, containerB, nil
}
