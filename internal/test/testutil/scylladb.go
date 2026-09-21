// Package testutil provides testing utilities for the helix project.
package testutil

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/testcontainers/testcontainers-go/modules/scylladb"
)

// ScyllaDBContainer wraps a ScyllaDB test container.
type ScyllaDBContainer struct {
	Container *scylladb.Container
	Host      string
	Port      string
	Session   *gocql.Session
}

// ScyllaDBOptions configures the ScyllaDB container.
type ScyllaDBOptions struct {
	// Image is the ScyllaDB image to use. Defaults to "scylladb/scylla:6.2".
	Image string
	// Keyspace is the keyspace to create. Defaults to "test_keyspace".
	Keyspace string
	// Memory is the memory limit for ScyllaDB. Defaults to "512M".
	Memory string
	// SMP is the number of CPU cores for ScyllaDB. Defaults to 1.
	SMP int
}

// DefaultScyllaDBOptions returns default options for ScyllaDB container.
func DefaultScyllaDBOptions() ScyllaDBOptions {
	return ScyllaDBOptions{
		Image:    "scylladb/scylla:6.2",
		Keyspace: "test_keyspace",
		Memory:   "512M",
		SMP:      1,
	}
}

// StartScyllaDB starts a ScyllaDB container for testing.
//
// The container is automatically terminated when the test completes.
//
// Parameters:
//   - ctx: Context for container operations
//   - t: Testing context for cleanup registration
//   - opts: Optional configuration (nil uses defaults)
//
// Returns:
//   - *ScyllaDBContainer: Container with connection details and session
//
// StartScyllaDB starts a ScyllaDB container for testing.
//
// The container is automatically terminated when the test completes.
// Uses --reactor-backend=epoll to avoid Linux AIO requirements.
//
// Parameters:
//   - ctx: Context for container operations
//   - t: Testing context for cleanup registration
//   - opts: Optional configuration (nil uses defaults)
//
// Returns:
//   - *ScyllaDBContainer: Container with connection details and session
//   - error: Error if container fails to start
//
// Note: ScyllaDB requires Linux AIO (aio-max-nr kernel limit). If your system's
// /proc/sys/fs/aio-nr equals /proc/sys/fs/aio-max-nr, ScyllaDB will fail to start.
// In this case, either increase aio-max-nr or use Cassandra as an alternative.
// To check: cat /proc/sys/fs/aio-nr /proc/sys/fs/aio-max-nr
// To fix: sudo sysctl -w fs.aio-max-nr=1048576
func StartScyllaDB(ctx context.Context, t *testing.T, opts *ScyllaDBOptions) (*ScyllaDBContainer, error) {
	t.Helper()

	if opts == nil {
		defaultOpts := DefaultScyllaDBOptions()
		opts = &defaultOpts
	}

	// Start ScyllaDB container with resource-efficient settings
	// --reactor-backend=epoll: Use epoll instead of linux-aio (helps but doesn't eliminate AIO need)
	// --developer-mode=1: Relax production checks
	// --overprovisioned=1: Optimize for overprovisioned environment
	container, err := scylladb.Run(ctx, opts.Image,
		scylladb.WithShardAwareness(),
		scylladb.WithCustomCommands(
			fmt.Sprintf("--memory=%s", opts.Memory),
			fmt.Sprintf("--smp=%d", opts.SMP),
			"--developer-mode=1",
			"--overprovisioned=1",
			"--reactor-backend=epoll", // Reduces AIO requirements
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start ScyllaDB container: %w", err)
	}

	// Register cleanup
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate ScyllaDB container: %v", err)
		}
	})

	// Get connection host
	host, err := container.NonShardAwareConnectionHost(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection host: %w", err)
	}

	// Create cluster config
	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second

	// Connect to system keyspace first
	cluster.Keyspace = "system"
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
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

	return &ScyllaDBContainer{
		Container: container,
		Host:      host,
		Session:   session,
	}, nil
}

// StartTwoScyllaDBContainers starts two independent ScyllaDB containers for dual-cluster testing.
//
// This is useful for testing Helix's dual-write and failover functionality.
//
// Parameters:
//   - ctx: Context for container operations
//   - t: Testing context for cleanup registration
//
// Returns:
//   - containerA: First ScyllaDB container
//   - containerB: Second ScyllaDB container
//   - error: Error if any container fails to start
func StartTwoScyllaDBContainers(ctx context.Context, t *testing.T) (containerA, containerB *ScyllaDBContainer, err error) {
	t.Helper()

	optsA := DefaultScyllaDBOptions()
	optsA.Keyspace = "helix_cluster_a"

	optsB := DefaultScyllaDBOptions()
	optsB.Keyspace = "helix_cluster_b"

	containerA, err = StartScyllaDB(ctx, t, &optsA)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start cluster A: %w", err)
	}

	containerB, err = StartScyllaDB(ctx, t, &optsB)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start cluster B: %w", err)
	}

	return containerA, containerB, nil
}

// CreateTable creates a table in the given session.
//
// Parameters:
//   - session: gocql session
//   - cql: CQL CREATE TABLE statement
//
// Returns:
//   - error: Error if table creation fails
func CreateTable(session *gocql.Session, cql string) error {
	return session.Query(cql).Exec()
}
