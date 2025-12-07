// Package testutil provides testing utilities for the helix project.
package testutil

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/cassandra"
	"github.com/testcontainers/testcontainers-go/modules/scylladb"
)

// CQLClusterType identifies the database backend.
type CQLClusterType int

const (
	// CQLClusterTypeNone indicates no cluster is running.
	CQLClusterTypeNone CQLClusterType = iota
	// CQLClusterTypeScyllaDB indicates ScyllaDB is being used.
	CQLClusterTypeScyllaDB
	// CQLClusterTypeCassandra indicates Cassandra is being used.
	CQLClusterTypeCassandra
)

// String returns the string representation of the cluster type.
func (t CQLClusterType) String() string {
	switch t {
	case CQLClusterTypeScyllaDB:
		return "ScyllaDB"
	case CQLClusterTypeCassandra:
		return "Cassandra"
	case CQLClusterTypeNone:
		return "None"
	}

	return "Unknown"
}

// CQLCluster represents a CQL-compatible database cluster for testing.
// It abstracts over ScyllaDB and Cassandra containers.
type CQLCluster struct {
	Type    CQLClusterType
	Host    string
	Session *gocql.Session

	// Internal container references for cleanup
	scyllaContainer    *scylladb.Container
	cassandraContainer *cassandra.CassandraContainer
}

// Close closes the session (does not terminate the container).
func (c *CQLCluster) Close() {
	if c.Session != nil {
		c.Session.Close()
		c.Session = nil
	}
}

// Terminate terminates the container.
func (c *CQLCluster) Terminate(ctx context.Context) error {
	c.Close()

	switch c.Type {
	case CQLClusterTypeScyllaDB:
		if c.scyllaContainer != nil {
			return c.scyllaContainer.Terminate(ctx)
		}
	case CQLClusterTypeCassandra:
		if c.cassandraContainer != nil {
			return c.cassandraContainer.Terminate(ctx)
		}
	case CQLClusterTypeNone:
		// Nothing to terminate
	}

	return nil
}

// CQLClusterOptions configures the CQL cluster container.
type CQLClusterOptions struct {
	// Keyspace is the keyspace to create. Required.
	Keyspace string
	// PreferScyllaDB attempts to use ScyllaDB first, falls back to Cassandra.
	// Default: true
	PreferScyllaDB bool
	// ScyllaDBImage is the ScyllaDB image. Default: "scylladb/scylla:6.2"
	ScyllaDBImage string
	// CassandraImage is the Cassandra image. Default: "cassandra:4.1"
	CassandraImage string
	// Memory for ScyllaDB. Default: "512M"
	ScyllaDBMemory string
	// SMP (CPU cores) for ScyllaDB. Default: 1
	ScyllaDBSMP int
}

// DefaultCQLClusterOptions returns default options.
func DefaultCQLClusterOptions(keyspace string) CQLClusterOptions {
	return CQLClusterOptions{
		Keyspace:       keyspace,
		PreferScyllaDB: true,
		ScyllaDBImage:  "scylladb/scylla:6.2",
		CassandraImage: "cassandra:4.1",
		ScyllaDBMemory: "512M",
		ScyllaDBSMP:    1,
	}
}

// IsAIOAvailable checks if the system has available AIO slots for ScyllaDB.
func IsAIOAvailable() bool {
	aioNrData, err := os.ReadFile("/proc/sys/fs/aio-nr")
	if err != nil {
		return false // Not on Linux or can't read
	}

	aioMaxNrData, err := os.ReadFile("/proc/sys/fs/aio-max-nr")
	if err != nil {
		return false
	}

	aioNr, _ := strconv.ParseInt(strings.TrimSpace(string(aioNrData)), 10, 64)
	aioMaxNr, _ := strconv.ParseInt(strings.TrimSpace(string(aioMaxNrData)), 10, 64)

	// ScyllaDB needs at least some AIO slots available
	return aioNr < aioMaxNr
}

// StartCQLCluster starts a CQL-compatible database cluster for testing.
// Prefers ScyllaDB (faster), falls back to Cassandra if AIO is unavailable.
//
// This function is designed for use in TestMain where *testing.T is not available.
// Caller is responsible for calling cluster.Terminate(ctx) for cleanup.
//
// Parameters:
//   - ctx: Context for container operations
//   - opts: Configuration options
//
// Returns:
//   - *CQLCluster: Cluster with connection details and session
//   - error: Error if cluster fails to start
func StartCQLCluster(ctx context.Context, opts CQLClusterOptions) (*CQLCluster, error) {
	if opts.PreferScyllaDB && IsAIOAvailable() {
		cluster, err := startScyllaDBCluster(ctx, opts)
		if err == nil {
			return cluster, nil
		}
		// Fall back to Cassandra on ScyllaDB failure
		fmt.Printf("ScyllaDB failed: %v, falling back to Cassandra...\n", err)
	}

	return startCassandraCluster(ctx, opts)
}

// StartTwoCQLClusters starts two independent CQL clusters for dual-cluster testing.
// Prefers ScyllaDB (faster), falls back to Cassandra if AIO is unavailable.
//
// This function is designed for use in TestMain where *testing.T is not available.
// Caller is responsible for calling Terminate(ctx) on both clusters for cleanup.
//
// Parameters:
//   - ctx: Context for container operations
//
// Returns:
//   - clusterA: First CQL cluster
//   - clusterB: Second CQL cluster
//   - error: Error if any cluster fails to start
func StartTwoCQLClusters(ctx context.Context) (clusterA, clusterB *CQLCluster, err error) {
	optsA := DefaultCQLClusterOptions("helix_test_a")
	optsB := DefaultCQLClusterOptions("helix_test_b")

	clusterA, err = StartCQLCluster(ctx, optsA)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start cluster A: %w", err)
	}

	// Use the same type for cluster B
	optsB.PreferScyllaDB = (clusterA.Type == CQLClusterTypeScyllaDB)

	clusterB, err = StartCQLCluster(ctx, optsB)
	if err != nil {
		_ = clusterA.Terminate(ctx)
		return nil, nil, fmt.Errorf("failed to start cluster B: %w", err)
	}

	return clusterA, clusterB, nil
}

func startScyllaDBCluster(ctx context.Context, opts CQLClusterOptions) (*CQLCluster, error) {
	container, err := scylladb.Run(ctx, opts.ScyllaDBImage,
		scylladb.WithShardAwareness(),
		scylladb.WithCustomCommands(
			fmt.Sprintf("--memory=%s", opts.ScyllaDBMemory),
			fmt.Sprintf("--smp=%d", opts.ScyllaDBSMP),
			"--developer-mode=1",
			"--overprovisioned=1",
			"--reactor-backend=epoll",
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start ScyllaDB container: %w", err)
	}

	host, err := container.NonShardAwareConnectionHost(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get connection host: %w", err)
	}

	session, err := createCQLSession(host, opts.Keyspace, 30*time.Second)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	return &CQLCluster{
		Type:            CQLClusterTypeScyllaDB,
		Host:            host,
		Session:         session,
		scyllaContainer: container,
	}, nil
}

func startCassandraCluster(ctx context.Context, opts CQLClusterOptions) (*CQLCluster, error) {
	container, err := cassandra.Run(ctx, opts.CassandraImage,
		testcontainers.WithEnv(map[string]string{
			"HEAP_NEWSIZE":     "128M",
			"MAX_HEAP_SIZE":    "512M",
			"CASSANDRA_SNITCH": "SimpleSnitch",
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start Cassandra container: %w", err)
	}

	host, err := container.ConnectionHost(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get connection host: %w", err)
	}

	session, err := createCQLSession(host, opts.Keyspace, 60*time.Second)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	return &CQLCluster{
		Type:               CQLClusterTypeCassandra,
		Host:               host,
		Session:            session,
		cassandraContainer: container,
	}, nil
}

func createCQLSession(host, keyspace string, timeout time.Duration) (*gocql.Session, error) {
	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = timeout
	cluster.ConnectTimeout = timeout
	cluster.Keyspace = "system"

	// Connect to system keyspace first
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to system keyspace: %w", err)
	}

	// Create test keyspace
	createKeyspaceQuery := fmt.Sprintf(`
		CREATE KEYSPACE IF NOT EXISTS %s
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
	`, keyspace)

	if err := session.Query(createKeyspaceQuery).Exec(); err != nil {
		session.Close()
		return nil, fmt.Errorf("failed to create keyspace: %w", err)
	}
	session.Close()

	// Reconnect to test keyspace
	cluster.Keyspace = keyspace

	return cluster.CreateSession()
}
