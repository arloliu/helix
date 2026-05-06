// Package testutil provides testing utilities for the helix project.
package testutil

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	gocqlv2 "github.com/apache/cassandra-gocql-driver/v2"
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
	Type      CQLClusterType
	Host      string
	Session   *gocql.Session
	SessionV2 *gocqlv2.Session

	// Internal container references for cleanup
	scyllaContainer    *scylladb.Container
	cassandraContainer *cassandra.CassandraContainer

	// Stash of construction parameters so Reconnect() can rebuild sessions
	// against the (possibly remapped) host with the same configuration.
	keyspace          string
	sessionTimeout    time.Duration
	connectTimeout    time.Duration
	reconnectInterval time.Duration
}

// Close closes the session (does not terminate the container).
func (c *CQLCluster) Close() {
	if c.Session != nil {
		c.Session.Close()
		c.Session = nil
	}
	if c.SessionV2 != nil {
		c.SessionV2.Close()
		c.SessionV2 = nil
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

	// SessionTimeout sets gocql's per-query Timeout for both v1 and v2.
	// Zero means use the backend-specific default (30s Scylla, 60s Cassandra).
	// e2e tests should set this to a short value (e.g., 2s) so failure modes
	// surface quickly and so v1/v2 parity assertions are meaningful.
	SessionTimeout time.Duration
	// ConnectTimeout overrides gocql's ConnectTimeout for both drivers.
	// Defaults to SessionTimeout when SessionTimeout is non-zero.
	ConnectTimeout time.Duration
	// ReconnectInterval sets gocql's ReconnectInterval (v1 only — v2 may
	// expose a similar knob or rely on its retry policy). Zero means default.
	ReconnectInterval time.Duration
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

	sessionTimeout := opts.SessionTimeout
	if sessionTimeout == 0 {
		sessionTimeout = 30 * time.Second
	}
	connectTimeout := opts.ConnectTimeout
	if connectTimeout == 0 {
		connectTimeout = sessionTimeout
	}

	session, err := createCQLSession(host, opts.Keyspace, sessionTimeout, connectTimeout, opts.ReconnectInterval)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	sessionV2, err := createCQLSessionV2(host, opts.Keyspace, sessionTimeout, connectTimeout)
	if err != nil {
		session.Close()
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to create session v2: %w", err)
	}

	return &CQLCluster{
		Type:              CQLClusterTypeScyllaDB,
		Host:              host,
		Session:           session,
		SessionV2:         sessionV2,
		scyllaContainer:   container,
		keyspace:          opts.Keyspace,
		sessionTimeout:    sessionTimeout,
		connectTimeout:    connectTimeout,
		reconnectInterval: opts.ReconnectInterval,
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

	sessionTimeout := opts.SessionTimeout
	if sessionTimeout == 0 {
		sessionTimeout = 60 * time.Second
	}
	connectTimeout := opts.ConnectTimeout
	if connectTimeout == 0 {
		connectTimeout = sessionTimeout
	}

	session, err := createCQLSession(host, opts.Keyspace, sessionTimeout, connectTimeout, opts.ReconnectInterval)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	sessionV2, err := createCQLSessionV2(host, opts.Keyspace, sessionTimeout, connectTimeout)
	if err != nil {
		session.Close()
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to create session v2: %w", err)
	}

	return &CQLCluster{
		Type:               CQLClusterTypeCassandra,
		Host:               host,
		Session:            session,
		SessionV2:          sessionV2,
		cassandraContainer: container,
		keyspace:           opts.Keyspace,
		sessionTimeout:     sessionTimeout,
		connectTimeout:     connectTimeout,
		reconnectInterval:  opts.ReconnectInterval,
	}, nil
}

func createCQLSession(host, keyspace string, timeout, connectTimeout, reconnectInterval time.Duration) (*gocql.Session, error) {
	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = timeout
	cluster.ConnectTimeout = connectTimeout
	if reconnectInterval > 0 {
		cluster.ReconnectInterval = reconnectInterval
	}
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

func createCQLSessionV2(host, keyspace string, timeout, connectTimeout time.Duration) (*gocqlv2.Session, error) {
	cluster := gocqlv2.NewCluster(host)
	cluster.Consistency = gocqlv2.Quorum
	cluster.Timeout = timeout
	cluster.ConnectTimeout = connectTimeout
	cluster.Keyspace = keyspace

	return cluster.CreateSession()
}
