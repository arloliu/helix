package testutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"strings"
	"time"

	gocqlv2 "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/gocql/gocql"
	dnet "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// twoNodeCQLPort is the CQL port every node listens on.
	// The nodes are reached on their bridge addresses,
	// so no host port mapping is used
	// and the port is the same inside and outside the container.
	twoNodeCQLPort = 9042

	// twoNodeFirstOctet is the last octet of the first node's address inside the cluster's subnet.
	// Node i sits at twoNodeFirstOctet+i, well clear of the gateway Docker takes at .1.
	twoNodeFirstOctet = 11

	// twoNodeReadyLog is the line ScyllaDB prints once its CQL server is accepting clients.
	twoNodeReadyLog = "Starting listening for CQL clients"

	// twoNodeStatusPoll is how often cluster formation is re-checked through nodetool,
	// which has no event to subscribe to.
	twoNodeStatusPoll = 500 * time.Millisecond
)

// TwoNodeCQLClusterOptions configures a [TwoNodeCQLCluster].
type TwoNodeCQLClusterOptions struct {
	// Keyspace is the keyspace to create at replication factor 2. Required.
	Keyspace string
	// Image is the ScyllaDB image.
	// Default: "scylladb/scylla:6.2".
	Image string
	// Subnet is the IPv4 /24 the cluster's bridge network is built on.
	// Pick one that cannot collide with the host's other networks.
	// Default: "172.31.99.0/24".
	Subnet string
	// Memory is the per-node memory budget.
	// Default: "1G".
	// ScyllaDB needs more headroom to gossip and stream than the 512M the single-node helper runs on;
	// 512M has not been shown to form a cluster.
	Memory string
	// SMP is the per-node shard count.
	// Default: 1.
	SMP int

	// SessionTimeout sets the drivers' per-query Timeout for both v1 and v2.
	// Default: 2s, matching the e2e suite's single-node clusters.
	SessionTimeout time.Duration
	// ConnectTimeout overrides the drivers' ConnectTimeout.
	// Defaults to SessionTimeout.
	ConnectTimeout time.Duration
	// ReconnectInterval sets the drivers' ReconnectInterval.
	// Zero keeps the drivers' 60s default,
	// which is far longer than any scenario waits for a paused node to come back.
	ReconnectInterval time.Duration
	// RetryNextHost gives both drivers a SimpleRetryPolicy with one retry per node,
	// so a request that a failing node cannot answer is re-issued against the other node.
	// Default: true.
	//
	// Neither driver installs a retry policy of its own,
	// and without one a two-node cluster loses a request whenever a node goes down:
	// the driver keeps handing out the dying node's connection for a moment after the cluster
	// reports it down, and the request on that connection dies with it.
	// The caller sees a cluster error for a fault that is confined to one node,
	// which is precisely the distinction a two-node cluster exists to make.
	// Set it to false to observe that raw behaviour.
	RetryNextHost bool
	// StartTimeout bounds one node's boot.
	// Default: 3m.
	StartTimeout time.Duration
	// FormationTimeout bounds the wait for both nodes to report up and normal to each other.
	// Default: 3m.
	FormationTimeout time.Duration
}

// TwoNodeCQLCluster is a two-node ScyllaDB cluster on a private bridge network,
// used to tell a node-level fault apart from a cluster-level one.
//
// The single-node [CQLCluster] cannot express that difference:
// its container is the whole cluster, so every fault it can inject is a cluster fault.
// This type is deliberately separate rather than an extension of it:
// [CQLCluster.Pause], [CQLCluster.Kill] and [CQLCluster.NetworkDisconnect]
// all act on one container and have no node to name.
//
// Both nodes hold every row: the keyspace is created at replication factor 2,
// so a read at consistency One survives one node being paused
// while a read at Quorum or All does not.
//
// The nodes are addressed directly on the bridge network, and no host port mapping is used.
// Testcontainers still publishes the CQL port to the host, which nothing connects to.
// ScyllaDB advertises those same addresses in system.peers,
// so the drivers' peer discovery resolves to addresses the test process can reach.
type TwoNodeCQLCluster struct {
	// Keyspace is the replication-factor-2 keyspace both sessions are bound to.
	Keyspace string
	// Session is a gocql v1 session whose contact point is node 0.
	Session *gocql.Session
	// SessionV2 is a gocql v2 session whose contact point is node 0.
	SessionV2 *gocqlv2.Session

	nodes   []testcontainers.Container
	nodeIPs []netip.Addr
	network *testcontainers.DockerNetwork
	opts    TwoNodeCQLClusterOptions
}

// DefaultTwoNodeCQLClusterOptions returns the options a two-node cluster runs on
// unless a caller overrides them.
//
// Parameters:
//   - keyspace: the keyspace to create at replication factor 2
//
// Returns:
//   - TwoNodeCQLClusterOptions: defaults ready to pass to [StartTwoNodeCQLCluster]
func DefaultTwoNodeCQLClusterOptions(keyspace string) TwoNodeCQLClusterOptions {
	return TwoNodeCQLClusterOptions{
		Keyspace:         keyspace,
		Image:            "scylladb/scylla:6.2",
		Subnet:           "172.31.99.0/24",
		Memory:           "1G",
		SMP:              1,
		SessionTimeout:   2 * time.Second,
		ConnectTimeout:   2 * time.Second,
		RetryNextHost:    true,
		StartTimeout:     3 * time.Minute,
		FormationTimeout: 3 * time.Minute,
	}
}

// StartTwoNodeCQLCluster brings up a two-node ScyllaDB cluster
// and returns it once both nodes see each other as up and normal.
//
// The caller owns the result and must call [TwoNodeCQLCluster.Terminate],
// which removes the nodes and the network they share.
//
// Parameters:
//   - ctx: bounds container creation and the wait for cluster formation
//   - opts: configuration; see [DefaultTwoNodeCQLClusterOptions]
//
// Returns:
//   - *TwoNodeCQLCluster: a formed cluster with both driver sessions open
//   - error: if the network, either node, the keyspace or a session fails
func StartTwoNodeCQLCluster(ctx context.Context, opts TwoNodeCQLClusterOptions) (*TwoNodeCQLCluster, error) {
	if opts.Keyspace == "" {
		return nil, errors.New("TwoNodeCQLCluster: Keyspace is required")
	}
	applyTwoNodeDefaults(&opts)

	ips, err := twoNodeAddresses(opts.Subnet, 2)
	if err != nil {
		return nil, err
	}

	ipam, err := twoNodeIPAM(opts.Subnet)
	if err != nil {
		return nil, err
	}
	nw, err := tcnetwork.New(ctx, tcnetwork.WithIPAM(ipam))
	if err != nil {
		return nil, fmt.Errorf("create network: %w", err)
	}

	c := &TwoNodeCQLCluster{
		Keyspace: opts.Keyspace,
		nodeIPs:  ips,
		network:  nw,
		opts:     opts,
	}

	if err := c.startNodes(ctx); err != nil {
		_ = c.Terminate(context.WithoutCancel(ctx))

		return nil, err
	}
	if err := c.openSessions(); err != nil {
		_ = c.Terminate(context.WithoutCancel(ctx))

		return nil, err
	}

	return c, nil
}

// applyTwoNodeDefaults fills in every option the caller left at its zero value.
func applyTwoNodeDefaults(opts *TwoNodeCQLClusterOptions) {
	defaults := DefaultTwoNodeCQLClusterOptions(opts.Keyspace)
	if opts.Image == "" {
		opts.Image = defaults.Image
	}
	if opts.Subnet == "" {
		opts.Subnet = defaults.Subnet
	}
	if opts.Memory == "" {
		opts.Memory = defaults.Memory
	}
	if opts.SMP == 0 {
		opts.SMP = defaults.SMP
	}
	if opts.SessionTimeout == 0 {
		opts.SessionTimeout = defaults.SessionTimeout
	}
	if opts.ConnectTimeout == 0 {
		opts.ConnectTimeout = opts.SessionTimeout
	}
	if opts.StartTimeout == 0 {
		opts.StartTimeout = defaults.StartTimeout
	}
	if opts.FormationTimeout == 0 {
		opts.FormationTimeout = defaults.FormationTimeout
	}
}

// twoNodeAddresses derives the nodes' fixed addresses from the subnet,
// one per node starting at twoNodeFirstOctet.
func twoNodeAddresses(subnet string, count int) ([]netip.Addr, error) {
	prefix, err := netip.ParsePrefix(subnet)
	if err != nil {
		return nil, fmt.Errorf("parse subnet %q: %w", subnet, err)
	}
	if !prefix.Addr().Is4() || prefix.Bits() != 24 {
		return nil, fmt.Errorf("subnet %q must be an IPv4 /24", subnet)
	}

	base := prefix.Masked().Addr().As4()
	ips := make([]netip.Addr, 0, count)
	for i := range count {
		octets := base
		octets[3] = twoNodeFirstOctet + byte(i)
		ips = append(ips, netip.AddrFrom4(octets))
	}

	return ips, nil
}

// twoNodeIPAM pins the network to the given subnet
// so the nodes' fixed addresses stay inside it.
func twoNodeIPAM(subnet string) (*dnet.IPAM, error) {
	prefix, err := netip.ParsePrefix(subnet)
	if err != nil {
		return nil, fmt.Errorf("parse subnet %q: %w", subnet, err)
	}

	return &dnet.IPAM{
		Driver: "default",
		Config: []dnet.IPAMConfig{{Subnet: prefix}},
	}, nil
}

// countUpNormal counts the nodes a nodetool status listing reports as up and normal,
// which is the "UN" marker in the first column of each node's row.
func countUpNormal(status string) int {
	count := 0
	for line := range strings.Lines(status) {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "UN" {
			count++
		}
	}

	return count
}

// ContactPoint returns the "ip:port" of the node the sessions connect to.
// The drivers discover the other node from there.
func (c *TwoNodeCQLCluster) ContactPoint() string {
	return c.nodeAddr(0)
}

// NodeCount returns how many nodes the cluster runs.
func (c *TwoNodeCQLCluster) NodeCount() int {
	return len(c.nodes)
}

// PauseNode freezes one node's processes with Docker's pause API.
//
// The node's TCP connections stay open and unanswered,
// which is what a node that is up but not serving looks like to the rest of the cluster,
// and to a driver holding a pool against it.
//
// Parameters:
//   - ctx: bounds the Docker call
//   - index: the node to pause, from 0 to [TwoNodeCQLCluster.NodeCount] minus one
//
// Returns:
//   - error: if the index is out of range or Docker refuses the pause
func (c *TwoNodeCQLCluster) PauseNode(ctx context.Context, index int) error {
	return c.nodeDocker(index, func(cli dockerCloser, id string) error {
		return cli.ContainerPause(ctx, id)
	})
}

// UnpauseNode resumes a node frozen by [TwoNodeCQLCluster.PauseNode].
//
// Parameters:
//   - ctx: bounds the Docker call
//   - index: the node to resume
//
// Returns:
//   - error: if the index is out of range or Docker refuses the unpause
func (c *TwoNodeCQLCluster) UnpauseNode(ctx context.Context, index int) error {
	return c.nodeDocker(index, func(cli dockerCloser, id string) error {
		return cli.ContainerUnpause(ctx, id)
	})
}

// UpNodeCount asks one node how many cluster members it currently sees as up and normal.
//
// The answer comes from nodetool,
// so it reports the cluster's own view of its membership rather than anything the drivers believe.
//
// Parameters:
//   - ctx: bounds the exec call
//   - viaIndex: the node to ask; it must not be paused
//
// Returns:
//   - int: the number of members reported up and normal
//   - error: if the index is out of range or nodetool cannot be run
func (c *TwoNodeCQLCluster) UpNodeCount(ctx context.Context, viaIndex int) (int, error) {
	if viaIndex < 0 || viaIndex >= len(c.nodes) {
		return 0, fmt.Errorf("TwoNodeCQLCluster: node index %d out of range", viaIndex)
	}

	_, reader, err := c.nodes[viaIndex].Exec(ctx, []string{"nodetool", "status"}, tcexec.Multiplexed())
	if err != nil {
		return 0, fmt.Errorf("nodetool status on node %d: %w", viaIndex, err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		return 0, fmt.Errorf("read nodetool status on node %d: %w", viaIndex, err)
	}

	return countUpNormal(string(out)), nil
}

// Terminate closes both sessions and removes every node and the network.
// It reports the first failure but still tries every resource.
//
// Parameters:
//   - ctx: bounds the Docker calls
//
// Returns:
//   - error: the first removal failure, or nil
func (c *TwoNodeCQLCluster) Terminate(ctx context.Context) error {
	c.Close()

	var firstErr error
	for i, node := range c.nodes {
		if node == nil {
			continue
		}
		if err := node.Terminate(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("terminate node %d: %w", i, err)
		}
	}
	c.nodes = nil

	if c.network != nil {
		if err := c.network.Remove(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("remove network: %w", err)
		}
		c.network = nil
	}

	return firstErr
}

// Close closes both driver sessions and leaves the containers running.
func (c *TwoNodeCQLCluster) Close() {
	if c.Session != nil {
		c.Session.Close()
		c.Session = nil
	}
	if c.SessionV2 != nil {
		c.SessionV2.Close()
		c.SessionV2 = nil
	}
}

func (c *TwoNodeCQLCluster) nodeAddr(index int) string {
	return fmt.Sprintf("%s:%d", c.nodeIPs[index], twoNodeCQLPort)
}

// startNodes boots node 0, waits for it to answer CQL,
// then boots the remaining nodes against it and waits for the cluster to form.
func (c *TwoNodeCQLCluster) startNodes(ctx context.Context) error {
	for i := range c.nodeIPs {
		node, err := c.runNode(ctx, i)
		if err != nil {
			return fmt.Errorf("start node %d: %w", i, err)
		}
		c.nodes = append(c.nodes, node)

		if i == 0 {
			if err := c.waitForCQL(ctx, 0); err != nil {
				return fmt.Errorf("node 0 never answered CQL: %w", err)
			}
		}
	}

	return c.waitForFormation(ctx)
}

func (c *TwoNodeCQLCluster) runNode(ctx context.Context, index int) (testcontainers.Container, error) {
	netName := c.network.Name
	ip := c.nodeIPs[index]
	req := testcontainers.ContainerRequest{
		Image:        c.opts.Image,
		Cmd:          c.nodeCmd(index),
		ExposedPorts: []string{fmt.Sprintf("%d/tcp", twoNodeCQLPort)},
		Networks:     []string{netName},
		NetworkAliases: map[string][]string{
			netName: {fmt.Sprintf("scylla-node-%d", index)},
		},
		EndpointSettingsModifier: func(settings map[string]*dnet.EndpointSettings) {
			es, ok := settings[netName]
			if !ok || es == nil {
				es = &dnet.EndpointSettings{}
				settings[netName] = es
			}
			es.IPAMConfig = &dnet.EndpointIPAMConfig{IPv4Address: ip}
		},
		WaitingFor: wait.ForLog(twoNodeReadyLog).WithStartupTimeout(c.opts.StartTimeout),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// nodeCmd returns the ScyllaDB arguments for one node.
// Every node names node 0 as its seed and advertises its own bridge address,
// which is what makes the addresses in system.peers reachable from the test process.
func (c *TwoNodeCQLCluster) nodeCmd(index int) []string {
	own := c.nodeIPs[index].String()

	return []string{
		"--developer-mode=1",
		"--overprovisioned=1",
		"--reactor-backend=epoll",
		fmt.Sprintf("--smp=%d", c.opts.SMP),
		fmt.Sprintf("--memory=%s", c.opts.Memory),
		"--seeds=" + c.nodeIPs[0].String(),
		"--listen-address=" + own,
		"--broadcast-address=" + own,
		"--broadcast-rpc-address=" + own,
	}
}

// waitForCQL polls one node until a CQL query against it succeeds.
// A node that has logged its CQL banner is not yet guaranteed to answer,
// and there is nothing to subscribe to in between.
func (c *TwoNodeCQLCluster) waitForCQL(ctx context.Context, index int) error {
	deadline := time.Now().Add(c.opts.StartTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		session, err := c.newProbeSession(index)
		if err == nil {
			var version string
			err = session.Query("SELECT release_version FROM system.local").Scan(&version)
			session.Close()
			if err == nil {
				return nil
			}
		}
		lastErr = err
		if waitErr := waitForRetry(ctx, twoNodeStatusPoll); waitErr != nil {
			return waitErr
		}
	}

	return fmt.Errorf("timed out waiting for CQL on node %d: %w", index, lastErr)
}

// newProbeSession opens a short-lived session pinned to one node.
// Peer discovery is off so the session cannot drift onto the other node.
func (c *TwoNodeCQLCluster) newProbeSession(index int) (*gocql.Session, error) {
	cluster := gocql.NewCluster(c.nodeAddr(index))
	cluster.Consistency = gocql.One
	cluster.Timeout = c.opts.SessionTimeout
	cluster.ConnectTimeout = c.opts.ConnectTimeout
	cluster.Keyspace = "system"
	cluster.DisableInitialHostLookup = true

	return cluster.CreateSession()
}

// waitForFormation polls node 0 until every node reports up and normal.
// Gossip convergence has no seam a test can subscribe to;
// the cluster's membership lives in the containers.
func (c *TwoNodeCQLCluster) waitForFormation(ctx context.Context) error {
	deadline := time.Now().Add(c.opts.FormationTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		up, err := c.UpNodeCount(ctx, 0)
		if err == nil && up == len(c.nodes) {
			return nil
		}
		lastErr = err
		if waitErr := waitForRetry(ctx, twoNodeStatusPoll); waitErr != nil {
			return waitErr
		}
	}

	return fmt.Errorf("timed out waiting for %d nodes to join: %w", len(c.nodes), lastErr)
}

// openSessions creates the replication-factor-2 keyspace and binds both driver sessions to it.
func (c *TwoNodeCQLCluster) openSessions() error {
	if err := c.createKeyspace(); err != nil {
		return err
	}

	session, err := c.newSessionV1()
	if err != nil {
		return fmt.Errorf("create v1 session: %w", err)
	}
	sessionV2, err := c.newSessionV2()
	if err != nil {
		session.Close()

		return fmt.Errorf("create v2 session: %w", err)
	}
	c.Session = session
	c.SessionV2 = sessionV2

	return nil
}

func (c *TwoNodeCQLCluster) createKeyspace() error {
	session, err := c.newProbeSession(0)
	if err != nil {
		return fmt.Errorf("connect to system keyspace: %w", err)
	}
	defer session.Close()

	stmt := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}`, c.Keyspace)
	if err := session.Query(stmt).Exec(); err != nil {
		return fmt.Errorf("create keyspace %s: %w", c.Keyspace, err)
	}

	return nil
}

func (c *TwoNodeCQLCluster) newSessionV1() (*gocql.Session, error) {
	cluster := gocql.NewCluster(c.ContactPoint())
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = c.opts.SessionTimeout
	cluster.ConnectTimeout = c.opts.ConnectTimeout
	cluster.Keyspace = c.Keyspace
	if c.opts.ReconnectInterval > 0 {
		cluster.ReconnectInterval = c.opts.ReconnectInterval
	}
	if c.opts.RetryNextHost {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: len(c.nodeIPs)}
	}

	return cluster.CreateSession()
}

func (c *TwoNodeCQLCluster) newSessionV2() (*gocqlv2.Session, error) {
	cluster := gocqlv2.NewCluster(c.ContactPoint())
	cluster.Consistency = gocqlv2.Quorum
	cluster.Timeout = c.opts.SessionTimeout
	cluster.ConnectTimeout = c.opts.ConnectTimeout
	cluster.Keyspace = c.Keyspace
	if c.opts.ReconnectInterval > 0 {
		cluster.ReconnectInterval = c.opts.ReconnectInterval
	}
	if c.opts.RetryNextHost {
		cluster.RetryPolicy = &gocqlv2.SimpleRetryPolicy{NumRetries: len(c.nodeIPs)}
	}

	return cluster.CreateSession()
}

// nodeDocker runs fn against one node's container through a Docker client that is closed afterwards.
func (c *TwoNodeCQLCluster) nodeDocker(index int, fn func(dockerCloser, string) error) error {
	if index < 0 || index >= len(c.nodes) {
		return fmt.Errorf("TwoNodeCQLCluster: node index %d out of range", index)
	}
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		return fmt.Errorf("docker provider: %w", err)
	}
	cli := providerCloser{provider}
	defer func() { _ = cli.Close() }()

	return fn(cli, c.nodes[index].GetContainerID())
}
