package testutil

import (
	"context"
	"errors"
	"fmt"
	"time"

	gocqlv2 "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/gocql/gocql"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const reconnectRetryInterval = 500 * time.Millisecond

// container returns the underlying testcontainers.Container, abstracting over
// the Scylla and Cassandra module wrappers.
func (c *CQLCluster) container() testcontainers.Container {
	switch c.Type {
	case CQLClusterTypeScyllaDB:
		if c.scyllaContainer != nil {
			return c.scyllaContainer.Container
		}
	case CQLClusterTypeCassandra:
		if c.cassandraContainer != nil {
			return c.cassandraContainer.Container
		}
	case CQLClusterTypeNone:
	}

	return nil
}

// Stop halts the container without removing it. Mapped ports are preserved
// across the matching Start call (testcontainers does not re-pick a port for
// an existing container). Sessions remain attached but their connections will
// be terminated; gocql's reconnect logic will attempt to recover.
//
// grace is the SIGTERM-to-SIGKILL window; pass 0 for an immediate kill.
func (c *CQLCluster) Stop(ctx context.Context, grace time.Duration) error {
	ctr := c.container()
	if ctr == nil {
		return errors.New("CQLCluster: no container to Stop")
	}
	g := grace
	return ctr.Stop(ctx, &g)
}

// Start resumes a previously stopped container.
func (c *CQLCluster) Start(ctx context.Context) error {
	ctr := c.container()
	if ctr == nil {
		return errors.New("CQLCluster: no container to Start")
	}
	return ctr.Start(ctx)
}

// Pause sends SIGSTOP to the container's processes via Docker's pause API.
// TCP connections remain open but unanswered; ops hang until the gocql
// per-query Timeout expires. Use Unpause to resume.
//
// This is the failure mode the chaos suite cannot simulate — chaos's
// LatencyFunc is a time.Sleep before the real op, not a hung TCP socket.
func (c *CQLCluster) Pause(ctx context.Context) error {
	cli, ctr, err := c.dockerClient()
	if err != nil {
		return err
	}
	defer cli.Close()
	return cli.ContainerPause(ctx, ctr.GetContainerID())
}

// Unpause sends SIGCONT to a paused container.
func (c *CQLCluster) Unpause(ctx context.Context) error {
	cli, ctr, err := c.dockerClient()
	if err != nil {
		return err
	}
	defer cli.Close()
	return cli.ContainerUnpause(ctx, ctr.GetContainerID())
}

// Kill sends SIGKILL to the container's PID 1, simulating a process crash.
// This is harder than Stop (which first sends SIGTERM and waits for graceful
// shutdown) — Kill terminates immediately. In-flight TCP connections are
// abruptly closed; the OS sends RST to peers. Use this to test how Helix
// reacts to abrupt cluster process death (OOM-kill, kernel panic, hard
// reboot) rather than orderly shutdown.
//
// After Kill, the container is exited but not removed. Call Start to
// restart it; the test still needs cluster.Reconnect afterwards because
// the host port is reassigned.
func (c *CQLCluster) Kill(ctx context.Context) error {
	cli, ctr, err := c.dockerClient()
	if err != nil {
		return err
	}
	defer cli.Close()
	return cli.ContainerKill(ctx, ctr.GetContainerID(), "SIGKILL")
}

// NetworkDisconnect detaches the container from its (default) network,
// simulating a network partition. The container keeps running but
// becomes unreachable from the host. Existing TCP connections from
// host-side clients are dropped or hang (zombie/half-open socket
// territory). This is the closest reproducible analog to a real network
// partition — different from Pause (process is still answering, just
// stopped) and Kill (process is dead).
//
// NetworkReconnect (companion) restores connectivity.
func (c *CQLCluster) NetworkDisconnect(ctx context.Context, networkName string) error {
	cli, ctr, err := c.dockerClient()
	if err != nil {
		return err
	}
	defer cli.Close()
	const force = true
	return cli.NetworkDisconnect(ctx, networkName, ctr.GetContainerID(), force)
}

// NetworkReconnect re-attaches the container to the named network after
// a NetworkDisconnect. Existing host-side gocql sessions will still be
// dead because their TCP state is invalid; tests should call Reconnect
// after this to rebuild sessions.
func (c *CQLCluster) NetworkReconnect(ctx context.Context, networkName string) error {
	cli, ctr, err := c.dockerClient()
	if err != nil {
		return err
	}
	defer cli.Close()
	return cli.NetworkReconnect(ctx, networkName, ctr.GetContainerID())
}

// dockerClient returns a Docker API client (sourced from testcontainers'
// DockerProvider so we don't add a direct dependency on docker/docker) and
// the underlying container reference.
func (c *CQLCluster) dockerClient() (closer dockerCloser, ctr testcontainers.Container, err error) {
	ctr = c.container()
	if ctr == nil {
		return nil, nil, errors.New("CQLCluster: no container available")
	}
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		return nil, nil, fmt.Errorf("docker provider: %w", err)
	}

	return providerCloser{provider}, ctr, nil
}

// dockerCloser is the minimal subset of the Docker API client surface we use,
// plus a Close hook that disposes the testcontainers DockerProvider.
type dockerCloser interface {
	ContainerPause(ctx context.Context, containerID string) error
	ContainerUnpause(ctx context.Context, containerID string) error
	ContainerKill(ctx context.Context, containerID, signal string) error
	NetworkDisconnect(ctx context.Context, networkName, containerID string, force bool) error
	NetworkReconnect(ctx context.Context, networkName, containerID string) error
	Close() error
}

type providerCloser struct {
	provider *testcontainers.DockerProvider
}

func (p providerCloser) ContainerPause(ctx context.Context, id string) error {
	_, err := p.provider.Client().ContainerPause(ctx, id, client.ContainerPauseOptions{})
	return err
}

func (p providerCloser) ContainerUnpause(ctx context.Context, id string) error {
	_, err := p.provider.Client().ContainerUnpause(ctx, id, client.ContainerUnpauseOptions{})
	return err
}

func (p providerCloser) ContainerKill(ctx context.Context, id, signal string) error {
	_, err := p.provider.Client().ContainerKill(ctx, id, client.ContainerKillOptions{Signal: signal})
	return err
}

func (p providerCloser) NetworkDisconnect(ctx context.Context, network, id string, force bool) error {
	_, err := p.provider.Client().NetworkDisconnect(ctx, network, client.NetworkDisconnectOptions{
		Container: id,
		Force:     force,
	})
	return err
}

func (p providerCloser) NetworkReconnect(ctx context.Context, network, id string) error {
	_, err := p.provider.Client().NetworkConnect(ctx, network, client.NetworkConnectOptions{Container: id})
	return err
}

func (p providerCloser) Close() error { return p.provider.Close() }

// Reconnect closes both v1 and v2 sessions and rebuilds them against the
// container's current host:port. Use this after Start when the existing
// gocql session does not auto-recover within the desired window. Returns
// the first error encountered; on partial failure the cluster's session
// fields may be nil.
func (c *CQLCluster) Reconnect(ctx context.Context) error {
	ctr := c.container()
	if ctr == nil {
		return errors.New("CQLCluster: no container to Reconnect")
	}

	host, err := c.RefreshHost(ctx)
	if err != nil {
		return fmt.Errorf("refresh host: %w", err)
	}
	c.Host = host

	if c.Session != nil {
		c.Session.Close()
		c.Session = nil
	}
	if c.SessionV2 != nil {
		c.SessionV2.Close()
		c.SessionV2 = nil
	}

	session, sessionV2, err := c.rebuildSessionsWithRetry(ctx, host)
	if err != nil {
		return err
	}
	c.Session = session
	c.SessionV2 = sessionV2

	return nil
}

func (c *CQLCluster) rebuildSessionsWithRetry(
	ctx context.Context,
	host string,
) (*gocql.Session, *gocqlv2.Session, error) {
	var lastErr error

	for {
		session, err := createCQLSession(host, c.keyspace, c.sessionTimeout, c.connectTimeout, c.reconnectInterval)
		if err == nil {
			sessionV2, v2Err := createCQLSessionV2(host, c.keyspace, c.sessionTimeout, c.connectTimeout, c.reconnectInterval)
			if v2Err == nil {
				return session, sessionV2, nil
			}

			session.Close()
			lastErr = fmt.Errorf("rebuild v2 session: %w", v2Err)
		} else {
			lastErr = fmt.Errorf("rebuild v1 session: %w", err)
		}

		if waitErr := waitForRetry(ctx, reconnectRetryInterval); waitErr != nil {
			return nil, nil, fmt.Errorf("%w: %w", lastErr, waitErr)
		}
	}
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// RefreshHost re-resolves the container's connection host. If the testcontainers
// runtime preserves the port mapping across Stop/Start (the documented behavior),
// this returns the same value as before; otherwise it returns the new mapping.
// Reconnect calls this internally; tests that just want to compare pre/post
// mappings can call it directly.
func (c *CQLCluster) RefreshHost(ctx context.Context) (string, error) {
	switch c.Type {
	case CQLClusterTypeScyllaDB:
		return c.scyllaContainer.NonShardAwareConnectionHost(ctx)
	case CQLClusterTypeCassandra:
		return c.cassandraContainer.ConnectionHost(ctx)
	case CQLClusterTypeNone:
	}

	return "", errors.New("CQLCluster: no container type")
}
