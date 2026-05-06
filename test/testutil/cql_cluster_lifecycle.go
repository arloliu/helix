package testutil

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

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
	Close() error
}

type providerCloser struct {
	provider *testcontainers.DockerProvider
}

func (p providerCloser) ContainerPause(ctx context.Context, id string) error {
	return p.provider.Client().ContainerPause(ctx, id)
}

func (p providerCloser) ContainerUnpause(ctx context.Context, id string) error {
	return p.provider.Client().ContainerUnpause(ctx, id)
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

	session, err := createCQLSession(host, c.keyspace, c.sessionTimeout, c.connectTimeout, c.reconnectInterval)
	if err != nil {
		return fmt.Errorf("rebuild v1 session: %w", err)
	}
	sessionV2, err := createCQLSessionV2(host, c.keyspace, c.sessionTimeout, c.connectTimeout)
	if err != nil {
		session.Close()
		return fmt.Errorf("rebuild v2 session: %w", err)
	}
	c.Session = session
	c.SessionV2 = sessionV2

	return nil
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
