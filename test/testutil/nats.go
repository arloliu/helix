package testutil

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// RestartableNATS is an embedded NATS server with JetStream
// that a test can shut down and start again.
//
// A restart reuses the same port and the same JetStream store directory,
// so streams, their messages and durable consumers survive it,
// and a client connected with [RestartableNATS.Connect]
// reconnects to the new process on its own.
type RestartableNATS struct {
	t    *testing.T
	opts *server.Options

	mu  sync.Mutex
	srv *server.Server
}

// StartEmbeddedNATS starts an embedded NATS server with JetStream enabled for testing.
//
// The server is configured with a random available port and uses t.TempDir()
// for JetStream storage. Both the connection and server are automatically
// cleaned up when the test completes.
//
// Parameters:
//   - t: The testing context
//
// Returns:
//   - jetstream.JetStream: A JetStream context ready for use
func StartEmbeddedNATS(t *testing.T) jetstream.JetStream {
	t.Helper()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // Random available port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}

	ns, err := server.NewServer(opts)
	require.NoError(t, err, "failed to create NATS server")

	ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready for connections")
	}

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err, "failed to connect to NATS server")

	js, err := jetstream.New(nc)
	require.NoError(t, err, "failed to create JetStream context")

	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
	})

	return js
}

// StartRestartableNATS starts an embedded NATS server with JetStream enabled
// that can be shut down and restarted during the test.
//
// The server listens on a free port chosen at start
// and stores JetStream state under t.TempDir().
// It is shut down when the test completes.
// Call Shutdown and Restart from the test goroutine: a failed restart fails the test.
//
// Parameters:
//   - t: The testing context
//
// Returns:
//   - *RestartableNATS: A handle to the running server
//
// Example:
//
//	ns := testutil.StartRestartableNATS(t)
//	js := ns.Connect(t)
//	ns.Shutdown()
//	ns.Restart()
func StartRestartableNATS(t *testing.T) *RestartableNATS {
	t.Helper()

	n := &RestartableNATS{
		t: t,
		opts: &server.Options{
			Host:      "127.0.0.1",
			Port:      -1, // fixed to the chosen port once the server is up
			JetStream: true,
			StoreDir:  t.TempDir(),
			NoSigs:    true,
		},
	}
	n.mu.Lock()
	n.startLocked()
	n.mu.Unlock()

	addr, ok := n.srv.Addr().(*net.TCPAddr)
	require.True(t, ok, "NATS server did not report a TCP address")
	n.opts.Port = addr.Port

	t.Cleanup(n.Shutdown)

	return n
}

// CreateKVConfig creates a KeyValueConfig with the given bucket name.
//
// This is a convenience helper for creating KV buckets in tests.
//
// Parameters:
//   - bucket: The name of the KV bucket
//
// Returns:
//   - jetstream.KeyValueConfig: A configuration for creating a KV bucket
func CreateKVConfig(bucket string) jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{
		Bucket: bucket,
	}
}

// URL returns the client URL of the server.
// It does not change across restarts.
//
// Returns:
//   - string: The nats:// URL clients connect to
func (n *RestartableNATS) URL() string {
	return "nats://" + net.JoinHostPort(n.opts.Host, strconv.Itoa(n.opts.Port))
}

// Connect opens a client connection that keeps reconnecting while the server is down
// and returns a JetStream context on it.
//
// The connection retries every 20ms without a limit
// and is closed when the test completes.
// opts are applied after those defaults.
//
// Parameters:
//   - t: The testing context
//   - opts: Extra client options, such as nats.ReconnectHandler
//
// Returns:
//   - jetstream.JetStream: A JetStream context; its Conn method returns the connection
func (n *RestartableNATS) Connect(t *testing.T, opts ...nats.Option) jetstream.JetStream {
	t.Helper()

	all := make([]nats.Option, 0, 3+len(opts))
	all = append(all,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(20*time.Millisecond),
		nats.ReconnectJitter(0, 0),
	)
	all = append(all, opts...)

	nc, err := nats.Connect(n.URL(), all...)
	require.NoError(t, err, "failed to connect to NATS server")
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	require.NoError(t, err, "failed to create JetStream context")

	return js
}

// Shutdown stops the server and waits until it has released its port.
// It is a no-op when the server is already down.
func (n *RestartableNATS) Shutdown() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.srv == nil {
		return
	}
	n.srv.Shutdown()
	n.srv.WaitForShutdown()
	n.srv = nil
}

// Restart starts the server again on the same port with the same JetStream store directory.
// The server must be down.
func (n *RestartableNATS) Restart() {
	n.t.Helper()

	n.mu.Lock()
	defer n.mu.Unlock()

	require.Nil(n.t, n.srv, "Restart called while the NATS server is running")
	n.startLocked()
}

func (n *RestartableNATS) startLocked() {
	n.t.Helper()

	srv, err := server.NewServer(n.opts.Clone())
	require.NoError(n.t, err, "failed to create NATS server")

	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		n.t.Fatal("NATS server not ready for connections")
	}
	n.srv = srv
}
