package testutil

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

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
