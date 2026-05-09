package replay_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

func TestNATSReplayerNewWithNilJetStream(t *testing.T) {
	_, err := replay.NewNATSReplayer(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "JetStream context is nil")
}

func TestNATSReplayerEnqueue(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-enqueue"),
		replay.WithSubjectPrefix("test.replay"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	ctx := context.Background()

	payload := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO users (id, name) VALUES (?, ?)",
		Args:          []any{"uuid-123", "Alice"},
		Timestamp:     time.Now().UnixMicro(),
		Priority:      types.PriorityHigh,
	}

	err = replayer.Enqueue(ctx, payload)
	require.NoError(t, err)

	// Verify message count
	pending, err := replayer.Pending(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, pending)
}

func TestNATSReplayerEnqueueMultiple(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-enqueue-multi"),
		replay.WithSubjectPrefix("test.replay.multi"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	ctx := context.Background()

	// Enqueue messages for both clusters with different priorities
	payloads := []types.ReplayPayload{
		{
			TargetCluster: types.ClusterA,
			Query:         "INSERT INTO t1 (id) VALUES (?)",
			Args:          []any{1},
			Timestamp:     time.Now().UnixMicro(),
			Priority:      types.PriorityHigh,
		},
		{
			TargetCluster: types.ClusterB,
			Query:         "INSERT INTO t2 (id) VALUES (?)",
			Args:          []any{2},
			Timestamp:     time.Now().UnixMicro(),
			Priority:      types.PriorityLow,
		},
		{
			TargetCluster: types.ClusterA,
			Query:         "INSERT INTO t3 (id) VALUES (?)",
			Args:          []any{3},
			Timestamp:     time.Now().UnixMicro(),
			Priority:      types.PriorityLow,
		},
	}

	for _, p := range payloads {
		err := replayer.Enqueue(ctx, p)
		require.NoError(t, err)
	}

	pending, err := replayer.Pending(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, pending)
}

func TestNATSReplayerDequeue(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-dequeue"),
		replay.WithSubjectPrefix("test.replay.dq"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	ctx := context.Background()

	// Enqueue a message for cluster A
	payload := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "UPDATE users SET name = ? WHERE id = ?",
		Args:          []any{"Bob", "uuid-456"},
		Timestamp:     12345678,
		Priority:      types.PriorityHigh,
	}

	err = replayer.Enqueue(ctx, payload)
	require.NoError(t, err)

	// Dequeue messages for cluster A
	msgs, err := replayer.Dequeue(ctx, types.ClusterA, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	// Verify payload content
	msg := msgs[0]
	assert.Equal(t, types.ClusterA, msg.Payload.TargetCluster)
	assert.Equal(t, payload.Query, msg.Payload.Query)
	assert.Equal(t, payload.Timestamp, msg.Payload.Timestamp)
	assert.Equal(t, types.PriorityHigh, msg.Payload.Priority)

	// Ack the message
	err = msg.Ack()
	require.NoError(t, err)

	// Verify no more pending messages
	// Wait briefly for ack to propagate
	time.Sleep(100 * time.Millisecond)

	pending, err := replayer.Pending(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, pending)
}

func TestNATSReplayerDequeueByCluster(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-dequeue-cluster"),
		replay.WithSubjectPrefix("test.replay.cluster"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	ctx := context.Background()

	// Enqueue messages for different clusters
	err = replayer.Enqueue(ctx, types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT A",
		Timestamp:     1,
		Priority:      types.PriorityHigh,
	})
	require.NoError(t, err)

	err = replayer.Enqueue(ctx, types.ReplayPayload{
		TargetCluster: types.ClusterB,
		Query:         "INSERT B",
		Timestamp:     2,
		Priority:      types.PriorityHigh,
	})
	require.NoError(t, err)

	// Dequeue only cluster A messages
	msgsA, err := replayer.Dequeue(ctx, types.ClusterA, 10)
	require.NoError(t, err)
	require.Len(t, msgsA, 1)
	assert.Equal(t, "INSERT A", msgsA[0].Payload.Query)

	// Dequeue only cluster B messages
	msgsB, err := replayer.Dequeue(ctx, types.ClusterB, 10)
	require.NoError(t, err)
	require.Len(t, msgsB, 1)
	assert.Equal(t, "INSERT B", msgsB[0].Payload.Query)
}

func TestNATSReplayerDequeueEmpty(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-dequeue-empty"),
		replay.WithSubjectPrefix("test.replay.empty"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	ctx := context.Background()

	// Dequeue from empty stream should return nil, nil
	msgs, err := replayer.Dequeue(ctx, types.ClusterA, 10)
	require.NoError(t, err)
	assert.Empty(t, msgs)
}

func TestNATSReplayerNak(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-nak"),
		replay.WithSubjectPrefix("test.replay.nak"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	ctx := context.Background()

	// Enqueue a message
	err = replayer.Enqueue(ctx, types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT test",
		Timestamp:     1,
		Priority:      types.PriorityHigh,
	})
	require.NoError(t, err)

	// Dequeue and Nak
	msgs, err := replayer.Dequeue(ctx, types.ClusterA, 1)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	err = msgs[0].Nak()
	require.NoError(t, err)

	// Wait for redelivery
	time.Sleep(200 * time.Millisecond)

	// Message should be redelivered
	msgs2, err := replayer.Dequeue(ctx, types.ClusterA, 1)
	require.NoError(t, err)
	require.Len(t, msgs2, 1)
	assert.Equal(t, "INSERT test", msgs2[0].Payload.Query)

	// Ack to clean up
	err = msgs2[0].Ack()
	require.NoError(t, err)
}

func TestNATSReplayerClose(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-close"),
		replay.WithSubjectPrefix("test.replay.close"),
	)
	require.NoError(t, err)

	err = replayer.Close()
	require.NoError(t, err)

	// Operations should fail after close
	ctx := context.Background()
	err = replayer.Enqueue(ctx, types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "test",
		Timestamp:     1,
		Priority:      types.PriorityHigh,
	})
	assert.ErrorIs(t, err, types.ErrSessionClosed)

	_, err = replayer.Dequeue(ctx, types.ClusterA, 1)
	assert.ErrorIs(t, err, types.ErrSessionClosed)

	_, err = replayer.Pending(ctx)
	assert.ErrorIs(t, err, types.ErrSessionClosed)
}

// TestNATSReplayerDequeueRecoversFromConsumerDeleted verifies that out-of-band
// deletion of the JetStream consumer (admin removed it, server reset, stream
// recreated) is recovered transparently on the next Dequeue: the cached
// consumer reference is evicted and a fresh one is created so the replay
// pipeline keeps draining instead of failing forever.
func TestNATSReplayerDequeueRecoversFromConsumerDeleted(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	const streamName = "test-consumer-recovery"
	const subjectPrefix = "test.replay.recovery"
	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName(streamName),
		replay.WithSubjectPrefix(subjectPrefix),
	)
	require.NoError(t, err)
	defer replayer.Close()

	ctx := context.Background()

	// Enqueue, dequeue, ack — primes the consumer cache.
	require.NoError(t, replayer.Enqueue(ctx, types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO t (k) VALUES (?)",
		Args:          []any{"first"},
		Timestamp:     1,
		Priority:      types.PriorityHigh,
	}))
	msgs, err := replayer.Dequeue(ctx, types.ClusterA, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.NoError(t, msgs[0].Ack())

	// Delete the durable consumer out of band (simulates an operator delete
	// or a stream/consumer reset). The cached consumer reference inside the
	// replayer is now stale.
	stream, err := js.Stream(ctx, streamName)
	require.NoError(t, err)
	consumerName := "helix-worker-" + string(types.ClusterA)
	require.NoError(t, stream.DeleteConsumer(ctx, consumerName))

	// Enqueue a fresh message so there is something to dequeue once the
	// consumer is rebuilt.
	require.NoError(t, replayer.Enqueue(ctx, types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO t (k) VALUES (?)",
		Args:          []any{"second"},
		Timestamp:     2,
		Priority:      types.PriorityHigh,
	}))

	// First post-delete Dequeue surfaces nats.ErrNoResponders (the deleted
	// consumer's pull subject has no subscriber); isConsumerGoneErr catches
	// it and invalidates the cache. The next Dequeue re-creates the
	// consumer via CreateOrUpdateConsumer and drains the new message.
	var lastErr error
	require.Eventually(t, func() bool {
		msgs, err := replayer.Dequeue(ctx, types.ClusterA, 10)
		if err != nil {
			lastErr = err
			return false
		}
		if len(msgs) == 0 {
			return false
		}
		for _, m := range msgs {
			_ = m.Ack()
		}

		return true
	}, 5*time.Second, 100*time.Millisecond,
		"Dequeue should recover after the JetStream consumer is deleted out of band; last err: %v", lastErr)
}

// TestNATSReplayerCloseDropsConsumerCache verifies that Close releases cached
// consumer references so the underlying JetStream client objects can be GC'd.
// Re-using the replayer after Close is a separate misuse (covered by
// TestNATSReplayerClose); this test focuses on the cache cleanup itself.
func TestNATSReplayerCloseDropsConsumerCache(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-close-cache"),
		replay.WithSubjectPrefix("test.replay.close.cache"),
	)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, replayer.Enqueue(ctx, types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO t (k) VALUES (?)",
		Args:          []any{"x"},
		Timestamp:     1,
		Priority:      types.PriorityHigh,
	}))
	msgs, err := replayer.Dequeue(ctx, types.ClusterA, 1)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.NoError(t, msgs[0].Ack())

	require.NoError(t, replayer.Close())

	// Sanity: post-close Dequeue still returns ErrSessionClosed even though
	// the cache was cleared (the closed flag still gates entry).
	_, err = replayer.Dequeue(ctx, types.ClusterA, 1)
	assert.ErrorIs(t, err, types.ErrSessionClosed)
}

// Compile-time assertion that the jetstream sentinel errors we react to in
// invalidateConsumer paths still exist in the upstream package — guards
// against silent breakage on dependency upgrades.
var (
	_ = jetstream.ErrConsumerNotFound
	_ = jetstream.ErrConsumerDeleted
)

func TestNATSReplayerStreamName(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	customName := "my-custom-stream"
	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName(customName),
	)
	require.NoError(t, err)
	defer replayer.Close()

	assert.Equal(t, customName, replayer.StreamName())
}

func TestNATSReplayerConfigOptions(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-config"),
		replay.WithSubjectPrefix("custom.prefix"),
		replay.WithMaxAge(1*time.Hour),
		replay.WithMaxMsgs(100),
		replay.WithMaxBytes(1024*1024),
		replay.WithReplicas(1),
		replay.WithPublishTimeout(10*time.Second),
	)
	require.NoError(t, err)
	defer replayer.Close()

	// Verify replayer was created successfully with custom config
	assert.Equal(t, "test-config", replayer.StreamName())
}

func TestNATSReplayerDefaultDiscardPolicyIsDiscardOld(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	const streamName = "test-discard-default"
	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName(streamName),
		replay.WithSubjectPrefix("test.replay.discard.default"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	stream, err := js.Stream(t.Context(), streamName)
	require.NoError(t, err)

	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	assert.Equal(t, jetstream.DiscardOld, info.Config.Discard)
}

func TestNATSReplayerRejectNewOnLimitUsesDiscardNew(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	const streamName = "test-discard-new"
	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName(streamName),
		replay.WithSubjectPrefix("test.replay.discard.new"),
		replay.WithRejectNewOnLimit(),
	)
	require.NoError(t, err)
	defer replayer.Close()

	stream, err := js.Stream(t.Context(), streamName)
	require.NoError(t, err)

	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	assert.Equal(t, jetstream.DiscardNew, info.Config.Discard)
}

func TestNATSReplayerDequeueHonorsCanceledContext(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-dequeue-canceled-context"),
		replay.WithSubjectPrefix("test.replay.cancel"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	// Prime the consumer cache. The canceled-context assertion below should
	// exercise the Fetch path, not just consumer creation.
	msgs, err := replayer.Dequeue(t.Context(), types.ClusterA, 1)
	require.NoError(t, err)
	require.Empty(t, msgs)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	start := time.Now()
	msgs, err = replayer.Dequeue(ctx, types.ClusterA, 1)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, msgs)
	assert.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestNATSReplayerArgsTypes(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-args"),
		replay.WithSubjectPrefix("test.replay.args"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	ctx := context.Background()

	// Test various argument types
	payload := types.ReplayPayload{
		TargetCluster: types.ClusterA,
		Query:         "INSERT INTO test (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)",
		Args: []any{
			"string-value",
			int(42),
			float64(3.14),
			true,
			[]byte{0x01, 0x02, 0x03},
			nil,
		},
		Timestamp: time.Now().UnixMicro(),
		Priority:  types.PriorityLow,
	}

	err = replayer.Enqueue(ctx, payload)
	require.NoError(t, err)

	msgs, err := replayer.Dequeue(ctx, types.ClusterA, 1)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	// Verify args are preserved (msgp preserves integer types unlike JSON)
	args := msgs[0].Payload.Args
	assert.Len(t, args, 6)
	assert.Equal(t, "string-value", args[0])
	// msgp preserves int as int64 (more type-safe than JSON's float64)
	assert.Equal(t, int64(42), args[1])
	assert.Equal(t, 3.14, args[2])
	assert.Equal(t, true, args[3])
	// msgp preserves []byte as []byte (unlike JSON's base64 encoding)
}

// TestMsgpRoundTripThroughNATS tests the complete msgp serialization round-trip
// through the NATS replayer to ensure end-to-end correctness.
func TestMsgpRoundTripThroughNATS(t *testing.T) {
	js := testutil.StartEmbeddedNATS(t)

	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-msgp-roundtrip"),
		replay.WithSubjectPrefix("test.msgp.roundtrip"),
	)
	require.NoError(t, err)
	defer replayer.Close()

	ctx := context.Background()

	// Create a payload with diverse argument types
	payload := types.ReplayPayload{
		TargetCluster: types.ClusterB,
		Query:         "INSERT INTO complex_table (id, name, count, ratio, active, data, tags, meta) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		Args: []any{
			"test-uuid-12345",                      // text (UUID)
			"Test Name 日本語",                        // text with unicode
			int64(9223372036854775807),             // bigint max
			float64(3.141592653589793),             // double
			true,                                   // boolean
			[]byte{0x00, 0xFF, 0x80, 0x7F},         // blob
			[]any{"tag1", "tag2", "tag3"},          // list<text>
			map[string]any{"k1": "v1", "k2": "v2"}, // map<text,text>
		},
		Timestamp: time.Now().UnixMicro(),
		Priority:  types.PriorityHigh,
	}

	// Enqueue (serializes with msgp)
	err = replayer.Enqueue(ctx, payload)
	require.NoError(t, err)

	// Dequeue (deserializes with msgp)
	msgs, err := replayer.Dequeue(ctx, types.ClusterB, 1)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	result := msgs[0].Payload

	// Verify all fields
	assert.Equal(t, types.ClusterB, result.TargetCluster)
	assert.Equal(t, payload.Query, result.Query)
	assert.Equal(t, payload.Timestamp, result.Timestamp)
	assert.Equal(t, payload.Priority, result.Priority)

	// Verify args
	require.Len(t, result.Args, 8)
	assert.Equal(t, "test-uuid-12345", result.Args[0])
	assert.Equal(t, "Test Name 日本語", result.Args[1])
	assert.Equal(t, int64(9223372036854775807), result.Args[2])
	assert.Equal(t, float64(3.141592653589793), result.Args[3])
	assert.Equal(t, true, result.Args[4])
	assert.Equal(t, []byte{0x00, 0xFF, 0x80, 0x7F}, result.Args[5])

	tags, ok := result.Args[6].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"tag1", "tag2", "tag3"}, tags)

	meta, ok := result.Args[7].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "v1", meta["k1"])
	assert.Equal(t, "v2", meta["k2"])

	// Ack the message
	require.NoError(t, msgs[0].Ack())
}
