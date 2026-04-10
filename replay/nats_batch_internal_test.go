package replay

// Tests for corrupt batch statement handling in Dequeue and DequeueByPriority.
//
// These are internal (white-box) tests that bypass the public Enqueue API and
// publish crafted natsReplayMessage structs directly to JetStream. This is the
// only way to inject messages with corrupt BatchStatement.Args fields, because
// the public Enqueue function validates and re-encodes every argument.
//
// Bugs being guarded:
//
//  1. A continue inside the inner "for i, stmt" loop only advances the inner
//     loop. Without a labeled continue, execution falls through to the outer
//     result = append(...) after the inner loop exits, so a malformed batch
//     message would be silently appended with zero-value entries. Fixed by
//     labeling the outer loop and using "continue msgLoop".
//
//  2. Corrupt messages were Nak'd (redelivered up to MaxDeliver times) rather
//     than Term'd. A message that cannot be decoded will never succeed, so it
//     must be terminated immediately on first encounter to avoid churning.
//     Fixed by calling msg.Term() in all decode-error paths.
//
// Note: testutil cannot be imported here — testutil → helix → replay creates a
// cycle. The NATS server startup is inlined via startNATSForTest.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"

	"github.com/arloliu/helix/types"
)

// startNATSForTest starts an in-process NATS server with JetStream and returns
// a ready-to-use JetStream context. Both the connection and the server are
// registered for cleanup with t.Cleanup. This mirrors testutil.StartEmbeddedNATS
// but lives here to avoid the testutil → helix → replay import cycle.
func startNATSForTest(t *testing.T) jetstream.JetStream {
	t.Helper()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random available port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}

	ns, err := server.NewServer(opts)
	require.NoError(t, err, "failed to create embedded NATS server")

	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "NATS server not ready")

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

// invalidMsgpBytes is a single-byte sequence that is not a valid msgpack array
// header, causing decodeArgs to fail. 0xc1 is "never used" in the msgpack spec.
var invalidMsgpBytes = msgp.Raw{0xc1}

// newTestReplayer creates a NATSReplayer for use in batch tests.
// It uses the default AckWait (30 s) intentionally: Dequeue internally calls
// consumer.Fetch with a 1-second window, and a shorter AckWait would cause
// unacked messages to be redelivered within that same window, producing
// duplicate entries in the result and making assertions unreliable.
func newTestReplayer(t *testing.T, js jetstream.JetStream, name, prefix string) *NATSReplayer {
	t.Helper()

	r, err := NewNATSReplayer(js,
		WithStreamName(name),
		WithSubjectPrefix(prefix),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = r.Close() })

	return r
}

// publishRawBatch encodes msg and publishes it directly to the NATS subject,
// bypassing Enqueue so we can inject arbitrary (possibly corrupt) data.
func publishRawBatch(t *testing.T, js jetstream.JetStream, subject string, msg natsReplayMessage) {
	t.Helper()

	data, err := msg.MarshalMsg(nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err = js.Publish(ctx, subject, data)
	require.NoError(t, err)
}

// validStmtEncoded creates a batchStatement with properly encoded args.
func validStmtEncoded(t *testing.T, query string, args ...any) batchStatement {
	t.Helper()

	raw, err := encodeArgs(args)
	require.NoError(t, err)

	return batchStatement{Query: query, Args: raw}
}

// corruptStmtEncoded creates a batchStatement whose Args bytes are invalid msgpack.
func corruptStmtEncoded(query string) batchStatement {
	return batchStatement{Query: query, Args: invalidMsgpBytes}
}

// makeBatchNATSMsg builds a natsReplayMessage representing a ClusterA batch write.
func makeBatchNATSMsg(stmts []batchStatement) natsReplayMessage {
	return natsReplayMessage{
		TargetCluster:   "A",
		IsBatch:         true,
		BatchStatements: stmts,
		Timestamp:       12345,
	}
}

// makeSimpleNATSMsg builds a natsReplayMessage representing a single non-batch ClusterA query.
func makeSimpleNATSMsg(t *testing.T, query string, args ...any) natsReplayMessage {
	t.Helper()

	raw, err := encodeArgs(args)
	require.NoError(t, err)

	return natsReplayMessage{
		TargetCluster: "A",
		Query:         query,
		Args:          raw,
		Timestamp:     12345,
	}
}

// highSubject returns the high-priority ClusterA NATS subject for the given prefix.
func highSubject(prefix string) string {
	return fmt.Sprintf("%s.high.A", prefix)
}

// publishRawBytes publishes arbitrary raw bytes to a NATS subject, bypassing
// MarshalMsg. Used to inject a completely invalid msgpack envelope that will
// cause natsReplayMessage.UnmarshalMsg to fail.
func publishRawBytes(t *testing.T, js jetstream.JetStream, subject string, data []byte) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err := js.Publish(ctx, subject, data)
	require.NoError(t, err)
}

// corruptPathCase describes a single corrupt-message scenario used to exercise
// all three decode-error paths in Dequeue and DequeueByPriority.
type corruptPathCase struct {
	name    string
	publish func(t *testing.T, js jetstream.JetStream, subj string)
}

// allCorruptPathCases returns one case per decode-error path:
//  1. Outer UnmarshalMsg failure  — the raw bytes are not a valid natsReplayMessage
//  2. Outer decodeArgs failure    — valid envelope, but Args field is corrupt msgpack
//  3. Inner batch stmt failure    — valid batch envelope, but one statement's Args is corrupt
func allCorruptPathCases() []corruptPathCase {
	return []corruptPathCase{
		{
			name: "invalid msgpack envelope",
			publish: func(t *testing.T, js jetstream.JetStream, subj string) {
				t.Helper()
				publishRawBytes(t, js, subj, []byte{0xc1}) // never-used msgpack byte
			},
		},
		{
			name: "corrupt outer args",
			publish: func(t *testing.T, js jetstream.JetStream, subj string) {
				t.Helper()
				publishRawBatch(t, js, subj, natsReplayMessage{
					TargetCluster: "A",
					Query:         "INSERT INTO t (id) VALUES (?)",
					Args:          invalidMsgpBytes,
					Timestamp:     12345,
				})
			},
		},
		{
			name: "corrupt batch statement args",
			publish: func(t *testing.T, js jetstream.JetStream, subj string) {
				t.Helper()
				publishRawBatch(t, js, subj, makeBatchNATSMsg([]batchStatement{
					corruptStmtEncoded("INSERT corrupt"),
				}))
			},
		},
	}
}

// =============================================================================
// Dequeue — corrupt batch statement tests
// =============================================================================

// TestDequeueSkipsCorruptBatchStmt verifies that a batch message with a corrupt
// statement is never appended to the Dequeue result, regardless of which
// statement in the batch is malformed.
func TestDequeueSkipsCorruptBatchStmt(t *testing.T) {
	tests := []struct {
		name       string
		buildStmts func(t *testing.T) []batchStatement
	}{
		{
			name: "first of three statements corrupt",
			buildStmts: func(t *testing.T) []batchStatement {
				t.Helper()
				return []batchStatement{
					corruptStmtEncoded("INSERT corrupt"),
					validStmtEncoded(t, "INSERT valid_1", "a"),
					validStmtEncoded(t, "INSERT valid_2", "b"),
				}
			},
		},
		{
			name: "middle of three statements corrupt",
			buildStmts: func(t *testing.T) []batchStatement {
				t.Helper()
				return []batchStatement{
					validStmtEncoded(t, "INSERT valid_1", "a"),
					corruptStmtEncoded("INSERT corrupt"),
					validStmtEncoded(t, "INSERT valid_2", "b"),
				}
			},
		},
		{
			name: "last of three statements corrupt",
			buildStmts: func(t *testing.T) []batchStatement {
				t.Helper()
				return []batchStatement{
					validStmtEncoded(t, "INSERT valid_1", "a"),
					validStmtEncoded(t, "INSERT valid_2", "b"),
					corruptStmtEncoded("INSERT corrupt"),
				}
			},
		},
		{
			name: "single corrupt statement",
			buildStmts: func(t *testing.T) []batchStatement {
				t.Helper()
				return []batchStatement{corruptStmtEncoded("INSERT corrupt")}
			},
		},
		{
			name: "all statements corrupt",
			buildStmts: func(t *testing.T) []batchStatement {
				t.Helper()
				return []batchStatement{
					corruptStmtEncoded("INSERT corrupt_1"),
					corruptStmtEncoded("INSERT corrupt_2"),
					corruptStmtEncoded("INSERT corrupt_3"),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := startNATSForTest(t)
			r := newTestReplayer(t, js, "test-deq-corrupt", "test.deq.corrupt")
			ctx := t.Context()

			subj := highSubject("test.deq.corrupt")
			publishRawBatch(t, js, subj, makeBatchNATSMsg(tt.buildStmts(t)))

			msgs, err := r.Dequeue(ctx, types.ClusterA, 10)
			require.NoError(t, err)
			assert.Empty(t, msgs, "corrupt batch message must not appear in dequeue result")
		})
	}
}

// TestDequeueTermsCorruptBatchMessage confirms that a corrupt batch message is
// immediately terminated (removed from the stream) rather than NAK'd for retry.
// Because the payload bytes are irrecoverably malformed, redelivery would never
// succeed; Term() removes it on the first encounter.
func TestDequeueTermsCorruptBatchMessage(t *testing.T) {
	js := startNATSForTest(t)
	r := newTestReplayer(t, js, "test-deq-term", "test.deq.term")
	ctx := t.Context()

	subj := highSubject("test.deq.term")
	publishRawBatch(t, js, subj, makeBatchNATSMsg([]batchStatement{
		corruptStmtEncoded("INSERT corrupt"),
	}))

	msgs, err := r.Dequeue(ctx, types.ClusterA, 10)
	require.NoError(t, err)
	assert.Empty(t, msgs)

	// Term() removes the message from the stream immediately.
	// Pending == 0 confirms the message was terminated, not silently lost.
	pending, err := r.Pending(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, pending, "Term'd corrupt message must be removed from the stream")
}

// TestDequeueKeptValidMessageAfterCorruptBatch is the regression test for the
// fallthrough bug: a corrupt batch followed by a valid message must yield only
// the valid message — never a partially-decoded copy of the corrupt one.
func TestDequeueKeptValidMessageAfterCorruptBatch(t *testing.T) {
	js := startNATSForTest(t)
	r := newTestReplayer(t, js, "test-deq-mixed", "test.deq.mixed")
	ctx := t.Context()

	subj := highSubject("test.deq.mixed")

	// Corrupt batch arrives first.
	publishRawBatch(t, js, subj, makeBatchNATSMsg([]batchStatement{
		corruptStmtEncoded("INSERT corrupt"),
	}))

	// Valid non-batch message arrives second.
	publishRawBatch(t, js, subj, makeSimpleNATSMsg(t, "INSERT INTO t (id) VALUES (?)", "uuid-ok"))

	msgs, err := r.Dequeue(ctx, types.ClusterA, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1, "only the valid message must be returned")

	assert.Equal(t, "INSERT INTO t (id) VALUES (?)", msgs[0].Payload.Query)
	assert.False(t, msgs[0].Payload.IsBatch)

	require.NoError(t, msgs[0].Ack())
}

// TestDequeueValidBatchRoundTrip verifies that a well-formed multi-statement
// batch message survives the raw-publish → Dequeue round trip with all
// statements and their arguments preserved.
func TestDequeueValidBatchRoundTrip(t *testing.T) {
	js := startNATSForTest(t)
	r := newTestReplayer(t, js, "test-deq-valid-rt", "test.deq.valid.rt")
	ctx := t.Context()

	subj := highSubject("test.deq.valid.rt")

	stmts := []batchStatement{
		validStmtEncoded(t, "INSERT INTO t (id) VALUES (?)", "uuid-1"),
		validStmtEncoded(t, "UPDATE t SET v = ? WHERE id = ?", int64(42), "uuid-2"),
		validStmtEncoded(t, "DELETE FROM t WHERE id = ?", "uuid-3"),
	}
	publishRawBatch(t, js, subj, makeBatchNATSMsg(stmts))

	msgs, err := r.Dequeue(ctx, types.ClusterA, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	p := msgs[0].Payload
	assert.True(t, p.IsBatch)
	require.Len(t, p.BatchStatements, 3)
	assert.Equal(t, "INSERT INTO t (id) VALUES (?)", p.BatchStatements[0].Query)
	assert.Equal(t, "UPDATE t SET v = ? WHERE id = ?", p.BatchStatements[1].Query)
	assert.Equal(t, "DELETE FROM t WHERE id = ?", p.BatchStatements[2].Query)

	require.NoError(t, msgs[0].Ack())
}

// TestDequeueEmptyBatchStatements verifies that a batch message with zero
// statements (unusual but valid) is returned without errors or panics.
func TestDequeueEmptyBatchStatements(t *testing.T) {
	js := startNATSForTest(t)
	r := newTestReplayer(t, js, "test-deq-empty-stmts", "test.deq.empty.stmts")
	ctx := t.Context()

	subj := highSubject("test.deq.empty.stmts")
	publishRawBatch(t, js, subj, makeBatchNATSMsg([]batchStatement{}))

	msgs, err := r.Dequeue(ctx, types.ClusterA, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1, "zero-statement batch should be returned normally")
	assert.True(t, msgs[0].Payload.IsBatch)
	assert.Empty(t, msgs[0].Payload.BatchStatements)

	require.NoError(t, msgs[0].Ack())
}

// TestDequeueMultipleCorruptBatchesOnlyReturnsValid ensures that when a stream
// contains several corrupt batch messages interspersed with valid ones, only the
// valid messages appear in the result.
func TestDequeueMultipleCorruptBatchesOnlyReturnsValid(t *testing.T) {
	js := startNATSForTest(t)
	r := newTestReplayer(t, js, "test-deq-multi-corrupt", "test.deq.multi.corrupt")
	ctx := t.Context()

	subj := highSubject("test.deq.multi.corrupt")

	// Publish: corrupt, valid, corrupt, valid
	publishRawBatch(t, js, subj, makeBatchNATSMsg([]batchStatement{corruptStmtEncoded("INSERT bad_1")}))
	publishRawBatch(t, js, subj, makeSimpleNATSMsg(t, "INSERT good_1", "v1"))
	publishRawBatch(t, js, subj, makeBatchNATSMsg([]batchStatement{corruptStmtEncoded("INSERT bad_2")}))
	publishRawBatch(t, js, subj, makeSimpleNATSMsg(t, "INSERT good_2", "v2"))

	msgs, err := r.Dequeue(ctx, types.ClusterA, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 2, "only valid messages must be returned")

	queries := []string{msgs[0].Payload.Query, msgs[1].Payload.Query}
	assert.Contains(t, queries, "INSERT good_1")
	assert.Contains(t, queries, "INSERT good_2")

	for _, m := range msgs {
		require.NoError(t, m.Ack())
	}
}

// =============================================================================
// DequeueByPriority — corrupt batch statement tests (mirror of Dequeue tests)
// =============================================================================

// TestDequeueByPrioritySkipsCorruptBatchStmt mirrors TestDequeueSkipsCorruptBatchStmt
// but exercises DequeueByPriority, which has its own message-processing loop and
// required a separate labeled-continue fix.
func TestDequeueByPrioritySkipsCorruptBatchStmt(t *testing.T) {
	tests := []struct {
		name       string
		buildStmts func(t *testing.T) []batchStatement
	}{
		{
			name: "first of three statements corrupt",
			buildStmts: func(t *testing.T) []batchStatement {
				t.Helper()
				return []batchStatement{
					corruptStmtEncoded("INSERT corrupt"),
					validStmtEncoded(t, "INSERT valid_1", "a"),
					validStmtEncoded(t, "INSERT valid_2", "b"),
				}
			},
		},
		{
			name: "middle of three statements corrupt",
			buildStmts: func(t *testing.T) []batchStatement {
				t.Helper()
				return []batchStatement{
					validStmtEncoded(t, "INSERT valid_1", "a"),
					corruptStmtEncoded("INSERT corrupt"),
					validStmtEncoded(t, "INSERT valid_2", "b"),
				}
			},
		},
		{
			name: "last of three statements corrupt",
			buildStmts: func(t *testing.T) []batchStatement {
				t.Helper()
				return []batchStatement{
					validStmtEncoded(t, "INSERT valid_1", "a"),
					validStmtEncoded(t, "INSERT valid_2", "b"),
					corruptStmtEncoded("INSERT corrupt"),
				}
			},
		},
		{
			name: "all statements corrupt",
			buildStmts: func(t *testing.T) []batchStatement {
				t.Helper()
				return []batchStatement{
					corruptStmtEncoded("INSERT corrupt_1"),
					corruptStmtEncoded("INSERT corrupt_2"),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := startNATSForTest(t)
			r := newTestReplayer(t, js, "test-deqpri-corrupt", "test.deqpri.corrupt")
			ctx := t.Context()

			// DequeueByPriority with PriorityHigh reads from {prefix}.high.{cluster}
			subj := highSubject("test.deqpri.corrupt")
			publishRawBatch(t, js, subj, makeBatchNATSMsg(tt.buildStmts(t)))

			msgs, err := r.DequeueByPriority(ctx, types.ClusterA, types.PriorityHigh, 10)
			require.NoError(t, err)
			assert.Empty(t, msgs, "corrupt batch must not appear in DequeueByPriority result")
		})
	}
}

// TestDequeueByPriorityTermsCorruptBatchMessage mirrors
// TestDequeueTermsCorruptBatchMessage for DequeueByPriority.
func TestDequeueByPriorityTermsCorruptBatchMessage(t *testing.T) {
	js := startNATSForTest(t)
	r := newTestReplayer(t, js, "test-deqpri-term", "test.deqpri.term")
	ctx := t.Context()

	subj := highSubject("test.deqpri.term")
	publishRawBatch(t, js, subj, makeBatchNATSMsg([]batchStatement{
		corruptStmtEncoded("INSERT corrupt"),
	}))

	msgs, err := r.DequeueByPriority(ctx, types.ClusterA, types.PriorityHigh, 10)
	require.NoError(t, err)
	assert.Empty(t, msgs)

	// Term() removes the message from the stream immediately.
	pending, err := r.Pending(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, pending, "Term'd corrupt message must be removed from the stream")
}

// TestDequeueByPriorityKeptValidAfterCorrupt mirrors
// TestDequeueKeptValidMessageAfterCorruptBatch for DequeueByPriority.
func TestDequeueByPriorityKeptValidAfterCorrupt(t *testing.T) {
	js := startNATSForTest(t)
	r := newTestReplayer(t, js, "test-deqpri-mixed", "test.deqpri.mixed")
	ctx := t.Context()

	subj := highSubject("test.deqpri.mixed")

	publishRawBatch(t, js, subj, makeBatchNATSMsg([]batchStatement{
		corruptStmtEncoded("INSERT corrupt"),
	}))
	publishRawBatch(t, js, subj, makeSimpleNATSMsg(t, "INSERT INTO t (id) VALUES (?)", "uuid-ok"))

	msgs, err := r.DequeueByPriority(ctx, types.ClusterA, types.PriorityHigh, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1, "only the valid message must be returned")
	assert.Equal(t, "INSERT INTO t (id) VALUES (?)", msgs[0].Payload.Query)
	assert.False(t, msgs[0].Payload.IsBatch)

	require.NoError(t, msgs[0].Ack())
}

// TestDequeueByPriorityValidBatchRoundTrip mirrors TestDequeueValidBatchRoundTrip
// for DequeueByPriority to confirm the happy path is unaffected by the fix.
func TestDequeueByPriorityValidBatchRoundTrip(t *testing.T) {
	js := startNATSForTest(t)
	r := newTestReplayer(t, js, "test-deqpri-valid-rt", "test.deqpri.valid.rt")
	ctx := t.Context()

	subj := highSubject("test.deqpri.valid.rt")

	stmts := []batchStatement{
		validStmtEncoded(t, "INSERT INTO t (id) VALUES (?)", "uuid-1"),
		validStmtEncoded(t, "UPDATE t SET v = ? WHERE id = ?", int64(7), "uuid-2"),
	}
	publishRawBatch(t, js, subj, makeBatchNATSMsg(stmts))

	msgs, err := r.DequeueByPriority(ctx, types.ClusterA, types.PriorityHigh, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	p := msgs[0].Payload
	assert.True(t, p.IsBatch)
	require.Len(t, p.BatchStatements, 2)
	assert.Equal(t, "INSERT INTO t (id) VALUES (?)", p.BatchStatements[0].Query)
	assert.Equal(t, "UPDATE t SET v = ? WHERE id = ?", p.BatchStatements[1].Query)

	require.NoError(t, msgs[0].Ack())
}

// TestDequeueByPriorityMultipleCorruptOnlyReturnsValid mirrors
// TestDequeueMultipleCorruptBatchesOnlyReturnsValid for DequeueByPriority.
func TestDequeueByPriorityMultipleCorruptOnlyReturnsValid(t *testing.T) {
	js := startNATSForTest(t)
	r := newTestReplayer(t, js, "test-deqpri-multi-corrupt", "test.deqpri.multi.corrupt")
	ctx := t.Context()

	subj := highSubject("test.deqpri.multi.corrupt")

	publishRawBatch(t, js, subj, makeBatchNATSMsg([]batchStatement{corruptStmtEncoded("INSERT bad_1")}))
	publishRawBatch(t, js, subj, makeSimpleNATSMsg(t, "INSERT good_1", "v1"))
	publishRawBatch(t, js, subj, makeBatchNATSMsg([]batchStatement{corruptStmtEncoded("INSERT bad_2")}))
	publishRawBatch(t, js, subj, makeSimpleNATSMsg(t, "INSERT good_2", "v2"))

	msgs, err := r.DequeueByPriority(ctx, types.ClusterA, types.PriorityHigh, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 2, "only valid messages must be returned")

	queries := []string{msgs[0].Payload.Query, msgs[1].Payload.Query}
	assert.Contains(t, queries, "INSERT good_1")
	assert.Contains(t, queries, "INSERT good_2")

	for _, m := range msgs {
		require.NoError(t, m.Ack())
	}
}

// =============================================================================
// All corrupt decode paths — Term + OnCorruptMessage callback
// =============================================================================

// TestDequeueCorruptPathTermsAndInvokesCallback verifies that every decode-error
// path in Dequeue (outer UnmarshalMsg, outer decodeArgs, inner batch stmt decodeArgs)
// terminates the message immediately (pending == 0) and fires the OnCorruptMessage
// callback exactly once.
func TestDequeueCorruptPathTermsAndInvokesCallback(t *testing.T) {
	for i, tc := range allCorruptPathCases() {
		t.Run(tc.name, func(t *testing.T) {
			js := startNATSForTest(t)

			var callbackCount atomic.Int32
			streamName := fmt.Sprintf("test-deq-corrupt-cb-%d", i)
			subjectPrefix := fmt.Sprintf("test.deq.corrupt.cb.%d", i)

			r, err := NewNATSReplayer(js,
				WithStreamName(streamName),
				WithSubjectPrefix(subjectPrefix),
				WithOnCorruptMessage(func(cbErr error) {
					require.Error(t, cbErr)
					callbackCount.Add(1)
				}),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = r.Close() })

			ctx := t.Context()
			tc.publish(t, js, highSubject(subjectPrefix))

			msgs, err := r.Dequeue(ctx, types.ClusterA, 10)
			require.NoError(t, err)
			assert.Empty(t, msgs, "corrupt message must not appear in Dequeue result")

			// Term removes the message immediately; Pending must be 0.
			pending, err := r.Pending(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, pending, "Term'd message must be removed from the stream")

			// OnCorruptMessage must have been called exactly once.
			assert.Equal(t, int32(1), callbackCount.Load(),
				"OnCorruptMessage callback must be invoked once per corrupt message")
		})
	}
}

// TestDequeueByPriorityCorruptPathTermsAndInvokesCallback mirrors
// TestDequeueCorruptPathTermsAndInvokesCallback for DequeueByPriority.
func TestDequeueByPriorityCorruptPathTermsAndInvokesCallback(t *testing.T) {
	for i, tc := range allCorruptPathCases() {
		t.Run(tc.name, func(t *testing.T) {
			js := startNATSForTest(t)

			var callbackCount atomic.Int32
			streamName := fmt.Sprintf("test-deqpri-corrupt-cb-%d", i)
			subjectPrefix := fmt.Sprintf("test.deqpri.corrupt.cb.%d", i)

			r, err := NewNATSReplayer(js,
				WithStreamName(streamName),
				WithSubjectPrefix(subjectPrefix),
				WithOnCorruptMessage(func(cbErr error) {
					require.Error(t, cbErr)
					callbackCount.Add(1)
				}),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = r.Close() })

			ctx := t.Context()
			tc.publish(t, js, highSubject(subjectPrefix))

			msgs, err := r.DequeueByPriority(ctx, types.ClusterA, types.PriorityHigh, 10)
			require.NoError(t, err)
			assert.Empty(t, msgs, "corrupt message must not appear in DequeueByPriority result")

			pending, err := r.Pending(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, pending, "Term'd message must be removed from the stream")

			assert.Equal(t, int32(1), callbackCount.Load(),
				"OnCorruptMessage callback must be invoked once per corrupt message")
		})
	}
}
