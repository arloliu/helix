package integration_test

// Integration tests for NATS replay under transport faults:
// an attempt cut short by a worker stopping,
// and a NATS server bounced while a backlog drains.
// Both run the real DefaultExecuteFunc against a real cluster,
// so a redelivered payload is written twice
// and the client timestamp is what keeps the second write harmless.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	cqlv1 "github.com/arloliu/helix/adapter/cql/v1"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/test/testutil"
	"github.com/arloliu/helix/types"
)

const (
	replayFaultsTableSchema = `
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			v TEXT
		)
	`
	replayFaultsWait = 30 * time.Second
)

// executionCounter counts executions of each payload, keyed by its timestamp,
// which the tests make unique per payload.
type executionCounter struct {
	mu     sync.Mutex
	counts map[int64]int
}

// failureSignalLogger reports, once,
// that the worker logged a failed read of the stream for cluster B, the cluster the backlog targets:
// a dequeue or a backlog depth read.
type failureSignalLogger struct {
	*testutil.TestLogger
	once   sync.Once
	failed chan struct{}
}

// TestNATSWorkerRedeliversAttemptCutShortByStop proves that a payload
// whose write landed but whose attempt was cut short by Stop
// is executed again by the next worker,
// and that the second write changes neither the value nor its WRITETIME.
//
// Worker 1 writes the first payload it receives, then holds the attempt open until Stop cancels it.
// Stop NAKs that message and the unprocessed rest of the batch,
// so worker 2 executes all n payloads: n+1 executions, two of them for the interrupted payload.
func TestNATSWorkerRedeliversAttemptCutShortByStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const n = 5

	sessionA, sessionB := getSharedSessions(t)
	table := createTestTableOnBoth(t, "nats_redeliver", replayFaultsTableSchema)

	js := testutil.StartEmbeddedNATS(t)
	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-redeliver-stop"),
		replay.WithSubjectPrefix("test.redeliver.stop"),
		replay.WithAckWait(5*time.Second), // Stop NAKs; redelivery must not depend on AckWait
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replayer.Close() })

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB))
	require.NoError(t, err)
	inner := client.DefaultExecuteFunc()

	payloads := replayFaultPayloads(table, n)
	for _, p := range payloads {
		require.NoError(t, replayer.Enqueue(t.Context(), p))
	}

	counter := &executionCounter{counts: make(map[int64]int)}

	// Worker 1: write, then hold the attempt until Stop cancels it.
	written := make(chan types.ReplayPayload, 1)
	var w1Successes atomic.Int32
	w1 := replay.NewNATSWorker(replayer,
		func(ctx context.Context, p types.ReplayPayload) error {
			if err := inner(ctx, p); err != nil {
				return err
			}
			counter.record(p)
			select {
			case written <- p:
			default:
			}
			<-ctx.Done()

			return ctx.Err()
		},
		replay.WithBatchSize(n),
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithExecuteTimeout(time.Minute), // Stop, not the attempt deadline, ends the hold
		replay.WithOnSuccess(func(types.ReplayPayload) { w1Successes.Add(1) }),
	)
	require.NoError(t, w1.Start())
	t.Cleanup(w1.Stop)

	var first types.ReplayPayload
	select {
	case first = <-written:
	case <-time.After(replayFaultsWait):
		w1.Stop()
		t.Fatal("worker 1 never executed a payload")
	}

	firstID := replayFaultID(first)
	valueBefore, writeTimeBefore := readReplayFaultRow(t, sessionB, table, firstID)
	require.Equal(t, replayFaultValue(firstID), valueBefore)
	require.Equal(t, first.Timestamp, writeTimeBefore, "the replay write carries the payload's timestamp")

	w1.Stop()
	require.Zero(t, w1Successes.Load(), "worker 1 settled nothing as a success")
	require.Equal(t, 1, counter.total(), "worker 1 executed exactly one payload before Stop")

	// Worker 2 on the same stream re-executes everything worker 1 handed back.
	successes := make(chan types.ReplayPayload, 2*n) // room for an unexpected duplicate, so it is counted rather than blocking Stop
	var w2Errors atomic.Int32
	w2 := replay.NewNATSWorker(replayer,
		func(ctx context.Context, p types.ReplayPayload) error {
			if err := inner(ctx, p); err != nil {
				return err
			}
			counter.record(p)

			return nil
		},
		replay.WithBatchSize(n),
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithOnSuccess(func(p types.ReplayPayload) { successes <- p }),
		replay.WithOnError(func(types.ReplayPayload, error, int) { w2Errors.Add(1) }),
	)
	require.NoError(t, w2.Start())
	t.Cleanup(w2.Stop)
	for i := range n {
		select {
		case <-successes:
		case <-time.After(replayFaultsWait):
			w2.Stop()
			t.Fatalf("worker 2 settled %d of %d payloads", i, n)
		}
	}
	w2.Stop() // joins the worker goroutines, so the counts below can no longer move

	require.Zero(t, w2Errors.Load())
	require.Empty(t, successes, "worker 2 settled no payload twice")
	require.Equal(t, n+1, counter.total())
	for _, p := range payloads {
		want := 1
		if p.Timestamp == first.Timestamp {
			want = 2
		}
		require.Equal(t, want, counter.count(p), "executions of payload %d", replayFaultID(p))
	}

	valueAfter, writeTimeAfter := readReplayFaultRow(t, sessionB, table, firstID)
	require.Equal(t, valueBefore, valueAfter, "the duplicate execution left the value unchanged")
	require.Equal(t, writeTimeBefore, writeTimeAfter, "the duplicate execution left WRITETIME unchanged")

	pending, err := replayer.PendingByCluster(t.Context(), types.ClusterB)
	require.NoError(t, err)
	require.Zero(t, pending)
}

// TestNATSWorkerConvergesAcrossServerBounceMidDrain shuts the NATS server down
// while a worker is in the middle of a backlog, restarts it on the same store,
// and proves every payload still lands with its own value and timestamp
// and none is dropped.
//
// The attempt in flight at the shutdown finishes while the server is down,
// so its acknowledgement crosses the outage.
// A redelivery after the restart is allowed; a missing or dropped payload is not.
func TestNATSWorkerConvergesAcrossServerBounceMidDrain(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const (
		n         = 40
		batchSize = 5
		bounceAt  = 2 * batchSize // the last attempt of the second batch
	)

	sessionA, sessionB := getSharedSessions(t)
	table := createTestTableOnBoth(t, "nats_bounce", replayFaultsTableSchema)

	ns := testutil.StartRestartableNATS(t)
	js := ns.Connect(t)
	replayer, err := replay.NewNATSReplayer(js,
		replay.WithStreamName("test-bounce-drain"),
		replay.WithSubjectPrefix("test.bounce.drain"),
		replay.WithAckWait(time.Second), // an acknowledgement lost in the outage redelivers quickly
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replayer.Close() })

	client, err := helix.NewCQLClient(cqlv1.WrapSession(sessionA), cqlv1.WrapSession(sessionB))
	require.NoError(t, err)
	inner := client.DefaultExecuteFunc()

	payloads := replayFaultPayloads(table, n)
	for _, p := range payloads {
		require.NoError(t, replayer.Enqueue(t.Context(), p))
	}

	counter := &executionCounter{counts: make(map[int64]int)}
	var executions atomic.Int32
	reached := make(chan struct{})
	release := make(chan struct{})

	var mu sync.Mutex
	settled := make(map[int64]bool, n)
	allSettled := make(chan struct{})
	var drops atomic.Int32

	logger := &failureSignalLogger{TestLogger: testutil.NewTestLogger(t), failed: make(chan struct{})}
	worker := replay.NewNATSWorker(replayer,
		func(ctx context.Context, p types.ReplayPayload) error {
			if err := inner(ctx, p); err != nil {
				return err
			}
			counter.record(p)
			if executions.Add(1) == bounceAt {
				close(reached)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		},
		replay.WithBatchSize(batchSize),
		replay.WithPollInterval(10*time.Millisecond),
		replay.WithWorkerLogger(logger),
		replay.WithOnSuccess(func(p types.ReplayPayload) {
			mu.Lock()
			defer mu.Unlock()
			if settled[p.Timestamp] {
				return
			}
			settled[p.Timestamp] = true
			if len(settled) == n {
				close(allSettled)
			}
		}),
		replay.WithOnDrop(func(types.ReplayPayload, error) { drops.Add(1) }),
	)
	require.NoError(t, worker.Start())
	t.Cleanup(worker.Stop)

	select {
	case <-reached:
	case <-time.After(replayFaultsWait):
		t.Fatal("the worker never reached the bounce point")
	}
	ns.Shutdown()
	close(release)

	select {
	case <-logger.failed:
	case <-time.After(replayFaultsWait):
		t.Fatal("the worker draining cluster B never saw the server go away")
	}
	ns.Restart()

	select {
	case <-allSettled:
	case <-time.After(replayFaultsWait):
		mu.Lock()
		got := len(settled)
		mu.Unlock()
		t.Fatalf("settled %d of %d payloads after the restart", got, n)
	}

	// The stream is state held by the server, so nothing fires when it empties;
	// a redelivery of an acknowledgement lost in the outage may still be in flight.
	require.Eventually(t, func() bool {
		pending, err := replayer.PendingByCluster(t.Context(), types.ClusterB)

		return err == nil && pending == 0
	}, replayFaultsWait, 50*time.Millisecond, "the stream drains once every payload has settled")
	worker.Stop()

	require.Zero(t, drops.Load(), "no payload was dropped")
	require.GreaterOrEqual(t, counter.total(), n)
	for _, p := range payloads {
		require.GreaterOrEqual(t, counter.count(p), 1, "payload %d executed", replayFaultID(p))
		id := replayFaultID(p)
		value, writeTime := readReplayFaultRow(t, sessionB, table, id)
		require.Equal(t, replayFaultValue(id), value)
		require.Equal(t, p.Timestamp, writeTime, "payload %d kept its timestamp", id)
	}
	t.Logf("executions: %d for %d payloads", counter.total(), n)
}

// replayFaultPayloads builds n payloads targeting cluster B,
// each with its own row and its own timestamp an hour in the past.
func replayFaultPayloads(table string, n int) []types.ReplayPayload {
	baseMicro := time.Now().Add(-time.Hour).UnixMicro()
	payloads := make([]types.ReplayPayload, n)
	for i := range payloads {
		payloads[i] = types.ReplayPayload{
			TargetCluster: types.ClusterB,
			Query:         "INSERT INTO " + table + " (id, v) VALUES (?, ?)",
			Args:          []any{i, replayFaultValue(i)},
			Timestamp:     baseMicro + int64(i),
			Priority:      types.PriorityHigh,
		}
	}

	return payloads
}

func replayFaultValue(id int) string {
	return fmt.Sprintf("value-%d", id)
}

// replayFaultID returns the row id of a payload built by replayFaultPayloads.
// The id is recovered from the value, which survives the NATS round trip as a string.
func replayFaultID(p types.ReplayPayload) int {
	var id int
	value, _ := p.Args[1].(string)
	_, _ = fmt.Sscanf(value, "value-%d", &id)

	return id
}

func readReplayFaultRow(t *testing.T, session *gocql.Session, table string, id int) (string, int64) {
	t.Helper()

	var (
		value     string
		writeTime int64
	)
	err := session.Query("SELECT v, WRITETIME(v) FROM "+table+" WHERE id = ?", id).
		Consistency(gocql.One).Scan(&value, &writeTime)
	require.NoError(t, err, "row %d", id)

	return value, writeTime
}

func (c *executionCounter) record(p types.ReplayPayload) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counts[p.Timestamp]++
}

func (c *executionCounter) count(p types.ReplayPayload) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.counts[p.Timestamp]
}

func (c *executionCounter) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	total := 0
	for _, v := range c.counts {
		total += v
	}

	return total
}

func (l *failureSignalLogger) Debug(msg string, keysAndValues ...any) {
	l.signal(msg, keysAndValues)
	l.TestLogger.Debug(msg, keysAndValues...)
}

func (l *failureSignalLogger) Error(msg string, keysAndValues ...any) {
	l.signal(msg, keysAndValues)
	l.TestLogger.Error(msg, keysAndValues...)
}

func (l *failureSignalLogger) signal(msg string, keysAndValues []any) {
	if !strings.HasPrefix(msg, "failed to dequeue") && !strings.HasPrefix(msg, "failed to read replay backlog depth") {
		return
	}
	for i := 0; i+1 < len(keysAndValues); i += 2 {
		if keysAndValues[i] == "cluster" && keysAndValues[i+1] == "B" {
			l.once.Do(func() { close(l.failed) })

			return
		}
	}
}
