package replay

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/stretchr/testify/require"
)

var ledgerCreated = time.Unix(1_700_000_000, 0)

// seededLedger returns a ledger whose first poll saw msgs messages up to
// lastSeq.
func seededLedger(msgs, lastSeq uint64) evictionLedger {
	var l evictionLedger
	l.observe(ledgerCreated, msgs, lastSeq, 0)

	return l
}

func TestEvictionLedger_CountsRemovalsThisProcessDidNotSettle(t *testing.T) {
	created := ledgerCreated
	var l evictionLedger

	require.Zero(t, l.observe(created, 5, 5, 0), "the first poll only seeds")
	require.Zero(t, l.observe(created, 8, 8, 0), "publishes alone remove nothing")
	require.Zero(t, l.observe(created, 6, 8, 2), "two acknowledged removals are settlements")
	require.Equal(t, uint64(3), l.observe(created, 3, 8, 0), "three removals without a settlement are evictions")
	require.Equal(t, uint64(1), l.observe(created, 3, 10, 1), "publishes and removals in one interval net out exactly")
}

func TestEvictionLedger_LateSettlementIsCreditedForOneInterval(t *testing.T) {
	created := ledgerCreated
	l := seededLedger(4, 4)

	// The server applied the acknowledgement after this poll, so the
	// worker counts it in the next interval.
	require.Zero(t, l.observe(created, 4, 4, 1), "a settlement the stream has not shown yet is a credit")
	require.Zero(t, l.observe(created, 3, 4, 0), "the credit covers the removal that shows up next")
	require.Equal(t, uint64(1), l.observe(created, 2, 4, 0), "the credit lasts one interval only")

	l = seededLedger(4, 4)
	l.observe(created, 4, 4, 1) // credit 1
	require.Zero(t, l.observe(created, 3, 4, 2), "the old credit covers the removal, the fresh settlements are surplus")
	require.Zero(t, l.observe(created, 1, 4, 0), "and the surplus carries on for one interval")
	require.Equal(t, uint64(1), l.observe(created, 0, 4, 0), "unused credit expires")
}

func TestEvictionLedger_ReseedsOnDiscontinuities(t *testing.T) {
	created := ledgerCreated
	l := seededLedger(5, 5)

	require.Zero(t, l.observe(created.Add(time.Minute), 1, 1, 0), "a recreated stream reseeds")
	require.Equal(t, uint64(1), l.observe(created.Add(time.Minute), 0, 1, 0), "and counts from the new baseline")

	l.observe(created, 5, 5, 0)
	require.Zero(t, l.observe(created, 2, 3, 0), "a lower LastSeq reseeds")
	require.Zero(t, l.observe(created, 9, 3, 0), "more messages than the sequences allow reseeds")
}

func TestEvictionLedger_ReportsPurges(t *testing.T) {
	created := ledgerCreated
	l := seededLedger(5, 5)

	require.Equal(t, uint64(2), l.observe(created, 3, 5, 0), "a partial purge is a removal this process did not make")
	require.Equal(t, uint64(3), l.observe(created, 0, 5, 0), "so is a whole purge")
}

// parkedStream blocks Info until the context ends.
type parkedStream struct {
	jetstream.Stream
	entered chan struct{}
}

func (p *parkedStream) Info(ctx context.Context, _ ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	close(p.entered)
	<-ctx.Done()

	return nil, ctx.Err()
}

func TestNATSBackend_StopCancelsAParkedEvictionPoll(t *testing.T) {
	stream := &parkedStream{entered: make(chan struct{})}
	cfg := newTestNATSBackendConfig()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	ticks := make(chan time.Time, 1)
	b := &natsBackend{
		replayer:    &NATSReplayer{stream: stream},
		config:      &cfg,
		stopCh:      stop,
		wg:          &wg,
		backoffWait: func(time.Duration) <-chan time.Time { return ticks },
	}
	wg.Add(1)
	go b.watchEvictions()
	ticks <- time.Time{}
	<-stream.entered

	close(stop)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop waited for the parked poll instead of cancelling it")
	}
}
