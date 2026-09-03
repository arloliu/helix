package replay

import (
	"context"
	"time"

	"github.com/arloliu/helix/types"
)

// evictionLedger turns consecutive stream states into a count of the
// messages the stream removed without this process acknowledging them.
//
// Between two polls every publish advances LastSeq by one and every
// removal lowers Msgs by one, so removed = msgs + published - msgs' holds
// whatever gaps the stream carries.
// Removals this process caused (acknowledgements and terminations) are
// subtracted. A settlement the server applied after the poll that counted
// it becomes a credit for the next interval only: the credit covers that
// interval's removals first, the interval's own settlements cover the
// rest, and only the settlements left over carry on, so a lost
// acknowledgement cannot mask later removals for good.
// A recreated stream, a lower LastSeq, or more messages than the sequences
// allow reseeds the baseline.
// A purge, whole or partial, is a removal this process did not acknowledge
// and is reported: the stream state does not distinguish it from an
// eviction.
type evictionLedger struct {
	seeded  bool
	created time.Time
	msgs    uint64
	lastSeq uint64
	credit  uint64
}

// observe records the stream state of one poll and the settlements the
// worker performed since the previous poll, and returns the messages
// removed without a settlement.
// The first poll only seeds.
func (l *evictionLedger) observe(created time.Time, msgs, lastSeq, settled uint64) uint64 {
	discontinuity := !l.seeded || !created.Equal(l.created) || lastSeq < l.lastSeq ||
		msgs > l.msgs+(lastSeq-l.lastSeq)
	if discontinuity {
		*l = evictionLedger{seeded: true, created: created, msgs: msgs, lastSeq: lastSeq}

		return 0
	}
	removed := l.msgs + (lastSeq - l.lastSeq) - msgs
	covered := settled + l.credit
	uncovered := removed - min(removed, l.credit) // removals the old credit did not cover
	l.credit = settled - min(settled, uncovered)  // fresh settlements left over; the old credit expires
	l.msgs, l.lastSeq = msgs, lastSeq

	return removed - min(removed, covered)
}

// watchEvictions polls the stream state once per depthRefreshInterval
// and reports the messages it removed without this process's
// acknowledgement, until the worker stops.
// Runs on its own goroutine; the only owner of the ledger.
func (b *natsBackend) watchEvictions() {
	defer b.wg.Done()

	// Stop cancels a poll in flight, so a server that stops answering
	// cannot hold Worker.Stop for the poll's timeout.
	base, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	go func() {
		select {
		case <-b.stopCh:
			cancelBase()
		case <-base.Done():
		}
	}()

	var ledger evictionLedger
	for {
		select {
		case <-b.stopCh:
			return
		case <-b.after(depthRefreshInterval):
		}
		ctx, cancel := context.WithTimeout(base, 5*time.Second)
		info, err := b.replayer.streamInfo(ctx)
		cancel()
		if err != nil {
			b.config.Logger.Debug("failed to read the replay stream state", "error", err.Error())

			continue
		}
		b.reportEvicted(ledger.observe(info.Created, info.State.Msgs, info.State.LastSeq, b.replayer.settled.Swap(0)))
	}
}

// reportEvicted publishes a positive eviction count as a counter, a
// cluster event, and a log line.
func (b *natsBackend) reportEvicted(n uint64) {
	count, err := msgsToInt(n)
	if err != nil || count == 0 {
		return
	}
	b.config.streamMetrics().AddReplayEvicted(count)
	if em := b.emitter.Load(); em != nil && *em != nil {
		(*em).EmitClusterEvent(types.ClusterEvent{Kind: types.EventReplayEvicted, Count: count, Timestamp: time.Now()})
	}
	b.config.Logger.Warn("replay stream removed messages without this worker's acknowledgement",
		"count", count)
}
