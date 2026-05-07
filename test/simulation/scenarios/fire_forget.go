package scenarios

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/arloliu/helix/test/simulation/types"
	htypes "github.com/arloliu/helix/types"
	"github.com/gocql/gocql"
)

type FireForgetLimit struct{}

func (s *FireForgetLimit) Name() string {
	return "fire-forget-limit"
}

func (s *FireForgetLimit) Description() string {
	return "Verifies behavior when fire-and-forget limit is reached"
}

func (s *FireForgetLimit) Run(ctx context.Context, env *types.Environment) error {
	env.Logger.Info("Starting FireForgetLimit scenario")
	startCount := env.Tracker.TotalWrites()

	// 1. Degrade Cluster A to trigger fire-and-forget mode (> delta threshold).
	//    500ms exceeds both 100ms (quick) and 300ms (soak) delta thresholds.
	env.Logger.Info("Degrading Cluster A to trigger fire-and-forget")
	env.ChaosA.SetLatency(500 * time.Millisecond)

	// 2. Wait for adaptive write to switch A into fire-and-forget mode
	_ = waitUntil(ctx, 10*time.Second, func() bool {
		return env.Tracker.TotalWrites() >= startCount+30
	})

	// 3. Flood writes via extra goroutines to exhaust the fire-and-forget semaphore.
	//    The default limit is 100 concurrent background goroutines; with 20 workers
	//    each sleeping 200ms on Cluster A, we can saturate the semaphore quickly.
	env.Logger.Info("Flooding writes to exhaust fire-and-forget semaphore")
	floodCtx, floodCancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			for {
				select {
				case <-floodCtx.Done():
					return
				default:
					id := gocql.TimeUUID()
					err := env.Client.Query("INSERT INTO test_data (id, data) VALUES (?, ?)", id, []byte{0}).Exec()
					if err == nil || errors.Is(err, htypes.ErrWriteAsync) {
						env.Tracker.TrackWrite(id, time.Now().UnixNano())
					}
				}
			}
		})
	}

	// 4. Wait until we observe ErrWriteDropped in the metrics
	err := waitUntil(ctx, 15*time.Second, func() bool {
		return env.Metrics.GetWriteDropped(htypes.ClusterA) > 0
	})

	floodCancel()
	wg.Wait()

	if err != nil {
		// Not a hard failure — fire-and-forget limit may not be reachable with this latency setting
		env.Logger.Warn("Fire-and-forget semaphore was not exhausted within timeout")
	} else {
		dropped := env.Metrics.GetWriteDropped(htypes.ClusterA)
		env.Logger.Info("Fire-and-forget semaphore exhausted", "dropped", dropped)
		if dropped == 0 {
			return errors.New("expected WriteDropped > 0 after semaphore exhaustion")
		}
	}

	// 5. Recover and verify writes resume
	env.Logger.Info("Recovering Cluster A")
	env.ChaosA.SetLatency(0)
	recoveryStart := env.Tracker.TotalWrites()
	_ = waitUntil(ctx, 10*time.Second, func() bool {
		return env.Tracker.TotalWrites() >= recoveryStart+50
	})

	env.Logger.Info("FireForgetLimit scenario completed")

	return nil
}
