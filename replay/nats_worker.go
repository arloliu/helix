package replay

import (
	"context"
	"time"

	"github.com/arloliu/helix/internal/logging"
	"github.com/arloliu/helix/internal/metrics"
	"github.com/arloliu/helix/types"
)

// NewNATSWorker creates a worker that processes messages from a NATSReplayer.
//
// Parameters:
//   - replayer: The NATS replayer to consume from
//   - execute: Function to execute replay payloads
//   - opts: Optional configuration options
//
// Returns:
//   - *Worker: A new worker instance
func NewNATSWorker(replayer *NATSReplayer, execute ExecuteFunc, opts ...WorkerOption) *Worker {
	config := DefaultWorkerConfig()
	for _, opt := range opts {
		opt(&config)
	}

	// Ensure metrics is never nil
	if config.Metrics == nil {
		config.Metrics = metrics.NewNopMetrics()
	}

	// Ensure logger is never nil
	if config.Logger == nil {
		config.Logger = logging.NewNopLogger()
	}

	return &Worker{
		config:       config,
		execute:      execute,
		stopCh:       make(chan struct{}),
		natsReplayer: replayer,
	}
}

// processNATSCluster processes messages for a specific cluster from NATSReplayer.
func (w *Worker) processNATSCluster(cluster types.ClusterID) {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msgs, err := w.natsReplayer.Dequeue(ctx, cluster, w.config.BatchSize)
		cancel()

		if err != nil {
			w.config.Logger.Error("failed to dequeue replay messages",
				"cluster", w.clusterName(cluster),
				"error", err.Error(),
			)
			// Wait before retrying
			select {
			case <-w.stopCh:
				return
			case <-time.After(w.config.PollInterval):
				continue
			}
		}

		if len(msgs) == 0 {
			// No messages, wait before polling again
			select {
			case <-w.stopCh:
				return
			case <-time.After(w.config.PollInterval):
				continue
			}
		}

		// Process each message
		for _, msg := range msgs {
			select {
			case <-w.stopCh:
				// Nak remaining messages for redelivery
				_ = msg.Nak()

				return
			default:
			}

			start := time.Now()
			err := w.executeOnce(msg.Payload)
			elapsed := time.Since(start).Seconds()

			if err != nil {
				// Nak for redelivery (NATS handles retry count via MaxDeliver)
				_ = msg.Nak()
				w.config.Metrics.IncReplayError(msg.Payload.TargetCluster)
				w.config.Metrics.ObserveReplayDuration(msg.Payload.TargetCluster, elapsed)
				w.config.Logger.Warn("replay execution failed, will retry",
					"cluster", w.clusterName(msg.Payload.TargetCluster),
					"error", err.Error(),
				)
				if w.config.OnError != nil {
					w.config.OnError(msg.Payload, err, 1)
				}
			} else {
				// Ack on success
				_ = msg.Ack()
				w.config.Metrics.IncReplaySuccess(msg.Payload.TargetCluster)
				w.config.Metrics.ObserveReplayDuration(msg.Payload.TargetCluster, elapsed)
				if w.config.OnSuccess != nil {
					w.config.OnSuccess(msg.Payload)
				}
			}
		}
	}
}
