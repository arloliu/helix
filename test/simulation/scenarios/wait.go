package scenarios

import (
	"context"
	"time"
)

func waitUntil(ctx context.Context, timeout time.Duration, condition func() bool) error {
	if condition() {
		return nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return context.DeadlineExceeded
		case <-ticker.C:
			if condition() {
				return nil
			}
		}
	}
}
