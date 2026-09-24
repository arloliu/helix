package helix

import (
	"context"
	"sync"
)

// backgroundLoop owns the context and goroutines of one background component
// the client starts at construction and stops on Close.
// The zero value is a loop that was never started;
// stop and wait do nothing on it.
type backgroundLoop struct {
	// ctx is cancelled by stop.
	// The goroutines read it to learn they should return.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// start creates the loop's context and runs each fn in its own goroutine.
func (l *backgroundLoop) start(fns ...func()) {
	l.ctx, l.cancel = context.WithCancel(context.Background())
	for _, fn := range fns {
		l.wg.Go(fn)
	}
}

// stop cancels the loop's context without waiting for its goroutines.
func (l *backgroundLoop) stop() {
	if l.cancel != nil {
		l.cancel()
	}
}

// wait blocks until every goroutine start launched has returned.
func (l *backgroundLoop) wait() {
	l.wg.Wait()
}
