package replay

import (
	"errors"
	"time"

	"github.com/arloliu/helix/types"
)

// defaultRetryWindow is how long the memory worker keeps retrying a payload
// under [RetryWhileRetained] before giving up.
// It matches the default NATS stream MaxAge so both backends offer the same
// survival window out of the box.
const defaultRetryWindow = 24 * time.Hour

// ReplayDisposition is what a worker does with a payload whose attempt failed.
//
// A [ReplayClassifier] maps the execution error to one of these values.
// The distinction only matters under [RetryWhileRetained]; the bounded policy
// counts every failed attempt the same way.
type ReplayDisposition int

const (
	// DispositionRetry is the default for errors the classifier does not
	// recognise: the payload is retried on the backoff schedule.
	// It never consumes the poison budget.
	DispositionRetry ReplayDisposition = iota

	// DispositionDefer means the target cluster could not be reached.
	// The payload is retried on the backoff schedule for as long as it is
	// retained.
	// It never consumes the poison budget.
	DispositionDefer

	// DispositionDeadLetter means the statement itself cannot be processed,
	// for example because its target cluster is unknown.
	// Each such attempt consumes one unit of the poison budget, MaxAttempts
	// on either backend.
	// When the budget is exhausted the payload is dropped through OnDrop.
	DispositionDeadLetter
)

// String returns the disposition name used in log output.
func (d ReplayDisposition) String() string {
	switch d {
	case DispositionRetry:
		return "retry"
	case DispositionDefer:
		return "defer"
	case DispositionDeadLetter:
		return "dead-letter"
	default:
		return "unknown"
	}
}

// ReplayRetryPolicy selects how a worker budgets failed replay attempts.
type ReplayRetryPolicy int

const (
	// RetryBounded gives a payload MaxAttempts attempts on the memory worker
	// or MaxDeliver deliveries on the NATS worker, then drops it through
	// OnDrop. It was the default before RetryWhileRetained existed and is
	// kept for callers that prefer a short, fixed retry buffer.
	// With default settings the memory worker's backoff totals about 1.5
	// seconds and the NATS worker redelivers without delay, so a payload only
	// survives an outage of a few seconds.
	RetryBounded ReplayRetryPolicy = iota

	// RetryWhileRetained is the default: a payload is retried for as long as
	// it is retained, the worker's RetryWindow on the memory backend, the
	// stream's MaxAge on the NATS backend.
	// Only [DispositionDeadLetter] attempts consume the attempt budget, which
	// is MaxAttempts on both backends; the NATS replayer's MaxDeliver is not
	// used.
	//
	// The memory worker holds the payload's queue slot for its whole
	// lifetime, so queue capacity bounds queued, executing, and waiting
	// payloads together and new admissions fail with
	// [types.ErrReplayQueueFull] when the backlog is full.
	// The NATS worker creates its consumers with unlimited deliveries and
	// asks the server to redeliver a failed message after the backoff delay.
	RetryWhileRetained
)

// valid reports whether p is one of the defined policies.
func (p ReplayRetryPolicy) valid() bool {
	return p == RetryBounded || p == RetryWhileRetained
}

// ReplayClassifier maps a replay execution error to a [ReplayDisposition].
//
// Unknown errors should map to [DispositionRetry]; the worker never
// dead-letters a payload the classifier did not explicitly reject.
type ReplayClassifier func(err error) ReplayDisposition

// DefaultReplayClassifier is the classifier a worker uses when none is set
// through [WithReplayClassifier].
//
// It relies on the typed sentinels the bundled adapters produce:
//   - [types.ErrClusterUnreachable] -> [DispositionDefer]
//   - [types.ErrInvalidCluster] -> [DispositionDeadLetter]
//   - anything else -> [DispositionRetry]
//
// Parameters:
//   - err: The error returned by the worker's ExecuteFunc
//
// Returns:
//   - ReplayDisposition: What the worker should do with the payload
func DefaultReplayClassifier(err error) ReplayDisposition {
	switch {
	case errors.Is(err, types.ErrClusterUnreachable):
		return DispositionDefer
	case errors.Is(err, types.ErrInvalidCluster):
		return DispositionDeadLetter
	default:
		return DispositionRetry
	}
}

// WithRetryPolicy selects the retry policy for a worker.
//
// The default is [RetryBounded].
// Choose [RetryWhileRetained] when the replay queue must survive cluster
// outages longer than a few seconds.
//
// Parameters:
//   - p: The policy to apply
//
// Returns:
//   - WorkerOption: Configuration option
func WithRetryPolicy(p ReplayRetryPolicy) WorkerOption {
	return func(c *WorkerConfig) {
		c.RetryPolicy = p
	}
}

// WithRetryWindow bounds how long the memory worker keeps retrying a payload
// under [RetryWhileRetained], measured from its first attempt.
//
// Memory only; the NATS backend is bounded by the stream's MaxAge instead.
// Default: 24 hours.
//
// Parameters:
//   - d: The window; values <= 0 fall back to the default
//
// Returns:
//   - WorkerOption: Configuration option
func WithRetryWindow(d time.Duration) WorkerOption {
	return func(c *WorkerConfig) {
		c.RetryWindow = d
	}
}

// WithReplayClassifier replaces [DefaultReplayClassifier] for a worker.
//
// Use it when a custom ExecuteFunc produces errors the default classifier
// does not recognise.
// A nil classifier keeps the default.
//
// Parameters:
//   - fn: The classifier to use
//
// Returns:
//   - WorkerOption: Configuration option
func WithReplayClassifier(fn ReplayClassifier) WorkerOption {
	return func(c *WorkerConfig) {
		if fn != nil {
			c.Classifier = fn
		}
	}
}
