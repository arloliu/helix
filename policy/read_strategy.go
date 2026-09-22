// Package policy provides read and write strategies for Helix dual-cluster operations.
package policy

import (
	"context"
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix/types"
)

// defaultStickyReadCooldown is the failover cooldown [NewStickyRead] applies
// when [WithStickyReadCooldown] is not used, or its value is negative.
const defaultStickyReadCooldown = 5 * time.Minute

// StickyRead implements a sticky read strategy that routes reads to a preferred cluster.
//
// The preferred cluster is randomly selected at initialization and sticks to it
// to maximize cache hits. On failure, it can fail over to the secondary cluster.
// Cooldown only gates future state changes; it does not trigger passive probing
// back to a recovered cluster in the absence of another read failure.
//
// Within the cooldown the preferred cluster still moves when the current
// preferred fails and the other cluster is known good: it has reported a
// success (OnSuccess) since its own last failure, for example through the
// per-request failover that ran on it. Two clusters that keep failing in
// turn therefore do not oscillate, while a preferred that is hard down after
// a single blip on the other is abandoned on the second failed read.
//
// [StickyRead.SetPreferred] and [StickyRead.Reset] let an operator move the
// preference by hand.
type StickyRead struct {
	preferred        atomic.Value // types.ClusterID
	initial          types.ClusterID
	mu               sync.Mutex // guards lastFailoverTime and every preference transition
	lastFailoverTime time.Time
	failoverCooldown time.Duration
	// knownBad marks a cluster that failed and has not succeeded since;
	// index 0 is cluster A and index 1 is cluster B.
	knownBad [2]atomic.Bool

	// invalidPreferredCluster and invalidCooldown record that some
	// [WithPreferredCluster] or [WithStickyReadCooldown] call was given an
	// invalid value, for [validateStickyRead] alone.
	// Setting either flag never touches initial or failoverCooldown, so
	// [NewStickyRead] keeps its legacy behavior exactly: an invalid call
	// is a no-op, and a valid call at any other position still takes effect.
	invalidPreferredCluster bool
	invalidCooldown         bool

	route routeReporter
}

// Read-route reasons carried by [types.EventReadRouteChanged].
const (
	routeReasonFailover  = "failover"
	routeReasonKnownGood = "alternative known good"
	routeReasonManual    = "manual"
	routeReasonRecovered = "recovered"
)

// routeReporter carries a read strategy's preference to the gauge and the
// event stream.
// A transition enqueues its event while the strategy's transition mutex is
// held, so event order equals state order; the gauge is written after the
// mutex is released.
type routeReporter struct {
	preferred  func() types.ClusterID // the strategy's current preference, lock-free
	gauge      atomic.Pointer[types.ReadRouteMetrics]
	configured atomic.Bool
	events     eventOutbox
	reporting  atomic.Bool // one goroutine at a time writes the gauge
	dirty      atomic.Bool // a transition happened since the last write
}

// recordLocked enqueues the event for a transition.
// The caller holds the strategy's transition mutex and calls report after
// releasing it.
func (r *routeReporter) recordLocked(from, to types.ClusterID, reason string) {
	r.events.enqueue(types.ClusterEvent{
		Kind:        types.EventReadRouteChanged,
		Cluster:     to,
		FromCluster: from,
		ToCluster:   to,
		Reason:      reason,
	})
}

// report writes the current preference to the gauge and drains the outbox.
// The caller has released the transition mutex.
// The goroutine holding the reporting flag rereads the preference after
// each write and writes again while transitions keep arriving; every other
// caller marks the state dirty and returns, trusting the holder.
// A collector that blocks therefore holds only the holder, and a collector
// that calls back into the strategy enqueues and returns.
func (r *routeReporter) report() {
	if gauge := r.gauge.Load(); gauge != nil {
		r.dirty.Store(true)
		for r.dirty.Load() && r.reporting.CompareAndSwap(false, true) {
			r.dirty.Store(false)
			preferred := r.preferred()
			(*gauge).SetReadPreferred(types.ClusterA, preferred == types.ClusterA)
			(*gauge).SetReadPreferred(types.ClusterB, preferred == types.ClusterB)
			r.reporting.Store(false)
		}
	}
	r.events.drain()
}

// setMetrics installs the collector and publishes the current preference
// through it.
func (r *routeReporter) setMetrics(m types.MetricsCollector) {
	if m == nil {
		return
	}
	r.configured.Store(true)
	if gauge, ok := m.(types.ReadRouteMetrics); ok {
		r.gauge.Store(&gauge)
		r.report()
	}
}

// otherCluster returns the sibling of a known cluster.
func otherCluster(cluster types.ClusterID) types.ClusterID {
	if cluster == types.ClusterA {
		return types.ClusterB
	}

	return types.ClusterA
}

// StickyReadOption configures a StickyRead strategy.
type StickyReadOption func(*StickyRead)

// WithStickyReadCooldown sets the cooldown period after a failover.
//
// A negative value is ignored by [NewStickyRead], leaving failoverCooldown
// at whatever it already was — the default, or an earlier valid call's
// value.
// Use [NewStickyReadChecked] to reject a negative value instead.
//
// Parameters:
//   - d: Duration to wait before allowing another failover
//
// Returns:
//   - StickyReadOption: Configuration option
func WithStickyReadCooldown(d time.Duration) StickyReadOption {
	return func(s *StickyRead) {
		if d < 0 {
			s.invalidCooldown = true

			return
		}
		s.failoverCooldown = d
	}
}

// WithPreferredCluster sets the initial preferred cluster.
//
// An unknown cluster is ignored by [NewStickyRead], leaving initial at
// whatever it already was — the random draw, or an earlier valid call's
// cluster.
// Use [NewStickyReadChecked] to reject an unknown cluster instead.
//
// Parameters:
//   - cluster: The cluster to prefer initially
//
// Returns:
//   - StickyReadOption: Configuration option
func WithPreferredCluster(cluster types.ClusterID) StickyReadOption {
	return func(s *StickyRead) {
		if !isKnownCluster(cluster) {
			s.invalidPreferredCluster = true

			return
		}
		s.initial = cluster
	}
}

// NewStickyRead creates a new StickyRead strategy.
//
// By default, the preferred cluster is randomly selected (50/50 between A and B)
// and the failover cooldown is 5 minutes.
// An unknown [WithPreferredCluster] cluster or a negative
// [WithStickyReadCooldown] is ignored: that call is a no-op, leaving
// whichever value already applies — the default, or an earlier valid call's
// value — in place, and a valid call at any other position still applies.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *StickyRead: A new sticky read strategy
//
// For production configuration that should fail fast on invalid option values,
// use [NewStickyReadChecked].
func NewStickyRead(opts ...StickyReadOption) *StickyRead {
	s := newStickyReadWithDefaults()
	applyStickyReadOptions(s, opts...)
	s.preferred.Store(s.initial)

	return s
}

// NewStickyReadChecked creates a new StickyRead strategy and returns a
// validation error when any option value is invalid.
//
// An unknown [WithPreferredCluster] cluster or a negative
// [WithStickyReadCooldown] is rejected even if a later call passes a valid
// value: a configuration that contains an invalid value is a bug, and this
// constructor exists to fail fast on it rather than silently drop it.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *StickyRead: A new sticky read strategy
//   - error: Joined [types.OptionError] values when one or more options are invalid
func NewStickyReadChecked(opts ...StickyReadOption) (*StickyRead, error) {
	s := newStickyReadWithDefaults()
	applyStickyReadOptions(s, opts...)
	if err := validateStickyRead(s); err != nil {
		return nil, err
	}
	s.preferred.Store(s.initial)

	return s, nil
}

// newStickyReadWithDefaults builds a StickyRead with its random initial
// cluster and default cooldown already set, before any option runs.
func newStickyReadWithDefaults() *StickyRead {
	s := &StickyRead{
		failoverCooldown: defaultStickyReadCooldown,
	}
	s.route.preferred = s.Preferred
	s.initial = randomCluster()

	return s
}

// randomCluster picks ClusterA or ClusterB with even odds, using crypto/rand
// for secure randomness.
func randomCluster() types.ClusterID {
	n, err := rand.Int(rand.Reader, big.NewInt(2))
	if err != nil || n.Int64() == 0 {
		return types.ClusterA
	}

	return types.ClusterB
}

func applyStickyReadOptions(s *StickyRead, opts ...StickyReadOption) {
	for _, opt := range opts {
		opt(s)
	}
}

// validateStickyRead reports every invalid option value applied to s, even
// one a later valid call for the same option superseded.
func validateStickyRead(s *StickyRead) error {
	errList := make([]error, 0, 2)

	if s.invalidPreferredCluster {
		errList = append(errList, optionErrUnknownCluster(stickyReadComponent, "WithPreferredCluster"))
	}
	if s.invalidCooldown {
		errList = append(errList, optionErrNonNegativeDuration(stickyReadComponent, "WithStickyReadCooldown"))
	}

	return joinValidationErrors(errList)
}

// knownBadSlot returns the known-bad mark of a cluster, or nil for an
// unknown cluster.
func (s *StickyRead) knownBadSlot(cluster types.ClusterID) *atomic.Bool {
	switch cluster {
	case types.ClusterA:
		return &s.knownBad[0]
	case types.ClusterB:
		return &s.knownBad[1]
	default:
		return nil
	}
}

// Select returns the preferred cluster for reading.
//
// Parameters:
//   - ctx: Context (unused but required by interface)
//
// Returns:
//   - types.ClusterID: The preferred cluster
func (s *StickyRead) Select(_ context.Context) types.ClusterID {
	if v, ok := s.preferred.Load().(types.ClusterID); ok {
		return v
	}

	return types.ClusterA
}

// OnSuccess is called when a read succeeds.
//
// Parameters:
//   - cluster: The cluster that succeeded; its known-bad mark is cleared
func (s *StickyRead) OnSuccess(cluster types.ClusterID) {
	// Store only on the transition so the steady state stays read-only.
	if bad := s.knownBadSlot(cluster); bad != nil && bad.Load() {
		bad.Store(false)
	}
}

// OnFailure handles read failures and determines failover.
//
// If the failed cluster is the preferred one, returns the alternative cluster
// for failover. When cooldown has passed, the preferred cluster is also
// switched. When cooldown is still active, the preferred cluster is switched
// only if the alternative is known good (see the type documentation);
// otherwise the alternative is returned for the current request but preferred
// is not changed — this prevents reads from failing entirely while still
// avoiding rapid preferred-cluster oscillation.
// Cooldown expiry alone does not change the preferred cluster; a later failure
// on the current preferred cluster is still required to switch back.
//
// Parameters:
//   - cluster: The cluster that failed
//   - err: The error (unused)
//
// Returns:
//   - types.ClusterID: Alternative cluster to try
//   - bool: true if failover should be attempted
func (s *StickyRead) OnFailure(cluster types.ClusterID, _ error) (types.ClusterID, bool) {
	if !isKnownCluster(cluster) {
		return "", false
	}
	s.knownBadSlot(cluster).Store(true)

	// Only failover if the preferred cluster failed
	if cluster != s.Preferred() {
		return "", false
	}
	alternative := otherCluster(cluster)

	// Within the cooldown the preference moves only when the alternative is
	// known good; otherwise the alternative serves this request alone, so
	// reads still succeed while two flapping clusters cannot swap the
	// preference back and forth.
	alternativeBad := s.knownBadSlot(alternative).Load()
	now := time.Now()
	s.mu.Lock()
	inCooldown := now.Sub(s.lastFailoverTime) < s.failoverCooldown
	reason := routeReasonFailover
	if inCooldown {
		reason = routeReasonKnownGood
	}
	moved := (!inCooldown || !alternativeBad) && s.settleLocked(alternative, reason)
	if moved {
		s.lastFailoverTime = now
	}
	s.mu.Unlock()
	if moved {
		s.route.report()
	}

	return alternative, true
}

// settleLocked moves the preference to cluster and records the transition,
// or reports false when cluster is already preferred (a concurrent caller
// moved it first, or the operator asked for the current one).
// The caller holds s.mu and reports after releasing it.
func (s *StickyRead) settleLocked(cluster types.ClusterID, reason string) bool {
	from := s.Preferred()
	if from == cluster {
		return false
	}
	s.preferred.Store(cluster)
	s.route.recordLocked(from, cluster, reason)

	return true
}

// SetPreferred moves the preferred cluster by hand and restarts the
// cooldown, for an operator who knows which cluster should serve reads.
// An unknown cluster is ignored.
//
// Parameters:
//   - cluster: The cluster to prefer from now on
func (s *StickyRead) SetPreferred(cluster types.ClusterID) {
	if !isKnownCluster(cluster) {
		return
	}
	s.mu.Lock()
	s.lastFailoverTime = time.Now()
	moved := s.settleLocked(cluster, routeReasonManual)
	s.mu.Unlock()
	if moved {
		s.route.report()
	}
}

// Reset returns the preference to the cluster chosen at construction (by
// [WithPreferredCluster] or at random), clears the cooldown, and forgets
// which clusters are known bad.
func (s *StickyRead) Reset() {
	s.mu.Lock()
	s.lastFailoverTime = time.Time{}
	for i := range s.knownBad {
		s.knownBad[i].Store(false)
	}
	moved := s.settleLocked(s.initial, routeReasonManual)
	s.mu.Unlock()
	if moved {
		s.route.report()
	}
}

// MetricsConfigured reports whether SetMetrics installed a collector.
func (s *StickyRead) MetricsConfigured() bool { return s.route.configured.Load() }

// SetMetrics installs the collector the preference gauge is written to and
// publishes the current preference; [helix.NewCQLClient] calls it with the
// client's collector. A nil collector is ignored.
func (s *StickyRead) SetMetrics(m types.MetricsCollector) { s.route.setMetrics(m) }

// SetEventEmitter installs the emitter [types.EventReadRouteChanged] events
// are delivered to; nil disables emission. [helix.NewCQLClient] calls it
// with the client's dispatcher.
func (s *StickyRead) SetEventEmitter(em types.ClusterEventEmitter) { s.route.events.setEmitter(em) }

// Preferred returns the current preferred cluster.
//
// Returns:
//   - types.ClusterID: The current preferred cluster
func (s *StickyRead) Preferred() types.ClusterID {
	if v, ok := s.preferred.Load().(types.ClusterID); ok {
		return v
	}

	return types.ClusterA
}

// PrimaryOnlyRead implements a read strategy that always reads from Cluster A.
//
// Cluster B is only used for writes and as a failover target. Once Cluster A
// fails, reads are redirected to Cluster B until one of:
//   - [PrimaryOnlyRead.Reset] is called manually, or
//   - The optional recovery timeout (set via [WithPrimaryOnlyRecoveryTimeout])
//     elapses, after which Select hands ClusterA to one caller as the probe, or
//   - Cluster B itself fails while in the failed-over state, at which point
//     OnFailure returns ClusterA as a probe; if that probe succeeds,
//     [PrimaryOnlyRead.OnSuccess] resets the failover state.
//
// Without a recovery timeout, failover is permanent until Reset is called or
// Cluster B also fails — if only ClusterA recovers while B stays healthy, the
// client stays on ClusterB indefinitely.
type PrimaryOnlyRead struct {
	failedOver atomic.Bool
	// nextProbeAt is the Unix nano after which one caller may be handed
	// ClusterA as the recovery probe; 0 = not failed over.
	// The Select that wins the probe re-arms it, so an unreported probe
	// expires after another recovery timeout.
	nextProbeAt     atomic.Int64
	recoveryTimeout time.Duration

	// mu serializes the transitions so their events are enqueued in state
	// order; failedOver is written under it and read lock-free by Select.
	mu    sync.Mutex
	route routeReporter
}

// PrimaryOnlyReadOption configures a PrimaryOnlyRead strategy.
type PrimaryOnlyReadOption func(*PrimaryOnlyRead)

// WithPrimaryOnlyRecoveryTimeout sets the duration after which a failed-over
// PrimaryOnlyRead will automatically attempt to return reads to ClusterA.
//
// When the timeout elapses, one Select call returns ClusterA as the probe;
// the other callers keep reading ClusterB until that probe reports, and a
// probe that never reports expires after another timeout.
// If the probe read succeeds (OnSuccess called), the strategy returns to
// ClusterA. If it fails again (OnFailure called), the failover timer resets.
//
// A zero or negative value disables auto-recovery (default: disabled).
// [NewPrimaryOnlyReadChecked] rejects a negative value instead; zero stays
// valid there too.
//
// Parameters:
//   - d: Recovery timeout duration
//
// Returns:
//   - PrimaryOnlyReadOption: Configuration option
func WithPrimaryOnlyRecoveryTimeout(d time.Duration) PrimaryOnlyReadOption {
	return func(p *PrimaryOnlyRead) {
		p.recoveryTimeout = d
	}
}

// NewPrimaryOnlyRead creates a new PrimaryOnlyRead strategy.
//
// A negative [WithPrimaryOnlyRecoveryTimeout] is kept as-is, which disables
// auto-recovery the same way zero does.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *PrimaryOnlyRead: A new primary-only read strategy
//
// For production configuration that should fail fast on invalid option values,
// use [NewPrimaryOnlyReadChecked].
func NewPrimaryOnlyRead(opts ...PrimaryOnlyReadOption) *PrimaryOnlyRead {
	p := newPrimaryOnlyReadWithDefaults()
	applyPrimaryOnlyReadOptions(p, opts...)

	return p
}

// NewPrimaryOnlyReadChecked creates a new PrimaryOnlyRead strategy and
// returns a validation error when any option value is invalid.
//
// A negative [WithPrimaryOnlyRecoveryTimeout] is rejected; zero remains
// valid and keeps disabling auto-recovery.
//
// Parameters:
//   - opts: Optional configuration options
//
// Returns:
//   - *PrimaryOnlyRead: A new primary-only read strategy
//   - error: Joined [types.OptionError] values when one or more options are invalid
func NewPrimaryOnlyReadChecked(opts ...PrimaryOnlyReadOption) (*PrimaryOnlyRead, error) {
	p := newPrimaryOnlyReadWithDefaults()
	applyPrimaryOnlyReadOptions(p, opts...)
	if err := validatePrimaryOnlyRead(p); err != nil {
		return nil, err
	}

	return p, nil
}

// newPrimaryOnlyReadWithDefaults builds a PrimaryOnlyRead with its route
// reporter wired up, before any option runs.
func newPrimaryOnlyReadWithDefaults() *PrimaryOnlyRead {
	p := &PrimaryOnlyRead{}
	p.route.preferred = p.preferred

	return p
}

func applyPrimaryOnlyReadOptions(p *PrimaryOnlyRead, opts ...PrimaryOnlyReadOption) {
	for _, opt := range opts {
		opt(p)
	}
}

// validatePrimaryOnlyRead reports every invalid option value applied to p.
func validatePrimaryOnlyRead(p *PrimaryOnlyRead) error {
	errList := make([]error, 0, 1)

	if p.recoveryTimeout < 0 {
		errList = append(errList, optionErrNonNegativeDuration(primaryOnlyReadComponent, "WithPrimaryOnlyRecoveryTimeout"))
	}

	return joinValidationErrors(errList)
}

// Select returns ClusterA unless it has failed over.
//
// If a recovery timeout is configured and has elapsed since the last failover,
// ClusterA is returned as a probe — allowing the caller to re-evaluate whether
// ClusterA has recovered.
//
// Parameters:
//   - ctx: Context (unused)
//
// Returns:
//   - types.ClusterID: ClusterA or ClusterB if failed over
func (p *PrimaryOnlyRead) Select(_ context.Context) types.ClusterID {
	if !p.failedOver.Load() {
		return types.ClusterA
	}
	// Auto-recovery: once the recovery timeout has elapsed, hand cluster A
	// to exactly one caller as the probe; everyone else keeps reading B
	// until that probe reports. A probe whose caller never reports (its
	// context ended) expires after another recovery timeout.
	if p.recoveryTimeout > 0 {
		nowNs := time.Now().UnixNano()
		next := p.nextProbeAt.Load()
		if next > 0 && nowNs >= next && p.nextProbeAt.CompareAndSwap(next, nowNs+int64(p.recoveryTimeout)) {
			return types.ClusterA
		}
	}

	return types.ClusterB
}

// OnSuccess is called when a read succeeds.
//
// If ClusterA succeeds while in failed-over state, the strategy resets back
// to ClusterA — completing auto-recovery.
//
// Parameters:
//   - cluster: The cluster that succeeded
func (p *PrimaryOnlyRead) OnSuccess(cluster types.ClusterID) {
	if cluster == types.ClusterA && p.failedOver.Load() {
		p.restore(routeReasonRecovered)
	}
}

// OnFailure handles read failures.
//
// If ClusterA fails and has not yet failed over, routes to ClusterB.
// If ClusterA fails during an auto-recovery probe, resets the recovery timer
// so another probe will only occur after another full recovery timeout.
// If ClusterB fails while in the failed-over state, returns ClusterA as a
// probe without resetting state. If the caller's retry on ClusterA succeeds,
// OnSuccess clears the failover flag. If ClusterA is also down, the state
// stays failed-over to B, avoiding request-level A/B flipping.
//
// Parameters:
//   - cluster: The cluster that failed
//   - err: The error
//
// Returns:
//   - types.ClusterID: Alternative cluster to try, or empty if no failover
//   - bool: true if failover should be attempted
func (p *PrimaryOnlyRead) OnFailure(cluster types.ClusterID, _ error) (types.ClusterID, bool) {
	if cluster == types.ClusterA {
		p.transition(true, time.Now().Add(p.recoveryTimeout).UnixNano(), routeReasonFailover)

		return types.ClusterB, true
	}

	// ClusterB failed while in the failed-over state — probe ClusterA.
	// This handles the scenario where A failed, we switched to B, A recovered,
	// and now B fails. Without this, reads would fail entirely even though A
	// may be available again.
	//
	// State is NOT reset here. If the probe succeeds, the caller invokes
	// OnSuccess(ClusterA) which resets the failover state. If A is also
	// down, both attempts fail (DualClusterError) and the next Select() still
	// returns ClusterB — avoiding request-level A/B flipping.
	//
	// Guard: only probe A when genuinely failed-over. The caller may route to B
	// due to drain-state override even when not failed over; in that case we
	// must not suggest A (the drain-excluded cluster) as an alternative.
	if cluster == types.ClusterB && p.failedOver.Load() {
		return types.ClusterA, true
	}

	return "", false
}

// Reset resets the failover state back to primary immediately.
func (p *PrimaryOnlyRead) Reset() {
	p.restore(routeReasonManual)
}

// restore returns reads to cluster A and reports the move with reason when
// the strategy was failed over.
func (p *PrimaryOnlyRead) restore(reason string) {
	p.transition(false, 0, reason)
}

// transition sets the failed-over state and the probe timer, and records
// the move with reason when the state changed.
func (p *PrimaryOnlyRead) transition(failedOver bool, nextProbeAt int64, reason string) {
	p.mu.Lock()
	moved := p.failedOver.Load() != failedOver
	if moved {
		p.failedOver.Store(failedOver)
	}
	p.nextProbeAt.Store(nextProbeAt)
	if moved {
		from, to := types.ClusterA, types.ClusterB
		if !failedOver {
			from, to = to, from
		}
		p.route.recordLocked(from, to, reason)
	}
	p.mu.Unlock()
	if moved {
		p.route.report()
	}
}

// preferred returns the cluster reads currently go to.
func (p *PrimaryOnlyRead) preferred() types.ClusterID {
	if p.failedOver.Load() {
		return types.ClusterB
	}

	return types.ClusterA
}

// MetricsConfigured reports whether SetMetrics installed a collector.
func (p *PrimaryOnlyRead) MetricsConfigured() bool { return p.route.configured.Load() }

// SetMetrics installs the collector the preference gauge is written to and
// publishes the current preference; [helix.NewCQLClient] calls it with the
// client's collector. A nil collector is ignored.
func (p *PrimaryOnlyRead) SetMetrics(m types.MetricsCollector) { p.route.setMetrics(m) }

// SetEventEmitter installs the emitter [types.EventReadRouteChanged] events
// are delivered to; nil disables emission. [helix.NewCQLClient] calls it
// with the client's dispatcher.
func (p *PrimaryOnlyRead) SetEventEmitter(em types.ClusterEventEmitter) {
	p.route.events.setEmitter(em)
}

// RoundRobinRead implements a read strategy that alternates between clusters.
//
// This provides even load distribution but lower cache efficiency.
type RoundRobinRead struct {
	counter atomic.Uint64
}

// NewRoundRobinRead creates a new RoundRobinRead strategy.
//
// Returns:
//   - *RoundRobinRead: A new round-robin read strategy
func NewRoundRobinRead() *RoundRobinRead {
	return &RoundRobinRead{}
}

// Select returns alternating clusters on each call, starting at ClusterA.
//
// Parameters:
//   - ctx: Context (unused)
//
// Returns:
//   - types.ClusterID: Alternating between ClusterA and ClusterB
func (r *RoundRobinRead) Select(_ context.Context) types.ClusterID {
	// Add returns the post-increment value, so the first caller sees 1;
	// subtract the increment to give the first read ClusterA.
	count := r.counter.Add(1) - 1
	if count%2 == 0 {
		return types.ClusterA
	}

	return types.ClusterB
}

// OnSuccess is called when a read succeeds.
//
// Parameters:
//   - cluster: The cluster that succeeded (unused)
func (r *RoundRobinRead) OnSuccess(_ types.ClusterID) {
	// Nothing to do
}

// OnFailure handles read failures.
//
// Parameters:
//   - cluster: The cluster that failed
//   - err: The error
//
// Returns:
//   - types.ClusterID: The other cluster
//   - bool: true (always attempt failover)
func (r *RoundRobinRead) OnFailure(cluster types.ClusterID, _ error) (types.ClusterID, bool) {
	if cluster == types.ClusterA {
		return types.ClusterB, true
	}
	if cluster != types.ClusterB {
		return "", false
	}
	return types.ClusterA, true
}
