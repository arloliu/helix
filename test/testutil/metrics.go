package testutil

import (
	"sync"
	"sync/atomic"

	"github.com/arloliu/helix/types"
)

// TestMetricsCollector is a test implementation of types.MetricsCollector
// that tracks method calls for assertion in integration tests.
type TestMetricsCollector struct {
	mu sync.RWMutex

	// Read operations
	ReadTotal      map[types.ClusterID]int64
	ReadErrors     map[types.ClusterID]int64
	ReadDuration   map[types.ClusterID][]float64
	ReadDivergence map[types.ClusterID]int64

	// Write operations
	WriteTotal    map[types.ClusterID]int64
	WriteErrors   map[types.ClusterID]int64
	WriteAsync    map[types.ClusterID]int64
	WriteDropped  map[types.ClusterID]int64
	WriteDuration map[types.ClusterID][]float64

	// Adaptive write transitions (optional types.AdaptiveWriteMetrics)
	WriteDegradedState map[types.ClusterID]bool
	WriteDegraded      map[types.ClusterID]int64
	WriteRecovered     map[types.ClusterID]int64

	// Failover
	FailoverTotal map[string]int64 // key: "from->to"

	// Read routing (optional types.ReadRouteMetrics)
	ReadPreferred map[types.ClusterID]bool

	// Circuit breaker
	CircuitBreakerState map[types.ClusterID]int
	CircuitBreakerTrips map[types.ClusterID]int64

	// Replay
	ReplayEnqueued   map[types.ClusterID]int64
	ReplaySuccess    map[types.ClusterID]int64
	ReplayErrors     map[types.ClusterID]int64
	ReplayDropped    map[types.ClusterID]int64
	ReplayQueueDepth map[types.ClusterID]int
	ReplayDuration   map[types.ClusterID][]float64

	// Replay backlog (optional types.ReplayBacklogMetrics)
	ReplayOldestAge     map[types.ClusterID]float64
	ReplayWorkerDropped map[types.ClusterID]map[string]int64

	// Replay stream (optional types.ReplayStreamMetrics)
	ReplayCorrupt    map[types.ClusterID]int64
	ReplayTermFailed map[types.ClusterID]int64
	ReplayEvicted    int64

	// Drain mode
	ClusterDraining  map[types.ClusterID]bool
	DrainModeEntered map[types.ClusterID]int64
	DrainModeExited  map[types.ClusterID]int64

	// Session refresh (optional types.SessionRefreshMetrics)
	SessionRefreshAttempts  map[types.ClusterID]int64
	SessionRefreshSuccesses map[types.ClusterID]int64
	SessionRefreshErrors    map[types.ClusterID]int64

	// Mirror replay (optional types.MirrorReplayMetrics)
	MirrorReplayDropped int64

	// Cluster events (optional types.ClusterEventMetrics)
	ClusterEventsDropped int64

	// Atomic counters for quick access
	totalReplayEnqueued atomic.Int64
	totalReplaySuccess  atomic.Int64
	totalFailovers      atomic.Int64
}

// Compile-time assertion that TestMetricsCollector implements types.MetricsCollector.
var _ types.MetricsCollector = (*TestMetricsCollector)(nil)

// NewTestMetricsCollector creates a new test metrics collector.
func NewTestMetricsCollector() *TestMetricsCollector {
	return &TestMetricsCollector{
		ReadTotal:               make(map[types.ClusterID]int64),
		ReadErrors:              make(map[types.ClusterID]int64),
		ReadDuration:            make(map[types.ClusterID][]float64),
		ReadDivergence:          make(map[types.ClusterID]int64),
		WriteTotal:              make(map[types.ClusterID]int64),
		WriteErrors:             make(map[types.ClusterID]int64),
		WriteAsync:              make(map[types.ClusterID]int64),
		WriteDropped:            make(map[types.ClusterID]int64),
		WriteDuration:           make(map[types.ClusterID][]float64),
		WriteDegradedState:      make(map[types.ClusterID]bool),
		ReadPreferred:           make(map[types.ClusterID]bool),
		WriteDegraded:           make(map[types.ClusterID]int64),
		WriteRecovered:          make(map[types.ClusterID]int64),
		FailoverTotal:           make(map[string]int64),
		CircuitBreakerState:     make(map[types.ClusterID]int),
		CircuitBreakerTrips:     make(map[types.ClusterID]int64),
		ReplayEnqueued:          make(map[types.ClusterID]int64),
		ReplaySuccess:           make(map[types.ClusterID]int64),
		ReplayErrors:            make(map[types.ClusterID]int64),
		ReplayDropped:           make(map[types.ClusterID]int64),
		ReplayQueueDepth:        make(map[types.ClusterID]int),
		ReplayOldestAge:         make(map[types.ClusterID]float64),
		ReplayWorkerDropped:     make(map[types.ClusterID]map[string]int64),
		ReplayCorrupt:           make(map[types.ClusterID]int64),
		ReplayTermFailed:        make(map[types.ClusterID]int64),
		ReplayDuration:          make(map[types.ClusterID][]float64),
		ClusterDraining:         make(map[types.ClusterID]bool),
		DrainModeEntered:        make(map[types.ClusterID]int64),
		DrainModeExited:         make(map[types.ClusterID]int64),
		SessionRefreshAttempts:  make(map[types.ClusterID]int64),
		SessionRefreshSuccesses: make(map[types.ClusterID]int64),
		SessionRefreshErrors:    make(map[types.ClusterID]int64),
	}
}

// ----------------------
// Read Operations
// ----------------------

func (m *TestMetricsCollector) IncReadTotal(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReadTotal[cluster]++
}

func (m *TestMetricsCollector) IncReadError(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReadErrors[cluster]++
}

func (m *TestMetricsCollector) ObserveReadDuration(cluster types.ClusterID, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReadDuration[cluster] = append(m.ReadDuration[cluster], seconds)
}

func (m *TestMetricsCollector) IncReadDivergence(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReadDivergence[cluster]++
}

// ----------------------
// Write Operations
// ----------------------

func (m *TestMetricsCollector) IncWriteTotal(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WriteTotal[cluster]++
}

func (m *TestMetricsCollector) IncWriteError(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WriteErrors[cluster]++
}

func (m *TestMetricsCollector) IncWriteAsync(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WriteAsync[cluster]++
}

func (m *TestMetricsCollector) IncWriteDropped(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WriteDropped[cluster]++
}

func (m *TestMetricsCollector) ObserveWriteDuration(cluster types.ClusterID, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WriteDuration[cluster] = append(m.WriteDuration[cluster], seconds)
}

// ----------------------
// Adaptive Write Transitions (optional types.AdaptiveWriteMetrics)
// ----------------------

// IncReplayCorrupt implements the optional types.ReplayStreamMetrics.
func (m *TestMetricsCollector) IncReplayCorrupt(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplayCorrupt[cluster]++
}

// IncReplayTermFailed implements the optional types.ReplayStreamMetrics.
func (m *TestMetricsCollector) IncReplayTermFailed(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplayTermFailed[cluster]++
}

// AddReplayEvicted implements the optional types.ReplayStreamMetrics.
func (m *TestMetricsCollector) AddReplayEvicted(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplayEvicted += int64(n)
}

// SetReadPreferred implements the optional types.ReadRouteMetrics.
func (m *TestMetricsCollector) SetReadPreferred(cluster types.ClusterID, preferred bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReadPreferred[cluster] = preferred
}

func (m *TestMetricsCollector) SetWriteDegraded(cluster types.ClusterID, degraded bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WriteDegradedState[cluster] = degraded
}

func (m *TestMetricsCollector) IncWriteDegraded(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WriteDegraded[cluster]++
}

func (m *TestMetricsCollector) IncWriteRecovered(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WriteRecovered[cluster]++
}

// GetWriteDegradedState returns the last recorded degraded-state gauge
// value for the given cluster.
// GetReplayCorrupt returns the corrupt-message count for a cluster.
func (m *TestMetricsCollector) GetReplayCorrupt(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.ReplayCorrupt[cluster]
}

// GetReplayTermFailed returns the refused-Term count for a cluster.
func (m *TestMetricsCollector) GetReplayTermFailed(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.ReplayTermFailed[cluster]
}

func (m *TestMetricsCollector) GetWriteDegradedState(cluster types.ClusterID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.WriteDegradedState[cluster]
}

// GetWriteDegradedTransitions returns the healthy-to-degraded transition
// count for the given cluster.
func (m *TestMetricsCollector) GetWriteDegradedTransitions(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.WriteDegraded[cluster]
}

// GetWriteRecoveredTransitions returns the degraded-to-healthy transition
// count for the given cluster.
func (m *TestMetricsCollector) GetWriteRecoveredTransitions(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.WriteRecovered[cluster]
}

// Compile-time assertion that TestMetricsCollector implements the
// optional types.AdaptiveWriteMetrics interface.
var _ types.AdaptiveWriteMetrics = (*TestMetricsCollector)(nil)

// ----------------------
// Failover
// ----------------------

func (m *TestMetricsCollector) IncFailoverTotal(fromCluster, toCluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := string(fromCluster) + "->" + string(toCluster)
	m.FailoverTotal[key]++
	m.totalFailovers.Add(1)
}

// ----------------------
// Circuit Breaker
// ----------------------

func (m *TestMetricsCollector) SetCircuitBreakerState(cluster types.ClusterID, state int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CircuitBreakerState[cluster] = state
}

func (m *TestMetricsCollector) IncCircuitBreakerTrip(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CircuitBreakerTrips[cluster]++
}

// ----------------------
// Replay Queue
// ----------------------

func (m *TestMetricsCollector) IncReplayEnqueued(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplayEnqueued[cluster]++
	m.totalReplayEnqueued.Add(1)
}

func (m *TestMetricsCollector) IncReplaySuccess(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplaySuccess[cluster]++
	m.totalReplaySuccess.Add(1)
}

func (m *TestMetricsCollector) IncReplayError(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplayErrors[cluster]++
}

func (m *TestMetricsCollector) IncReplayDropped(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplayDropped[cluster]++
}

func (m *TestMetricsCollector) SetReplayQueueDepth(cluster types.ClusterID, depth int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplayQueueDepth[cluster] = depth
}

func (m *TestMetricsCollector) ObserveReplayDuration(cluster types.ClusterID, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplayDuration[cluster] = append(m.ReplayDuration[cluster], seconds)
}

// ----------------------
// Replay backlog (optional types.ReplayBacklogMetrics)
// ----------------------

func (m *TestMetricsCollector) SetReplayOldestAge(cluster types.ClusterID, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplayOldestAge[cluster] = seconds
}

func (m *TestMetricsCollector) IncReplayWorkerDropped(cluster types.ClusterID, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ReplayWorkerDropped[cluster] == nil {
		m.ReplayWorkerDropped[cluster] = make(map[string]int64)
	}
	m.ReplayWorkerDropped[cluster][reason]++
}

// GetReplayQueueDepth returns the last queue depth reported for a cluster.
func (m *TestMetricsCollector) GetReplayQueueDepth(cluster types.ClusterID) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.ReplayQueueDepth[cluster]
}

// GetReplayWorkerDropped returns the worker drop count for one cluster and
// reason.
func (m *TestMetricsCollector) GetReplayWorkerDropped(cluster types.ClusterID, reason string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.ReplayWorkerDropped[cluster][reason]
}

// GetReplayOldestAge returns the last backlog-head age reported for a cluster.
func (m *TestMetricsCollector) GetReplayOldestAge(cluster types.ClusterID) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.ReplayOldestAge[cluster]
}

// Compile-time assertion that TestMetricsCollector implements the
// optional types.ReplayBacklogMetrics interface.
var _ types.ReplayBacklogMetrics = (*TestMetricsCollector)(nil)

// ----------------------
// Cluster Health
// ----------------------

func (m *TestMetricsCollector) SetClusterDraining(cluster types.ClusterID, draining bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ClusterDraining[cluster] = draining
}

func (m *TestMetricsCollector) IncDrainModeEntered(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DrainModeEntered[cluster]++
}

func (m *TestMetricsCollector) IncDrainModeExited(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DrainModeExited[cluster]++
}

// ----------------------
// Session Refresh (optional types.SessionRefreshMetrics)
// ----------------------

func (m *TestMetricsCollector) IncSessionRefreshAttempt(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SessionRefreshAttempts[cluster]++
}

func (m *TestMetricsCollector) IncSessionRefreshSuccess(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SessionRefreshSuccesses[cluster]++
}

func (m *TestMetricsCollector) IncSessionRefreshError(cluster types.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SessionRefreshErrors[cluster]++
}

// GetSessionRefreshAttempts returns the cumulative auto-refresh attempt
// count for the given cluster.
func (m *TestMetricsCollector) GetSessionRefreshAttempts(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.SessionRefreshAttempts[cluster]
}

// GetSessionRefreshSuccesses returns the cumulative auto-refresh success
// count for the given cluster.
func (m *TestMetricsCollector) GetSessionRefreshSuccesses(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.SessionRefreshSuccesses[cluster]
}

// GetSessionRefreshErrors returns the cumulative auto-refresh error
// count for the given cluster.
func (m *TestMetricsCollector) GetSessionRefreshErrors(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.SessionRefreshErrors[cluster]
}

// Compile-time assertion that TestMetricsCollector implements the
// optional types.SessionRefreshMetrics interface.
var _ types.SessionRefreshMetrics = (*TestMetricsCollector)(nil)

// ----------------------
// Mirror Replay (optional types.MirrorReplayMetrics)
// ----------------------

func (m *TestMetricsCollector) IncMirrorReplayDropped() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.MirrorReplayDropped++
}

// GetMirrorReplayDropped returns the cumulative count of failed mirror
// writes that could not be enqueued for mirror replay.
func (m *TestMetricsCollector) GetMirrorReplayDropped() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.MirrorReplayDropped
}

// Compile-time assertion that TestMetricsCollector implements the
// optional types.MirrorReplayMetrics interface.
var _ types.MirrorReplayMetrics = (*TestMetricsCollector)(nil)

// ----------------------
// Cluster Events (optional types.ClusterEventMetrics)
// ----------------------

func (m *TestMetricsCollector) AddClusterEventsDropped(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ClusterEventsDropped += int64(n)
}

// GetClusterEventsDropped returns the cumulative count of cluster events
// dropped by the event dispatcher.
func (m *TestMetricsCollector) GetClusterEventsDropped() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.ClusterEventsDropped
}

// Compile-time assertion that TestMetricsCollector implements the
// optional types.ClusterEventMetrics interface.
var _ types.ClusterEventMetrics = (*TestMetricsCollector)(nil)

// ----------------------
// Test Helpers
// ----------------------

// GetReplayEnqueued returns the total replay enqueued count for a cluster.
func (m *TestMetricsCollector) GetReplayEnqueued(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ReplayEnqueued[cluster]
}

// GetReplaySuccess returns the total replay success count for a cluster.
func (m *TestMetricsCollector) GetReplaySuccess(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ReplaySuccess[cluster]
}

// GetReplayDropped returns the total replay dropped count for a cluster.
func (m *TestMetricsCollector) GetReplayDropped(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.ReplayDropped[cluster]
}

// GetTotalReplayEnqueued returns the total replay enqueued count across all clusters.
func (m *TestMetricsCollector) GetTotalReplayEnqueued() int64 {
	return m.totalReplayEnqueued.Load()
}

// GetTotalReplaySuccess returns the total replay success count across all clusters.
func (m *TestMetricsCollector) GetTotalReplaySuccess() int64 {
	return m.totalReplaySuccess.Load()
}

// GetTotalFailovers returns the total failover count across all cluster pairs.
func (m *TestMetricsCollector) GetTotalFailovers() int64 {
	return m.totalFailovers.Load()
}

// GetFailoverCount returns the failover count from one cluster to another.
func (m *TestMetricsCollector) GetFailoverCount(fromCluster, toCluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := string(fromCluster) + "->" + string(toCluster)
	return m.FailoverTotal[key]
}

// GetWriteErrors returns the total write error count for a cluster.
func (m *TestMetricsCollector) GetWriteErrors(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.WriteErrors[cluster]
}

// GetWriteAsync returns the async write count for a cluster.
func (m *TestMetricsCollector) GetWriteAsync(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.WriteAsync[cluster]
}

// GetWriteDropped returns the dropped write count for a cluster.
func (m *TestMetricsCollector) GetWriteDropped(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.WriteDropped[cluster]
}

// GetReadErrors returns the total read error count for a cluster.
func (m *TestMetricsCollector) GetReadErrors(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ReadErrors[cluster]
}

// GetReadDivergence returns the read divergence count for a cluster.
func (m *TestMetricsCollector) GetReadDivergence(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ReadDivergence[cluster]
}

// Reset clears all collected metrics.
func (m *TestMetricsCollector) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ReadTotal = make(map[types.ClusterID]int64)
	m.ReadErrors = make(map[types.ClusterID]int64)
	m.ReadDuration = make(map[types.ClusterID][]float64)
	m.ReadDivergence = make(map[types.ClusterID]int64)
	m.WriteTotal = make(map[types.ClusterID]int64)
	m.WriteErrors = make(map[types.ClusterID]int64)
	m.WriteAsync = make(map[types.ClusterID]int64)
	m.WriteDropped = make(map[types.ClusterID]int64)
	m.WriteDuration = make(map[types.ClusterID][]float64)
	m.WriteDegradedState = make(map[types.ClusterID]bool)
	m.WriteDegraded = make(map[types.ClusterID]int64)
	m.WriteRecovered = make(map[types.ClusterID]int64)
	m.FailoverTotal = make(map[string]int64)
	m.CircuitBreakerState = make(map[types.ClusterID]int)
	m.CircuitBreakerTrips = make(map[types.ClusterID]int64)
	m.ReplayEnqueued = make(map[types.ClusterID]int64)
	m.ReplaySuccess = make(map[types.ClusterID]int64)
	m.ReplayErrors = make(map[types.ClusterID]int64)
	m.ReplayDropped = make(map[types.ClusterID]int64)
	m.ReplayQueueDepth = make(map[types.ClusterID]int)
	m.ReplayDuration = make(map[types.ClusterID][]float64)
	m.ReplayOldestAge = make(map[types.ClusterID]float64)
	m.ReplayWorkerDropped = make(map[types.ClusterID]map[string]int64)
	m.ClusterDraining = make(map[types.ClusterID]bool)
	m.DrainModeEntered = make(map[types.ClusterID]int64)
	m.DrainModeExited = make(map[types.ClusterID]int64)
	m.SessionRefreshAttempts = make(map[types.ClusterID]int64)
	m.SessionRefreshSuccesses = make(map[types.ClusterID]int64)
	m.SessionRefreshErrors = make(map[types.ClusterID]int64)
	m.MirrorReplayDropped = 0
	m.ClusterEventsDropped = 0

	m.totalReplayEnqueued.Store(0)
	m.totalReplaySuccess.Store(0)
	m.totalFailovers.Store(0)
}
