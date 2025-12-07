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
	ReadTotal    map[types.ClusterID]int64
	ReadErrors   map[types.ClusterID]int64
	ReadDuration map[types.ClusterID][]float64

	// Write operations
	WriteTotal    map[types.ClusterID]int64
	WriteErrors   map[types.ClusterID]int64
	WriteDuration map[types.ClusterID][]float64

	// Failover
	FailoverTotal map[string]int64 // key: "from->to"

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

	// Drain mode
	ClusterDraining  map[types.ClusterID]bool
	DrainModeEntered map[types.ClusterID]int64
	DrainModeExited  map[types.ClusterID]int64

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
		ReadTotal:           make(map[types.ClusterID]int64),
		ReadErrors:          make(map[types.ClusterID]int64),
		ReadDuration:        make(map[types.ClusterID][]float64),
		WriteTotal:          make(map[types.ClusterID]int64),
		WriteErrors:         make(map[types.ClusterID]int64),
		WriteDuration:       make(map[types.ClusterID][]float64),
		FailoverTotal:       make(map[string]int64),
		CircuitBreakerState: make(map[types.ClusterID]int),
		CircuitBreakerTrips: make(map[types.ClusterID]int64),
		ReplayEnqueued:      make(map[types.ClusterID]int64),
		ReplaySuccess:       make(map[types.ClusterID]int64),
		ReplayErrors:        make(map[types.ClusterID]int64),
		ReplayDropped:       make(map[types.ClusterID]int64),
		ReplayQueueDepth:    make(map[types.ClusterID]int),
		ReplayDuration:      make(map[types.ClusterID][]float64),
		ClusterDraining:     make(map[types.ClusterID]bool),
		DrainModeEntered:    make(map[types.ClusterID]int64),
		DrainModeExited:     make(map[types.ClusterID]int64),
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

func (m *TestMetricsCollector) ObserveWriteDuration(cluster types.ClusterID, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WriteDuration[cluster] = append(m.WriteDuration[cluster], seconds)
}

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

// GetReadErrors returns the total read error count for a cluster.
func (m *TestMetricsCollector) GetReadErrors(cluster types.ClusterID) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ReadErrors[cluster]
}

// Reset clears all collected metrics.
func (m *TestMetricsCollector) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ReadTotal = make(map[types.ClusterID]int64)
	m.ReadErrors = make(map[types.ClusterID]int64)
	m.ReadDuration = make(map[types.ClusterID][]float64)
	m.WriteTotal = make(map[types.ClusterID]int64)
	m.WriteErrors = make(map[types.ClusterID]int64)
	m.WriteDuration = make(map[types.ClusterID][]float64)
	m.FailoverTotal = make(map[string]int64)
	m.CircuitBreakerState = make(map[types.ClusterID]int)
	m.CircuitBreakerTrips = make(map[types.ClusterID]int64)
	m.ReplayEnqueued = make(map[types.ClusterID]int64)
	m.ReplaySuccess = make(map[types.ClusterID]int64)
	m.ReplayErrors = make(map[types.ClusterID]int64)
	m.ReplayDropped = make(map[types.ClusterID]int64)
	m.ReplayQueueDepth = make(map[types.ClusterID]int)
	m.ReplayDuration = make(map[types.ClusterID][]float64)
	m.ClusterDraining = make(map[types.ClusterID]bool)
	m.DrainModeEntered = make(map[types.ClusterID]int64)
	m.DrainModeExited = make(map[types.ClusterID]int64)

	m.totalReplayEnqueued.Store(0)
	m.totalReplaySuccess.Store(0)
	m.totalFailovers.Store(0)
}
