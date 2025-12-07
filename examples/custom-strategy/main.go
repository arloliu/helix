// Package main demonstrates how to implement custom strategies for Helix.
//
// This example shows how to create custom:
// - ReadStrategy: Weighted routing based on cluster performance
// - WriteStrategy: Primary-preferred writes with fallback
// - FailoverPolicy: Threshold-based failover decision
//
// # Running
//
//	go run main.go
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/replay"
	"github.com/arloliu/helix/types"
)

// WeightedReadStrategy routes reads based on cluster weights.
//
// Clusters with higher weights receive more traffic. This is useful
// when clusters have different capacities or you want to gradually
// shift traffic during migrations.
type WeightedReadStrategy struct {
	weightA int
	weightB int
	mu      sync.RWMutex
}

// NewWeightedReadStrategy creates a strategy with specified weights.
//
// Example: NewWeightedReadStrategy(70, 30) routes ~70% to A, ~30% to B.
func NewWeightedReadStrategy(weightA, weightB int) *WeightedReadStrategy {
	return &WeightedReadStrategy{
		weightA: weightA,
		weightB: weightB,
	}
}

// Select selects a cluster based on weighted random selection.
func (s *WeightedReadStrategy) Select(_ context.Context) types.ClusterID {
	s.mu.RLock()
	defer s.mu.RUnlock()

	total := s.weightA + s.weightB
	if total <= 0 {
		return types.ClusterA
	}

	r := rand.IntN(total)
	if r < s.weightA {
		return types.ClusterA
	}
	return types.ClusterB
}

// OnSuccess is called when a read succeeds.
func (s *WeightedReadStrategy) OnSuccess(_ types.ClusterID) {
	// Could be used to adjust weights based on performance
}

// OnFailure is called when a read fails.
func (s *WeightedReadStrategy) OnFailure(cluster types.ClusterID, _ error) (types.ClusterID, bool) {
	// Simple failover: try the other cluster
	if cluster == types.ClusterA {
		return types.ClusterB, true
	}
	return types.ClusterA, true
}

// PrimaryFirstWriteStrategy attempts the primary cluster first,
// then falls back to secondary if primary fails.
//
// This differs from ConcurrentDualWrite which writes to both simultaneously.
// Use this when you want to reduce load on the secondary cluster.
type PrimaryFirstWriteStrategy struct {
	primary types.ClusterID
}

// NewPrimaryFirstWriteStrategy creates a strategy that prefers the given cluster.
func NewPrimaryFirstWriteStrategy(primary types.ClusterID) *PrimaryFirstWriteStrategy {
	return &PrimaryFirstWriteStrategy{primary: primary}
}

// Execute writes to primary first, then secondary.
func (s *PrimaryFirstWriteStrategy) Execute(
	ctx context.Context,
	writeA func(context.Context) error,
	writeB func(context.Context) error,
) (error, error) {
	var primaryWrite, secondaryWrite func(context.Context) error
	if s.primary == types.ClusterA {
		primaryWrite, secondaryWrite = writeA, writeB
	} else {
		primaryWrite, secondaryWrite = writeB, writeA
	}

	// Try primary first
	var errA, errB error
	errPrimary := primaryWrite(ctx)
	if errPrimary != nil {
		fmt.Printf("  Primary cluster failed: %v\n", errPrimary)
	}

	// Then secondary (best effort)
	errSecondary := secondaryWrite(ctx)
	if errSecondary != nil {
		fmt.Printf("  Secondary cluster failed: %v\n", errSecondary)
	}

	// Map back to A/B
	if s.primary == types.ClusterA {
		errA, errB = errPrimary, errSecondary
	} else {
		errA, errB = errSecondary, errPrimary
	}

	return errA, errB
}

// ThresholdFailoverPolicy triggers failover after a threshold of errors.
//
// This is more sophisticated than simple ActiveFailover as it prevents
// failover on transient errors.
type ThresholdFailoverPolicy struct {
	threshold int
	window    time.Duration
	errorsA   []time.Time
	errorsB   []time.Time
	mu        sync.Mutex
}

// NewThresholdFailoverPolicy creates a policy that fails over after
// `threshold` errors within `window` duration.
func NewThresholdFailoverPolicy(threshold int, window time.Duration) *ThresholdFailoverPolicy {
	return &ThresholdFailoverPolicy{
		threshold: threshold,
		window:    window,
	}
}

// ShouldFailover returns true if error count exceeds threshold.
func (p *ThresholdFailoverPolicy) ShouldFailover(cluster types.ClusterID, _ error) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	var errors *[]time.Time
	if cluster == types.ClusterA {
		errors = &p.errorsA
	} else {
		errors = &p.errorsB
	}

	// Count errors within window
	now := time.Now()
	cutoff := now.Add(-p.window)
	validErrors := make([]time.Time, 0, len(*errors))
	for _, t := range *errors {
		if t.After(cutoff) {
			validErrors = append(validErrors, t)
		}
	}
	*errors = validErrors

	return len(*errors) >= p.threshold
}

// RecordFailure records a failure for the specified cluster.
func (p *ThresholdFailoverPolicy) RecordFailure(cluster types.ClusterID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	if cluster == types.ClusterA {
		p.errorsA = append(p.errorsA, now)
	} else {
		p.errorsB = append(p.errorsB, now)
	}
	fmt.Printf("  Recorded failure for cluster %s\n", cluster)
}

// RecordSuccess resets failure counter for the cluster.
func (p *ThresholdFailoverPolicy) RecordSuccess(cluster types.ClusterID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if cluster == types.ClusterA {
		p.errorsA = nil
	} else {
		p.errorsB = nil
	}
}

// MetricsCollector demonstrates how to wrap strategies with metrics.
type MetricsCollector struct {
	readCount  atomic.Int64
	writeCount atomic.Int64
	errorCount atomic.Int64
}

// RecordRead increments read counter.
func (m *MetricsCollector) RecordRead() {
	m.readCount.Add(1)
}

// RecordWrite increments write counter.
func (m *MetricsCollector) RecordWrite() {
	m.writeCount.Add(1)
}

// RecordError increments error counter.
func (m *MetricsCollector) RecordError() {
	m.errorCount.Add(1)
}

// Stats returns current metrics.
func (m *MetricsCollector) Stats() (reads, writes, errors int64) {
	return m.readCount.Load(), m.writeCount.Load(), m.errorCount.Load()
}

func main() {
	fmt.Println("=== Helix Custom Strategy Demo ===")

	// Demonstrate weighted read strategy
	fmt.Println("--- Weighted Read Strategy ---")
	readStrategy := NewWeightedReadStrategy(70, 30)
	clusterACounts := 0
	clusterBCounts := 0

	ctx := context.Background()
	for range 100 {
		cluster := readStrategy.Select(ctx)
		if cluster == types.ClusterA {
			clusterACounts++
		} else {
			clusterBCounts++
		}
	}
	fmt.Printf("100 reads with 70/30 weight: A=%d, B=%d\n", clusterACounts, clusterBCounts)

	// Demonstrate threshold failover
	fmt.Println("\n--- Threshold Failover Policy ---")
	failoverPolicy := NewThresholdFailoverPolicy(3, time.Minute)

	testErr := fmt.Errorf("simulated error")

	for i := 1; i <= 5; i++ {
		// Record failure first, then check if should failover
		failoverPolicy.RecordFailure(types.ClusterA)
		shouldFail := failoverPolicy.ShouldFailover(types.ClusterA, testErr)
		fmt.Printf("Error %d: shouldFailover=%v\n", i, shouldFail)
	}

	// Demonstrate metrics collection
	fmt.Println("\n--- Metrics Collector ---")
	metrics := &MetricsCollector{}

	// Simulate some operations
	for range 50 {
		metrics.RecordRead()
	}
	for range 20 {
		metrics.RecordWrite()
	}
	for range 3 {
		metrics.RecordError()
	}

	reads, writes, errors := metrics.Stats()
	fmt.Printf("Collected metrics: reads=%d, writes=%d, errors=%d\n", reads, writes, errors)

	// Show that custom types implement the interfaces
	fmt.Println("\n--- Interface Compliance ---")

	// These compile-time assertions verify interface implementation
	var _ helix.ReadStrategy = (*WeightedReadStrategy)(nil)
	var _ helix.WriteStrategy = (*PrimaryFirstWriteStrategy)(nil)
	var _ helix.FailoverPolicy = (*ThresholdFailoverPolicy)(nil)

	fmt.Println("   All custom strategies implement required interfaces!")

	// Create a simple demo showing integration
	replayer := replay.NewMemoryReplayer(replay.WithQueueCapacity(100))
	fmt.Printf("\n   Replayer capacity: %d\n", replayer.Cap())
	fmt.Printf("   Replayer current length: %d\n", replayer.Len())

	// Demonstrate replayer
	err := replayer.Enqueue(context.Background(), types.ReplayPayload{
		TargetCluster: types.ClusterB,
		Query:         "INSERT INTO demo (id) VALUES (?)",
		Args:          []any{1},
		Timestamp:     time.Now().UnixMicro(),
		Priority:      types.PriorityHigh,
	})
	if err != nil {
		log.Printf("Failed to enqueue: %v", err)
	}

	fmt.Printf("   After enqueue, length: %d\n", replayer.Len())

	// Drain and show payload
	payloads := replayer.DrainAll()
	if len(payloads) > 0 {
		fmt.Printf("   Drained payload: cluster=%s, query=%s\n",
			payloads[0].TargetCluster, payloads[0].Query)
	}

	fmt.Println("\n=== Custom Strategy Demo Complete ===")
}
