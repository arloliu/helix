package topology

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix"
	"github.com/arloliu/helix/types"
)

// drainChannel drains a channel in the background.
func drainChannel[T any](ch <-chan T) {
	go func() {
		for range ch {
			_ = struct{}{} // consume item
		}
	}()
}

func TestNewLocal(t *testing.T) {
	local := NewLocal()
	require.NotNil(t, local)
	defer local.Close()

	assert.False(t, local.IsDraining(types.ClusterA))
	assert.False(t, local.IsDraining(types.ClusterB))
}

func TestLocalSetDrain(t *testing.T) {
	local := NewLocal()
	defer local.Close()

	ctx := t.Context()
	updates := local.Watch(ctx)

	// Set drain on cluster A
	err := local.SetDrain(ctx, types.ClusterA, true, "maintenance")
	require.NoError(t, err)

	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterA, update.Cluster)
		assert.True(t, update.DrainMode)
		assert.False(t, update.Available)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for update")
	}

	assert.True(t, local.IsDraining(types.ClusterA))
	assert.False(t, local.IsDraining(types.ClusterB))
	assert.Equal(t, "maintenance", local.GetDrainReason())
}

func TestLocalClearDrain(t *testing.T) {
	local := NewLocal()
	defer local.Close()

	ctx := t.Context()
	updates := local.Watch(ctx)

	// Set then clear drain
	_ = local.SetDrain(ctx, types.ClusterB, true, "test")
	<-updates // consume first update

	err := local.SetDrain(ctx, types.ClusterB, false, "")
	require.NoError(t, err)

	select {
	case update := <-updates:
		assert.Equal(t, types.ClusterB, update.Cluster)
		assert.False(t, update.DrainMode)
		assert.True(t, update.Available)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for clear update")
	}

	assert.False(t, local.IsDraining(types.ClusterB))
}

func TestLocalNoUpdateOnSameState(t *testing.T) {
	local := NewLocal()
	defer local.Close()

	ctx := t.Context()
	updates := local.Watch(ctx)

	// Set drain twice with same value
	_ = local.SetDrain(ctx, types.ClusterA, true, "")
	<-updates // consume first update

	_ = local.SetDrain(ctx, types.ClusterA, true, "") // same value

	// Should not receive another update
	select {
	case <-updates:
		t.Fatal("should not receive update for same state")
	case <-time.After(100 * time.Millisecond):
		// Expected - no update
	}
}

func TestLocalMultipleClusters(t *testing.T) {
	local := NewLocal()
	defer local.Close()

	ctx := t.Context()
	updates := local.Watch(ctx)

	// Set drain on both clusters
	_ = local.SetDrain(ctx, types.ClusterA, true, "")
	_ = local.SetDrain(ctx, types.ClusterB, true, "")

	// Collect updates
	received := make(map[types.ClusterID]helix.TopologyUpdate)
	for range 2 {
		select {
		case update := <-updates:
			received[update.Cluster] = update
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for update")
		}
	}

	assert.Len(t, received, 2)
	assert.True(t, received[types.ClusterA].DrainMode)
	assert.True(t, received[types.ClusterB].DrainMode)

	assert.True(t, local.IsDraining(types.ClusterA))
	assert.True(t, local.IsDraining(types.ClusterB))
}

func TestLocalClose(t *testing.T) {
	local := NewLocal()

	ctx := t.Context()
	updates := local.Watch(ctx)

	err := local.Close()
	require.NoError(t, err)

	// Double close should be safe
	err = local.Close()
	require.NoError(t, err)

	// SetDrain after close should not panic
	_ = local.SetDrain(ctx, types.ClusterA, true, "")

	// Channel should eventually close
	select {
	case _, ok := <-updates:
		if ok {
			drainChannel(updates)
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after Close()")
	}
}

func TestLocalContextCancellation(t *testing.T) {
	local := NewLocal()
	defer func() { _ = local.Close() }()

	ctx, cancel := context.WithCancel(t.Context())
	updates := local.Watch(ctx)

	cancel()

	// Manually close to ensure cleanup
	_ = local.Close()

	// Channel should close - wait for it with Eventually pattern
	closed := make(chan bool, 1)
	go func() {
		for range updates {
			_ = struct{}{} // drain
		}
		closed <- true
	}()

	select {
	case <-closed:
		// Success - channel closed
	case <-time.After(time.Second):
		t.Fatal("channel not closed")
	}
}

func TestLocalUnknownCluster(t *testing.T) {
	local := NewLocal()
	defer local.Close()

	ctx := t.Context()

	// Unknown cluster should return false
	assert.False(t, local.IsDraining("C"))

	// SetDrain on unknown cluster should not panic
	err := local.SetDrain(ctx, "C", true, "")
	require.NoError(t, err)
}

func TestLocalImplementsInterfaces(t *testing.T) {
	// Compile-time check that Local implements TopologyWatcher and TopologyOperator
	var _ helix.TopologyWatcher = (*Local)(nil)
	var _ helix.TopologyOperator = (*Local)(nil)
}

// TestLocalUpdatesDeliveredWhenChannelFull verifies the goroutine-fallback send
// path: if the updates channel buffer is full, the overflow update is still
// delivered (not silently dropped) once the consumer starts reading.
func TestLocalUpdatesDeliveredWhenChannelFull(t *testing.T) {
	const bufferCap = 10 // must match the capacity used in NewLocal

	local := NewLocal()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer local.Close()

	updates := local.Watch(ctx)

	// Alternate drain states without consuming any updates, filling the buffer.
	// i=0 → true, i=1 → false, ..., i=9 → false. That is bufferCap state changes.
	for i := range bufferCap {
		_ = local.SetDrain(ctx, types.ClusterA, i%2 == 0, "fill")
	}
	// Channel buffer is now at capacity (10 items). Current state: drainA=false (i=9).

	// This (bufferCap+1)th state change would previously be silently dropped.
	_ = local.SetDrain(ctx, types.ClusterA, true, "must-deliver")

	// Now drain the channel. All bufferCap+1 updates must arrive.
	want := bufferCap + 1
	got := make([]helix.TopologyUpdate, 0, want)
	deadline := time.After(2 * time.Second)

	for len(got) < want {
		select {
		case u := <-updates:
			got = append(got, u)
		case <-deadline:
			t.Fatalf("timed out after receiving %d/%d updates; goroutine-fallback send was dropped",
				len(got), want)
		}
	}

	assert.Len(t, got, want, "all %d updates must be delivered, including the overflow via goroutine fallback", want)
}

func TestNATSImplementsInterface(t *testing.T) {
	// Compile-time check that NATS implements TopologyWatcher
	var _ helix.TopologyWatcher = (*NATS)(nil)
}
