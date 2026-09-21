package testutil

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// checkAIOAvailability checks if the system has available AIO slots.
// ScyllaDB requires Linux AIO even with --reactor-backend=epoll.
func checkAIOAvailability(t *testing.T) {
	t.Helper()

	aioNrData, err := os.ReadFile("/proc/sys/fs/aio-nr")
	if err != nil {
		t.Skipf("Cannot read /proc/sys/fs/aio-nr: %v (not on Linux?)", err)
	}

	aioMaxNrData, err := os.ReadFile("/proc/sys/fs/aio-max-nr")
	if err != nil {
		t.Skipf("Cannot read /proc/sys/fs/aio-max-nr: %v", err)
	}

	aioNr, _ := strconv.ParseInt(strings.TrimSpace(string(aioNrData)), 10, 64)
	aioMaxNr, _ := strconv.ParseInt(strings.TrimSpace(string(aioMaxNrData)), 10, 64)

	// ScyllaDB needs at least some AIO slots available
	if aioNr >= aioMaxNr {
		t.Skipf("No AIO slots available: aio-nr=%d >= aio-max-nr=%d. "+
			"Fix with: sudo sysctl -w fs.aio-max-nr=1048576", aioNr, aioMaxNr)
	}

	t.Logf("AIO slots available: aio-nr=%d, aio-max-nr=%d (free=%d)",
		aioNr, aioMaxNr, aioMaxNr-aioNr)
}

func TestStartTwoScyllaDBContainers(t *testing.T) {
	// This test is only for debugging
	t.Skip()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Check if AIO is available before attempting to start ScyllaDB
	checkAIOAvailability(t)

	ctx := t.Context()

	containerA, containerB, err := StartTwoScyllaDBContainers(ctx, t)
	require.NoError(t, err, "failed to start two ScyllaDB containers")
	require.NotNil(t, containerA, "containerA should not be nil")
	require.NotNil(t, containerB, "containerB should not be nil")

	// Verify sessions are working
	require.NotNil(t, containerA.Session, "containerA session should not be nil")
	require.NotNil(t, containerB.Session, "containerB session should not be nil")

	// Verify hosts are different
	require.NotEmpty(t, containerA.Host, "containerA host should not be empty")
	require.NotEmpty(t, containerB.Host, "containerB host should not be empty")

	// Execute a simple query on both sessions to verify they work
	// Use a simple query that doesn't depend on specific column values
	var releaseVersionA string
	err = containerA.Session.Query("SELECT release_version FROM system.local").Scan(&releaseVersionA)
	require.NoError(t, err, "failed to query cluster A")
	require.NotEmpty(t, releaseVersionA, "release version A should not be empty")

	var releaseVersionB string
	err = containerB.Session.Query("SELECT release_version FROM system.local").Scan(&releaseVersionB)
	require.NoError(t, err, "failed to query cluster B")
	require.NotEmpty(t, releaseVersionB, "release version B should not be empty")

	t.Logf("ScyllaDB Container A: host=%s, version=%s", containerA.Host, releaseVersionA)
	t.Logf("ScyllaDB Container B: host=%s, version=%s", containerB.Host, releaseVersionB)
}
