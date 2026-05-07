package helix

import "context"

// Test-only exports that let helix_test access internals without making
// them part of the public API. Files named *_test.go are only compiled
// for tests in this package.

// SetClientNowFuncForTest replaces the client's NowProvider with the
// given function. Used by auto-refresh tests to drive a deterministic
// clock instead of wall-clock.
func SetClientNowFuncForTest(c *CQLClient, fn NowProvider) {
	c.config.NowProvider = fn
}

// MaybeAutoRefreshForTest drives a single auto-refresh evaluation for
// the given cluster, bypassing the goroutine ticker. Tests use this for
// time-deterministic assertions; production code never calls this
// directly because the autoRefreshLoop goroutine drives it.
func MaybeAutoRefreshForTest(c *CQLClient, cluster ClusterID) {
	c.maybeAutoRefresh(cluster)
}

// AutoRefreshEnabledForTest reports whether the auto-refresh detector
// goroutine was started for this client.
func AutoRefreshEnabledForTest(c *CQLClient) bool {
	return c.config.AutoRefresh.Enabled
}

// AutoRefreshCtxForTest returns the auto-refresh detector's context, or
// nil if the detector was not started. Tests use this to assert the
// goroutine's lifecycle (e.g., that Close cancels it).
func AutoRefreshCtxForTest(c *CQLClient) context.Context {
	return c.autoRefreshCtx
}
