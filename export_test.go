package helix

import "context"

// Test-only exports that let helix_test access internals without making
// them part of the public API. Files named *_test.go are only compiled
// for tests in this package.

// SetClientNowFuncForTest replaces the client's NowProvider with the
// given function and re-seeds the installed sessions' last-success stamps
// from it, as if the client had been constructed under that clock. Used by
// auto-refresh tests to drive a deterministic clock instead of wall-clock.
func SetClientNowFuncForTest(c *CQLClient, fn NowProvider) {
	c.config.NowProvider = fn
	c.health.now = fn
	now := fn()
	c.sessionA.Load().stats.lastSuccessNanos.Store(now)
	if !c.singleCluster {
		c.sessionB.Load().stats.lastSuccessNanos.Store(now)
	}
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

// WithConfigCaptureForTest returns an Option that stashes the *ClientConfig
// NewCQLClient builds into dst, so a test can inspect the effective
// configuration even when construction later fails and NewCQLClient
// returns a nil client.
func WithConfigCaptureForTest(dst **ClientConfig) Option {
	return func(c *ClientConfig) {
		*dst = c
	}
}
