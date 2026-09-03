package testutil

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/helix/types"
)

// TestLogger writes Helix log lines to the test log, so a scenario that
// fails in CI shows what the client observed (probe errors, refresh
// attempts) next to the assertion that failed.
type TestLogger struct {
	tb    testing.TB
	debug bool
}

// NewTestLogger returns a logger that writes Info, Warn, and Error lines
// to tb.Logf with a timestamp; Debug lines are dropped.
func NewTestLogger(tb testing.TB) *TestLogger {
	tb.Helper()

	return &TestLogger{tb: tb}
}

// WithDebug makes the logger write Debug lines as well.
func (l *TestLogger) WithDebug() *TestLogger {
	l.debug = true

	return l
}

func (l *TestLogger) log(level, msg string, kv []any) {
	var b strings.Builder
	fmt.Fprintf(&b, "%s helix/%s %s", time.Now().Format("15:04:05.000"), level, msg)
	for i := 0; i+1 < len(kv); i += 2 {
		fmt.Fprintf(&b, " %v=%v", kv[i], kv[i+1])
	}
	l.tb.Log(b.String())
}

// Debug implements types.Logger.
func (l *TestLogger) Debug(msg string, keysAndValues ...any) {
	if l.debug {
		l.log("debug", msg, keysAndValues)
	}
}

// Info implements types.Logger.
func (l *TestLogger) Info(msg string, keysAndValues ...any) { l.log("info", msg, keysAndValues) }

// Warn implements types.Logger.
func (l *TestLogger) Warn(msg string, keysAndValues ...any) { l.log("warn", msg, keysAndValues) }

// Error implements types.Logger.
func (l *TestLogger) Error(msg string, keysAndValues ...any) { l.log("error", msg, keysAndValues) }

// Fatal implements types.Logger; it logs and fails the test at once.
func (l *TestLogger) Fatal(msg string, keysAndValues ...any) {
	l.log("fatal", msg, keysAndValues)
	l.tb.FailNow()
}

var _ types.Logger = (*TestLogger)(nil)
