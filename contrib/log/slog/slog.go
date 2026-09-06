package slog

import (
	"context"
	"log/slog"
	"runtime"
	"time"
)

// Logger adapts a *log/slog.Logger to types.Logger.
//
// The embedded *slog.Logger already supplies Debug, Info, Warn and Error
// with the (msg string, args ...any) signature types.Logger requires, so
// only Fatal is declared below.
// The embedded logger stays reachable, so an application that keeps the
// adapter can still use the full log/slog API through it.
//
// Build one with [New]; the zero value has no logger to forward to.
type Logger struct{ *slog.Logger }

// New wraps a *log/slog.Logger so it satisfies types.Logger.
//
// The returned logger forwards Debug, Info, Warn and Error to l unchanged,
// including any attributes accumulated on l by With or WithGroup.
// Fatal logs at [log/slog.LevelError] with a leading fatal=true attribute
// and returns without terminating the process; see [Logger.Fatal].
//
// Parameters:
//   - l: The logger to wrap. nil selects [log/slog.Default], resolved when
//     New is called; a later [log/slog.SetDefault] does not affect the
//     returned logger.
//
// Returns:
//   - *Logger: A logger ready for helix.WithLogger
//
// Example:
//
//	import (
//	    "log/slog"
//
//	    helixslog "github.com/arloliu/helix/contrib/log/slog"
//	)
//
//	client, err := helix.NewCQLClient(sessionA, sessionB,
//	    helix.WithLogger(helixslog.New(slog.Default())),
//	)
func New(l *slog.Logger) *Logger {
	if l == nil {
		l = slog.Default()
	}

	return &Logger{l}
}

// Fatal logs msg at [log/slog.LevelError] with a leading fatal=true
// attribute, then returns.
//
// It does not terminate the process.
// A library must not decide that for the program that imports it: an
// os.Exit here would skip every deferred Close in the process and discard
// a replay backlog a graceful shutdown would have drained.
// The fatal=true attribute keeps the caller's intent visible to a log
// pipeline, which can alert or page on it.
//
// The record obeys the handler's level like any other: a handler
// configured above [log/slog.LevelError] discards it, and Fatal returns
// having written nothing.
//
// The record is built and handed to the handler directly rather than
// forwarded through Error, so a handler configured with AddSource
// attributes the record to the caller of Fatal instead of to this file.
//
// Parameters:
//   - msg: The log message
//   - keysAndValues: Alternating keys and values, as for the other levels
func (l *Logger) Fatal(msg string, keysAndValues ...any) {
	ctx := context.Background()
	if !l.Enabled(ctx, slog.LevelError) {
		return
	}

	// Skip runtime.Callers and Fatal itself, so the program counter names
	// the call site the caller wrote.
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:])

	record := slog.NewRecord(time.Now(), slog.LevelError, msg, pcs[0])

	// The marker leads the pairs so a caller that passes an odd number of
	// arguments cannot consume it: appended last, a dangling key would
	// pair with "fatal" and turn the marker into a !BADKEY value.
	// The fresh slice also keeps the append off the caller's backing array.
	record.Add(append([]any{"fatal", true}, keysAndValues...)...)

	// A logger has no channel to report its own handler's failure.
	_ = l.Handler().Handle(ctx, record)
}
