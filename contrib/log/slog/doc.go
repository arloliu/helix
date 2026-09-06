// Package slog adapts a standard library *log/slog.Logger to types.Logger.
//
// Helix logs through types.Logger, whose methods have the signature
// (msg string, keysAndValues ...any).
// *slog.Logger's Debug, Info, Warn and Error already have that exact
// signature, so the only method it is missing is Fatal.
// This package supplies that one method and nothing else.
//
// # Usage
//
// Import it under an alias, because the package name collides with the
// standard library's:
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
//
// Without a logger Helix uses a no-op one, so every startup warning,
// circuit breaker transition and replay drop is silent.
//
// # Fatal does not terminate the process
//
// Helix itself never calls types.Logger.Fatal.
// A library-provided adapter must therefore never call os.Exit or panic
// for it: doing so would let a log call made by unrelated application code
// tear down a process that imported Helix only for its database client,
// skipping every deferred Close and losing the replay backlog a graceful
// shutdown would have drained.
// The adapter logs at log/slog.LevelError with a leading fatal=true
// attribute and returns.
// It is a log call like any other, so a handler configured above Error
// discards it rather than making an exception for its severity.
// An application that wants a fatal log to end the process should own that
// decision in its own types.Logger implementation.
package slog
