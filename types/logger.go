package types

// Logger is the structured logging interface Helix writes to.
//
// Every method has the same signature: a message, followed by alternating
// keys and values.
//
//	Debug(msg string, keysAndValues ...any)
//
// An implementation only has to match that signature; Helix never inspects
// the keys or values it passes.
//
// If no logger is configured Helix uses a no-op one that discards
// everything, so every startup warning, circuit breaker transition and
// replay drop is silent.
// Wiring a logger is the only way to see them.
//
// # log/slog
//
// *log/slog.Logger's Debug, Info, Warn and Error already have this exact
// signature, so the only method it lacks is Fatal.
// The bundled adapter in contrib/log/slog supplies it:
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
// # zap
//
// *zap.SugaredLogger does not satisfy this interface.
// Its Debug, Info, Warn and Error take Debug(args ...any) — a message
// built from the arguments, not a message plus key-value pairs.
// The methods that match are the "w" variants,
// Debugw(msg string, keysAndValues ...any), so a zap user wires an
// application-owned wrapper.
// Helix does not depend on zap:
//
//	type zapLogger struct{ *zap.SugaredLogger }
//
//	func (l zapLogger) Debug(msg string, kv ...any) { l.Debugw(msg, kv...) }
//	func (l zapLogger) Info(msg string, kv ...any)  { l.Infow(msg, kv...) }
//	func (l zapLogger) Warn(msg string, kv ...any)  { l.Warnw(msg, kv...) }
//	func (l zapLogger) Error(msg string, kv ...any) { l.Errorw(msg, kv...) }
//	func (l zapLogger) Fatal(msg string, kv ...any) { l.Fatalw(msg, kv...) }
//
// Mapping Fatal to Fatalw, which exits the process, is a reasonable choice
// for a wrapper the application owns and controls.
type Logger interface {
	// Debug logs a message at DebugLevel.
	// The message includes any fields passed at the log site,
	// as well as any fields accumulated on the logger.
	Debug(msg string, keysAndValues ...any)

	// Info logs a message at InfoLevel.
	// The message includes any fields passed at the log site,
	// as well as any fields accumulated on the logger.
	Info(msg string, keysAndValues ...any)

	// Warn logs a message at WarnLevel.
	// The message includes any fields passed at the log site,
	// as well as any fields accumulated on the logger.
	Warn(msg string, keysAndValues ...any)

	// Error logs a message at ErrorLevel.
	// The message includes any fields passed at the log site,
	// as well as any fields accumulated on the logger.
	Error(msg string, keysAndValues ...any)

	// Fatal logs a message at the implementation's most severe level.
	// The message includes any fields passed at the log site,
	// as well as any fields accumulated on the logger.
	//
	// Helix never calls this method; it exists so an application can share
	// one logger with the library.
	// Whether it terminates the process is the implementation's choice:
	// an application-owned logger may call os.Exit(1), but a
	// library-provided adapter such as contrib/log/slog must not, because
	// ending the process from a Helix dependency would skip the caller's
	// deferred shutdown.
	// That adapter logs at error level with a fatal=true attribute
	// instead, so a log pipeline can still alert on the call.
	Fatal(msg string, keysAndValues ...any)
}
