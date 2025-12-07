// Package logging provides internal logging utilities for Helix.
package logging

import "github.com/arloliu/helix/types"

// NopLogger is a no-op logger that discards all log messages.
//
// This is used as the default logger when no logger is configured,
// avoiding nil checks throughout the codebase.
type NopLogger struct{}

// Compile-time assertion that NopLogger implements types.Logger.
var _ types.Logger = (*NopLogger)(nil)

// NewNopLogger creates a new no-op logger.
//
// Returns:
//   - *NopLogger: A logger that discards all messages
func NewNopLogger() *NopLogger {
	return &NopLogger{}
}

// Debug discards the message.
func (l *NopLogger) Debug(_ string, _ ...any) {}

// Info discards the message.
func (l *NopLogger) Info(_ string, _ ...any) {}

// Warn discards the message.
func (l *NopLogger) Warn(_ string, _ ...any) {}

// Error discards the message.
func (l *NopLogger) Error(_ string, _ ...any) {}

// Fatal discards the message.
//
// Note: Unlike a real logger, this does NOT call os.Exit(1).
// This is intentional for safety in testing and default configurations.
func (l *NopLogger) Fatal(_ string, _ ...any) {}
