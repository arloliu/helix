package slog

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/helix/types"
)

// Compile-time assertion that the adapter satisfies types.Logger. Placed
// in a _test.go file per the public-package assertion convention.
var _ types.Logger = (*Logger)(nil)

// debugLogger builds a *slog.Logger writing JSON records into buf, with the
// handler level lowered to Debug so every level under test is emitted.
func debugLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// decode parses the single JSON record buf holds.
func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()

	var record map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))

	return record
}

func TestNew_LevelsAndFields(t *testing.T) {
	tests := []struct {
		name      string
		log       func(l types.Logger)
		wantLevel string
		wantAttrs map[string]any
	}{
		{
			name:      "debug",
			log:       func(l types.Logger) { l.Debug("debug msg", "cluster", "A") },
			wantLevel: slog.LevelDebug.String(),
			wantAttrs: map[string]any{"cluster": "A"},
		},
		{
			name:      "info",
			log:       func(l types.Logger) { l.Info("info msg", "cluster", "B") },
			wantLevel: slog.LevelInfo.String(),
			wantAttrs: map[string]any{"cluster": "B"},
		},
		{
			name:      "warn",
			log:       func(l types.Logger) { l.Warn("warn msg", "attempts", 3) },
			wantLevel: slog.LevelWarn.String(),
			wantAttrs: map[string]any{"attempts": float64(3)},
		},
		{
			name:      "error",
			log:       func(l types.Logger) { l.Error("error msg", "reason", "timeout") },
			wantLevel: slog.LevelError.String(),
			wantAttrs: map[string]any{"reason": "timeout"},
		},
		{
			name:      "fatal lands at error level with fatal=true",
			log:       func(l types.Logger) { l.Fatal("fatal msg", "reason", "corrupt") },
			wantLevel: slog.LevelError.String(),
			wantAttrs: map[string]any{"reason": "corrupt", "fatal": true},
		},
		{
			name:      "no key-value pairs",
			log:       func(l types.Logger) { l.Info("bare msg") },
			wantLevel: slog.LevelInfo.String(),
			wantAttrs: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			tt.log(New(debugLogger(&buf)))

			record := decode(t, &buf)
			require.Equal(t, tt.wantLevel, record["level"])
			for key, want := range tt.wantAttrs {
				require.Equal(t, want, record[key], "attribute %q", key)
			}
		})
	}
}

func TestNew_FatalReturnsAndDoesNotPanic(t *testing.T) {
	var buf bytes.Buffer
	l := New(debugLogger(&buf))

	require.NotPanics(t, func() { l.Fatal("shutting down", "reason", "disk") })

	// Execution continues past Fatal, so a later record is still written.
	l.Info("still running")
	require.Contains(t, buf.String(), "still running")
}

func TestNew_FatalMarkerSurvivesDanglingKey(t *testing.T) {
	var buf bytes.Buffer

	// An odd number of arguments is a caller mistake; the marker leads the
	// pairs so slog cannot consume it as the dangling key's value.
	New(debugLogger(&buf)).Fatal("shutting down", "reason")

	record := decode(t, &buf)
	require.Equal(t, slog.LevelError.String(), record["level"])
	require.Equal(t, true, record["fatal"])
}

func TestNew_FatalDoesNotMutateCallerSlice(t *testing.T) {
	var buf bytes.Buffer
	l := New(debugLogger(&buf))

	// Spare capacity gives Fatal a backing array it could write into; a
	// full slice would force append to allocate and never exercise that.
	args := make([]any, 2, 8)
	args[0], args[1] = "reason", "disk"

	l.Fatal("shutting down", args...)

	require.Equal(t, []any{"reason", "disk"}, args)
}

func TestNew_NilUsesDefaultLogger(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	slog.SetDefault(debugLogger(&buf))

	New(nil).Info("from default", "cluster", "A")

	record := decode(t, &buf)
	require.Equal(t, "from default", record["msg"])
	require.Equal(t, "A", record["cluster"])
}

func TestNew_NilResolvesDefaultOnce(t *testing.T) {
	var atNew, afterNew bytes.Buffer
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	slog.SetDefault(debugLogger(&atNew))

	l := New(nil)

	// A later SetDefault must not redirect a logger already built.
	slog.SetDefault(debugLogger(&afterNew))
	l.Info("to the original default")

	require.Contains(t, atNew.String(), "to the original default")
	require.Empty(t, afterNew.String())
}

func TestNew_FatalAttributesSourceToCaller(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	})

	New(slog.New(handler)).Fatal("shutting down", "reason", "disk")

	record := decode(t, &buf)
	source, ok := record["source"].(map[string]any)
	require.True(t, ok, "record has no source group: %s", buf.String())

	file, ok := source["file"].(string)
	require.True(t, ok, "source group has no file: %s", buf.String())

	// The record must name this test file, not the adapter's own file.
	require.Equal(t, "slog_test.go", filepath.Base(file))
}

func TestNew_FatalRespectsHandlerLevel(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError + 1})

	New(slog.New(handler)).Fatal("shutting down", "reason", "disk")

	require.Empty(t, buf.String())
}

func TestNew_PreservesAccumulatedAttributes(t *testing.T) {
	var buf bytes.Buffer
	base := debugLogger(&buf)

	New(base.With("component", "replay")).Warn("drop", "reason", "max_attempts")

	record := decode(t, &buf)
	require.Equal(t, "replay", record["component"])
	require.Equal(t, "max_attempts", record["reason"])
}
