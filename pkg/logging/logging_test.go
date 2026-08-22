package logging_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/logging"
)

func TestParseLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  slog.Level
	}{
		{"debug", "debug", slog.LevelDebug},
		{"info", "info", slog.LevelInfo},
		{"warn", "warn", slog.LevelWarn},
		{"error", "error", slog.LevelError},
		{"uppercase", "DEBUG", slog.LevelDebug},
		{"mixed case", "WaRn", slog.LevelWarn},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := logging.ParseLevel(test.input)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestParseLevelUnknown(t *testing.T) {
	t.Parallel()

	_, err := logging.ParseLevel("trace")
	require.ErrorIs(t, err, logging.ErrUnknownLogLevel)
}

func TestParseFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  logging.Format
	}{
		{"console", "console", logging.FormatConsole},
		{"text", "text", logging.FormatText},
		{"json", "json", logging.FormatJSON},
		{"uppercase", "JSON", logging.FormatJSON},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := logging.ParseFormat(test.input)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestParseFormatUnknown(t *testing.T) {
	t.Parallel()

	_, err := logging.ParseFormat("xml")
	require.ErrorIs(t, err, logging.ErrUnknownLogFormat)
}

func TestNewJSON(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := logging.New(slog.LevelInfo, logging.FormatJSON, &buf)

	logger.Info("hello", "key", "value")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	assert.Equal(t, "hello", entry["msg"])
	assert.Equal(t, "value", entry["key"])
}

func TestNewJSONRespectsLevel(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := logging.New(slog.LevelWarn, logging.FormatJSON, &buf)

	logger.Info("should be filtered out")

	assert.Empty(t, buf.String())
}

func TestNewConsole(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := logging.New(slog.LevelInfo, logging.FormatConsole, &buf)

	logger.Info("hello")

	assert.Contains(t, buf.String(), "hello")
}

func TestNewText(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := logging.New(slog.LevelInfo, logging.FormatText, &buf)

	logger.Info("hello")

	assert.Contains(t, buf.String(), "hello")
}

func TestNewDefaultsToJSON(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := logging.New(slog.LevelInfo, logging.Format("unknown"), &buf)

	logger.Info("hello")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	assert.Equal(t, "hello", entry["msg"])
}
