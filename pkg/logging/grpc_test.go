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

func decodeLastEntry(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()

	lines := bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte("\n"))
	require.NotEmpty(t, lines)

	var entry map[string]any
	require.NoError(t, json.Unmarshal(lines[len(lines)-1], &entry))

	return entry
}

func TestNewGRPCLoggerTagsComponent(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	grpcLogger := logging.NewGRPCLogger(logging.New(slog.LevelDebug, logging.FormatJSON, &buf))
	grpcLogger.Infof("dialing %s", "hub")

	entry := decodeLastEntry(t, &buf)
	assert.Equal(t, "grpc", entry["component"])
	assert.Equal(t, "dialing hub", entry["msg"])
	assert.Equal(t, "INFO", entry["level"])
}

func TestNewGRPCLoggerDowngradesTooManyPings(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	grpcLogger := logging.NewGRPCLogger(logging.New(slog.LevelDebug, logging.FormatJSON, &buf))
	grpcLogger.Errorf(`Client received GoAway with error code ENHANCE_YOUR_CALM and debug data equal to ASCII "too_many_pings".`)

	entry := decodeLastEntry(t, &buf)
	assert.Equal(t, "WARN", entry["level"])
	assert.Contains(t, entry["msg"], "too_many_pings")
}

func TestNewGRPCLoggerKeepsOtherErrorsAtError(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	grpcLogger := logging.NewGRPCLogger(logging.New(slog.LevelDebug, logging.FormatJSON, &buf))
	grpcLogger.Errorf("failed to dial %s", "hub")

	entry := decodeLastEntry(t, &buf)
	assert.Equal(t, "ERROR", entry["level"])
}

func TestNewGRPCLoggerErrorlnDowngradesTooManyPings(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	grpcLogger := logging.NewGRPCLogger(logging.New(slog.LevelDebug, logging.FormatJSON, &buf))
	// Mirrors grpclog.Component's own call shape: a "[component]" prefix
	// arg followed by the message, joined by Errorln — see http2Client's
	// package-level logger, which every GOAWAY too_many_pings notice goes
	// through.
	grpcLogger.Errorln("[transport]", `Client received GoAway with error code ENHANCE_YOUR_CALM and debug data equal to ASCII "too_many_pings".`)

	entry := decodeLastEntry(t, &buf)
	assert.Equal(t, "WARN", entry["level"])
	assert.Equal(t, `[transport] Client received GoAway with error code ENHANCE_YOUR_CALM and debug data equal to ASCII "too_many_pings".`, entry["msg"])
}

func TestNewGRPCLoggerWarningAndInfoPassThrough(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	grpcLogger := logging.NewGRPCLogger(logging.New(slog.LevelDebug, logging.FormatJSON, &buf))

	grpcLogger.Warning("connectivity change")
	entry := decodeLastEntry(t, &buf)
	assert.Equal(t, "WARN", entry["level"])
	assert.Equal(t, "connectivity change", entry["msg"])

	grpcLogger.Info("subchannel ready")
	entry = decodeLastEntry(t, &buf)
	assert.Equal(t, "INFO", entry["level"])
	assert.Equal(t, "subchannel ready", entry["msg"])
}

func TestNewGRPCLoggerVDisablesVerboseTracing(t *testing.T) {
	t.Parallel()

	grpcLogger := logging.NewGRPCLogger(logging.New(slog.LevelDebug, logging.FormatJSON, &bytes.Buffer{}))

	assert.False(t, grpcLogger.V(0))
	assert.False(t, grpcLogger.V(2))
}
