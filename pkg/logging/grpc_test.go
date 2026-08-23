package logging_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/grpclog"

	"github.com/nicklasfrahm/kontinuum/pkg/logging"
)

// grpcTooManyPingsMsg mirrors the exact message grpc-go's transport package
// logs on a GOAWAY ENHANCE_YOUR_CALM/too_many_pings notice (see
// internal/transport/http2_client.go's handleGoAway), split across lines to
// stay under this repo's line-length limit.
const grpcTooManyPingsMsg = `Client received GoAway with error code ` +
	`ENHANCE_YOUR_CALM and debug data equal to ASCII "too_many_pings".`

// grpcTransportPrefix mirrors the "[component] " prefix grpclog.Component
// prepends to every line logged through its "transport" component — the
// one http2Client.handleGoAway logs the too_many_pings notice through.
const grpcTransportPrefix = "[transport] "

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
	grpcLogger.Errorf("%s", grpcTooManyPingsMsg)

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
	grpcLogger.Errorln("[transport]", grpcTooManyPingsMsg)

	entry := decodeLastEntry(t, &buf)
	assert.Equal(t, "WARN", entry["level"])
	assert.Equal(t, grpcTransportPrefix+grpcTooManyPingsMsg, entry["msg"])
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

func TestNewGRPCLoggerCallShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		call     func(grpc grpclog.LoggerV2)
		wantMsg  string
		wantWarn bool
	}{
		{
			name:     "Infoln",
			call:     func(grpc grpclog.LoggerV2) { grpc.Infoln("subchannel", "ready") },
			wantMsg:  "subchannel ready",
			wantWarn: false,
		},
		{
			name:     "Warningln",
			call:     func(grpc grpclog.LoggerV2) { grpc.Warningln("connectivity", "change") },
			wantMsg:  "connectivity change",
			wantWarn: true,
		},
		{
			name:     "Warningf",
			call:     func(grpc grpclog.LoggerV2) { grpc.Warningf("retrying %s", "dial") },
			wantMsg:  "retrying dial",
			wantWarn: true,
		},
		{
			name:     "Error",
			call:     func(grpc grpclog.LoggerV2) { grpc.Error("failed to dial hub") },
			wantMsg:  "failed to dial hub",
			wantWarn: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			grpcLogger := logging.NewGRPCLogger(logging.New(slog.LevelDebug, logging.FormatJSON, &buf))
			test.call(grpcLogger)

			entry := decodeLastEntry(t, &buf)
			assert.Equal(t, test.wantMsg, entry["msg"])

			if test.wantWarn {
				assert.Equal(t, "WARN", entry["level"])
			} else {
				assert.NotEqual(t, "WARN", entry["level"])
			}
		})
	}
}

func TestNewGRPCLoggerVDisablesVerboseTracing(t *testing.T) {
	t.Parallel()

	grpcLogger := logging.NewGRPCLogger(logging.New(slog.LevelDebug, logging.FormatJSON, &bytes.Buffer{}))

	assert.False(t, grpcLogger.V(0))
	assert.False(t, grpcLogger.V(2))
}
