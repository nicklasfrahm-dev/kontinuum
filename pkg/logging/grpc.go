package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"google.golang.org/grpc/grpclog"
)

// grpcTooManyPingsSubstr identifies the one grpc-go log line this bridge
// downgrades from Error to Warn: the GOAWAY ENHANCE_YOUR_CALM/too_many_pings
// notice logged by internal/transport/http2_client.go's handleGoAway.
// grpc-go's own comment there explains why it logs at Error in the first
// place — that's simply the one severity its own default logger leaves
// enabled, not a judgment that the condition is fatal. grpc-go recovers on
// its own: it doubles the connection's keepalive interval and lets its
// normal reconnect backoff redial, so by the time this process's logger
// sees the line, the transport is already being replaced.
const grpcTooManyPingsSubstr = "too_many_pings"

// grpcLogger adapts grpc-go's internal grpclog.LoggerV2 interface onto a
// slog.Logger, so gRPC's own connection-lifecycle logging (dial attempts,
// GOAWAY frames, transport resets) comes out structured and leveled like
// every other line this process logs, instead of the unstructured,
// Error-only text grpc-go writes straight to stderr by default.
type grpcLogger struct {
	logger *slog.Logger
}

// NewGRPCLogger wraps logger for use as a grpclog.LoggerV2, tagged
// component=grpc. Install the result with grpclog.SetLoggerV2 once, before
// any gRPC client or server in the process is created — SetLoggerV2 sets a
// package-level global inside grpc-go and isn't safe to change concurrently
// with gRPC activity. grpcLogger stays unexported: it exists only to
// satisfy grpclog.LoggerV2, the type grpclog.SetLoggerV2 itself requires,
// and exporting it would just obligate doc comments on a dozen methods
// that exist purely to implement someone else's interface.
//
//nolint:ireturn // see comment above
func NewGRPCLogger(logger *slog.Logger) grpclog.LoggerV2 {
	return &grpcLogger{logger: logger.With("component", "grpc")}
}

func (g *grpcLogger) Info(args ...any)   { g.logger.Info(trimmed(fmt.Sprint(args...))) }
func (g *grpcLogger) Infoln(args ...any) { g.logger.Info(trimmed(fmt.Sprintln(args...))) }
func (g *grpcLogger) Infof(format string, args ...any) {
	g.logger.Info(fmt.Sprintf(format, args...))
}

func (g *grpcLogger) Warning(args ...any)   { g.logger.Warn(trimmed(fmt.Sprint(args...))) }
func (g *grpcLogger) Warningln(args ...any) { g.logger.Warn(trimmed(fmt.Sprintln(args...))) }
func (g *grpcLogger) Warningf(format string, args ...any) {
	g.logger.Warn(fmt.Sprintf(format, args...))
}

func (g *grpcLogger) Error(args ...any)   { g.logError(trimmed(fmt.Sprint(args...))) }
func (g *grpcLogger) Errorln(args ...any) { g.logError(trimmed(fmt.Sprintln(args...))) }
func (g *grpcLogger) Errorf(format string, args ...any) {
	g.logError(fmt.Sprintf(format, args...))
}

func (g *grpcLogger) Fatal(args ...any) {
	g.logger.Error(trimmed(fmt.Sprint(args...)))
	os.Exit(1)
}

func (g *grpcLogger) Fatalln(args ...any) {
	g.logger.Error(trimmed(fmt.Sprintln(args...)))
	os.Exit(1)
}

func (g *grpcLogger) Fatalf(format string, args ...any) {
	g.logger.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}

// V reports grpc-go's own verbose tracing (V(2)+ connectivity chatter) as
// disabled — the Info/Warning/Error lines above already cover what's worth
// this process's log volume.
func (g *grpcLogger) V(int) bool { return false }

// logError routes grpc-go's Error-level lines to Warn when they match a
// known self-recovering condition — see grpcTooManyPingsSubstr — and to
// Error otherwise.
func (g *grpcLogger) logError(msg string) {
	if strings.Contains(msg, grpcTooManyPingsSubstr) {
		g.logger.Warn(msg)

		return
	}

	g.logger.Error(msg)
}

// trimmed strips the trailing newline fmt.Sprint/fmt.Sprintln leave on
// messages built from grpclog.Component's arg-prepending (see its Info,
// Warning, and Error methods) — slog already terminates each line itself.
func trimmed(msg string) string {
	return strings.TrimRight(msg, "\n")
}
