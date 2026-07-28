package kms_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kmsapi "github.com/siderolabs/kms-client/api/kms"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/kms"
)

// TestServerServesTalosKMSClient dials Server with
// github.com/siderolabs/kms-client's own generated client stub — the same
// one Talos's disk-encryption KMS provider uses — and round-trips a
// passphrase through it, proving Server is wire-compatible, not just
// method-compatible.
func TestServerServesTalosKMSClient(t *testing.T) {
	t.Parallel()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := kms.New(listener.Addr().String(), slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- server.Serve(ctx, listener)
	}()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	defer conn.Close() //nolint:errcheck

	client := kmsapi.NewKMSServiceClient(conn)
	passphrase := testPassphrase()

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()

	sealed, err := client.Seal(callCtx, &kmsapi.Request{NodeUuid: testNodeUUID, Data: passphrase})
	require.NoError(t, err)

	unsealed, err := client.Unseal(callCtx, &kmsapi.Request{NodeUuid: testNodeUUID, Data: sealed.GetData()})
	require.NoError(t, err)
	assert.Equal(t, passphrase, unsealed.GetData())

	cancel()
	require.NoError(t, <-serveErr)
}
