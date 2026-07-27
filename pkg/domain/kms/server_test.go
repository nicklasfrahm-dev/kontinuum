package kms_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kmsapi "github.com/siderolabs/kms-client/api/kms"
	"github.com/siderolabs/kms-client/pkg/constants"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/kms"
)

const testNodeUUID = "3f5f2e1a-0000-4000-8000-000000000001"

func newTestServer() *kms.Server {
	return kms.New(":0", slog.New(slog.DiscardHandler))
}

func testPassphrase() []byte {
	passphrase := make([]byte, constants.PassphraseSize)
	for i := range passphrase {
		passphrase[i] = byte(i)
	}

	return passphrase
}

func TestServerSealUnsealRoundTrip(t *testing.T) {
	t.Parallel()

	server := newTestServer()
	ctx := context.Background()
	passphrase := testPassphrase()

	sealed, err := server.Seal(ctx, &kmsapi.Request{NodeUuid: testNodeUUID, Data: passphrase})
	require.NoError(t, err)
	assert.NotEqual(t, passphrase, sealed.GetData(), "sealed data should not equal the plaintext passphrase")

	unsealed, err := server.Unseal(ctx, &kmsapi.Request{NodeUuid: testNodeUUID, Data: sealed.GetData()})
	require.NoError(t, err)
	assert.Equal(t, passphrase, unsealed.GetData())
}

func TestServerSealDifferentNodesUseDifferentKeys(t *testing.T) {
	t.Parallel()

	server := newTestServer()
	ctx := context.Background()
	passphrase := testPassphrase()

	sealedA, err := server.Seal(ctx, &kmsapi.Request{NodeUuid: "node-a", Data: passphrase})
	require.NoError(t, err)

	// A sealed the passphrase for "node-a"; unsealing that same blob under
	// "node-b" must fail, since each node gets its own key.
	_, err = server.Unseal(ctx, &kmsapi.Request{NodeUuid: "node-b", Data: sealedA.GetData()})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestServerSealRejectsWrongSizedData(t *testing.T) {
	t.Parallel()

	server := newTestServer()

	_, err := server.Seal(context.Background(), &kmsapi.Request{NodeUuid: testNodeUUID, Data: []byte("too short")})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestServerUnsealRejectsTruncatedData(t *testing.T) {
	t.Parallel()

	server := newTestServer()

	_, err := server.Unseal(context.Background(), &kmsapi.Request{NodeUuid: testNodeUUID, Data: []byte("x")})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
