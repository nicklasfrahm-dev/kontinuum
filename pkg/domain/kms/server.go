package kms

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kmsapi "github.com/siderolabs/kms-client/api/kms"
	"github.com/siderolabs/kms-client/pkg/constants"
)

// aesKeySize is the size, in bytes, of the AES-256 key Server generates for
// each node.
const aesKeySize = 32

// Server is a dummy implementation of kmsapi.KMSServiceServer — the gRPC
// service Talos's KMS disk-encryption client calls to Seal/Unseal a node's
// disk-encryption passphrase. See the package doc for why it's a dummy: keys
// are generated on first use and held only in memory, never persisted.
type Server struct {
	kmsapi.UnimplementedKMSServiceServer

	// Addr is the "host:port" Start listens on.
	Addr string
	// Logger receives one entry per Seal/Unseal call.
	Logger *slog.Logger

	mu   sync.Mutex
	keys map[string][]byte
}

// New creates a Server listening on addr, logging through logger.
func New(addr string, logger *slog.Logger) *Server {
	return &Server{
		Addr:   addr,
		Logger: logger,
		keys:   make(map[string][]byte),
	}
}

// Start implements manager.Runnable: it listens on s.Addr and serves the KMS
// gRPC service until ctx is canceled, then stops gracefully.
func (s *Server) Start(ctx context.Context) error {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %q: %w", s.Addr, err)
	}

	return s.Serve(ctx, listener)
}

// Serve runs the KMS gRPC service on listener until ctx is canceled, then
// stops gracefully. Split out from Start so tests can supply a listener
// already bound to a known address, rather than depending on whatever port
// s.Addr (":0" in tests) happens to resolve to.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	grpcServer := grpc.NewServer()
	kmsapi.RegisterKMSServiceServer(grpcServer, s)

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		grpcServer.GracefulStop()

		return nil
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("kms grpc server exited: %w", err)
		}

		return nil
	}
}

// Seal encrypts req.Data — a disk-encryption passphrase — with the
// AES-256-GCM key for req.NodeUuid, generating that key on first use.
func (s *Server) Seal(_ context.Context, req *kmsapi.Request) (*kmsapi.Response, error) {
	if len(req.GetData()) != constants.PassphraseSize {
		return nil, status.Errorf(codes.InvalidArgument, "%s: got %d bytes, want %d",
			ErrInvalidPassphraseSize, len(req.GetData()), constants.PassphraseSize)
	}

	aesgcm, err := s.cipherFor(req.GetNodeUuid())
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, aesgcm.NonceSize())

	_, err = io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	sealed := aesgcm.Seal(nonce, nonce, req.GetData(), nil)

	s.Logger.Debug("Sealed passphrase", "node_uuid", req.GetNodeUuid())

	return &kmsapi.Response{Data: sealed}, nil
}

// Unseal decrypts req.Data with the AES-256-GCM key for req.NodeUuid,
// generating that key on first use — which, for Unseal, only ever happens
// if this Server was restarted (losing its keys) since the matching Seal
// call, and so authentication below fails and an error is returned rather
// than a decrypted passphrase.
func (s *Server) Unseal(_ context.Context, req *kmsapi.Request) (*kmsapi.Response, error) {
	aesgcm, err := s.cipherFor(req.GetNodeUuid())
	if err != nil {
		return nil, err
	}

	nonceSize := aesgcm.NonceSize()
	if len(req.GetData()) < nonceSize {
		return nil, status.Error(codes.InvalidArgument, ErrSealedDataTooShort.Error())
	}

	nonce, ciphertext := req.GetData()[:nonceSize], req.GetData()[nonceSize:]

	data, err := aesgcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s: %s", ErrAuthenticationFailed, err)
	}

	s.Logger.Debug("Unsealed passphrase", "node_uuid", req.GetNodeUuid())

	return &kmsapi.Response{Data: data}, nil
}

// cipherFor returns an AES-256-GCM AEAD keyed with nodeUUID's key,
// generating and storing that key on first use (see keyFor).
func (s *Server) cipherFor(nodeUUID string) (cipher.AEAD, error) {
	key, err := s.keyFor(nodeUUID)
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES-GCM: %w", err)
	}

	return aesgcm, nil
}

// keyFor returns the AES-256 key for nodeUUID, generating and storing one on
// first use.
func (s *Server) keyFor(nodeUUID string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if key, ok := s.keys[nodeUUID]; ok {
		return key, nil
	}

	key := make([]byte, aesKeySize)

	_, err := io.ReadFull(rand.Reader, key)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key for node %q: %w", nodeUUID, err)
	}

	s.keys[nodeUUID] = key

	return key, nil
}
