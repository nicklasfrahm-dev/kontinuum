package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/kommodity-io/kommodity/pkg/libkapi"
	"google.golang.org/grpc/grpclog"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	instancedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
	"github.com/nicklasfrahm/kontinuum/pkg/logging"
)

const (
	// hubReadyTimeout bounds how long an in-process hub may take to serve.
	hubReadyTimeout = 2 * time.Minute
	// hubReadyInterval is how often a starting hub is checked on.
	hubReadyInterval = 500 * time.Millisecond
	// probeTimeout bounds one maintenance-mode probe.
	probeTimeout = 15 * time.Second
	// identitySuffixBytes is how much randomness tells two up runs apart.
	identitySuffixBytes = 4
)

// errHubExited is a starting hub's error for one that stopped serving
// before it ever became ready.
var errHubExited = errors.New("in-process hub exited before becoming ready")

// productionUpDeps wires RunUp to the real world — the in-process hub,
// Talos maintenance-mode probing and the CLI's own version.
func productionUpDeps() UpDeps {
	discoverer := instancedomain.NewTalosDiscoverer()

	return UpDeps{
		StartHub: func(ctx context.Context, cfg *config.Config, controllers bool) (UpHub, error) {
			return startUpHub(ctx, cfg, controllers)
		},
		Probe: func(ctx context.Context, address string) error {
			probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
			defer cancel()

			_, err := discoverer.Discover(probeCtx, address, false)
			if err != nil {
				return fmt.Errorf("not answering in talos maintenance mode: %w", err)
			}

			return nil
		},
		Version: version,
	}
}

// upHub is a running in-process hub — see startUpHub.
type upHub struct {
	server    *libkapi.Server
	client    client.Client
	identity  string
	logger    *slog.Logger
	serveErr  chan error
	cleanup   func()
	socketDir string
}

// Client implements UpHub.
func (h *upHub) Client() client.Client {
	return h.client
}

// Identity implements UpHub.
func (h *upHub) Identity() string {
	return h.identity
}

// Stop implements UpHub. A graceful shutdown that runs over its deadline
// is only logged: everything up writes is a synchronous API call that has
// already completed by the time it stops a hub, and the process exits
// right after.
func (h *upHub) Stop() error {
	err := shutdownServer(h.server, h.logger)
	if err != nil {
		h.logger.Warn("In-process hub did not shut down gracefully", "error", err)
	}

	select {
	case serveErr := <-h.serveErr:
		if serveErr != nil {
			h.logger.Warn("In-process hub exited with an error", "error", serveErr)
		}
	case <-time.After(shutdownTimeout):
		h.logger.Warn("In-process hub did not exit in time")
	}

	h.cleanup()

	err = os.RemoveAll(h.socketDir)
	if err != nil {
		return fmt.Errorf("failed to remove socket directory: %w", err)
	}

	return nil
}

// startUpHub starts kontinuum up's in-process hub: the same apiserver and
// controllers serve runs, against cfg's shared storage, but bound to
// loopback, never registering a Kontinuum, and never taking the
// fleet-wide lease (see zonelease.Identity.Temporary). Everything it writes
// lands in the shared storage immediately, so a killed up loses nothing;
// once it stops, its zone leases expire and the zone peers carry on.
func startUpHub(ctx context.Context, cfg *config.Config, controllers bool) (*upHub, error) {
	err := configureHelmHome()
	if err != nil {
		return nil, err
	}

	// up's own process logs default to warnings only; KONTINUUM_LOG_LEVEL
	// raises them for debugging, like it does for serve.
	rawLevel := os.Getenv("KONTINUUM_LOG_LEVEL")
	if rawLevel == "" {
		rawLevel = "warn"
	}

	level, err := logging.ParseLevel(rawLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to parse KONTINUUM_LOG_LEVEL: %w", err)
	}

	logger := logging.New(level, logging.FormatConsole, os.Stderr)
	grpclog.SetLoggerV2(logging.NewGRPCLogger(logger))

	addr, err := freeLoopbackAddr(ctx)
	if err != nil {
		return nil, err
	}

	hubConfig := *cfg
	hubConfig.Server.Addr = addr

	socketDir, err := os.MkdirTemp("", "kontinuum-up-")
	if err != nil {
		return nil, fmt.Errorf("failed to create socket directory: %w", err)
	}

	identity := upIdentity()
	started := make(chan struct{})

	server, _, cleanup, err := buildServerWith(context.WithoutCancel(ctx), &hubConfig, logger, nil, nil, serverProfile{
		zoneLease:   zonelease.Identity{HolderIdentity: identity, HubLess: true, Temporary: true},
		storageMode: config.StorageModeDirect,
		register:    false,
		controllers: controllers,
		socketDir:   socketDir,
		started:     started,
	})
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(socketDir))
	}

	hub := &upHub{
		server: server, identity: identity, logger: logger, serveErr: make(chan error, 1),
		cleanup: cleanup, socketDir: socketDir,
	}

	go func() { hub.serveErr <- server.ListenAndServe(context.WithoutCancel(ctx)) }()

	// A hub that never became ready isn't shut down: stopping the
	// apiserver while its own post-start hooks still run makes it exit the
	// whole process, hiding this error. up exits right after anyway.
	hub.client, err = waitForHub(ctx, addr, started, hub.serveErr)
	if err != nil {
		return nil, err
	}

	return hub, nil
}

// waitForHub builds a client against the hub at addr and waits until it
// is fully started — every post-start hook run (started closed), /readyz
// passing, and the Zone and Kontinuum kinds served — or serveErr reports
// it stopped. Using or stopping it any earlier races the apiserver's own
// post-start hooks.
func waitForHub(
	ctx context.Context, addr string, started <-chan struct{}, serveErr chan error,
) (client.Client, error) {
	scheme, err := buildScheme()
	if err != nil {
		return nil, err
	}

	hubClient, err := client.New(&rest.Config{Host: "http://" + addr}, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to build in-process hub client: %w", err)
	}

	readyCtx, cancel := context.WithTimeout(ctx, hubReadyTimeout)
	defer cancel()

	err = waitForStarted(readyCtx, started, serveErr)
	if err != nil {
		return nil, err
	}

	for {
		err = hubReady(readyCtx, addr, hubClient)
		if err == nil {
			return hubClient, nil
		}

		select {
		case exitErr := <-serveErr:
			serveErr <- exitErr

			return nil, fmt.Errorf("%w: %w", errHubExited, exitErr)
		case <-readyCtx.Done():
			return nil, fmt.Errorf("failed to reach in-process hub: %w", err)
		case <-time.After(hubReadyInterval):
		}
	}
}

// waitForStarted waits until started is closed — every post-start hook
// run — or serveErr reports the hub stopped, or ctx expires.
func waitForStarted(ctx context.Context, started <-chan struct{}, serveErr chan error) error {
	select {
	case <-started:
		return nil
	case exitErr := <-serveErr:
		serveErr <- exitErr

		return fmt.Errorf("%w: %w", errHubExited, exitErr)
	case <-ctx.Done():
		return fmt.Errorf("failed to start in-process hub: %w", ctx.Err())
	}
}

// errHubNotReady is hubReady's error for a /readyz that doesn't pass yet.
var errHubNotReady = errors.New("in-process hub is not ready yet")

// hubReady checks one time whether the hub at addr is fully started.
func hubReady(ctx context.Context, addr string, hubClient client.Client) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/readyz", nil)
	if err != nil {
		return fmt.Errorf("failed to build readiness request: %w", err)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("failed to check readiness: %w", err)
	}

	_ = response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: /readyz returned %d", errHubNotReady, response.StatusCode)
	}

	err = hubClient.List(ctx, &v1alpha2.ZoneList{})
	if err != nil {
		return fmt.Errorf("failed to list zones: %w", err)
	}

	err = hubClient.List(ctx, &v1alpha2.KontinuumList{})
	if err != nil {
		return fmt.Errorf("failed to list kontinuums: %w", err)
	}

	return nil
}

// freeLoopbackAddr picks a free loopback port for the in-process hub.
func freeLoopbackAddr(ctx context.Context) (string, error) {
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("failed to pick a loopback port: %w", err)
	}

	addr, isTCP := listener.Addr().(*net.TCPAddr)

	closeErr := listener.Close()
	if closeErr != nil {
		return "", fmt.Errorf("failed to release loopback port: %w", closeErr)
	}

	if !isTCP {
		return "", fmt.Errorf("%w: %s", errHubExited, listener.Addr())
	}

	return net.JoinHostPort("127.0.0.1", strconv.Itoa(addr.Port)), nil
}

// upIdentity names one up run's zonelease holder, distinct per run so two
// concurrent ups are just two peers.
func upIdentity() string {
	suffix := make([]byte, identitySuffixBytes)

	_, err := rand.Read(suffix)
	if err != nil {
		return "kontinuum-up"
	}

	host, _ := os.Hostname()

	return "kontinuum-up-" + host + "-" + hex.EncodeToString(suffix)
}
