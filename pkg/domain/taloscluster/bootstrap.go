package taloscluster

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"time"

	clusterapi "github.com/siderolabs/talos/pkg/machinery/api/cluster"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
)

// maintenanceModePort is the gRPC port apid listens on while a Talos node
// is unconfigured and in maintenance mode — same constant, and same
// rationale, as instance's own maintenanceModePort (see
// pkg/domain/instance/talos.go).
const maintenanceModePort = 50000

// rpcTimeout bounds every single-shot (non-streaming) RPC this package
// makes on the client side — ApplyConfiguration, Bootstrap, Kubeconfig.
// None of these calls carry their own server-side timeout field the way
// ClusterHealthCheck's WaitTimeout does, but without a client-side
// deadline a wedged or unreachable node could still block the calling
// reconcile indefinitely.
const rpcTimeout = 30 * time.Second

// ClusterBootstrapper is the Discoverer-style seam this package's
// controller dials Talos through — see instance.Discoverer's own doc for
// why this pattern exists. talosBootstrapper is the production
// implementation, dialing real nodes; tests inject a fake to avoid a real
// gRPC dial.
type ClusterBootstrapper interface {
	// ApplyConfiguration applies data (a generated machine config) to addr
	// in maintenance mode — talosctl apply-config's programmatic
	// equivalent. addr is expected to move out of maintenance mode as a
	// result (the node installs and reboots), so a failure here is often
	// just "already applied and rebooted," not a real problem — see this
	// interface's callers for how that's handled.
	ApplyConfiguration(ctx context.Context, addr string, data []byte) error
	// Bootstrap triggers etcd bootstrap on addr — talosctl bootstrap's
	// programmatic equivalent — dialing with the real (non-maintenance-mode)
	// admin identity in talosCfg.
	Bootstrap(ctx context.Context, addr string, talosCfg *clientconfig.Config) error
	// HealthCheck blocks (bounded by timeout) until the cluster reachable
	// via addr reports healthy, or returns an error if it doesn't in time.
	HealthCheck(
		ctx context.Context, addr string, talosCfg *clientconfig.Config, controlPlaneNodes []string, timeout time.Duration,
	) error
	// Kubeconfig fetches the cluster's kubeconfig via addr.
	Kubeconfig(ctx context.Context, addr string, talosCfg *clientconfig.Config) ([]byte, error)
	// Version fetches the Talos version reported by node, dialing endpoint
	// with the real (non-maintenance-mode) admin identity in talosCfg and
	// routing the request to node via client.WithNode — see this
	// interface's own Version implementation doc for why maintenance-mode
	// discovery (pkg/domain/instance's own Discoverer) can no longer learn
	// this on current Talos releases, making endpoint/node's post-config
	// identity the only place left to fetch it.
	Version(ctx context.Context, endpoint, node string, talosCfg *clientconfig.Config) (string, error)
}

// talosBootstrapper is ClusterBootstrapper's production implementation.
type talosBootstrapper struct {
	// Logger receives each HealthCheckProgress message Talos streams back
	// — the same per-check detail talosctl health -v prints (e.g. "waiting
	// for all k8s nodes to report ready"), otherwise silently discarded and
	// leaving only a generic context-deadline-exceeded error once
	// HealthCheck's own timeout elapses.
	Logger *slog.Logger
}

// NewTalosBootstrapper returns the production ClusterBootstrapper, which
// dials real Talos nodes. ClusterBootstrapper is this package's own seam
// for injecting a fake in tests — the whole point of this constructor is
// to hide talosBootstrapper behind it (mirrors
// instance.NewTalosDiscoverer's own doc).
//
//nolint:ireturn // see doc above
func NewTalosBootstrapper(logger *slog.Logger) ClusterBootstrapper {
	return talosBootstrapper{Logger: logger}
}

// ApplyConfiguration implements ClusterBootstrapper. A maintenance-mode
// node serves gRPC over a self-signed certificate with no CA yet issued —
// InsecureSkipVerify mirrors talosctl's own dial behavior against a node in
// this state, same as instance.talosDiscoverer.Discover's identical
// rationale.
func (talosBootstrapper) ApplyConfiguration(ctx context.Context, addr string, data []byte) error {
	endpoint := net.JoinHostPort(addr, strconv.Itoa(maintenanceModePort))

	talosClient, err := talosclient.New(ctx,
		//nolint:gosec // maintenance mode has no issued CA yet — see this func's doc
		talosclient.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}),
		talosclient.WithEndpoints(endpoint),
	)
	if err != nil {
		return fmt.Errorf("failed to dial %s in maintenance mode: %w", endpoint, err)
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived apply-config connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	_, err = talosClient.ApplyConfiguration(rpcCtx, &machineapi.ApplyConfigurationRequest{
		Data: data,
		Mode: machineapi.ApplyConfigurationRequest_REBOOT,
	})
	if err != nil {
		return fmt.Errorf("failed to apply configuration to %s: %w", endpoint, err)
	}

	return nil
}

// Bootstrap implements ClusterBootstrapper.
func (t talosBootstrapper) Bootstrap(ctx context.Context, addr string, talosCfg *clientconfig.Config) error {
	talosClient, err := t.dial(ctx, addr, talosCfg)
	if err != nil {
		return err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived bootstrap connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	err = talosClient.Bootstrap(rpcCtx, &machineapi.BootstrapRequest{})
	if err != nil {
		return fmt.Errorf("failed to bootstrap %s: %w", addr, err)
	}

	return nil
}

// HealthCheck implements ClusterBootstrapper. timeout bounds this call on
// the client side (via ctx) as well as being sent to the server as
// HealthCheckRequest.WaitTimeout — that field only governs how long the
// server keeps retrying internally, it is not a client-side deadline, so
// without wrapping ctx here a slow or wedged server could block this call
// (and the reconcile that invoked it) indefinitely.
func (t talosBootstrapper) HealthCheck(
	ctx context.Context, addr string, talosCfg *clientconfig.Config, controlPlaneNodes []string, timeout time.Duration,
) error {
	talosClient, err := t.dial(ctx, addr, talosCfg)
	if err != nil {
		return err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived health-check connection

	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	clusterInfo := &clusterapi.ClusterInfo{ControlPlaneNodes: controlPlaneNodes}

	stream, err := talosClient.ClusterHealthCheck(checkCtx, timeout, clusterInfo)
	if err != nil {
		return fmt.Errorf("failed to start health check against %s: %w", addr, err)
	}

	for {
		progress, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}

		if recvErr != nil {
			return fmt.Errorf("cluster health check against %s failed: %w", addr, recvErr)
		}

		t.Logger.Debug("health check progress", "address", addr, "message", progress.GetMessage())
	}
}

// Kubeconfig implements ClusterBootstrapper.
func (t talosBootstrapper) Kubeconfig(ctx context.Context, addr string, talosCfg *clientconfig.Config) ([]byte, error) {
	talosClient, err := t.dial(ctx, addr, talosCfg)
	if err != nil {
		return nil, err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived kubeconfig-fetch connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	kubeconfig, err := talosClient.Kubeconfig(rpcCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch kubeconfig from %s: %w", addr, err)
	}

	return kubeconfig, nil
}

// Version implements ClusterBootstrapper. Unlike ApplyConfiguration, this
// deliberately doesn't dial node directly in maintenance mode: recent Talos
// releases gate the maintenance-mode Version RPC behind an os:admin role
// check that no maintenance-mode caller — kontinuum's discoverer or
// talosctl alike — can ever satisfy, since there's no CA yet to issue that
// role's client cert from (see pkg/domain/instance/talos.go's own Discover
// doc). Once node has moved past maintenance mode, though, talosCfg's real
// admin identity satisfies that check, so this dials endpoint (any
// reachable, already-configured cluster member) and targets node via
// client.WithNode, the same way talosctl -n routes a single dial to any
// cluster member.
func (t talosBootstrapper) Version(
	ctx context.Context, endpoint, node string, talosCfg *clientconfig.Config,
) (string, error) {
	talosClient, err := t.dial(ctx, endpoint, talosCfg)
	if err != nil {
		return "", err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived version-fetch connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	versionResp, err := talosClient.Version(talosclient.WithNode(rpcCtx, node))
	if err != nil {
		return "", fmt.Errorf("failed to fetch talos version for %s via %s: %w", node, endpoint, err)
	}

	if messages := versionResp.GetMessages(); len(messages) > 0 {
		return messages[0].GetVersion().GetTag(), nil
	}

	return "", nil
}

// dial connects to addr with talosCfg's real (non-maintenance-mode) admin
// identity — talosCfg carries the OS CA and an admin client cert both
// signed by the cluster's own secrets bundle (see generateConfigs), so no
// InsecureSkipVerify is needed here, unlike ApplyConfiguration's
// maintenance-mode dial.
func (talosBootstrapper) dial(
	ctx context.Context, addr string, talosCfg *clientconfig.Config,
) (*talosclient.Client, error) {
	talosClient, err := talosclient.New(ctx,
		talosclient.WithConfig(talosCfg),
		talosclient.WithEndpoints(addr),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	return talosClient, nil
}
