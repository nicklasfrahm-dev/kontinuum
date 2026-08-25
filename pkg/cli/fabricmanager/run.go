package fabricmanager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/nicklasfrahm/kontinuum/pkg/logging"
)

// nodeNameEnvVar is the Downward API env var (fieldRef: spec.nodeName —
// see pkg/domain/zone.buildFabricManagerDaemonSet's own Pod spec) this
// node's own name is read from — the same string Talos sets as this
// Kubernetes Node's own metadata.name (see Reconciler.NodeName's own
// doc), so it always matches exactly whatever a gatewayNodeRef electing
// this node names.
const nodeNameEnvVar = "NODE_NAME"

// errNodeNameNotSet is NewRunCmd's sentinel for a missing nodeNameEnvVar —
// only reachable if this process is run outside the DaemonSet this
// package's own controller expects (see nodeNameEnvVar's own doc), never
// in normal operation.
var errNodeNameNotSet = fmt.Errorf("%s must be set (see the Downward API spec.nodeName field)", nodeNameEnvVar)

// NewRunCmd builds the "fabricmanager run" command. No flags: this
// process discovers everything it needs by watching Fabric objects
// through this node's own downstream kontinuum-server (see
// newInClusterConfig's own doc) rather than being told a specific
// Fabric/interface up front — a gateway node re-elected away, or a
// second Fabric electing this same node for a different interface, is
// something this process now notices on its own, live, instead of
// needing pkg/domain/fabric's own controller to rebuild a Deployment
// with new --id/--interface args every time. Only NAT is implemented
// today (see this package's own doc) — future zone network duties
// (DHCP, ...) are expected to extend this same reconcile loop, not spawn
// a second, separately named agent.
func NewRunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Watch this zone's own Fabrics and reconcile this node's own NAT gateway state",
		Long: "Enables ipv4 forwarding, then watches every Fabric visible through this " +
			"zone's own downstream kontinuum-server and keeps this node's own nftables " +
			"masquerade rules in sync with whichever Fabric(s) currently elect it as " +
			"their own gateway (see Reconciler's own doc) — until ctx is canceled or a " +
			"termination signal is received.",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runFabricManager(cmd.Context())
		},
	}

	return cmd
}

// runFabricManager enables ipv4 forwarding, then builds and runs a
// controller-runtime manager around Reconciler until ctx is canceled or a
// termination signal is received — controller-runtime's own manager
// already handles that signal wiring internally (see ctrl.Manager.Start),
// mirroring pkg/cli.runServe's own use of the identical machinery.
func runFabricManager(ctx context.Context) error {
	logger := logging.New(slog.LevelInfo, logging.FormatJSON, os.Stdout)

	nodeName := os.Getenv(nodeNameEnvVar)
	if nodeName == "" {
		return errNodeNameNotSet
	}

	err := enableIPForwarding()
	if err != nil {
		return fmt.Errorf("failed to enable ipv4 forwarding: %w", err)
	}

	restCfg, err := newInClusterConfig()
	if err != nil {
		return err
	}

	scheme, err := fabricScheme()
	if err != nil {
		return err
	}

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		return fmt.Errorf("failed to build controller-runtime manager: %w", err)
	}

	reconciler := &Reconciler{Client: mgr.GetClient(), NodeName: nodeName, Logger: logger}

	err = reconciler.SetupWithManager(mgr)
	if err != nil {
		return err
	}

	logger.Info("fabricmanager starting", "node", nodeName)

	err = mgr.Start(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("controller-runtime manager exited: %w", err)
	}

	return nil
}
