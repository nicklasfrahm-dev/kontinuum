package fabricmanager

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/nicklasfrahm/kontinuum/pkg/logging"
)

// NewRunCmd builds the "fabricmanager run" command. Only NAT is
// implemented today (see this package's own doc) — future zone network
// duties (DHCP, ...) are expected to extend this same command with more
// flags, not spawn a second, separately named one.
func NewRunCmd() *cobra.Command {
	var fabricID string

	var iface string

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Enable ipv4 forwarding and masquerade outbound traffic through --interface",
		Long: "Enables ipv4 forwarding and installs an nftables masquerade rule so " +
			"every packet leaving this node through --interface is source-NATed to " +
			"this node's own address, then blocks until terminated — the process " +
			"itself is the workload's own liveness signal (see " +
			"pkg/domain/fabric.ensureFabricManagerWorkload); the rule is removed on a " +
			"graceful shutdown.",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runFabricManager(cmd.Context(), fabricID, iface)
		},
	}

	cmd.Flags().StringVar(&fabricID, "id", "",
		"Owning Fabric's own metadata.name, scoping this process's nftables table so it "+
			"never collides with a different Fabric electing the same node/interface")
	cmd.Flags().StringVar(&iface, "interface", "", "Uplink network interface to masquerade outbound traffic through")

	for _, name := range []string{"id", "interface"} {
		err := cmd.MarkFlagRequired(name)
		if err != nil {
			// Only reachable if a flag name above is misspelled — a defect
			// in this file itself, not a condition any caller could
			// meaningfully recover from.
			panic(fmt.Sprintf("failed to mark --%s required: %v", name, err))
		}
	}

	return cmd
}

// runFabricManager enables ipv4 forwarding, installs the masquerade rule,
// then blocks until ctx is canceled or a termination signal is received —
// mirrors pkg/cli.runServe's own signal-handling shape.
func runFabricManager(ctx context.Context, fabricID, iface string) error {
	logger := logging.New(slog.LevelInfo, logging.FormatJSON, os.Stdout)

	err := enableIPForwarding()
	if err != nil {
		return fmt.Errorf("failed to enable ipv4 forwarding: %w", err)
	}

	err = ensureMasquerade(fabricID, iface)
	if err != nil {
		return fmt.Errorf("failed to configure nftables masquerade rule: %w", err)
	}

	logger.Info("NAT gateway configured", "fabric", fabricID, "interface", iface)

	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	select {
	case sig := <-sigChan:
		logger.Info("Received signal, shutting down", "signal", sig.String())
	case <-ctx.Done():
		logger.Info("Context canceled, shutting down")
	}

	err = deleteMasquerade(fabricID, iface)
	if err != nil {
		logger.Warn("Failed to remove nftables masquerade rule on shutdown", "error", err)
	}

	return nil
}
