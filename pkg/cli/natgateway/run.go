package natgateway

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

// NewRunCmd builds the "nat-gateway run" command.
func NewRunCmd() *cobra.Command {
	var iface string

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Enable ipv4 forwarding and masquerade outbound traffic through --interface",
		Long: "Enables ipv4 forwarding and installs an nftables masquerade rule so " +
			"every packet leaving this node through --interface is source-NATed to " +
			"this node's own address, then blocks until terminated — the process " +
			"itself is the workload's own liveness signal (see " +
			"pkg/domain/fabric.ensureNATGatewayWorkload); the rule is removed on a " +
			"graceful shutdown.",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runNATGateway(cmd.Context(), iface)
		},
	}

	cmd.Flags().StringVar(&iface, "interface", "", "Uplink network interface to masquerade outbound traffic through")

	err := cmd.MarkFlagRequired("interface")
	if err != nil {
		// Only reachable if the flag name above is misspelled — a defect in
		// this file itself, not a condition any caller could meaningfully
		// recover from.
		panic(fmt.Sprintf("failed to mark --interface required: %v", err))
	}

	return cmd
}

// runNATGateway enables ipv4 forwarding, installs the masquerade rule, then
// blocks until ctx is canceled or a termination signal is received —
// mirrors pkg/cli.runServe's own signal-handling shape.
func runNATGateway(ctx context.Context, iface string) error {
	logger := logging.New(slog.LevelInfo, logging.FormatJSON, os.Stdout)

	err := enableIPForwarding()
	if err != nil {
		return fmt.Errorf("failed to enable ipv4 forwarding: %w", err)
	}

	err = ensureMasquerade(iface)
	if err != nil {
		return fmt.Errorf("failed to configure nftables masquerade rule: %w", err)
	}

	logger.Info("NAT gateway configured", "interface", iface)

	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	select {
	case sig := <-sigChan:
		logger.Info("Received signal, shutting down", "signal", sig.String())
	case <-ctx.Done():
		logger.Info("Context canceled, shutting down")
	}

	err = deleteMasquerade()
	if err != nil {
		logger.Warn("Failed to remove nftables masquerade rule on shutdown", "error", err)
	}

	return nil
}
