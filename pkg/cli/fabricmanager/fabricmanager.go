// Package fabricmanager implements kontinuum's "fabricmanager" cobra
// command tree (currently just "fabricmanager run") — the node-local
// network agent pkg/domain/zone.ensureFabricManagerDaemonSet installs as
// a DaemonSet onto every node of a zone's own downstream cluster (not a
// per-gateway Deployment: see that function's own doc for why one
// standing daemon, self-discovering which Fabric(s) it's actually
// responsible for, replaced the earlier per-elected-node-and-interface
// model). Named for that growing scope rather than "nat-gateway": NAT is
// the only thing it does today, but DHCP and other per-zone network
// responsibilities are expected to land here as further subcommands/
// reconcile logic later, not as a second, separately named agent.
//
// This repo's own container image is distroless
// (gcr.io/distroless/static-debian13, no shell, no nft(8) binary — see
// Containerfile), so "run nft(8) as a subprocess" isn't an option for the
// NAT piece; "run" instead programs the kernel's nftables ruleset directly
// over netlink, via github.com/google/nftables — a pure Go client library
// with no external binary or CGO dependency, matching this repo's
// CGO_ENABLED=0 build.
package fabricmanager

import (
	"github.com/spf13/cobra"
)

// NewCmd builds the fabricmanager command, which groups this node agent's
// own subcommands.
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fabricmanager",
		Short: "Run this node as a fabric zone's own network agent",
	}

	cmd.AddCommand(NewRunCmd())

	return cmd
}
