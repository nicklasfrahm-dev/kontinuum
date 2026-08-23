// Package natgateway implements kontinuum's "nat-gateway" cobra command
// tree (currently just "nat-gateway run") — the workload
// pkg/domain/fabric's own Fabric controller deploys onto a zone's elected
// gateway node (see that package's ensureNATGatewayWorkload) to actually
// make NAT work: this repo's own container image is distroless
// (gcr.io/distroless/static-debian13, no shell, no nft(8) binary — see
// Containerfile), so "run nft(8) as a subprocess" isn't an option; this
// subcommand instead programs the kernel's nftables ruleset directly over
// netlink, via github.com/google/nftables — a pure Go client library with
// no external binary or CGO dependency, matching this repo's
// CGO_ENABLED=0 build.
package natgateway

import (
	"github.com/spf13/cobra"
)

// NewCmd builds the nat-gateway command, which groups NAT-gateway-related
// subcommands.
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "nat-gateway",
		Short: "Run this node as a fabric zone's NAT gateway",
	}

	cmd.AddCommand(NewRunCmd())

	return cmd
}
