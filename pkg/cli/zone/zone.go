// Package zone implements kontinuum's "zone" cobra command tree (currently
// just "zone join"), which creates the hub-side objects that fan a new
// zone out (see pkg/domain/zone's shared BuildJoinObjects/Apply).
package zone

import (
	"github.com/spf13/cobra"
)

// NewCmd builds the zone command, which groups zone-related subcommands.
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "zone",
		Short: "Manage zones",
	}

	cmd.AddCommand(NewJoinCmd())

	return cmd
}
