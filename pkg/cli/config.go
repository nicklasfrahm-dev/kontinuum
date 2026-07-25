package cli

import (
	"github.com/spf13/cobra"
)

// NewConfigCmd builds the config command, which groups kubeconfig-related
// subcommands.
func NewConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage kubeconfig entries",
	}

	cmd.AddCommand(NewConfigImportCmd())

	return cmd
}
