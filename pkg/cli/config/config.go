// Package config implements kontinuum's "config" cobra command tree
// (currently just "config import"), which manages the user's local
// kubeconfig.
package config

import (
	"github.com/spf13/cobra"
)

// NewCmd builds the config command, which groups kubeconfig-related
// subcommands.
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage kubeconfig entries",
	}

	cmd.AddCommand(NewImportCmd())

	return cmd
}
