// Package cli holds kontinuum's cobra command tree.
package cli

import (
	"github.com/spf13/cobra"
)

// NewRootCmd builds kontinuum's root command.
func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kontinuum",
		Short: "A Kubernetes-style API server built on kommodity's libkapi",
		Long: "Kontinuum embeds kommodity's libkapi to run a generic apiserver + " +
			"apiextensions (CRD) server + aggregation layer, backed by pluggable " +
			"storage (SQLite, PostgreSQL, etcd, ...).",
		Version: version,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			help, _ := cmd.Flags().GetBool("help")
			if help {
				return cmd.Help()
			}

			return nil
		},
	}

	cmd.AddCommand(NewServeCmd())
	cmd.AddCommand(NewVersionCmd())
	cmd.AddCommand(NewConfigCmd())

	return cmd
}
