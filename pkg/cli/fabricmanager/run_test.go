package fabricmanager_test

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/cli/fabricmanager"
)

// TestNewRunCmdHasNoFlags is a regression test: an earlier version of this
// command required --id/--interface, telling it exactly which Fabric and
// interface to serve. It now discovers that by watching Fabric objects
// instead (see Reconciler's own doc), so it takes no flags at all.
func TestNewRunCmdHasNoFlags(t *testing.T) {
	t.Parallel()

	cmd := fabricmanager.NewRunCmd()

	flagCount := 0

	cmd.Flags().VisitAll(func(*pflag.Flag) { flagCount++ })
	assert.Zero(t, flagCount)
}

// TestNewRunCmdRequiresNodeNameEnvVar exercises the one thing RunE checks
// before doing anything else: the Downward API-projected NODE_NAME env var
// this process needs to know which gatewayNodeRef is its own (see
// Reconciler.NodeName's own doc). Unset in this test process, so
// Execute() fails immediately without ever reaching enableIPForwarding or
// dialing an in-cluster apiserver — safe to run without root or a real
// cluster.
func TestNewRunCmdRequiresNodeNameEnvVar(t *testing.T) {
	t.Setenv("NODE_NAME", "")

	cmd := fabricmanager.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetArgs([]string{})

	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NODE_NAME")
}
