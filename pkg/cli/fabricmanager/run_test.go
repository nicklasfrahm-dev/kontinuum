package fabricmanager_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/cli/fabricmanager"
)

// TestNewRunCmdRequiresIDAndInterfaceFlags exercises cobra's own required-flag
// validation, which runs before RunE — so this never reaches
// runFabricManager's real network/nftables side effects.
func TestNewRunCmdRequiresIDAndInterfaceFlags(t *testing.T) {
	t.Parallel()

	cmd := fabricmanager.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetArgs([]string{})

	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required flag")
	assert.Contains(t, err.Error(), "id")
	assert.Contains(t, err.Error(), "interface")
}

func TestNewRunCmdDeclaresIDAndInterfaceFlags(t *testing.T) {
	t.Parallel()

	cmd := fabricmanager.NewRunCmd()

	assert.NotNil(t, cmd.Flags().Lookup("id"))
	assert.NotNil(t, cmd.Flags().Lookup("interface"))
}
