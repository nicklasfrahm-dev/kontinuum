package fabricmanager_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/cli/fabricmanager"
)

func TestNewCmdRegistersRunSubcommand(t *testing.T) {
	t.Parallel()

	cmd := fabricmanager.NewCmd()
	assert.Equal(t, "fabricmanager", cmd.Use)

	runCmd, _, err := cmd.Find([]string{"run"})
	require.NoError(t, err)
	assert.Equal(t, "run", runCmd.Use)
}
