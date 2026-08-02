package taloscluster //nolint:testpackage // exercises unexported loadAddonDefaults/builtinAddonNames directly

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadAddonDefaultsReturnsErrorOnMissingFile(t *testing.T) {
	t.Parallel()

	_, err := loadAddonDefaults("does-not-exist")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does-not-exist.yaml")
}

func TestLoadAddonDefaultsBuiltins(t *testing.T) {
	t.Parallel()

	for _, name := range builtinAddonNames() {
		def, err := loadAddonDefaults(name)
		require.NoError(t, err)
		assert.NotEmpty(t, def.Chart.Repo)
		assert.NotEmpty(t, def.Namespace)
	}
}
