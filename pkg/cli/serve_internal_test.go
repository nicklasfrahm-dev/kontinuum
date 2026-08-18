package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConfigureHelmHomePointsUnderTempDir guards the fix for kontinuum#103:
// on Cloud Run and every zone's own downstream Deployment, this process
// runs under a read-only root filesystem with only os.TempDir() writable,
// so Helm's own cache/config/data dirs must never fall back to $HOME.
//
//nolint:paralleltest // configureHelmHome mutates process-wide env vars; a parallel run would race t.Setenv
func TestConfigureHelmHomePointsUnderTempDir(t *testing.T) {
	envVars := []string{"HELM_CACHE_HOME", "HELM_CONFIG_HOME", "HELM_DATA_HOME"}
	for _, envVar := range envVars {
		t.Setenv(envVar, os.Getenv(envVar))
	}

	err := configureHelmHome()
	require.NoError(t, err)

	for _, envVar := range envVars {
		dir := os.Getenv(envVar)
		require.NotEmpty(t, dir, "%s must be set", envVar)
		require.True(t, strings.HasPrefix(dir, os.TempDir()), "%s=%q must live under os.TempDir()=%q",
			envVar, dir, os.TempDir())

		info, err := os.Stat(dir) //nolint:gosec // dir comes from configureHelmHome, not untrusted input
		require.NoErrorf(t, err, "%s=%q must exist", envVar, dir)
		require.True(t, info.IsDir(), "%s=%q must be a directory", envVar, dir)
	}

	// The three dirs must be distinct — otherwise Helm's own cache, config,
	// and data files (credentials.json, repositories.yaml, chart caches)
	// would collide in a single directory.
	require.NotEqual(t, os.Getenv("HELM_CACHE_HOME"), os.Getenv("HELM_CONFIG_HOME"))
	require.NotEqual(t, os.Getenv("HELM_CACHE_HOME"), os.Getenv("HELM_DATA_HOME"))
	require.NotEqual(t, os.Getenv("HELM_CONFIG_HOME"), os.Getenv("HELM_DATA_HOME"))

	require.Equal(t, filepath.Join(os.TempDir(), "kontinuum-helm", "cache"), os.Getenv("HELM_CACHE_HOME"))
}
