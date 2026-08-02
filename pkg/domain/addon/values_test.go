package addon //nolint:testpackage // exercises unexported loadAddonDefaults/builtinAddonNames directly

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
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

func TestReleaseNameDefaultsToMetadataName(t *testing.T) {
	t.Parallel()

	unset := &v1alpha2.Addon{ObjectMeta: metav1.ObjectMeta{Name: "cilium"}}
	assert.Equal(t, "cilium", ReleaseName(unset))

	explicit := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: "eu-1a-cilium"},
		Spec:       v1alpha2.AddonSpec{ReleaseName: "cilium"},
	}
	assert.Equal(t, "cilium", ReleaseName(explicit))
}
