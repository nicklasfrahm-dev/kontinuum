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

	for _, name := range addonNamesWithDefaults() {
		def, err := loadAddonDefaults(name)
		require.NoError(t, err)
		assert.True(t, def.Chart.Repo != "" || def.Chart.Name != "",
			"%s: chart repo or name (for an OCI ref) must be set", name)
		assert.NotEmpty(t, def.Namespace.Name)
	}
}

func TestExternalDNSAddonNotAutoSeeded(t *testing.T) {
	t.Parallel()

	assert.NotContains(t, builtinAddonNames(), externalDNSAddonName,
		"external-dns must stay opt-in, not auto-seeded onto every zone")
	assert.Contains(t, addonNamesWithDefaults(), externalDNSAddonName,
		"external-dns must still resolve via its own embedded defaults when an operator creates it")
}

func TestExternalDNSAddonDefaultsUseCRDSource(t *testing.T) {
	t.Parallel()

	def, err := loadAddonDefaults(externalDNSAddonName)
	require.NoError(t, err)

	sources, ok := def.Values["sources"].([]any)
	require.True(t, ok, "external-dns defaults must set values.sources")
	assert.Contains(t, sources, "crd")
}

func TestResolveAddonChartOCIReferenceNeedsNoRepo(t *testing.T) {
	t.Parallel()

	def, err := loadAddonDefaults(gatewayAPICRDsAddonName)
	require.NoError(t, err)

	spec := v1alpha2.AddonSpec{ReleaseName: gatewayAPICRDsAddonName}

	repo, chartName, version, err := resolveAddonChart(spec, def)
	require.NoError(t, err)
	assert.Empty(t, repo)
	assert.Equal(t, "oci://docker.io/envoyproxy/gateway-crds-helm", chartName)
	assert.NotEmpty(t, version)
}

func TestEffectivePriorityResolution(t *testing.T) {
	t.Parallel()

	gatewayCRDs := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: gatewayAPICRDsAddonName},
	}

	priority, err := EffectivePriority(gatewayCRDs)
	require.NoError(t, err)
	assert.Less(t, priority, defaultAddonPriority,
		"gateway-api-crds must default to a lower priority than the global default")

	custom := &v1alpha2.Addon{ObjectMeta: metav1.ObjectMeta{Name: "my-addon"}}

	priority, err = EffectivePriority(custom)
	require.NoError(t, err)
	assert.Equal(t, defaultAddonPriority, priority,
		"a non-built-in addon with no explicit priority defaults to the global default")

	overridden := int32(7)
	custom.Spec.Lifecycle.Priority = &overridden

	priority, err = EffectivePriority(custom)
	require.NoError(t, err)
	assert.Equal(t, overridden, priority, "an explicit spec.lifecycle.priority always wins")
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
