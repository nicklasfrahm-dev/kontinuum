package registry_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	fakeapiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
)

// testCABundle is a function, not a package-level []byte var, purely to
// satisfy the no-global-mutable-state lint rule — the value itself is a
// fixed test fixture, never mutated.
func testCABundle() []byte {
	return []byte("test-ca-bundle")
}

// findVersion returns the CustomResourceDefinitionVersion named name from
// versions. Tests look versions up by name, not index, because their order
// in spec.versions is controller-gen's own (currently alphabetical, see
// config/crd's generated manifest) — an implementation detail of the
// generator, not something registry.CustomResourceDefinition's callers
// should depend on.
func findVersion(
	t *testing.T, versions []apiextensionsv1.CustomResourceDefinitionVersion, name string,
) apiextensionsv1.CustomResourceDefinitionVersion {
	t.Helper()

	for _, version := range versions {
		if version.Name == name {
			return version
		}
	}

	t.Fatalf("version %q not found in %#v", name, versions)

	return apiextensionsv1.CustomResourceDefinitionVersion{}
}

func TestCustomResourceDefinition(t *testing.T) {
	t.Parallel()

	crd := registry.CustomResourceDefinition(testCABundle())

	assert.Equal(t, "kontinuums.kontinuum.sh", crd.Name)
	assert.Equal(t, v1alpha2.GroupName, crd.Spec.Group)
	assert.Equal(t, apiextensionsv1.NamespaceScoped, crd.Spec.Scope)
	assert.Equal(t, "Kontinuum", crd.Spec.Names.Kind)
	assert.Equal(t, "KontinuumList", crd.Spec.Names.ListKind)
	assert.Equal(t, "kontinuums", crd.Spec.Names.Plural)
	assert.Equal(t, "kontinuum", crd.Spec.Names.Singular)

	require.Len(t, crd.Spec.Versions, 2)

	version := findVersion(t, crd.Spec.Versions, v1alpha2.APIVersion)

	assert.Equal(t, v1alpha2.APIVersion, version.Name)
	assert.True(t, version.Served)
	assert.True(t, version.Storage)
	require.NotNil(t, version.Subresources)
	assert.NotNil(t, version.Subresources.Status)

	require.NotNil(t, version.Schema)
	require.NotNil(t, version.Schema.OpenAPIV3Schema)

	specSchema, hasSpec := version.Schema.OpenAPIV3Schema.Properties["spec"]
	require.True(t, hasSpec)
	assert.Contains(t, specSchema.Properties, "region")
	assert.Contains(t, specSchema.Properties, "zone")

	statusSchema, hasStatus := version.Schema.OpenAPIV3Schema.Properties["status"]
	require.True(t, hasStatus)
	assert.Contains(t, statusSchema.Properties, "role")
	assert.Contains(t, statusSchema.Properties, "lastHeartbeatTime")
	assert.Contains(t, statusSchema.Properties, "version")
	assert.Contains(t, statusSchema.Properties, "secretRef")

	wantColumns := []apiextensionsv1.CustomResourceColumnDefinition{
		{Name: "Role", Type: "string", JSONPath: ".status.role"},
		{Name: "Region", Type: "string", JSONPath: ".spec.region"},
		{Name: "Zone", Type: "string", JSONPath: ".spec.zone"},
		{Name: "Version", Type: "string", JSONPath: ".status.version"},
	}
	assert.Equal(t, wantColumns, version.AdditionalPrinterColumns)
}

// TestCustomResourceDefinitionConversionWebhook covers the conversion
// webhook config that lets v1alpha1 keep working (see
// TestCustomResourceDefinitionLegacyVersion) despite no longer being the
// storage version.
func TestCustomResourceDefinitionConversionWebhook(t *testing.T) {
	t.Parallel()

	crd := registry.CustomResourceDefinition(testCABundle())

	require.NotNil(t, crd.Spec.Conversion)
	assert.Equal(t, apiextensionsv1.WebhookConverter, crd.Spec.Conversion.Strategy)
	require.NotNil(t, crd.Spec.Conversion.Webhook)
	require.NotNil(t, crd.Spec.Conversion.Webhook.ClientConfig)
	require.NotNil(t, crd.Spec.Conversion.Webhook.ClientConfig.URL)
	assert.Equal(t, "https://localhost:9443/convert", *crd.Spec.Conversion.Webhook.ClientConfig.URL)
	assert.Equal(t, testCABundle(), crd.Spec.Conversion.Webhook.ClientConfig.CABundle)
	assert.Equal(t, []string{"v1"}, crd.Spec.Conversion.Webhook.ConversionReviewVersions)
}

// TestCustomResourceDefinitionLegacyVersion covers the v1alpha1 entry: kept
// in spec.versions, served (converted through the webhook), but no longer
// the storage version.
func TestCustomResourceDefinitionLegacyVersion(t *testing.T) {
	t.Parallel()

	crd := registry.CustomResourceDefinition(testCABundle())

	require.Len(t, crd.Spec.Versions, 2)

	legacy := findVersion(t, crd.Spec.Versions, v1alpha1.APIVersion)

	assert.Equal(t, v1alpha1.APIVersion, legacy.Name)
	assert.True(t, legacy.Served, "v1alpha1 stays served — the conversion webhook makes it fully usable again")
	assert.False(t, legacy.Storage)
	require.NotNil(t, legacy.Subresources)
	assert.NotNil(t, legacy.Subresources.Status)
	require.NotNil(t, legacy.Schema)
	require.NotNil(t, legacy.Schema.OpenAPIV3Schema)

	legacySpecSchema, hasLegacySpec := legacy.Schema.OpenAPIV3Schema.Properties["spec"]
	require.True(t, hasLegacySpec)
	assert.Equal(t, []string{"role"}, legacySpecSchema.Required)
	assert.Contains(t, legacySpecSchema.Properties, "role")
}

// TestApplyCRDCreatesWhenMissing covers the common case: no CRD registered
// yet, so ApplyCRD's Create succeeds outright.
func TestApplyCRDCreatesWhenMissing(t *testing.T) {
	t.Parallel()

	crds := fakeapiextensions.NewSimpleClientset().ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, registry.ApplyCRD(context.Background(), crds, testCABundle()))

	crd, err := crds.Get(context.Background(), registry.CustomResourceDefinition(testCABundle()).Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, registry.CustomResourceDefinition(testCABundle()).Spec, crd.Spec)
}

// TestApplyCRDUpdatesStaleDefinition is the regression case: a CRD left
// over from a previous run — e.g. one predating the conversion webhook —
// must be reconciled to the current spec rather than left in place, which
// is what Create alone did.
func TestApplyCRDUpdatesStaleDefinition(t *testing.T) {
	t.Parallel()

	stale := registry.CustomResourceDefinition(testCABundle())
	stale.Spec.Versions = stale.Spec.Versions[:1]
	stale.Spec.Versions[0].Name = "v1alpha0"

	crds := fakeapiextensions.NewSimpleClientset(stale).ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, registry.ApplyCRD(context.Background(), crds, testCABundle()))

	crd, err := crds.Get(context.Background(), registry.CustomResourceDefinition(testCABundle()).Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, registry.CustomResourceDefinition(testCABundle()).Spec, crd.Spec)
}

// TestApplyCRDNoopWhenAlreadyCurrent asserts ApplyCRD skips the Update call
// once the stored spec already matches — otherwise every retry in
// ensureCRD's poll loop would churn the CRD's resourceVersion forever.
func TestApplyCRDNoopWhenAlreadyCurrent(t *testing.T) {
	t.Parallel()

	current := registry.CustomResourceDefinition(testCABundle())

	crds := fakeapiextensions.NewSimpleClientset(current).ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, registry.ApplyCRD(context.Background(), crds, testCABundle()))

	crd, err := crds.Get(context.Background(), current.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, current.ResourceVersion, crd.ResourceVersion,
		"spec already matched the desired definition, so no update should have been issued")
}
