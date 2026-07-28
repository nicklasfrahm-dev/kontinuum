package crd_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fakeapiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdconfig "github.com/nicklasfrahm/kontinuum/config/crd"
	"github.com/nicklasfrahm/kontinuum/pkg/crd"
)

// testDefinition targets instances.kontinuum.sh's real generated manifest —
// a kind with no Conversion, unlike registry's own Kontinuum-specific
// tests (pkg/domain/registry/crd_test.go), which already cover the
// conversion-webhook-patching path.
func testDefinition() crd.Definition {
	return crd.Definition{
		Name:         "instances.kontinuum.sh",
		ManifestFile: "kontinuum.sh_instances.yaml",
	}
}

func TestBuildNoConversion(t *testing.T) {
	t.Parallel()

	built := crd.Build(crdconfig.Files, testDefinition())

	assert.Equal(t, "instances.kontinuum.sh", built.Name)
	assert.Nil(t, built.Spec.Conversion)
}

func TestBuildConversionPatchesClientConfig(t *testing.T) {
	t.Parallel()

	def := testDefinition()
	def.Conversion = &crd.ConversionWebhook{
		Path:     "/convert",
		DNSName:  "localhost",
		Port:     9443,
		CABundle: []byte("test-ca-bundle"),
	}

	built := crd.Build(crdconfig.Files, def)

	require.NotNil(t, built.Spec.Conversion)
	require.NotNil(t, built.Spec.Conversion.Webhook)
	require.NotNil(t, built.Spec.Conversion.Webhook.ClientConfig)
	require.NotNil(t, built.Spec.Conversion.Webhook.ClientConfig.URL)
	assert.Equal(t, "https://localhost:9443/convert", *built.Spec.Conversion.Webhook.ClientConfig.URL)
	assert.Equal(t, []byte("test-ca-bundle"), built.Spec.Conversion.Webhook.ClientConfig.CABundle)
}

func TestApplyCreatesWhenMissing(t *testing.T) {
	t.Parallel()

	crds := fakeapiextensions.NewSimpleClientset().ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, crd.Apply(context.Background(), crds, crdconfig.Files, testDefinition()))

	got, err := crds.Get(context.Background(), testDefinition().Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, crd.Build(crdconfig.Files, testDefinition()).Spec, got.Spec)
}

// TestApplyUpdatesStaleDefinition is the regression case: a CRD left over
// from a previous run must be reconciled to the current spec rather than
// left in place, which is what Create alone did.
func TestApplyUpdatesStaleDefinition(t *testing.T) {
	t.Parallel()

	stale := crd.Build(crdconfig.Files, testDefinition())
	stale.Spec.Versions = stale.Spec.Versions[:1]
	stale.Spec.Versions[0].Name = "v1alpha0"

	crds := fakeapiextensions.NewSimpleClientset(stale).ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, crd.Apply(context.Background(), crds, crdconfig.Files, testDefinition()))

	got, err := crds.Get(context.Background(), testDefinition().Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, crd.Build(crdconfig.Files, testDefinition()).Spec, got.Spec)
}

// TestApplyNoopWhenAlreadyCurrent asserts Apply skips the Update call once
// the stored spec already matches — otherwise every retry in Ensure's poll
// loop would churn the CRD's resourceVersion forever.
func TestApplyNoopWhenAlreadyCurrent(t *testing.T) {
	t.Parallel()

	current := crd.Build(crdconfig.Files, testDefinition())

	crds := fakeapiextensions.NewSimpleClientset(current).ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, crd.Apply(context.Background(), crds, crdconfig.Files, testDefinition()))

	got, err := crds.Get(context.Background(), current.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, current.ResourceVersion, got.ResourceVersion,
		"spec already matched the desired definition, so no update should have been issued")
}
