package registry_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	fakeapiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
)

func TestCustomResourceDefinition(t *testing.T) {
	t.Parallel()

	crd := registry.CustomResourceDefinition()

	assert.Equal(t, "kontinuums.kontinuum.sh", crd.Name)
	assert.Equal(t, v1alpha2.GroupName, crd.Spec.Group)
	assert.Equal(t, apiextensionsv1.ClusterScoped, crd.Spec.Scope)
	assert.Equal(t, "Kontinuum", crd.Spec.Names.Kind)
	assert.Equal(t, "KontinuumList", crd.Spec.Names.ListKind)
	assert.Equal(t, "kontinuums", crd.Spec.Names.Plural)
	assert.Equal(t, "kontinuum", crd.Spec.Names.Singular)

	require.Len(t, crd.Spec.Versions, 1)

	version := crd.Spec.Versions[0]

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

	wantColumns := []apiextensionsv1.CustomResourceColumnDefinition{
		{Name: "Role", Type: "string", JSONPath: ".status.role"},
		{Name: "Region", Type: "string", JSONPath: ".spec.region"},
		{Name: "Zone", Type: "string", JSONPath: ".spec.zone"},
	}
	assert.Equal(t, wantColumns, version.AdditionalPrinterColumns)
}

// TestApplyCRDCreatesWhenMissing covers the common case: no CRD registered
// yet, so ApplyCRD's Create succeeds outright.
func TestApplyCRDCreatesWhenMissing(t *testing.T) {
	t.Parallel()

	crds := fakeapiextensions.NewSimpleClientset().ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, registry.ApplyCRD(context.Background(), crds))

	crd, err := crds.Get(context.Background(), registry.CustomResourceDefinition().Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, registry.CustomResourceDefinition().Spec, crd.Spec)
}

// TestApplyCRDUpdatesStaleDefinition is the regression case: a CRD left
// over from a previous run — e.g. one still only serving a since-removed
// API version like v1alpha1 — must be reconciled to the current spec
// rather than left in place, which is what Create alone did.
func TestApplyCRDUpdatesStaleDefinition(t *testing.T) {
	t.Parallel()

	stale := registry.CustomResourceDefinition()
	stale.Spec.Versions[0].Name = "v1alpha1"

	crds := fakeapiextensions.NewSimpleClientset(stale).ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, registry.ApplyCRD(context.Background(), crds))

	crd, err := crds.Get(context.Background(), registry.CustomResourceDefinition().Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, registry.CustomResourceDefinition().Spec, crd.Spec)
}

// TestApplyCRDNoopWhenAlreadyCurrent asserts ApplyCRD skips the Update call
// once the stored spec already matches — otherwise every retry in
// ensureCRD's poll loop would churn the CRD's resourceVersion forever.
func TestApplyCRDNoopWhenAlreadyCurrent(t *testing.T) {
	t.Parallel()

	current := registry.CustomResourceDefinition()

	crds := fakeapiextensions.NewSimpleClientset(current).ApiextensionsV1().CustomResourceDefinitions()

	require.NoError(t, registry.ApplyCRD(context.Background(), crds))

	crd, err := crds.Get(context.Background(), current.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, current.ResourceVersion, crd.ResourceVersion,
		"spec already matched the desired definition, so no update should have been issued")
}
