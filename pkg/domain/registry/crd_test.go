package registry_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
)

func TestCustomResourceDefinition(t *testing.T) {
	t.Parallel()

	crd := registry.CustomResourceDefinition()

	assert.Equal(t, "kontinuums.kontinuum.io", crd.Name)
	assert.Equal(t, v1alpha1.GroupName, crd.Spec.Group)
	assert.Equal(t, apiextensionsv1.ClusterScoped, crd.Spec.Scope)
	assert.Equal(t, "Kontinuum", crd.Spec.Names.Kind)
	assert.Equal(t, "KontinuumList", crd.Spec.Names.ListKind)
	assert.Equal(t, "kontinuums", crd.Spec.Names.Plural)
	assert.Equal(t, "kontinuum", crd.Spec.Names.Singular)

	require.Len(t, crd.Spec.Versions, 1)

	version := crd.Spec.Versions[0]

	assert.Equal(t, v1alpha1.APIVersion, version.Name)
	assert.True(t, version.Served)
	assert.True(t, version.Storage)
	require.NotNil(t, version.Subresources)
	assert.NotNil(t, version.Subresources.Status)

	require.NotNil(t, version.Schema)
	require.NotNil(t, version.Schema.OpenAPIV3Schema)

	specSchema, hasSpec := version.Schema.OpenAPIV3Schema.Properties["spec"]
	require.True(t, hasSpec)
	assert.Equal(t, []string{"role"}, specSchema.Required)
	assert.Contains(t, specSchema.Properties, "role")
	assert.Contains(t, specSchema.Properties, "region")
	assert.Contains(t, specSchema.Properties, "zone")

	statusSchema, hasStatus := version.Schema.OpenAPIV3Schema.Properties["status"]
	require.True(t, hasStatus)
	assert.Contains(t, statusSchema.Properties, "lastHeartbeatTime")

	wantColumns := []apiextensionsv1.CustomResourceColumnDefinition{
		{Name: "Role", Type: "string", JSONPath: ".spec.role"},
		{Name: "Region", Type: "string", JSONPath: ".spec.region"},
		{Name: "Zone", Type: "string", JSONPath: ".spec.zone"},
	}
	assert.Equal(t, wantColumns, version.AdditionalPrinterColumns)
}
