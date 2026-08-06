package zone_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

// fakeClientWithoutV1alpha2Scheme builds a client whose scheme can't
// resolve any kontinuum.sh Kind — used to exercise Apply's non-AlreadyExists
// error-wrapping path deterministically, without needing a real conflicting
// object.
func fakeClientWithoutV1alpha2Scheme(t *testing.T) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

func testJoinOptions() zone.JoinOptions {
	return zone.JoinOptions{
		Region:       testRegion,
		Zone:         testZone,
		Domain:       testDomain,
		TalosAddress: "10.0.0.5",
	}
}

func TestBuildJoinObjectsSharesNameAcrossZoneInstancePoolAndTalosCluster(t *testing.T) {
	t.Parallel()

	zoneObj, instance, pool, cluster := zone.BuildJoinObjects(testJoinOptions())

	assert.Equal(t, testZoneName, zoneObj.Name)
	assert.Equal(t, testZoneName, pool.Name)
	assert.Equal(t, testZoneName, cluster.Name)
	assert.Equal(t, testZoneName+"-seed", instance.Name)

	assert.Equal(t, testRegion, zoneObj.Spec.Region)
	assert.Equal(t, testZone, zoneObj.Spec.Zone)
	assert.Equal(t, testDomain, zoneObj.Spec.Domain)

	assert.Equal(t, []string{"10.0.0.5"}, instance.Spec.Interfaces)
	assert.Equal(t, testRegion, instance.Labels[v1alpha2.LabelRegion])
	assert.Equal(t, testZone, instance.Labels[v1alpha2.LabelZone])

	assert.Equal(t, int32(1), pool.Spec.Replicas)
	assert.Equal(t, instance.Labels, pool.Spec.Selector.MatchLabels)

	assert.Equal(t, testZoneName, cluster.Spec.ControlPlane.PoolRef.Name)
	assert.Empty(t, cluster.Spec.Talos.Version)
	assert.Empty(t, cluster.Spec.Kubernetes.Version)
}

func TestApplyCreatesAllFourObjects(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	got, err := zone.Apply(t.Context(), hubClient, testJoinOptions())
	require.NoError(t, err)
	assert.Equal(t, testZoneName, got.Name)

	var z v1alpha2.Zone
	assert.NoError(t, hubClient.Get(t.Context(), client.ObjectKey{Name: testZoneName}, &z))

	var instance v1alpha2.Instance
	assert.NoError(t, hubClient.Get(t.Context(), client.ObjectKey{Name: testZoneName + "-seed"}, &instance))

	var pool v1alpha2.InstancePool
	assert.NoError(t, hubClient.Get(t.Context(), client.ObjectKey{Name: testZoneName}, &pool))

	var cluster v1alpha2.TalosCluster
	assert.NoError(t, hubClient.Get(t.Context(), client.ObjectKey{Name: testZoneName}, &cluster))
}

func TestApplyToleratesAlreadyJoinedZone(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	_, err := zone.Apply(t.Context(), hubClient, testJoinOptions())
	require.NoError(t, err)

	_, err = zone.Apply(t.Context(), hubClient, testJoinOptions())
	require.NoError(t, err)
}

func TestApplyRejectsMissingRequiredFields(t *testing.T) {
	t.Parallel()

	opts := testJoinOptions()
	opts.TalosAddress = ""

	_, err := zone.Apply(t.Context(), newHubFakeClient(t), opts)
	require.Error(t, err)
}

func TestApplyRejectsInvalidRegionLabel(t *testing.T) {
	t.Parallel()

	opts := testJoinOptions()
	opts.Region = "Not A Valid Label!"

	_, err := zone.Apply(t.Context(), newHubFakeClient(t), opts)
	require.Error(t, err)
}

func TestApplyPropagatesUnexpectedCreateError(t *testing.T) {
	t.Parallel()

	// A hub client with no Zone/Instance/InstancePool/TalosCluster kinds
	// registered in its scheme makes every Create fail with something
	// other than AlreadyExists, exercising Apply's own error-wrapping path.
	hubClient := fakeClientWithoutV1alpha2Scheme(t)

	_, err := zone.Apply(t.Context(), hubClient, testJoinOptions())
	require.Error(t, err)
	assert.False(t, apierrors.IsAlreadyExists(err))
}
