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
// resolve any kontinuum.sh Kind — used to exercise Add's non-AlreadyExists
// error-wrapping path deterministically, without needing a real conflicting
// object.
func fakeClientWithoutV1alpha2Scheme(t *testing.T) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

func testAddOptions() zone.AddOptions {
	return zone.AddOptions{
		Region:       testRegion,
		Zone:         testZone,
		Domain:       testDomain,
		TalosAddress: "10.0.0.5",
	}
}

func TestBuildAddObjectsSharesNameAcrossZoneInstancePoolAndTalosCluster(t *testing.T) {
	t.Parallel()

	zoneObj, instance, pool, cluster := zone.BuildAddObjects(testAddOptions())

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

func TestAddCreatesAllFourObjects(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	got, err := zone.Add(t.Context(), hubClient, testAddOptions())
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

func TestAddToleratesAlreadyAddedZone(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	_, err := zone.Add(t.Context(), hubClient, testAddOptions())
	require.NoError(t, err)

	_, err = zone.Add(t.Context(), hubClient, testAddOptions())
	require.NoError(t, err)
}

func TestAddRejectsMissingRequiredFields(t *testing.T) {
	t.Parallel()

	opts := testAddOptions()
	opts.TalosAddress = ""

	_, err := zone.Add(t.Context(), newHubFakeClient(t), opts)
	require.Error(t, err)
}

func TestAddRejectsInvalidRegionLabel(t *testing.T) {
	t.Parallel()

	opts := testAddOptions()
	opts.Region = "Not A Valid Label!"

	_, err := zone.Add(t.Context(), newHubFakeClient(t), opts)
	require.Error(t, err)
}

func TestAddPropagatesUnexpectedCreateError(t *testing.T) {
	t.Parallel()

	// A hub client with no Zone/Instance/InstancePool/TalosCluster kinds
	// registered in its scheme makes every Create fail with something
	// other than AlreadyExists, exercising Add's own error-wrapping path.
	hubClient := fakeClientWithoutV1alpha2Scheme(t)

	_, err := zone.Add(t.Context(), hubClient, testAddOptions())
	require.Error(t, err)
	assert.False(t, apierrors.IsAlreadyExists(err))
}
