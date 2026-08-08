package zone_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// TestAddSetsOwnerReferencesFromZoneToDependents covers the fan-out's own
// ownership metadata: the seed Instance, InstancePool, and TalosCluster
// are each owned by the created Zone. Nothing acts on this yet —
// libkapi.WithGarbageCollector, which would cascade a Zone deletion to all
// three, is deliberately not enabled (see pkg/cli/serve.go's own doc) —
// but it's still correct, real metadata (kubectl tree already reads it),
// and ready for whichever cleanup mechanism ends up using it.
func TestAddSetsOwnerReferencesFromZoneToDependents(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	got, err := zone.Add(t.Context(), hubClient, testAddOptions())
	require.NoError(t, err)

	var instance v1alpha2.Instance
	require.NoError(t, hubClient.Get(t.Context(), client.ObjectKey{Name: testZoneName + "-seed"}, &instance))
	assertOwnedByZone(t, got, instance.OwnerReferences)

	var pool v1alpha2.InstancePool
	require.NoError(t, hubClient.Get(t.Context(), client.ObjectKey{Name: testZoneName}, &pool))
	assertOwnedByZone(t, got, pool.OwnerReferences)

	var cluster v1alpha2.TalosCluster
	require.NoError(t, hubClient.Get(t.Context(), client.ObjectKey{Name: testZoneName}, &cluster))
	assertOwnedByZone(t, got, cluster.OwnerReferences)
}

func assertOwnedByZone(t *testing.T, zoneObj *v1alpha2.Zone, refs []metav1.OwnerReference) {
	t.Helper()

	require.Len(t, refs, 1)
	assert.Equal(t, "Zone", refs[0].Kind)
	assert.Equal(t, zoneObj.Name, refs[0].Name)
	assert.Equal(t, zoneObj.UID, refs[0].UID)
	require.NotNil(t, refs[0].Controller)
	assert.True(t, *refs[0].Controller)
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

func TestAddInfersDomainFromRegisteredKontinuumWhenUnset(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub", testStorage)
	kontinuum.Status.Config.Server.DNS.Domain = testDomain

	hubClient := newHubFakeClient(t, kontinuum, kontinuumSecret)

	opts := testAddOptions()
	opts.Domain = ""

	got, err := zone.Add(t.Context(), hubClient, opts)
	require.NoError(t, err)
	assert.Equal(t, testDomain, got.Spec.Domain)
}

func TestAddPrefersExplicitDomainOverInference(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub", testStorage)
	kontinuum.Status.Config.Server.DNS.Domain = "inferred.example.com"

	hubClient := newHubFakeClient(t, kontinuum, kontinuumSecret)

	opts := testAddOptions()
	opts.Domain = "explicit.example.com"

	got, err := zone.Add(t.Context(), hubClient, opts)
	require.NoError(t, err)
	assert.Equal(t, "explicit.example.com", got.Spec.Domain)
}

func TestAddFailsWhenNoRegisteredKontinuumPublishesDomain(t *testing.T) {
	t.Parallel()

	// A Kontinuum is registered, but hasn't set KONTINUUM_SERVER_DNS_DOMAIN
	// — nothing for inference to find.
	kontinuum, kontinuumSecret := registeredKontinuum("hub", testStorage)
	hubClient := newHubFakeClient(t, kontinuum, kontinuumSecret)

	opts := testAddOptions()
	opts.Domain = ""

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no registered kontinuum publishes a DNS domain")
}

func TestAddFailsWhenNoKontinuumRegisteredAtAllForDomainInference(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	opts := testAddOptions()
	opts.Domain = ""

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no registered kontinuum found")
}
