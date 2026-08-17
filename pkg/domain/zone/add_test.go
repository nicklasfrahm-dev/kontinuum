package zone_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	instancedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

const testTalosHostname = "talos.example.com"

// stubResolver is a test-only instancedomain.Resolver that never touches the
// network — LookupHost either returns addrs or, when addrs is empty,
// errStubLookupFailed. Mirrors pkg/domain/instance's own add_test.go
// stubResolver — that one lives in package instance_test, unexported, so
// this package needs its own copy rather than reusing it.
type stubResolver struct {
	addrs []string
}

var errStubLookupFailed = errors.New("stub resolver: lookup failed")

func (s stubResolver) LookupHost(_ context.Context, _ string) ([]string, error) {
	if len(s.addrs) == 0 {
		return nil, errStubLookupFailed
	}

	return s.addrs, nil
}

// getSeedInstance finds Add's seed Instance by its region/zone labels
// rather than by name — its name is instance.NameFromAddress(TalosAddress)
// (see BuildAddObjects' own doc), an implementation detail tests shouldn't
// need to reproduce by hand.
func getSeedInstance(t *testing.T, hubClient client.Client) v1alpha2.Instance {
	t.Helper()

	var list v1alpha2.InstanceList

	require.NoError(t, hubClient.List(t.Context(), &list, client.InNamespace(v1alpha2.KontinuumSystemNamespace),
		client.MatchingLabels{v1alpha2.LabelRegion: testRegion, v1alpha2.LabelZone: testZone}))
	require.Len(t, list.Items, 1)

	return list.Items[0]
}

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
		TalosAddress: testTalosAddress,
	}
}

func TestBuildAddObjectsSharesNameAcrossZoneInstancePoolAndTalosCluster(t *testing.T) {
	t.Parallel()

	zoneObj, instance, pool, cluster := zone.BuildAddObjects(testAddOptions())

	assert.Equal(t, testZoneName, zoneObj.Name)
	assert.Equal(t, testZoneName, pool.Name)
	assert.Equal(t, testZoneName, cluster.Name)
	// The seed Instance's own name is deliberately *not* scoped under
	// testZoneName — see BuildAddObjects' own doc: it must match whatever
	// instancedomain.NameFromAddress derives for the same TalosAddress, the
	// exact same name a standalone "Add instance" registration for that
	// address would also use, so the two never independently duplicate one
	// another.
	assert.Equal(t, instancedomain.NameFromAddress(testTalosAddress), instance.Name)

	assert.Equal(t, testRegion, zoneObj.Spec.Region)
	assert.Equal(t, testZone, zoneObj.Spec.Zone)
	assert.Equal(t, testDomain, zoneObj.Spec.Domain)

	assert.Equal(t, []string{testTalosAddress}, instance.Spec.Interfaces)
	assert.Equal(t, testRegion, instance.Labels[v1alpha2.LabelRegion])
	assert.Equal(t, testZone, instance.Labels[v1alpha2.LabelZone])

	assert.Equal(t, int32(1), pool.Spec.Replicas)
	assert.Equal(t, instance.Labels, pool.Spec.Selector.MatchLabels)

	assert.Equal(t, testZoneName, cluster.Spec.ControlPlane.PoolRef.Name)
	assert.Empty(t, cluster.Spec.Talos.Version)
	assert.Empty(t, cluster.Spec.Kubernetes.Version)
	assert.False(t, cluster.Spec.Teardown.UnregisterInstances,
		"instances stay in inventory by default — see TeardownSpec's own doc")
}

// TestBuildAddObjectsThreadsUnregisterInstancesOnDeleteToClusterSpec covers
// the "Unregister instances on decommissioning" checkbox's own path onto
// the created TalosCluster's spec.teardown.unregisterInstances — the field
// TalosClusterFinalizer's own teardown actually reads.
func TestBuildAddObjectsThreadsUnregisterInstancesOnDeleteToClusterSpec(t *testing.T) {
	t.Parallel()

	opts := testAddOptions()
	opts.UnregisterInstancesOnDelete = true

	zoneObj, _, _, cluster := zone.BuildAddObjects(opts)

	assert.Equal(t, testZoneName, zoneObj.Name)
	assert.True(t, cluster.Spec.Teardown.UnregisterInstances)
}

// TestBuildAddObjectsInstanceNameHashesTalosAddress covers the seed
// Instance's name deriving from opts.TalosAddress the way a Kubernetes
// ReplicaSet derives a Pod's name suffix from its pod template hash: the
// same address must hash to the same name every time (re-running zone-add
// is idempotent, see Add's own doc), and a different address must hash to
// a different name (rather than colliding with, or silently leaving stale,
// the old Instance) — and, critically, that name must match
// instancedomain.NameFromAddress's own output for the same address exactly
// (see BuildAddObjects' own doc), not merely be internally consistent
// within this package.
func TestBuildAddObjectsInstanceNameHashesTalosAddress(t *testing.T) {
	t.Parallel()

	opts := testAddOptions()

	firstZone, firstInstance, firstPool, firstCluster := zone.BuildAddObjects(opts)
	assert.Equal(t, instancedomain.NameFromAddress(opts.TalosAddress), firstInstance.Name)

	secondZone, secondInstance, secondPool, secondCluster := zone.BuildAddObjects(opts)
	assert.Equal(t, firstInstance.Name, secondInstance.Name)
	assert.Equal(t, firstZone.Name, secondZone.Name)
	assert.Equal(t, firstPool.Name, secondPool.Name)
	assert.Equal(t, firstCluster.Name, secondCluster.Name)

	opts.TalosAddress = "10.0.0.6"
	thirdZone, thirdInstance, thirdPool, thirdCluster := zone.BuildAddObjects(opts)
	assert.NotEqual(t, firstInstance.Name, thirdInstance.Name,
		"a different --talos-address must get a new Instance identity")
	assert.Equal(t, instancedomain.NameFromAddress(opts.TalosAddress), thirdInstance.Name)
	assert.Equal(t, firstZone.Name, thirdZone.Name, "Zone/InstancePool/TalosCluster names don't depend on the spec hash")
	assert.Equal(t, firstPool.Name, thirdPool.Name)
	assert.Equal(t, firstCluster.Name, thirdCluster.Name)
}

func TestAddCreatesAllFourObjects(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	got, err := zone.Add(t.Context(), hubClient, testAddOptions())
	require.NoError(t, err)
	assert.Equal(t, testZoneName, got.Name)

	sharedKey := client.ObjectKey{Name: testZoneName, Namespace: v1alpha2.KontinuumSystemNamespace}

	var z v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), sharedKey, &z))

	getSeedInstance(t, hubClient)

	var pool v1alpha2.InstancePool
	assert.NoError(t, hubClient.Get(t.Context(), sharedKey, &pool))

	var cluster v1alpha2.TalosCluster
	assert.NoError(t, hubClient.Get(t.Context(), sharedKey, &cluster))
}

// TestAddSetsOwnershipChain covers the fan-out's own ownership metadata: a
// strict Zone > TalosCluster > InstancePool chain, not four siblings all
// owned directly by Zone — see taloscluster.TalosClusterFinalizer's own
// doc for why. libkapi.WithGarbageCollector is enabled (see
// pkg/cli/serve.go's own doc), so this is what actually drives cascade
// deletion, not just inert metadata. The seed Instance is owned by
// nobody — its fate on cluster teardown is the explicit
// spec.teardown.unregisterInstances opt-in (see TeardownSpec's own doc),
// never inferred from ownership.
func TestAddSetsOwnershipChain(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	got, err := zone.Add(t.Context(), hubClient, testAddOptions())
	require.NoError(t, err)

	sharedKey := client.ObjectKey{Name: testZoneName, Namespace: v1alpha2.KontinuumSystemNamespace}

	var cluster v1alpha2.TalosCluster
	require.NoError(t, hubClient.Get(t.Context(), sharedKey, &cluster))
	assertOwnedBy(t, "Zone", got.Name, got.UID, cluster.OwnerReferences)

	var pool v1alpha2.InstancePool
	require.NoError(t, hubClient.Get(t.Context(), sharedKey, &pool))
	assertOwnedBy(t, "TalosCluster", cluster.Name, cluster.UID, pool.OwnerReferences)

	instance := getSeedInstance(t, hubClient)
	assert.Empty(t, instance.OwnerReferences)
}

func assertOwnedBy(t *testing.T, kind, name string, uid types.UID, refs []metav1.OwnerReference) {
	t.Helper()

	require.Len(t, refs, 1)
	assert.Equal(t, kind, refs[0].Kind)
	assert.Equal(t, name, refs[0].Name)
	assert.Equal(t, uid, refs[0].UID)
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

// TestAddResolvesTalosAddressHostnameToIP covers issue #98's own gap: unlike
// instance.Add's standalone registration, zone.Add used to store
// opts.TalosAddress verbatim, so a hostname (e.g. a Docker Compose service
// name) ended up dialed literally by the taloscluster controller instead of
// a real IP, breaking TLS verification once Talos left maintenance mode.
func TestAddResolvesTalosAddressHostnameToIP(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	opts := testAddOptions()
	opts.TalosAddress = testTalosHostname
	opts.Resolver = stubResolver{addrs: []string{testTalosAddress}}

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.NoError(t, err)

	got := getSeedInstance(t, hubClient)
	assert.Equal(t, []string{testTalosAddress}, got.Spec.Interfaces,
		"spec must carry the resolved IP, not the hostname")
	assert.Equal(t, testTalosHostname, got.Annotations[instancedomain.AnnotationHostname])
}

// TestAddByTalosHostnameMatchesAddByResolvedIP covers the same convergence
// instance.Add's own TestAddByHostnameMatchesAddByResolvedIP covers: a zone
// added by hostname and a zone added directly by that hostname's resolved IP
// must land on the exact same seed Instance identity (see BuildAddObjects'
// own doc), not two independent duplicates.
func TestAddByTalosHostnameMatchesAddByResolvedIP(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	existing := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name: instancedomain.NameFromAddress(testTalosAddress), Namespace: v1alpha2.KontinuumSystemNamespace,
		},
		Spec: v1alpha2.InstanceSpec{Interfaces: []string{testTalosAddress}},
	}
	require.NoError(t, hubClient.Create(t.Context(), existing))

	opts := testAddOptions()
	opts.TalosAddress = testTalosHostname
	opts.Resolver = stubResolver{addrs: []string{testTalosAddress}}

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.NoError(t, err)

	var list v1alpha2.InstanceList
	require.NoError(t, hubClient.List(t.Context(), &list, client.InNamespace(v1alpha2.KontinuumSystemNamespace)))
	require.Len(t, list.Items, 1, "hostname and its resolved IP must not create two separate Instances")
	assert.Equal(t, existing.Name, list.Items[0].Name)
}

// TestAddSurfacesTalosAddressResolveFailure covers a hostname that fails to
// resolve — Add must surface that error rather than silently storing the
// unresolved hostname in spec.interfaces[0], the exact bug this test guards
// against regressing.
func TestAddSurfacesTalosAddressResolveFailure(t *testing.T) {
	t.Parallel()

	opts := testAddOptions()
	opts.TalosAddress = testTalosHostname
	opts.Resolver = stubResolver{}

	_, err := zone.Add(t.Context(), newHubFakeClient(t), opts)
	require.Error(t, err)
}

func TestAddInfersDomainFromRegisteredKontinuumWhenUnset(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
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

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
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
	kontinuum, kontinuumSecret := registeredKontinuum("hub")
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

// preRegisteredInstance is an already-registered, unclaimed Instance in
// v1alpha2.KontinuumSystemNamespace — the fixture every ExistingInstanceName
// test below adopts, standing in for one created via instance.Add's own
// standalone "Add instance" flow (see issue #81).
func preRegisteredInstance(name string, labels map[string]string) *v1alpha2.Instance {
	return &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace, Labels: labels},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{"10.0.0.9"}},
	}
}

func TestAddAdoptsExistingInstanceInsteadOfCreatingANewOne(t *testing.T) {
	t.Parallel()

	existing := preRegisteredInstance("instance-preexisting", nil)
	hubClient := newHubFakeClient(t, existing)

	opts := testAddOptions()
	opts.ExistingInstanceName = existing.Name

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.NoError(t, err)

	var list v1alpha2.InstanceList
	require.NoError(t, hubClient.List(t.Context(), &list, client.InNamespace(v1alpha2.KontinuumSystemNamespace)))
	require.Len(t, list.Items, 1, "adopting an existing instance must not also create a new one")

	assert.Equal(t, existing.Name, list.Items[0].Name)
	assert.Equal(t, testRegion, list.Items[0].Labels[v1alpha2.LabelRegion])
	assert.Equal(t, testZone, list.Items[0].Labels[v1alpha2.LabelZone])
	assert.Equal(t, []string{"10.0.0.9"}, list.Items[0].Spec.Interfaces,
		"adoption must not touch the existing instance's own discovered address")
}

// TestAddOverwritesStaleLabelsOnAdoptedInstance covers reusing an Instance
// released from a since-torn-down zone (see instancepool.Reconciler's own
// release, which strips v1alpha2.LabelClaimedBy but leaves the old
// region/zone pair behind) — adopting it into a new zone must relabel it,
// not leave it pointing at the old one, or the new InstancePool's own
// selector would never match it.
func TestAddOverwritesStaleLabelsOnAdoptedInstance(t *testing.T) {
	t.Parallel()

	existing := preRegisteredInstance("instance-released",
		map[string]string{v1alpha2.LabelRegion: "old-region", v1alpha2.LabelZone: "old-zone"})
	hubClient := newHubFakeClient(t, existing)

	opts := testAddOptions()
	opts.ExistingInstanceName = existing.Name

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.NoError(t, err)

	var got v1alpha2.Instance
	require.NoError(t, hubClient.Get(t.Context(),
		client.ObjectKey{Name: existing.Name, Namespace: v1alpha2.KontinuumSystemNamespace}, &got))
	assert.Equal(t, testRegion, got.Labels[v1alpha2.LabelRegion])
	assert.Equal(t, testZone, got.Labels[v1alpha2.LabelZone])
}

func TestAddRejectsAlreadyClaimedExistingInstance(t *testing.T) {
	t.Parallel()

	existing := preRegisteredInstance("instance-claimed",
		map[string]string{v1alpha2.LabelClaimedBy: "some-other-pool"})
	hubClient := newHubFakeClient(t, existing)

	opts := testAddOptions()
	opts.ExistingInstanceName = existing.Name

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already claimed")
}

func TestAddRejectsMissingExistingInstance(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)

	opts := testAddOptions()
	opts.ExistingInstanceName = "does-not-exist"

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.Error(t, err)
}

// TestAddAdoptsInstanceRegisteredStandaloneForTheSameAddress covers issue
// #81's own naming unification end to end: a freshly typed TalosAddress (no
// ExistingInstanceName — the ordinary "type a new one" path, not the
// instance-picker) that happens to match an address already registered via
// instance.Add's own standalone "Add instance" flow must adopt that exact
// object, not create a byte-for-byte duplicate under a second name — because
// both now derive an Instance's name from its address identically (see
// BuildAddObjects' own doc).
func TestAddAdoptsInstanceRegisteredStandaloneForTheSameAddress(t *testing.T) {
	t.Parallel()

	existing := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name: instancedomain.NameFromAddress(testTalosAddress), Namespace: v1alpha2.KontinuumSystemNamespace,
		},
		Spec: v1alpha2.InstanceSpec{Interfaces: []string{testTalosAddress}},
	}
	hubClient := newHubFakeClient(t, existing)

	opts := testAddOptions()
	opts.TalosAddress = testTalosAddress

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.NoError(t, err)

	var list v1alpha2.InstanceList
	require.NoError(t, hubClient.List(t.Context(), &list, client.InNamespace(v1alpha2.KontinuumSystemNamespace)))
	require.Len(t, list.Items, 1, "a freeform address matching an already-registered instance must not duplicate it")

	assert.Equal(t, existing.Name, list.Items[0].Name)
	assert.Equal(t, testRegion, list.Items[0].Labels[v1alpha2.LabelRegion])
	assert.Equal(t, testZone, list.Items[0].Labels[v1alpha2.LabelZone])
}

// TestAddRejectsAlreadyClaimedInstanceRegisteredStandaloneForTheSameAddress
// is TestAddRejectsAlreadyClaimedExistingInstance's own counterpart for the
// freeform-address collision path above: an already-claimed Instance whose
// name happens to collide with the typed address must still be rejected,
// not silently left claimed by whichever pool got there first.
func TestAddRejectsAlreadyClaimedInstanceRegisteredStandaloneForTheSameAddress(t *testing.T) {
	t.Parallel()

	existing := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name: instancedomain.NameFromAddress(testTalosAddress), Namespace: v1alpha2.KontinuumSystemNamespace,
			Labels: map[string]string{v1alpha2.LabelClaimedBy: "some-other-pool"},
		},
		Spec: v1alpha2.InstanceSpec{Interfaces: []string{testTalosAddress}},
	}
	hubClient := newHubFakeClient(t, existing)

	opts := testAddOptions()
	opts.TalosAddress = testTalosAddress

	_, err := zone.Add(t.Context(), hubClient, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already claimed")
}
