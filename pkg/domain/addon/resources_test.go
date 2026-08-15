package addon_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/addon"
)

const (
	resourcesTestClusterName = "eu-1a"
	resourcesTestNamespace   = "eu-1a-ns"
	resourcesTestAddonName   = resourcesTestClusterName + "-" + ciliumReleaseName
)

func resourcesTestCluster(namespace string) *v1alpha2.TalosCluster {
	return &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: resourcesTestClusterName, Namespace: namespace, UID: "cluster-uid"},
		Spec: v1alpha2.TalosClusterSpec{
			ControlPlane: v1alpha2.TalosClusterMemberSpec{
				PoolRef: v1alpha2.InstancePoolReference{Name: "cp-pool"},
			},
		},
	}
}

func resourcesFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

// TestEnsureBuiltinSeedsNamespacesAndOwnsEachSeed covers the actual fix
// this refactor makes: every built-in seed lands in its owning
// TalosCluster's own namespace, with a real owner reference — which is
// what makes native GC delete it the moment that TalosCluster is deleted,
// instead of it surviving as an orphan a same-named cluster recreated
// later could silently inherit stale Ready status from.
func TestEnsureBuiltinSeedsNamespacesAndOwnsEachSeed(t *testing.T) {
	t.Parallel()

	cluster := resourcesTestCluster(resourcesTestNamespace)
	fakeClient := resourcesFakeClient(t, cluster)

	require.NoError(t, addon.EnsureBuiltinSeeds(context.Background(), fakeClient, cluster))

	var cilium v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: resourcesTestAddonName, Namespace: resourcesTestNamespace}, &cilium))
	assert.Equal(t, resourcesTestNamespace, cilium.Namespace)
	assert.True(t, metav1.IsControlledBy(&cilium, cluster), "seed must be owned by its TalosCluster")
}

// TestEnsureBuiltinSeedsCreateOnly covers EnsureBuiltinSeeds' own doc: a
// seed that already exists (e.g. a user edited it, or reconcile ran
// twice) is never overwritten.
func TestEnsureBuiltinSeedsCreateOnly(t *testing.T) {
	t.Parallel()

	cluster := resourcesTestCluster(resourcesTestNamespace)

	existing := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: resourcesTestAddonName, Namespace: resourcesTestNamespace},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: resourcesTestClusterName},
			ReleaseName:     ciliumReleaseName,
			Enabled:         new(bool), // explicitly disabled by a user — must survive re-seeding
		},
	}

	fakeClient := resourcesFakeClient(t, cluster, existing)

	require.NoError(t, addon.EnsureBuiltinSeeds(context.Background(), fakeClient, cluster))

	var cilium v1alpha2.Addon

	require.NoError(t, fakeClient.Get(context.Background(),
		types.NamespacedName{Name: resourcesTestAddonName, Namespace: resourcesTestNamespace}, &cilium))
	require.NotNil(t, cilium.Spec.Enabled)
	assert.False(t, *cilium.Spec.Enabled, "an existing seed must never be overwritten")
}

// TestListForClusterScopesByNamespace covers the namespace-scoped half of
// ListForCluster's own filter — two clusters, in different namespaces,
// each seeding an addon with the same release name (and so, thanks to
// addonResourceName's clusterName prefix, different resource names too):
// only the matching-namespace one comes back.
func TestListForClusterScopesByNamespace(t *testing.T) {
	t.Parallel()

	clusterA := resourcesTestCluster("tenant-a")
	clusterB := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: resourcesTestClusterName, Namespace: "tenant-b", UID: "cluster-b-uid"},
	}

	addonA := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: resourcesTestAddonName, Namespace: "tenant-a"},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: resourcesTestClusterName}, ReleaseName: ciliumReleaseName,
		},
	}
	addonB := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: resourcesTestAddonName, Namespace: "tenant-b"},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: resourcesTestClusterName}, ReleaseName: ciliumReleaseName,
		},
	}

	fakeClient := resourcesFakeClient(t, clusterA, clusterB, addonA, addonB)

	got, err := addon.ListForCluster(context.Background(), fakeClient, "tenant-a", resourcesTestClusterName)
	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Equal(t, "tenant-a", got.Items[0].Namespace)
}
