package identityprovider_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/identityprovider"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, coordinationv1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&v1alpha2.IdentityProvider{}, &v1alpha2.ClusterIdentityProvider{}).
		Build()
}

func newReconciler(fakeClient client.Client, selfZoneKey string) (*identityprovider.Reconciler, testRegistry) {
	setup := newTestRegistry()

	return &identityprovider.Reconciler{
		Client:               fakeClient,
		Registry:             setup.registry,
		Cache:                setup.cache,
		ErrorRequeueInterval: time.Second,
		Locker:               zonelease.NewLocker(fakeClient, fakeClient, "test-hub", selfZoneKey, 0),
		Logger:               slog.Default(),
	}, setup
}

func TestReconcileClusterIdentityProviderPopulatesKeysAndStatus(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	obj := &v1alpha2.ClusterIdentityProvider{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Generation: 3},
		Spec:       celSpec(issuer),
	}
	fakeClient := newFakeClient(t, obj)
	reconciler, setup := newReconciler(fakeClient, "")

	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: clusterName}}

	result, err := reconciler.Reconcile(context.Background(), request)
	require.NoError(t, err)
	assert.Positive(t, result.RequeueAfter)

	_, cached := setup.cache.Snapshot(issuer.Source())
	assert.True(t, cached, "reconciling a new provider must populate the signing key cache")

	var got v1alpha2.ClusterIdentityProvider
	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: clusterName}, &got))

	ready := meta.FindStatusCondition(got.Status.Conditions, identityprovider.ReadyConditionType)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionTrue, ready.Status)
	assert.Equal(t, identityprovider.ReasonActive, ready.Reason)
	assert.Equal(t, int32(1), got.Status.SigningKeys)
	assert.NotNil(t, got.Status.LastKeyRefreshTime)
	assert.Equal(t, int64(3), got.Status.ObservedGeneration)
	assert.Empty(t, got.Status.UsernamePrefix)

	authenticate(t, setup.authenticator, issuer.sign(t, firstKeyID, nil))
}

func TestReconcileIdentityProviderReportsTenantPrefix(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	obj := &v1alpha2.IdentityProvider{
		ObjectMeta: metav1.ObjectMeta{Namespace: tenantName, Name: tenantProviderName},
		Spec:       celSpec(issuer),
	}
	fakeClient := newFakeClient(t, obj)
	reconciler, _ := newReconciler(fakeClient, "")

	_, err := reconciler.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Namespace: tenantName, Name: tenantProviderName}})
	require.NoError(t, err)

	var got v1alpha2.IdentityProvider

	key := types.NamespacedName{Namespace: tenantName, Name: tenantProviderName}
	require.NoError(t, fakeClient.Get(context.Background(), key, &got))
	assert.Equal(t, "tenant:acme:", got.Status.UsernamePrefix)
	assert.Equal(t, "tenant:acme:", got.Status.GroupsPrefix)
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, identityprovider.ReadyConditionType))
}

func TestReconcileReportsKeyFetchFailure(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	issuer.failing.Store(true)

	obj := &v1alpha2.ClusterIdentityProvider{ObjectMeta: metav1.ObjectMeta{Name: clusterName}, Spec: celSpec(issuer)}
	fakeClient := newFakeClient(t, obj)
	reconciler, _ := newReconciler(fakeClient, "")

	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: clusterName}}

	result, err := reconciler.Reconcile(context.Background(), request)
	require.NoError(t, err)
	assert.LessOrEqual(t, result.RequeueAfter, 2*time.Second)

	var got v1alpha2.ClusterIdentityProvider
	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: clusterName}, &got))

	ready := meta.FindStatusCondition(got.Status.Conditions, identityprovider.ReadyConditionType)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, identityprovider.ReasonKeyFetchFailed, ready.Reason)
}

func TestReconcileSkipsStatusWithoutGlobalLease(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	obj := &v1alpha2.ClusterIdentityProvider{ObjectMeta: metav1.ObjectMeta{Name: clusterName}, Spec: celSpec(issuer)}
	fakeClient := newFakeClient(t, obj)
	reconciler, setup := newReconciler(fakeClient, zonelease.Key("eu", "a"))

	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: clusterName}}

	_, err := reconciler.Reconcile(context.Background(), request)
	require.NoError(t, err)

	var got v1alpha2.ClusterIdentityProvider
	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: clusterName}, &got))
	assert.Empty(t, got.Status.Conditions, "a zone worker must not write hub-owned status")

	authenticate(t, setup.authenticator, issuer.sign(t, firstKeyID, nil))
}

func TestReconcileDeletedProviderStopsAuthenticating(t *testing.T) {
	t.Parallel()

	issuer := newTestIssuer(t)
	obj := &v1alpha2.ClusterIdentityProvider{ObjectMeta: metav1.ObjectMeta{Name: clusterName}, Spec: celSpec(issuer)}
	fakeClient := newFakeClient(t, obj)
	reconciler, setup := newReconciler(fakeClient, "")
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: clusterName}}
	token := issuer.sign(t, firstKeyID, nil)

	_, err := reconciler.Reconcile(context.Background(), request)
	require.NoError(t, err)
	authenticate(t, setup.authenticator, token)

	require.NoError(t, fakeClient.Delete(context.Background(), obj))

	_, err = reconciler.Reconcile(context.Background(), request)
	require.NoError(t, err)

	_, ok, err := setup.authenticator.AuthenticateToken(context.Background(), token)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestDefinitionsCoverBothKinds(t *testing.T) {
	t.Parallel()

	definitions := identityprovider.Definitions()
	require.Len(t, definitions, 2)
	assert.Equal(t, "ClusterIdentityProvider", definitions[0].GVKs[0].Kind)
	assert.Equal(t, "IdentityProvider", definitions[1].GVKs[0].Kind)
}
