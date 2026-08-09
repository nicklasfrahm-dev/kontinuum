package zone_test

import (
	"log/slog"
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

const (
	testZoneName      = "eu-eu-1a"
	testRegion        = "eu"
	testZone          = "eu-1a"
	testDomain        = "kontinuum.example.com"
	testRetryInterval = 15 * time.Second
	testImage         = "ghcr.io/nicklasfrahm/kontinuum:test"
)

// testZoneKey() is testZoneName's own ObjectKey — every zone-add fixture in
// this file lives in v1alpha2.DefaultSecretNamespace (see BuildAddObjects'
// own doc), so every Get below needs both, not just Name.
func testZoneKey() client.ObjectKey {
	return client.ObjectKey{Name: testZoneName, Namespace: v1alpha2.DefaultSecretNamespace}
}

// testStorage is a fake connection string, not a real credential.
//
//nolint:gosec // false positive: fixture data, not a real credential
const testStorage = "postgres://user:pass@host/db"

// fakeDownstreamClientBuilder is zone.DownstreamClientBuilder's test
// double — it never dials a real kubeconfig, always returning the same
// pre-built fake client so a test can inspect what got created on it.
type fakeDownstreamClientBuilder struct {
	client client.Client
	err    error
}

func (f fakeDownstreamClientBuilder) Build(_ []byte) (client.Client, error) {
	if f.err != nil {
		return nil, f.err
	}

	return f.client, nil
}

func newHubFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.Zone{}, &v1alpha2.TalosCluster{}).
		WithObjects(objects...).
		Build()
}

func newDownstreamFakeClient(t *testing.T) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, gatewayv1.Install(scheme))
	require.NoError(t, certmanagerv1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		Build()
}

func testZoneObject() *v1alpha2.Zone {
	return &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneName, Namespace: v1alpha2.DefaultSecretNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: testRegion, Zone: testZone, Domain: testDomain},
	}
}

func readyTalosCluster() *v1alpha2.TalosCluster {
	return &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneName, Namespace: v1alpha2.DefaultSecretNamespace},
		Status: v1alpha2.TalosClusterStatus{
			Conditions: []metav1.Condition{
				{Type: taloscluster.ReadyConditionType, Status: metav1.ConditionTrue, Reason: "AddonsInstalled"},
			},
			SecretRef: v1alpha2.SecretReference{Name: "taloscluster-" + testZoneName, Namespace: "kontinuum-system"},
		},
	}
}

func kubeconfigSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "taloscluster-" + testZoneName, Namespace: "kontinuum-system"},
		Data:       map[string][]byte{"kubeconfig": []byte("fake-kubeconfig")},
	}
}

func registeredKontinuum(name, storage string) (*v1alpha2.Kontinuum, *corev1.Secret) {
	secretName := "kontinuum-" + name

	kontinuum := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.DefaultSecretNamespace},
		Status: v1alpha2.KontinuumStatus{
			SecretRef: v1alpha2.KontinuumSecretReference{Name: secretName, Namespace: "kontinuum-system"},
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: "kontinuum-system"},
		Data:       map[string][]byte{"KONTINUUM_SERVER_STORAGE": []byte(storage)},
	}

	return kontinuum, secret
}

func newReconciler(hubClient client.Client, downstreamBuilder zone.DownstreamClientBuilder) *zone.Reconciler {
	return &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: downstreamBuilder,
		ACMEEmail:               "ops@example.com",
		ACMEServer:              "https://acme-v02.api.letsencrypt.org/directory",
		Image:                   testImage,
		RetryInterval:           testRetryInterval,
		Logger:                  slog.Default(),
	}
}

func reconcileRequest() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testZoneName, Namespace: v1alpha2.DefaultSecretNamespace},
	}
}

func TestReconcileReportsTalosClusterNotFound(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t, testZoneObject())
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.ClusterReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "TalosClusterNotFound", cond.Reason)
}

func TestReconcileWaitsForTalosClusterReady(t *testing.T) {
	t.Parallel()

	notReadyCluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneName, Namespace: v1alpha2.DefaultSecretNamespace},
	}
	hubClient := newHubFakeClient(t, testZoneObject(), notReadyCluster)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.ClusterReadyConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "WaitingForTalosCluster", cond.Reason)
}

func TestReconcileReportsNoStorageSecretFound(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret())
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: newDownstreamFakeClient(t)})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "NoStorageSecretFound", cond.Reason)
}

func TestReconcileInstallsDownstreamObjectsAndWaitsForCertificate(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub", testStorage)
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "WaitingForCertificate", cond.Reason)

	var ns corev1.Namespace
	assert.NoError(t, downstream.Get(t.Context(), client.ObjectKey{Name: "kontinuum-system"}, &ns))

	var secret corev1.Secret
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: "kontinuum-env", Namespace: "kontinuum-system"}, &secret))
	// A real apiserver converts StringData into the base64-encoded Data via
	// admission logic the fake client doesn't replicate — see
	// pkg/domain/registry/heartbeat_test.go's identical note.
	assert.Equal(t, testStorage, secret.StringData["KONTINUUM_SERVER_STORAGE"])

	var configMap corev1.ConfigMap
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: "kontinuum-env", Namespace: "kontinuum-system"}, &configMap))
	assert.Equal(t, testRegion, configMap.Data["KONTINUUM_SERVER_REGION"])
	assert.Equal(t, testZone, configMap.Data["KONTINUUM_SERVER_ZONE"])

	var deployment appsv1.Deployment
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: "kontinuum", Namespace: "kontinuum-system"}, &deployment))
	assert.Equal(t, testImage, deployment.Spec.Template.Spec.Containers[0].Image)

	var service corev1.Service
	assert.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: "kontinuum", Namespace: "kontinuum-system"}, &service))

	var issuer certmanagerv1.ClusterIssuer
	require.NoError(t, downstream.Get(t.Context(), client.ObjectKey{Name: "kontinuum"}, &issuer))
	assert.Equal(t, "ops@example.com", issuer.Spec.ACME.Email)

	var gateway gatewayv1.Gateway
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: "kontinuum", Namespace: "kontinuum-system"}, &gateway))
	assert.Len(t, gateway.Spec.Listeners, 2)

	var httpRoute gatewayv1.HTTPRoute
	assert.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: "kontinuum", Namespace: "kontinuum-system"}, &httpRoute))

	var cert certmanagerv1.Certificate
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: "kontinuum", Namespace: "kontinuum-system"}, &cert))
	assert.Equal(t, []string{testZone + "." + testRegion + "." + testDomain}, cert.Spec.DNSNames)
}

// TestReconcileFlipsInstalledOnceCertificateReady covers both the
// Certificate-Ready gate and re-reconcile idempotency in one pass: the
// first Reconcile creates every downstream object (a fresh Certificate has
// no Ready condition yet, so Installed stays False); the test then
// simulates cert-manager's own controller finishing issuance directly on
// the downstream fake client, and a second Reconcile call — hitting every
// ensureX helper's update-not-create path — must both leave every object
// unchanged and flip Installed True.
func TestReconcileFlipsInstalledOnceCertificateReady(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub", testStorage)
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var cert certmanagerv1.Certificate
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: "kontinuum", Namespace: "kontinuum-system"}, &cert))

	cert.Status.Conditions = []certmanagerv1.CertificateCondition{
		{Type: certmanagerv1.CertificateConditionReady, Status: cmmeta.ConditionTrue, Reason: "Ready"},
	}
	require.NoError(t, downstream.Status().Update(t.Context(), &cert))

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), result.RequeueAfter)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Installed", cond.Reason)
}

func TestReconcileIgnoresMissingZone(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}

func TestReconcileUsesAnyRegisteredKontinuumForStorage(t *testing.T) {
	t.Parallel()

	// Two registered Kontinuums (as would happen once this same zone's own
	// kontinuum-server joins the shared registry) — findKontinuumStorage
	// picks the name-sorted first, regardless of role, per issue #29's
	// explicit "do not discriminate by role" decision.
	worker, workerSecret := registeredKontinuum("aaa-worker", "postgres://worker-copy/db")
	hub, hubSecret := registeredKontinuum("zzz-hub", testStorage)

	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		worker, workerSecret, hub, hubSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var secret corev1.Secret
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: "kontinuum-env", Namespace: "kontinuum-system"}, &secret))
	assert.Equal(t, "postgres://worker-copy/db", secret.StringData["KONTINUUM_SERVER_STORAGE"])
}
