package zone_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

const (
	testZoneName      = "eu-eu-1a"
	testRegion        = "eu"
	testZone          = "eu-1a"
	testDomain        = "kontinuum.example.com"
	testTalosAddress  = "10.0.0.5"
	testRetryInterval = 15 * time.Second
	testImageRepo     = "ghcr.io/nicklasfrahm-dev/kontinuum"
	// testKontinuumVersion is registeredKontinuum's own fixture status.version
	// — what resolveImage always reads off a registered Kontinuum and
	// deploys (see that function's own doc).
	testKontinuumVersion = "v1.2.3"
	// testImage is what resolveImage resolves to across most of this file's
	// tests — see registeredKontinuum/testKontinuumVersion.
	testImage = testImageRepo + ":" + testKontinuumVersion
	// testGRPCEndpoint is newReconciler's own Reconciler.GRPCEndpoint —
	// zoneStorageDSN's own KONTINUUM_SERVER_GRPC_ENDPOINT stand-in.
	testGRPCEndpoint = "hub.example.com:8080"

	// testDownstreamNamespace/testDownstreamResourceName mirror
	// pkg/domain/zone's own unexported downstreamNamespace/deploymentName
	// et al. (see workload.go/network.go) — every kontinuum-server object
	// the zone controller installs downstream shares this one namespace and
	// this one resource name, so tests across this package assert against
	// these local copies rather than a literal repeated at every call site.
	testDownstreamNamespace    = "kontinuum-system"
	testDownstreamResourceName = "kontinuum"
	testDownstreamEnvName      = "kontinuum-env"
)

// testZoneKey() is testZoneName's own ObjectKey — every zone-add fixture in
// this file lives in v1alpha2.KontinuumSystemNamespace (see BuildAddObjects'
// own doc), so every Get below needs both, not just Name.
func testZoneKey() client.ObjectKey {
	return client.ObjectKey{Name: testZoneName, Namespace: v1alpha2.KontinuumSystemNamespace}
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

// secretAdmissionClient wraps a client.Client, converting a corev1.Secret's
// StringData into Data on Create/Update — mirroring what a real
// apiserver's admission does, which the fake client doesn't replicate on
// its own. Without this, a test (or, for that matter, production
// reconcile logic — see reconcileAuthKeys followed later in the same
// Reconcile pass by zoneStorageDSN's own Get) that writes a Secret via
// StringData and reads it back via Data in the same test would see an
// apparently-empty object, even though that exact sequence works
// correctly against a real cluster.
type secretAdmissionClient struct {
	client.Client
}

func (c secretAdmissionClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	admitSecret(obj)

	err := c.Client.Create(ctx, obj, opts...)
	if err != nil {
		return fmt.Errorf("failed to create object: %w", err)
	}

	return nil
}

func (c secretAdmissionClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	admitSecret(obj)

	err := c.Client.Update(ctx, obj, opts...)
	if err != nil {
		return fmt.Errorf("failed to update object: %w", err)
	}

	return nil
}

func admitSecret(obj client.Object) {
	secret, ok := obj.(*corev1.Secret)
	if !ok || len(secret.StringData) == 0 {
		return
	}

	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}

	for k, v := range secret.StringData {
		secret.Data[k] = []byte(v)
	}

	secret.StringData = nil
}

func newHubFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	return secretAdmissionClient{fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.Zone{}, &v1alpha2.TalosCluster{}).
		WithObjects(objects...).
		Build()}
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
		ObjectMeta: metav1.ObjectMeta{Name: testZoneName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: testRegion, Zone: testZone, Domain: testDomain},
	}
}

func readyTalosCluster() *v1alpha2.TalosCluster {
	return &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Status: v1alpha2.TalosClusterStatus{
			Conditions: []metav1.Condition{
				{Type: taloscluster.ReadyConditionType, Status: metav1.ConditionTrue, Reason: "AddonsInstalled"},
			},
			SecretRef: v1alpha2.SecretReference{Name: "taloscluster-" + testZoneName, Namespace: testDownstreamNamespace},
		},
	}
}

func kubeconfigSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "taloscluster-" + testZoneName, Namespace: testDownstreamNamespace},
		Data:       map[string][]byte{"kubeconfig": []byte("fake-kubeconfig")},
	}
}

func registeredKontinuum(name string) (*v1alpha2.Kontinuum, *corev1.Secret) {
	secretName := "kontinuum-" + name

	kontinuum := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
		Status: v1alpha2.KontinuumStatus{
			SecretRef: v1alpha2.KontinuumSecretReference{Name: secretName, Namespace: testDownstreamNamespace},
			Version:   testKontinuumVersion,
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: testDownstreamNamespace},
		Data:       map[string][]byte{"KONTINUUM_SERVER_STORAGE": []byte(testStorage)},
	}

	return kontinuum, secret
}

// joinedKontinuum returns a Kontinuum (plus its backing storage-credential
// Secret, mirroring registeredKontinuum) registered for
// testRegion/testZone with a fresh heartbeat — the shape
// zone.FindJoinedKontinuum looks for once this zone's own kontinuum-server
// has actually joined the hub's registry (see
// TestReconcileFlipsReadyOnceKontinuumJoinsRegistry). name is expected to
// sort after "hub" (the other fixture Kontinuum most of this file's tests
// register) so anyRegisteredKontinuum's own name-sorted pick — irrelevant
// to what this helper is testing — keeps landing on a Kontinuum whose
// Secret actually holds a storage connection string.
func joinedKontinuum(name string) (*v1alpha2.Kontinuum, *corev1.Secret) {
	kontinuum, secret := registeredKontinuum(name)
	kontinuum.Spec = v1alpha2.KontinuumSpec{Region: testRegion, Zone: testZone}
	kontinuum.Status.LastHeartbeatTime = metav1.Now()

	return kontinuum, secret
}

func newReconciler(hubClient client.Client, downstreamBuilder zone.DownstreamClientBuilder) *zone.Reconciler {
	return &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: downstreamBuilder,
		ACMEEmail:               "ops@example.com",
		ACMEServer:              "https://acme-v02.api.letsencrypt.org/directory",
		Auth:                    zone.AuthConfig{InsecureAllowAnonymous: "true"},
		ImageRepo:               testImageRepo,
		GRPCEndpoint:            testGRPCEndpoint,
		RetryInterval:           testRetryInterval,
		Logger:                  slog.Default(),
	}
}

func reconcileRequest() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testZoneName, Namespace: v1alpha2.KontinuumSystemNamespace},
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

// assertReadyMirrors asserts that got's own aggregate Ready condition (see
// zone.ReadyConditionType's own doc) reports status/reason — shared by
// every test below that also checks whichever of ClusterReady/Installed
// actually drove it, since Ready always just mirrors that.
func assertReadyMirrors(t *testing.T, got v1alpha2.Zone, status metav1.ConditionStatus, reason string) {
	t.Helper()

	readyCond := meta.FindStatusCondition(got.Status.Conditions, zone.ReadyConditionType)
	require.NotNil(t, readyCond)
	assert.Equal(t, status, readyCond.Status)
	assert.Equal(t, reason, readyCond.Reason)
}

func TestReconcileWaitsForTalosClusterReady(t *testing.T) {
	t.Parallel()

	notReadyCluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testZoneName, Namespace: v1alpha2.KontinuumSystemNamespace},
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

	// A blocked ClusterReady propagates to the aggregate Ready condition too.
	assertReadyMirrors(t, got, metav1.ConditionFalse, "WaitingForTalosCluster")
}

// TestReconcileReportsNoStorageSecretFound covers zoneStorageDSN's own
// errGRPCEndpointNotConfigured path: an operator who hasn't set
// KONTINUUM_SERVER_GRPC_ENDPOINT on the hub has nothing to point a newly
// joined zone's own storage at, even though its auth keys (see
// reconcileAuthKeys, which runs regardless) are already in place.
func TestReconcileReportsNoStorageSecretFound(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret())
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: newDownstreamFakeClient(t)})
	reconciler.GRPCEndpoint = ""

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

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
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

	// Installed always mirrors onto Ready, regardless of its own status —
	// confirms the False branch, not just the True happy path (see
	// TestReconcileFlipsInstalledOnceCertificateReady).
	assertReadyMirrors(t, got, metav1.ConditionFalse, "WaitingForCertificate")

	assertDownstreamFootprintInstalled(t, downstream)
}

// TestReconcileSkipsNetworkInstallWhenDomainUnset covers issue #98's own
// gap: a Zone with no spec.domain (network exposure never configured, e.g.
// a local Talos dev cluster with no public DNS to satisfy ACME's own
// HTTP-01 challenge — see docs/local-setup.md) used to hostname-format its
// way to a malformed "<zone>.<region>." Certificate DNS name and sit stuck
// at WaitingForCertificate forever. With spec.domain unset, Installed must
// flip True as soon as the workload itself installs, and none of
// ClusterIssuer/Gateway/HTTPRoute/Certificate — meaningless without a real
// hostname — get created at all.
func TestReconcileSkipsNetworkInstallWhenDomainUnset(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")

	zoneObj := testZoneObject()
	zoneObj.Spec.Domain = ""

	hubClient := newHubFakeClient(t, zoneObj, readyTalosCluster(), kubeconfigSecret(), kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Installed", cond.Reason)

	var deployment appsv1.Deployment
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &deployment),
		"the workload itself must still install with no domain configured")

	var issuer certmanagerv1.ClusterIssuer

	err = downstream.Get(t.Context(), client.ObjectKey{Name: testDownstreamResourceName}, &issuer)
	assert.True(t, apierrors.IsNotFound(err), "no clusterissuer without a domain to issue a certificate for")

	var gateway gatewayv1.Gateway

	err = downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &gateway)
	assert.True(t, apierrors.IsNotFound(err), "no gateway without a domain to route traffic for")

	var httpRoute gatewayv1.HTTPRoute

	err = downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &httpRoute)
	assert.True(t, apierrors.IsNotFound(err), "no httproute without a domain to route traffic for")

	var cert certmanagerv1.Certificate

	err = downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &cert)
	assert.True(t, apierrors.IsNotFound(err), "no certificate without a domain to issue one for")
}

// TestReconcileInheritsDevVersionFromRegisteredKontinuum covers resolveImage
// deploying ImageRepo:dev when that's literally what a registered
// Kontinuum reports on its own status.version — the case a local
// `make dev` hub's own self-registration produces (pkg/cli/version.go's
// default, unless built with a real -ldflags -X override). resolveImage no
// longer special-cases this: it always inherits whatever version is
// registered, the same as any other tag, trusting it to be real and
// pullable — CI keeps ImageRepo:dev in sync with main on every push, and
// `make image-push` (see the Makefile) publishes the working tree's own
// build under it for exactly this local zone-join scenario.
func TestReconcileInheritsDevVersionFromRegisteredKontinuum(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	kontinuum.Status.Version = "dev"

	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var deployment appsv1.Deployment
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &deployment))
	assert.Equal(t, testImageRepo+":dev", deployment.Spec.Template.Spec.Containers[0].Image)
}

// TestReconcileReportsNoVersionFoundWhenRegisteredKontinuumHasNoVersion
// covers resolveImage's own error path: a registered Kontinuum exists (so
// storage inference already succeeds) but hasn't reported a version yet —
// an unlikely but real momentary window (Heartbeat sets status.version on
// its very first beat, immediately after Create — see
// pkg/domain/registry/heartbeat.go), treated as retryable, mirroring
// TestReconcileReportsNoStorageSecretFound.
func TestReconcileReportsNoVersionFoundWhenRegisteredKontinuumHasNoVersion(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	kontinuum.Status.Version = ""

	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: newDownstreamFakeClient(t)})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "NoVersionFound", cond.Reason)
}

// assertDownstreamFootprintInstalled asserts every object a single
// Reconcile pass installs onto the zone's own downstream cluster exists
// with the expected content — namespace, env Secret/ConfigMap, Deployment/
// Service, ClusterIssuer, Gateway, and Certificate.
func assertDownstreamFootprintInstalled(t *testing.T, downstream client.Client) {
	t.Helper()

	var ns corev1.Namespace
	assert.NoError(t, downstream.Get(t.Context(), client.ObjectKey{Name: testDownstreamNamespace}, &ns))

	var secret corev1.Secret
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &secret))
	// The zone's own storage no longer carries a copy of the hub's raw
	// database DSN (see zoneStorageDSN's own doc) — it's an
	// etcdproxy.BuildRelayDSN pointing back at the hub's own etcd gRPC
	// proxy, carrying this zone's own (randomly generated, so not
	// asserted verbatim) current auth key.
	zoneName, _, hubEndpoint, ok := etcdproxy.ParseRelayDSN(secret.StringData["KONTINUUM_SERVER_STORAGE"])
	require.True(t, ok, "KONTINUUM_SERVER_STORAGE must be a valid etcdproxy relay DSN")
	assert.Equal(t, testZoneName, zoneName)
	assert.Equal(t, testGRPCEndpoint, hubEndpoint)

	var configMap corev1.ConfigMap
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &configMap))
	assert.Equal(t, testRegion, configMap.Data["KONTINUUM_SERVER_REGION"])
	assert.Equal(t, testZone, configMap.Data["KONTINUUM_SERVER_ZONE"])
	// Without this, the deployed process refuses to even start (see
	// pkg/config.Config.ValidateAuthentication) and so never gets as far as
	// heartbeating — the root cause tracked by issue #95.
	assert.Equal(t, "true", configMap.Data["KONTINUUM_INSECURE_ALLOW_ANONYMOUS"])

	var deployment appsv1.Deployment
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &deployment))
	assert.Equal(t, testImage, deployment.Spec.Template.Spec.Containers[0].Image)

	var service corev1.Service
	assert.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &service))

	var issuer certmanagerv1.ClusterIssuer
	require.NoError(t, downstream.Get(t.Context(), client.ObjectKey{Name: testDownstreamResourceName}, &issuer))
	assert.Equal(t, "ops@example.com", issuer.Spec.ACME.Email)

	var gateway gatewayv1.Gateway
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &gateway))
	assert.Len(t, gateway.Spec.Listeners, 2)

	var httpRoute gatewayv1.HTTPRoute
	assert.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &httpRoute))

	var cert certmanagerv1.Certificate
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &cert))
	assert.Equal(t, []string{testZone + "." + testRegion + "." + testDomain}, cert.Spec.DNSNames)
}

// TestReconcileFlipsInstalledOnceCertificateReady covers both the
// Certificate-Ready gate and re-reconcile idempotency in one pass: the
// first Reconcile creates every downstream object (a fresh Certificate has
// no Ready condition yet, so Installed stays False); the test then
// simulates cert-manager's own controller finishing issuance directly on
// the downstream fake client, and a second Reconcile call — hitting every
// ensureX helper's update-not-create path — must both leave every object
// unchanged and flip Installed True. The aggregate Ready condition does not
// flip yet here — see TestReconcileFlipsReadyOnceKontinuumJoinsRegistry for
// that: Installed only means the downstream footprint exists and TLS was
// issued, not that this zone's own kontinuum-server has actually joined the
// hub's registry (see zone.RegistryJoinedConditionType's own doc).
func TestReconcileFlipsInstalledOnceCertificateReady(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var cert certmanagerv1.Certificate
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &cert))

	cert.Status.Conditions = []certmanagerv1.CertificateCondition{
		{Type: certmanagerv1.CertificateConditionReady, Status: cmmeta.ConditionTrue, Reason: "Ready"},
	}
	require.NoError(t, downstream.Status().Update(t.Context(), &cert))

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "Installed", cond.Reason)

	registryCond := meta.FindStatusCondition(got.Status.Conditions, zone.RegistryJoinedConditionType)
	require.NotNil(t, registryCond)
	assert.Equal(t, metav1.ConditionFalse, registryCond.Status)
	assert.Equal(t, "WaitingForRegistry", registryCond.Reason)

	// RegistryJoined, not Installed, is what the aggregate Ready condition
	// (see zone.ReadyConditionType's own doc) mirrors once Installed itself
	// is true.
	assertReadyMirrors(t, got, metav1.ConditionFalse, "WaitingForRegistry")
}

// TestReconcileFlipsReadyOnceKontinuumJoinsRegistry continues past
// TestReconcileFlipsInstalledOnceCertificateReady: once a Kontinuum matching
// this zone's own region/zone shows up in the hub's registry with a fresh
// heartbeat (exactly what this zone's own kontinuum-server produces once it
// can actually start and beat — see zone.AuthConfig's own doc for why it
// used to never get that far), a third Reconcile call must flip
// RegistryJoined — and, with it, the aggregate Ready condition — true.
func TestReconcileFlipsReadyOnceKontinuumJoinsRegistry(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var cert certmanagerv1.Certificate
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &cert))

	cert.Status.Conditions = []certmanagerv1.CertificateCondition{
		{Type: certmanagerv1.CertificateConditionReady, Status: cmmeta.ConditionTrue, Reason: "Ready"},
	}
	require.NoError(t, downstream.Status().Update(t.Context(), &cert))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	worker, workerSecret := joinedKontinuum("zzz-worker")
	require.NoError(t, hubClient.Create(t.Context(), worker))
	require.NoError(t, hubClient.Create(t.Context(), workerSecret))

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	// No longer a bare 0s once fully Ready: reconcileAuthKeys' own requeue
	// deadline (see Reconcile) now always folds in, keeping the Zone
	// reconciling for its whole lifetime so its auth keys keep rotating —
	// see TestReconcileKeepsRequeuingForAuthKeyRotationOnceReady for that
	// behavior's own dedicated coverage.
	assert.LessOrEqual(t, result.RequeueAfter, 5*time.Minute)
	assert.Positive(t, result.RequeueAfter)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	registryCond := meta.FindStatusCondition(got.Status.Conditions, zone.RegistryJoinedConditionType)
	require.NotNil(t, registryCond)
	assert.Equal(t, metav1.ConditionTrue, registryCond.Status)
	assert.Equal(t, "RegistryJoined", registryCond.Reason)

	assertReadyMirrors(t, got, metav1.ConditionTrue, "RegistryJoined")
}

func TestReconcileIgnoresMissingZone(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}

// TestReconcileUsesAnyRegisteredKontinuumForVersion covers
// anyRegisteredKontinuum's own name-sorted-first determinism (see its own
// doc) as it applies to resolveImage's own version lookup — the same
// mechanism findKontinuumStorage used to lean on for storage inference,
// before a zone's own storage started pointing through the hub's etcd
// gRPC proxy instead (see zoneStorageDSN).
func TestReconcileUsesAnyRegisteredKontinuumForVersion(t *testing.T) {
	t.Parallel()

	aaa, aaaSecret := registeredKontinuum("aaa-worker")
	aaa.Status.Version = "v0.0.1-aaa"

	zzz, zzzSecret := registeredKontinuum("zzz-hub")
	zzz.Status.Version = "v0.0.2-zzz"

	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		aaa, aaaSecret, zzz, zzzSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var deployment appsv1.Deployment
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &deployment))
	assert.Equal(t, testImageRepo+":v0.0.1-aaa", deployment.Spec.Template.Spec.Containers[0].Image)
}
