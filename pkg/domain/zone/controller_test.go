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
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
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
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
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
	// testGRPCEndpoint is newReconciler's own
	// Reconciler.HubConfig.Server.GRPC.Endpoint — zoneStorageDSN's own
	// KONTINUUM_SERVER_GRPC_ENDPOINT stand-in.
	testGRPCEndpoint = "hub.example.com:8080"
	// testHubOIDCRedirectURL is the hub's own configured
	// KONTINUUM_OIDC_REDIRECT_URL in the handful of tests that turn OIDC on
	// (testHubConfig's own default leaves OIDC unconfigured) — a redirect
	// URL registered with a real issuer for the hub's own host, standing in
	// for what a joined zone with no domain of its own must fall back to.
	//
	//nolint:gosec // false positive: a URL, not a credential
	testHubOIDCRedirectURL = "https://hub.example.com/app"

	// testDownstreamNamespace/testDownstreamResourceName mirror
	// pkg/domain/zone's own unexported downstreamNamespace/deploymentName
	// et al. (see workload.go/network.go) — every kontinuum-server object
	// the zone controller installs downstream shares this one namespace and
	// this one resource name, so tests across this package assert against
	// these local copies rather than a literal repeated at every call site.
	testDownstreamNamespace    = "kontinuum-system"
	testDownstreamResourceName = "kontinuum"
	testDownstreamEnvName      = "kontinuum-env"

	// testCertificateReadyReason is the cert-manager Reason this file's
	// fixtures use to flip a downstream Certificate's own Ready condition
	// true — shared so repeating the literal across call sites doesn't trip
	// goconst.
	testCertificateReadyReason = "Ready"
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

// statusUpdateCountingClient wraps a client.Client, counting every
// Status().Update call made through it — see
// TestReconcileSkipsRedundantStatusUpdate's own doc for what this is used
// to verify.
type statusUpdateCountingClient struct {
	client.Client

	statusUpdates *int
}

//nolint:ireturn // client.Client's own Status() signature dictates this; wrapping it is the point.
func (c statusUpdateCountingClient) Status() client.SubResourceWriter {
	return countingStatusWriter{c.Client.Status(), c.statusUpdates}
}

type countingStatusWriter struct {
	client.SubResourceWriter

	count *int
}

func (w countingStatusWriter) Update(
	ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption,
) error {
	*w.count++

	err := w.SubResourceWriter.Update(ctx, obj, opts...)
	if err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	return nil
}

func newHubFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, coordinationv1.AddToScheme(scheme))

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
	require.NoError(t, rbacv1.AddToScheme(scheme))
	require.NoError(t, gatewayv1.Install(scheme))
	require.NoError(t, certmanagerv1.AddToScheme(scheme))
	zone.AddExternalDNSToScheme(scheme)

	// Wrapped the same way newHubFakeClient is (see secretAdmissionClient's
	// own doc) — ensureEtcdIdentity's own re-reconcile path (see auth.go)
	// reads a downstream identity Secret's .Data back to decide whether to
	// rotate it, which only ever gets populated by real admission
	// converting an earlier .StringData write; without this, a second
	// Reconcile pass would see an apparently cert-less Secret.
	return secretAdmissionClient{fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		Build()}
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

// testDNSCredential is a fake DNS provider credential, not a real one.
//
//nolint:gosec // false positive: fixture data, not a real credential
const testDNSCredential = "AKIAEXAMPLE:secret"

// registeredKontinuumWithDNS extends registeredKontinuum with a DNS provider
// credential stored under the same Secret — see
// v1alpha2.KontinuumDNSConfigStatus.Credential's own doc for why it lives
// alongside Storage. Named "hub", not parameterized: every dns_test.go
// caller wants the same fixture, mirroring how those tests never need
// registeredKontinuum's own multi-Kontinuum flexibility either.
func registeredKontinuumWithDNS() (*v1alpha2.Kontinuum, *corev1.Secret) {
	kontinuum, secret := registeredKontinuum("hub")
	secret.Data["KONTINUUM_SERVER_DNS_CREDENTIAL"] = []byte(testDNSCredential)

	return kontinuum, secret
}

// testHubConfig is newReconciler's own Reconciler.HubConfig — a hub
// configured with anonymous access (not OIDC) and a real ACME/GRPC
// setup, mirroring what a real hub's KONTINUUM_-prefixed env vars would
// produce (see pkg/config.Load).
func testHubConfig() *config.Config {
	return &config.Config{
		InsecureAllowAnonymous: "true",
		// Non-default values with no zoneEnvOverrides entry of their own —
		// assertDownstreamFootprintInstalled checks these land on a joined
		// zone's own ConfigMap unchanged, proving ensureEnv's straight-copy
		// path actually works, not just the explicitly-overridden fields.
		Log: v1alpha2.KontinuumLogConfigStatus{
			Level:  "debug",
			Format: "console",
		},
		ACME: v1alpha2.KontinuumACMEConfigStatus{
			Email:  "ops@example.com",
			Server: "https://acme-v02.api.letsencrypt.org/directory",
		},
		Server: v1alpha2.KontinuumServerConfigStatus{
			GRPC: v1alpha2.KontinuumGRPCConfigStatus{
				Endpoint:              testGRPCEndpoint,
				InsecureTLSSkipVerify: "true",
			},
		},
	}
}

// enableOIDCForTest turns hubConfig's authentication choice from
// testHubConfig's own anonymous default to a real OIDC setup — shared by
// the two tests exercising KONTINUUM_OIDC_REDIRECT_URL's own zone-specific
// override (see zoneEnvOverrides), so neither hand-rolls the same
// four-field struct literal.
func enableOIDCForTest(hubConfig *config.Config) {
	hubConfig.InsecureAllowAnonymous = "false"
	hubConfig.OIDC = v1alpha2.KontinuumOIDCConfigStatus{
		IssuerURL:   "https://auth.example.com",
		ClientID:    "kontinuum",
		RedirectURL: testHubOIDCRedirectURL,
		AdminGroups: "example:platform",
	}
}

func newReconciler(hubClient client.Client, downstreamBuilder zone.DownstreamClientBuilder) *zone.Reconciler {
	return &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: downstreamBuilder,
		HubConfig:               testHubConfig(),
		ImageRepo:               testImageRepo,
		RetryInterval:           testRetryInterval,
		Locker:                  zonelease.NewLocker(hubClient, hubClient, "test-hub", "", 0),
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

// TestReconcileSkipsRedundantStatusUpdate guards against a reconcile storm:
// this controller's own Zone watch (see SetupWithManager) carries no
// predicate, so any Status().Update — even one that changes nothing —
// re-triggers Reconcile, and two such reconciles racing each other is
// exactly what produces the "the object has been modified" conflicts this
// test exists to prevent. Reconciling twice in a row against unchanged
// state (no TalosCluster, same as TestReconcileReportsTalosClusterNotFound)
// must only write status once — the second pass computes the identical
// condition and should skip the write entirely.
func TestReconcileSkipsRedundantStatusUpdate(t *testing.T) {
	t.Parallel()

	statusUpdates := 0
	hubClient := statusUpdateCountingClient{newHubFakeClient(t, testZoneObject()), &statusUpdates}
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, 1, statusUpdates, "first reconcile should persist the new ClusterReady=False condition")

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	assert.Equal(t, 1, statusUpdates, "second reconcile computes the same condition and should not write again")
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
// joined zone's own storage at, even though its etcd proxy identity (see
// ensureEtcdIdentity, which runs earlier in the same reconcileInstall
// pass) is already in place.
func TestReconcileReportsNoStorageSecretFound(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret())
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: newDownstreamFakeClient(t)})
	reconciler.HubConfig.Server.GRPC.Endpoint = ""

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

// TestReconcileRotatesEtcdIdentity covers the core rotation contract at
// the controller level: once a zone's own identity is due (backdated here
// to simulate etcdproxy.IdentityRotationInterval having elapsed), the next
// Reconcile pass must deliver a brand-new keypair downstream. Unlike the
// pod-restart scheme this replaced, nothing on the Deployment itself needs
// to change for that new keypair to take effect — the already-running
// pod's own etcdproxy.WatchIdentity picks it up directly (exercised at the
// etcdproxy package level, not here: this controller has no way to run
// that pod's own process against the fake downstream client).
func TestReconcileRotatesEtcdIdentity(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var downstreamIdentityBefore corev1.Secret
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: etcdproxy.IdentitySecretName, Namespace: testDownstreamNamespace}, &downstreamIdentityBefore))

	var hubIdentitySecret corev1.Secret
	require.NoError(t, hubClient.Get(t.Context(),
		client.ObjectKey{Name: etcdproxy.AuthSecretName(testZoneName), Namespace: v1alpha2.KontinuumSystemNamespace},
		&hubIdentitySecret))

	pair, ok := etcdproxy.ParsePublicSecret(&hubIdentitySecret)
	require.True(t, ok)

	// Backdate Current past its own DueAt, as if IdentityRotationInterval
	// had genuinely elapsed since the first Reconcile issued it.
	staleCurrent := pair.Current
	staleCurrent.IssuedAt = time.Now().Add(-etcdproxy.IdentityRotationInterval - time.Minute)
	backdated := etcdproxy.BuildPublicSecret(testZoneName, v1alpha2.KontinuumSystemNamespace,
		etcdproxy.IdentityPair{Current: staleCurrent, Previous: pair.Previous})
	backdated.ResourceVersion = hubIdentitySecret.ResourceVersion
	require.NoError(t, hubClient.Update(t.Context(), backdated))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var downstreamIdentityAfter corev1.Secret
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: etcdproxy.IdentitySecretName, Namespace: testDownstreamNamespace}, &downstreamIdentityAfter))
	assert.NotEqual(t, downstreamIdentityBefore.Data[corev1.TLSCertKey], downstreamIdentityAfter.Data[corev1.TLSCertKey],
		"rotation must deliver a brand-new private key downstream")
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
//
// It also covers the same gap for KONTINUUM_OIDC_REDIRECT_URL: with no
// hostname to compute a zone-specific one from, zoneEnvOverrides used to
// still unconditionally set "https://" + "" + "/app" — a malformed URL, on
// every zone with no domain configured, not just this one's own local-dev
// case. It must instead fall back to the hub's own configured redirect
// URL, exactly like every other field ensureEnv doesn't override.
func TestReconcileSkipsNetworkInstallWhenDomainUnset(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")

	zoneObj := testZoneObject()
	zoneObj.Spec.Domain = ""

	hubClient := newHubFakeClient(t, zoneObj, readyTalosCluster(), kubeconfigSecret(), kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})
	enableOIDCForTest(reconciler.HubConfig)

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

	var configMap corev1.ConfigMap
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &configMap))
	assert.Equal(t, testHubOIDCRedirectURL, configMap.Data["KONTINUUM_OIDC_REDIRECT_URL"],
		"no hostname to compute a zone-specific redirect URL from — must fall back to the hub's own "+
			`value, not a malformed "https:///app"`)

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

// TestReconcileComputesZoneSpecificOIDCRedirectURL covers the opposite
// case from TestReconcileSkipsNetworkInstallWhenDomainUnset: a zone with a
// real hostname (testZoneObject's own spec.domain) must get its own
// "https://<zone>.<region>.<domain>/app" redirect URL, not a straight copy
// of the hub's own value — a redirect URL registered with the issuer for
// the hub's own host would never match a browser completing a login
// against this zone's own domain.
func TestReconcileComputesZoneSpecificOIDCRedirectURL(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})
	enableOIDCForTest(reconciler.HubConfig)

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var configMap corev1.ConfigMap
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &configMap))
	assert.Equal(t, "https://"+testZone+"."+testRegion+"."+testDomain+"/app",
		configMap.Data["KONTINUUM_OIDC_REDIRECT_URL"])
	// Everything else OIDC still copies straight from the hub.
	assert.Equal(t, "https://auth.example.com", configMap.Data["KONTINUUM_OIDC_ISSUER_URL"])
	assert.Equal(t, "example:platform", configMap.Data["KONTINUUM_OIDC_ADMIN_GROUPS"])
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

// assertDownstreamEnvConfigMap asserts the kontinuum-env ConfigMap
// assertDownstreamFootprintInstalled fetches carries both the fields
// zoneEnvOverrides computes fresh for this zone and, unchanged, the
// fields ensureEnv copies straight off the hub's own HubConfig.EnvVars —
// split out of that larger helper only to keep it under funlen's limit.
func assertDownstreamEnvConfigMap(t *testing.T, configMap corev1.ConfigMap) {
	t.Helper()

	assert.Equal(t, testRegion, configMap.Data["KONTINUUM_SERVER_REGION"])
	assert.Equal(t, testZone, configMap.Data["KONTINUUM_SERVER_ZONE"])
	// Without this, the deployed process refuses to even start (see
	// pkg/config.Config.ValidateAuthentication) and so never gets as far as
	// heartbeating — the root cause tracked by issue #95.
	assert.Equal(t, "true", configMap.Data["KONTINUUM_INSECURE_ALLOW_ANONYMOUS"])
	// Without this, the deployed process's own relay back through the
	// hub's own KONTINUUM_SERVER_STORAGE endpoint fails real TLS
	// certificate verification against a self-signed dev proxy and
	// crash-loops — this process's own env is what actually needs it, not
	// the hub's (see ensureConfigMap's own doc).
	assert.Equal(t, "true", configMap.Data["KONTINUUM_SERVER_GRPC_INSECURE_TLS_SKIP_VERIFY"])
	// Without this, this deployed process's own Zone controller (it runs
	// the full kontinuum server too — see ensureConfigMap's own doc) logs
	// "this hub has no KONTINUUM_SERVER_GRPC_ENDPOINT configured" on every
	// reconcile of any Zone visible in its shared storage, and can't build
	// a working KONTINUUM_SERVER_STORAGE for any further zone it joins.
	assert.Equal(t, testGRPCEndpoint, configMap.Data["KONTINUUM_SERVER_GRPC_ENDPOINT"])
	// Log.Level/Format (see testHubConfig) have no zoneEnvOverrides entry
	// — they're copied straight off HubConfig.EnvVars() like any other
	// field with no reason to differ per zone, confirming that path
	// actually works, not just the explicitly-overridden fields above.
	assert.Equal(t, "debug", configMap.Data["KONTINUUM_LOG_LEVEL"])
	assert.Equal(t, "console", configMap.Data["KONTINUUM_LOG_FORMAT"])
	// Storage is tagged `secret:"true"` (see api/v1alpha2.KontinuumServerConfigStatus)
	// — ensureEnv must route it into the Secret only, never duplicate it
	// into the broadly-readable ConfigMap.
	assert.NotContains(t, configMap.Data, "KONTINUUM_SERVER_STORAGE")
}

// assertDownstreamFootprintInstalled asserts every object a single
// Reconcile pass installs onto the zone's own downstream cluster exists
// with the expected content — namespace, env Secret/ConfigMap, Deployment/
// Service, ClusterIssuer, Gateway, and Certificate.
// etcdIdentityServiceAccountName mirrors the zone package's own unexported
// constant of the same name (see workload.go) — this file is package
// zone_test, so it can't reference it directly.
const etcdIdentityServiceAccountName = "kontinuum-etcd-identity-watcher"

// assertIdentityRBACInstalled checks the ServiceAccount, Role, and
// RoleBinding ensureIdentityRBAC installs. get is scoped by ResourceNames
// to exactly the zone's own identity Secret; list/watch can't be scoped
// that way at all — Kubernetes RBAC only supports ResourceNames for verbs
// targeting one already-identified object, so a Role combining
// ResourceNames with list/watch is rejected outright by the apiserver
// regardless of the name given — see workload.go's ensureIdentityRole doc.
func assertIdentityRBACInstalled(t *testing.T, downstream client.Client) {
	t.Helper()

	var sa corev1.ServiceAccount
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: etcdIdentityServiceAccountName, Namespace: testDownstreamNamespace}, &sa))

	var role rbacv1.Role
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: etcdIdentityServiceAccountName, Namespace: testDownstreamNamespace}, &role))
	require.Len(t, role.Rules, 2)

	assert.Equal(t, []string{"secrets"}, role.Rules[0].Resources)
	assert.Equal(t, []string{etcdproxy.IdentitySecretName}, role.Rules[0].ResourceNames,
		"get must be scoped to exactly this one Secret, not every Secret in the namespace")
	assert.Equal(t, []string{"get"}, role.Rules[0].Verbs)

	assert.Equal(t, []string{"secrets"}, role.Rules[1].Resources)
	assert.Empty(t, role.Rules[1].ResourceNames,
		"list/watch cannot be scoped by ResourceNames — Kubernetes RBAC rejects that combination outright")
	assert.ElementsMatch(t, []string{"list", "watch"}, role.Rules[1].Verbs)

	var binding rbacv1.RoleBinding
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: etcdIdentityServiceAccountName, Namespace: testDownstreamNamespace}, &binding))
	require.Len(t, binding.Subjects, 1)
	assert.Equal(t, etcdIdentityServiceAccountName, binding.Subjects[0].Name)
	assert.Equal(t, etcdIdentityServiceAccountName, binding.RoleRef.Name)
}

func assertDownstreamFootprintInstalled(t *testing.T, downstream client.Client) {
	t.Helper()

	var ns corev1.Namespace
	assert.NoError(t, downstream.Get(t.Context(), client.ObjectKey{Name: testDownstreamNamespace}, &ns))

	var secret corev1.Secret
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &secret))
	// The zone's own storage no longer carries a copy of the hub's raw
	// database DSN (see zoneStorageDSN's own doc) — it's a plain
	// etcdproxy.BuildRelayDSN pointing back at the hub's own etcd gRPC
	// proxy, carrying no credential of its own (see that function's own
	// doc for why — the zone's actual identity lives in its own mounted
	// kubernetes.io/tls Secret instead, asserted separately below).
	zoneName, hubEndpoint, ok := etcdproxy.ParseRelayDSN(string(secret.Data["KONTINUUM_SERVER_STORAGE"]))
	require.True(t, ok, "KONTINUUM_SERVER_STORAGE must be a valid etcdproxy relay DSN")
	assert.Equal(t, testZoneName, zoneName)
	assert.Equal(t, testGRPCEndpoint, hubEndpoint)

	var identitySecret corev1.Secret
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: etcdproxy.IdentitySecretName, Namespace: testDownstreamNamespace}, &identitySecret))
	assert.Equal(t, corev1.SecretTypeTLS, identitySecret.Type)
	assert.NotEmpty(t, identitySecret.Data[corev1.TLSCertKey])
	assert.NotEmpty(t, identitySecret.Data[corev1.TLSPrivateKeyKey])

	assertIdentityRBACInstalled(t, downstream)

	var configMap corev1.ConfigMap
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &configMap))
	assertDownstreamEnvConfigMap(t, configMap)

	var deployment appsv1.Deployment
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &deployment))
	assert.Equal(t, testImage, deployment.Spec.Template.Spec.Containers[0].Image)
	assert.Equal(t, corev1.PullIfNotPresent, deployment.Spec.Template.Spec.Containers[0].ImagePullPolicy,
		"a real semver tag is immutable once published — safe to cache")
	assert.Equal(t, etcdIdentityServiceAccountName, deployment.Spec.Template.Spec.ServiceAccountName,
		"the pod must run as the narrowly scoped identity-watching ServiceAccount, not the namespace default")

	var service corev1.Service
	assert.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &service))

	var issuer certmanagerv1.ClusterIssuer
	require.NoError(t, downstream.Get(t.Context(), client.ObjectKey{Name: testDownstreamResourceName}, &issuer))
	assert.Equal(t, testACMEEmail, issuer.Spec.ACME.Email)

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
		{Type: certmanagerv1.CertificateConditionReady, Status: cmmeta.ConditionTrue, Reason: testCertificateReadyReason},
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
		{Type: certmanagerv1.CertificateConditionReady, Status: cmmeta.ConditionTrue, Reason: testCertificateReadyReason},
	}
	require.NoError(t, downstream.Status().Update(t.Context(), &cert))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	worker, workerSecret := joinedKontinuum("zzz-worker")
	require.NoError(t, hubClient.Create(t.Context(), worker))
	require.NoError(t, hubClient.Create(t.Context(), workerSecret))

	result, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)
	// No longer a bare 0s once fully Ready: reconcileIdentityRotationSchedule's
	// own requeue deadline (see Reconcile) now always folds in, keeping the
	// Zone reconciling for its whole lifetime so its etcd proxy identity
	// keeps rotating. 30*time.Minute mirrors the zone package's own
	// unexported identityCheckInterval.
	assert.LessOrEqual(t, result.RequeueAfter, 30*time.Minute)
	assert.Positive(t, result.RequeueAfter)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	registryCond := meta.FindStatusCondition(got.Status.Conditions, zone.RegistryJoinedConditionType)
	require.NotNil(t, registryCond)
	assert.Equal(t, metav1.ConditionTrue, registryCond.Status)
	assert.Equal(t, "RegistryJoined", registryCond.Reason)

	assertReadyMirrors(t, got, metav1.ConditionTrue, "RegistryJoined")
}

// TestReconcileFlipsRegistryJoinedFalseOnceKontinuumGoesStale is the
// regression test for the bug mapKontinuumToZone's watch closes: before it
// existed, nothing ever re-triggered Reconcile once RegistryJoined (and the
// aggregate Ready) flipped true — persistStatus stops requeuing on
// ConditionTrue, and SetupWithManager watched only Zone and TalosCluster,
// never Kontinuum — so a zone's own kontinuum-server later going stale (its
// Kontinuum deleted by TTLReconciler after StaleThreshold, a crash that
// never re-registers, manual deregistration) would leave the condition
// stuck reporting "registered and heartbeating" forever. This continues
// past TestReconcileFlipsReadyOnceKontinuumJoinsRegistry — once joined,
// delete the joined Kontinuum out from under it — and asserts that a
// Reconcile call (standing in for the one mapKontinuumToZone's watch would
// now enqueue) correctly flips RegistryJoined back to false, proving
// Reconcile's own re-derivation was always correct and only ever needed a
// trigger.
func TestReconcileFlipsRegistryJoinedFalseOnceKontinuumGoesStale(t *testing.T) {
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
		{Type: certmanagerv1.CertificateConditionReady, Status: cmmeta.ConditionTrue, Reason: testCertificateReadyReason},
	}
	require.NoError(t, downstream.Status().Update(t.Context(), &cert))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	worker, workerSecret := joinedKontinuum("zzz-worker")
	require.NoError(t, hubClient.Create(t.Context(), worker))
	require.NoError(t, hubClient.Create(t.Context(), workerSecret))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var joined v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &joined))
	require.Equal(t, metav1.ConditionTrue,
		meta.FindStatusCondition(joined.Status.Conditions, zone.RegistryJoinedConditionType).Status,
		"test setup must reach RegistryJoined=true before the regression itself can be exercised")

	require.NoError(t, hubClient.Delete(t.Context(), worker))

	_, err = reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	registryCond := meta.FindStatusCondition(got.Status.Conditions, zone.RegistryJoinedConditionType)
	require.NotNil(t, registryCond)
	assert.Equal(t, metav1.ConditionFalse, registryCond.Status)
	assert.Equal(t, "WaitingForRegistry", registryCond.Reason)

	assertReadyMirrors(t, got, metav1.ConditionFalse, "WaitingForRegistry")
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
