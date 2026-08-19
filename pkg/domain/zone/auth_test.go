package zone //nolint:testpackage // exercises unexported ensureEtcdIdentity directly

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

const authTestZoneName = "eu-eu-1a"

func newAuthTestClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func newAuthTestDownstreamClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func authTestZoneObject() *v1alpha2.Zone {
	return &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: authTestZoneName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: "eu", Zone: "eu-1a", Domain: "example.com"},
	}
}

// simulateAdmission copies StringData into Data (base64-decoded, as a real
// apiserver's admission would) and persists the result — the fake client
// doesn't run that conversion itself, so a test that writes via StringData
// and then wants to re-read the same object through ParsePublicSecret
// (which reads Data) needs this in between, exactly as
// pkg/domain/registry/heartbeat_test.go's own tests already do for the
// same reason.
func simulateAdmission(t *testing.T, targetClient client.Client, secret *corev1.Secret) {
	t.Helper()

	secret.Data = map[string][]byte{}
	for k, v := range secret.StringData {
		secret.Data[k] = []byte(v)
	}

	secret.StringData = nil
	require.NoError(t, targetClient.Update(t.Context(), secret))
}

func getHubIdentitySecret(t *testing.T, c client.Client) *corev1.Secret {
	t.Helper()

	var secret corev1.Secret
	require.NoError(t, c.Get(t.Context(),
		client.ObjectKey{Name: etcdproxy.AuthSecretName(authTestZoneName), Namespace: v1alpha2.KontinuumSystemNamespace},
		&secret))

	return &secret
}

func getDownstreamIdentitySecret(t *testing.T, c client.Client) *corev1.Secret {
	t.Helper()

	var secret corev1.Secret
	require.NoError(t, c.Get(t.Context(),
		client.ObjectKey{Name: etcdproxy.IdentitySecretName, Namespace: downstreamNamespace}, &secret))

	return &secret
}

func admitAndParseHubPair(t *testing.T, c client.Client, secret *corev1.Secret) etcdproxy.IdentityPair {
	t.Helper()

	simulateAdmission(t, c, secret)

	pair, ok := etcdproxy.ParsePublicSecret(getHubIdentitySecret(t, c))
	require.True(t, ok)

	return pair
}

func TestEnsureEtcdIdentityIssuesFreshPairWhenMissing(t *testing.T) {
	t.Parallel()

	zoneObj := authTestZoneObject()
	hubClient := newAuthTestClient(t, zoneObj)
	downstream := newAuthTestDownstreamClient(t)
	reconciler := &Reconciler{Client: hubClient}

	rotated, err := reconciler.ensureEtcdIdentity(t.Context(), downstream, zoneObj)
	require.NoError(t, err)
	assert.False(t, rotated, "a brand new zone's very first identity is never a \"rotation\"")

	downstreamSecret := getDownstreamIdentitySecret(t, downstream)
	assert.Equal(t, corev1.SecretTypeTLS, downstreamSecret.Type)
	assert.NotEmpty(t, downstreamSecret.StringData[corev1.TLSCertKey])
	assert.NotEmpty(t, downstreamSecret.StringData[corev1.TLSPrivateKeyKey])

	hubSecret := getHubIdentitySecret(t, hubClient)

	require.Len(t, hubSecret.OwnerReferences, 1)
	assert.Equal(t, "Zone", hubSecret.OwnerReferences[0].Kind)
	assert.Equal(t, authTestZoneName, hubSecret.OwnerReferences[0].Name)
	assert.True(t, *hubSecret.OwnerReferences[0].Controller)

	pair := admitAndParseHubPair(t, hubClient, hubSecret)
	assert.Equal(t, downstreamSecret.StringData[corev1.TLSCertKey], string(pair.Current.CertPEM))
	assert.Equal(t, pair.Current.CertPEM, pair.Previous.CertPEM,
		"freshly issued: Current and Previous start out identical, since there's no real previous one yet")
}

func TestEnsureEtcdIdentityNoOpWhenNotDue(t *testing.T) {
	t.Parallel()

	zoneObj := authTestZoneObject()

	certPEM, keyPEM, err := etcdproxy.GenerateIdentity(authTestZoneName)
	require.NoError(t, err)

	now := time.Now()
	identity := etcdproxy.Identity{CertPEM: certPEM, IssuedAt: now}

	downstreamSecret := etcdproxy.BuildDownstreamIdentitySecret(downstreamNamespace, certPEM, keyPEM)
	downstream := newAuthTestDownstreamClient(t, downstreamSecret)
	simulateAdmission(t, downstream, downstreamSecret)

	hubSecret := etcdproxy.BuildPublicSecret(authTestZoneName, v1alpha2.KontinuumSystemNamespace,
		etcdproxy.IdentityPair{Current: identity, Previous: identity})
	hubClient := newAuthTestClient(t, zoneObj, hubSecret)
	simulateAdmission(t, hubClient, hubSecret)

	beforeDownstreamRV := getDownstreamIdentitySecret(t, downstream).ResourceVersion
	beforeHubRV := getHubIdentitySecret(t, hubClient).ResourceVersion

	reconciler := &Reconciler{Client: hubClient}

	rotated, err := reconciler.ensureEtcdIdentity(t.Context(), downstream, zoneObj)
	require.NoError(t, err)
	assert.False(t, rotated)

	assert.Equal(t, beforeDownstreamRV, getDownstreamIdentitySecret(t, downstream).ResourceVersion,
		"a not-yet-due identity should not trigger a downstream write")
	assert.Equal(t, beforeHubRV, getHubIdentitySecret(t, hubClient).ResourceVersion,
		"a not-yet-due identity should not trigger a hub write")
}

// TestEnsureEtcdIdentityRotatesDueIdentity covers the core rotation
// contract: once the current identity is due, a fresh keypair replaces it
// downstream and the old one is demoted into the hub's own Previous slot.
func TestEnsureEtcdIdentityRotatesDueIdentity(t *testing.T) {
	t.Parallel()

	zoneObj := authTestZoneObject()

	oldCertPEM, oldKeyPEM, err := etcdproxy.GenerateIdentity(authTestZoneName)
	require.NoError(t, err)

	staleIssuedAt := time.Now().Add(-etcdproxy.IdentityRotationInterval - time.Minute)
	oldIdentity := etcdproxy.Identity{CertPEM: oldCertPEM, IssuedAt: staleIssuedAt}

	downstreamSecret := etcdproxy.BuildDownstreamIdentitySecret(downstreamNamespace, oldCertPEM, oldKeyPEM)
	downstream := newAuthTestDownstreamClient(t, downstreamSecret)
	simulateAdmission(t, downstream, downstreamSecret)

	hubSecret := etcdproxy.BuildPublicSecret(authTestZoneName, v1alpha2.KontinuumSystemNamespace,
		etcdproxy.IdentityPair{Current: oldIdentity, Previous: oldIdentity})
	hubClient := newAuthTestClient(t, zoneObj, hubSecret)
	simulateAdmission(t, hubClient, hubSecret)

	reconciler := &Reconciler{Client: hubClient}

	rotated, err := reconciler.ensureEtcdIdentity(t.Context(), downstream, zoneObj)
	require.NoError(t, err)
	assert.True(t, rotated)

	newDownstreamCert := getDownstreamIdentitySecret(t, downstream).StringData[corev1.TLSCertKey]
	assert.NotEqual(t, string(oldCertPEM), newDownstreamCert, "rotation must deliver a brand-new private key downstream")

	pair := admitAndParseHubPair(t, hubClient, getHubIdentitySecret(t, hubClient))
	assert.Equal(t, newDownstreamCert, string(pair.Current.CertPEM))
	assert.Equal(t, oldCertPEM, pair.Previous.CertPEM, "the just-superseded identity is demoted into Previous")
	assert.WithinDuration(t, staleIssuedAt, pair.Previous.IssuedAt, 0,
		"Previous keeps its own original IssuedAt, so its own ExpiresAt stays fixed")
}

// TestEnsureEtcdIdentityResyncsHubWhenDownstreamAlreadyRotated covers
// recovering from a partial failure: downstream already got a rotation's
// new keypair, but the matching hub write never landed.
func TestEnsureEtcdIdentityResyncsHubWhenDownstreamAlreadyRotated(t *testing.T) {
	t.Parallel()

	zoneObj := authTestZoneObject()

	staleCertPEM, _, err := etcdproxy.GenerateIdentity(authTestZoneName)
	require.NoError(t, err)

	newCertPEM, newKeyPEM, err := etcdproxy.GenerateIdentity(authTestZoneName)
	require.NoError(t, err)

	now := time.Now()
	staleIdentity := etcdproxy.Identity{CertPEM: staleCertPEM, IssuedAt: now.Add(-time.Hour)}

	// downstream already has the *new* keypair — as if a previous
	// rotation's persistDownstreamIdentity succeeded but its matching
	// persistHubPublicSecret call never got the chance to run.
	downstreamSecret := etcdproxy.BuildDownstreamIdentitySecret(downstreamNamespace, newCertPEM, newKeyPEM)
	downstream := newAuthTestDownstreamClient(t, downstreamSecret)
	simulateAdmission(t, downstream, downstreamSecret)

	hubSecret := etcdproxy.BuildPublicSecret(authTestZoneName, v1alpha2.KontinuumSystemNamespace,
		etcdproxy.IdentityPair{Current: staleIdentity, Previous: staleIdentity})
	hubClient := newAuthTestClient(t, zoneObj, hubSecret)
	simulateAdmission(t, hubClient, hubSecret)

	reconciler := &Reconciler{Client: hubClient}

	rotated, err := reconciler.ensureEtcdIdentity(t.Context(), downstream, zoneObj)
	require.NoError(t, err)
	assert.False(t, rotated, "downstream already holds the right key — nothing new to deliver, so no restart is needed")

	assert.Equal(t, newCertPEM, getDownstreamIdentitySecret(t, downstream).Data[corev1.TLSCertKey],
		"downstream must be left untouched")

	pair := admitAndParseHubPair(t, hubClient, getHubIdentitySecret(t, hubClient))
	assert.Equal(t, newCertPEM, pair.Current.CertPEM, "the hub must be resynced to match downstream's own cert")
	assert.Equal(t, staleCertPEM, pair.Previous.CertPEM, "the hub's own prior Current is demoted into Previous")
}

func TestEnsureEtcdIdentityResyncsHubWhenMissingEntirely(t *testing.T) {
	t.Parallel()

	zoneObj := authTestZoneObject()

	certPEM, keyPEM, err := etcdproxy.GenerateIdentity(authTestZoneName)
	require.NoError(t, err)

	downstreamSecret := etcdproxy.BuildDownstreamIdentitySecret(downstreamNamespace, certPEM, keyPEM)
	downstream := newAuthTestDownstreamClient(t, downstreamSecret)
	simulateAdmission(t, downstream, downstreamSecret)

	hubClient := newAuthTestClient(t, zoneObj)
	reconciler := &Reconciler{Client: hubClient}

	rotated, err := reconciler.ensureEtcdIdentity(t.Context(), downstream, zoneObj)
	require.NoError(t, err)
	assert.False(t, rotated)

	pair := admitAndParseHubPair(t, hubClient, getHubIdentitySecret(t, hubClient))
	assert.Equal(t, certPEM, pair.Current.CertPEM)
}

func TestReconcileIdentityRotationScheduleReportsZeroWhenNotIssuedYet(t *testing.T) {
	t.Parallel()

	zoneObj := authTestZoneObject()
	reconciler := &Reconciler{Client: newAuthTestClient(t, zoneObj)}

	requeue, err := reconciler.reconcileIdentityRotationSchedule(t.Context(), zoneObj)
	require.NoError(t, err)
	assert.Zero(t, requeue)
}

func TestReconcileIdentityRotationScheduleReportsTimeUntilDue(t *testing.T) {
	t.Parallel()

	zoneObj := authTestZoneObject()

	identity := etcdproxy.Identity{CertPEM: []byte("cert"), IssuedAt: time.Now()}
	hubSecret := etcdproxy.BuildPublicSecret(authTestZoneName, v1alpha2.KontinuumSystemNamespace,
		etcdproxy.IdentityPair{Current: identity, Previous: identity})

	hubClient := newAuthTestClient(t, zoneObj, hubSecret)
	simulateAdmission(t, hubClient, hubSecret)

	reconciler := &Reconciler{Client: hubClient}

	requeue, err := reconciler.reconcileIdentityRotationSchedule(t.Context(), zoneObj)
	require.NoError(t, err)
	assert.Positive(t, requeue)
	assert.LessOrEqual(t, requeue, identityCheckInterval)
}

func TestReconcileIdentityRotationScheduleReportsCheckIntervalWhenDue(t *testing.T) {
	t.Parallel()

	zoneObj := authTestZoneObject()

	staleIdentity := etcdproxy.Identity{
		CertPEM: []byte("cert"), IssuedAt: time.Now().Add(-etcdproxy.IdentityRotationInterval - time.Minute),
	}
	hubSecret := etcdproxy.BuildPublicSecret(authTestZoneName, v1alpha2.KontinuumSystemNamespace,
		etcdproxy.IdentityPair{Current: staleIdentity, Previous: staleIdentity})

	hubClient := newAuthTestClient(t, zoneObj, hubSecret)
	simulateAdmission(t, hubClient, hubSecret)

	reconciler := &Reconciler{Client: hubClient}

	requeue, err := reconciler.reconcileIdentityRotationSchedule(t.Context(), zoneObj)
	require.NoError(t, err)
	assert.Equal(t, identityCheckInterval, requeue)
}
