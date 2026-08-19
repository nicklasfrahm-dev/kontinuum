package etcdproxy_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

const (
	verifierTestNamespace = "kontinuum-system"
	verifierTestZone      = "eu-eu-1a"
)

// testIdentity is one GenerateIdentity call's own cert+key, bundled
// together for this file's own tests.
type testIdentity struct {
	certPEM []byte
	keyPEM  []byte
}

func generateTestIdentity(t *testing.T) testIdentity {
	t.Helper()

	certPEM, keyPEM, err := etcdproxy.GenerateIdentity(verifierTestZone)
	require.NoError(t, err)

	return testIdentity{certPEM: certPEM, keyPEM: keyPEM}
}

func signTestToken(t *testing.T, identity testIdentity) string {
	t.Helper()

	priv, err := etcdproxy.LoadPrivateKey(identity.keyPEM)
	require.NoError(t, err)

	token, err := etcdproxy.SignToken(verifierTestZone, priv)
	require.NoError(t, err)

	return token
}

func newVerifierTestSecret(t *testing.T, pair etcdproxy.IdentityPair) *corev1.Secret {
	t.Helper()

	secret := etcdproxy.BuildPublicSecret(verifierTestZone, verifierTestNamespace, pair)
	secret.Data = map[string][]byte{}

	for k, v := range secret.StringData {
		secret.Data[k] = []byte(v)
	}

	secret.StringData = nil

	return secret
}

func newVerifierTestClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func TestVerifierAuthenticateAcceptsCurrentIdentity(t *testing.T) {
	t.Parallel()

	now := time.Now()
	current := generateTestIdentity(t)
	previous := generateTestIdentity(t)

	secret := newVerifierTestSecret(t, etcdproxy.IdentityPair{
		Current:  etcdproxy.Identity{CertPEM: current.certPEM, IssuedAt: now},
		Previous: etcdproxy.Identity{CertPEM: previous.certPEM, IssuedAt: now.Add(-etcdproxy.IdentityRotationInterval)},
	})

	verifier, err := etcdproxy.NewVerifier(newVerifierTestClient(t, secret), verifierTestNamespace)
	require.NoError(t, err)

	zone, err := verifier.Authenticate(t.Context(), signTestToken(t, current))
	require.NoError(t, err)
	assert.Equal(t, verifierTestZone, zone)
}

func TestVerifierAuthenticateAcceptsPreviousIdentityWithinOverlap(t *testing.T) {
	t.Parallel()

	now := time.Now()
	current := generateTestIdentity(t)
	previous := generateTestIdentity(t)

	secret := newVerifierTestSecret(t, etcdproxy.IdentityPair{
		Current:  etcdproxy.Identity{CertPEM: current.certPEM, IssuedAt: now},
		Previous: etcdproxy.Identity{CertPEM: previous.certPEM, IssuedAt: now.Add(-etcdproxy.IdentityRotationInterval)},
	})

	verifier, err := etcdproxy.NewVerifier(newVerifierTestClient(t, secret), verifierTestNamespace)
	require.NoError(t, err)

	zone, err := verifier.Authenticate(t.Context(), signTestToken(t, previous))
	require.NoError(t, err)
	assert.Equal(t, verifierTestZone, zone)
}

func TestVerifierAuthenticateRejectsExpiredPreviousIdentity(t *testing.T) {
	t.Parallel()

	now := time.Now()
	current := generateTestIdentity(t)
	previous := generateTestIdentity(t)

	// Demoted well past its own overlap window.
	secret := newVerifierTestSecret(t, etcdproxy.IdentityPair{
		Current:  etcdproxy.Identity{CertPEM: current.certPEM, IssuedAt: now},
		Previous: etcdproxy.Identity{CertPEM: previous.certPEM, IssuedAt: now.Add(-2 * etcdproxy.IdentityRotationInterval)},
	})

	verifier, err := etcdproxy.NewVerifier(newVerifierTestClient(t, secret), verifierTestNamespace)
	require.NoError(t, err)

	_, err = verifier.Authenticate(t.Context(), signTestToken(t, previous))
	require.ErrorIs(t, err, etcdproxy.ErrUnauthenticated)
}

func TestVerifierAuthenticateRejectsWrongKey(t *testing.T) {
	t.Parallel()

	now := time.Now()
	current := generateTestIdentity(t)
	previous := generateTestIdentity(t)
	unrelated := generateTestIdentity(t)

	secret := newVerifierTestSecret(t, etcdproxy.IdentityPair{
		Current:  etcdproxy.Identity{CertPEM: current.certPEM, IssuedAt: now},
		Previous: etcdproxy.Identity{CertPEM: previous.certPEM, IssuedAt: now},
	})

	verifier, err := etcdproxy.NewVerifier(newVerifierTestClient(t, secret), verifierTestNamespace)
	require.NoError(t, err)

	_, err = verifier.Authenticate(t.Context(), signTestToken(t, unrelated))
	require.ErrorIs(t, err, etcdproxy.ErrUnauthenticated)
}

func TestVerifierAuthenticateRejectsUnknownZone(t *testing.T) {
	t.Parallel()

	verifier, err := etcdproxy.NewVerifier(newVerifierTestClient(t), verifierTestNamespace)
	require.NoError(t, err)

	_, err = verifier.Authenticate(t.Context(), signTestToken(t, generateTestIdentity(t)))
	require.ErrorIs(t, err, etcdproxy.ErrUnauthenticated)
}

func TestVerifierAuthenticateRejectsMalformedToken(t *testing.T) {
	t.Parallel()

	verifier, err := etcdproxy.NewVerifier(newVerifierTestClient(t), verifierTestNamespace)
	require.NoError(t, err)

	_, err = verifier.Authenticate(t.Context(), "not-a-jwt")
	require.ErrorIs(t, err, etcdproxy.ErrUnauthenticated)
}

// TestVerifierAuthenticateUsesCacheBetweenLookups covers the LRU cache
// itself: once a zone's identity is fetched, a second Authenticate call
// for the same zone must not need the Secret to still exist — proving the
// second call was served from cache, not a fresh API read.
func TestVerifierAuthenticateUsesCacheBetweenLookups(t *testing.T) {
	t.Parallel()

	now := time.Now()
	current := generateTestIdentity(t)

	secret := newVerifierTestSecret(t, etcdproxy.IdentityPair{
		Current:  etcdproxy.Identity{CertPEM: current.certPEM, IssuedAt: now},
		Previous: etcdproxy.Identity{CertPEM: current.certPEM, IssuedAt: now},
	})

	testClient := newVerifierTestClient(t, secret)
	verifier, err := etcdproxy.NewVerifier(testClient, verifierTestNamespace)
	require.NoError(t, err)

	token := signTestToken(t, current)

	_, err = verifier.Authenticate(t.Context(), token)
	require.NoError(t, err)

	require.NoError(t, testClient.Delete(t.Context(), secret))

	zone, err := verifier.Authenticate(t.Context(), token)
	require.NoError(t, err, "the second lookup should be served from cache, not require the Secret to still exist")
	assert.Equal(t, verifierTestZone, zone)
}
