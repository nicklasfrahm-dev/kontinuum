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

func newVerifierTestSecret(t *testing.T, pair etcdproxy.AuthKeyPair) *corev1.Secret {
	t.Helper()

	secret := etcdproxy.BuildAuthSecret(verifierTestZone, verifierTestNamespace, pair)
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

func TestVerifierAuthenticateAcceptsCurrentKey(t *testing.T) {
	t.Parallel()

	now := time.Now()
	secret := newVerifierTestSecret(t, etcdproxy.AuthKeyPair{
		Current:  etcdproxy.AuthKey{Value: testCurrentSecretKey, CreatedAt: now},
		Previous: etcdproxy.AuthKey{Value: testPreviousSecretKey, CreatedAt: now.Add(-etcdproxy.RotationInterval)},
	})

	verifier, err := etcdproxy.NewVerifier(newVerifierTestClient(t, secret), verifierTestNamespace)
	require.NoError(t, err)

	zone, err := verifier.Authenticate(t.Context(), etcdproxy.EncodeToken(verifierTestZone, testCurrentSecretKey))
	require.NoError(t, err)
	assert.Equal(t, verifierTestZone, zone)
}

func TestVerifierAuthenticateAcceptsPreviousKeyWithinOverlap(t *testing.T) {
	t.Parallel()

	now := time.Now()
	secret := newVerifierTestSecret(t, etcdproxy.AuthKeyPair{
		Current:  etcdproxy.AuthKey{Value: testCurrentSecretKey, CreatedAt: now},
		Previous: etcdproxy.AuthKey{Value: testPreviousSecretKey, CreatedAt: now.Add(-etcdproxy.RotationInterval)},
	})

	verifier, err := etcdproxy.NewVerifier(newVerifierTestClient(t, secret), verifierTestNamespace)
	require.NoError(t, err)

	zone, err := verifier.Authenticate(t.Context(), etcdproxy.EncodeToken(verifierTestZone, testPreviousSecretKey))
	require.NoError(t, err)
	assert.Equal(t, verifierTestZone, zone)
}

func TestVerifierAuthenticateRejectsExpiredPreviousKey(t *testing.T) {
	t.Parallel()

	now := time.Now()
	// Demoted well past its own overlap window.
	secret := newVerifierTestSecret(t, etcdproxy.AuthKeyPair{
		Current:  etcdproxy.AuthKey{Value: testCurrentSecretKey, CreatedAt: now},
		Previous: etcdproxy.AuthKey{Value: testPreviousSecretKey, CreatedAt: now.Add(-2 * etcdproxy.RotationInterval)},
	})

	verifier, err := etcdproxy.NewVerifier(newVerifierTestClient(t, secret), verifierTestNamespace)
	require.NoError(t, err)

	_, err = verifier.Authenticate(t.Context(), etcdproxy.EncodeToken(verifierTestZone, testPreviousSecretKey))
	require.ErrorIs(t, err, etcdproxy.ErrUnauthenticated)
}

func TestVerifierAuthenticateRejectsWrongKey(t *testing.T) {
	t.Parallel()

	secret := newVerifierTestSecret(t, etcdproxy.AuthKeyPair{
		Current:  etcdproxy.AuthKey{Value: testCurrentSecretKey, CreatedAt: time.Now()},
		Previous: etcdproxy.AuthKey{Value: testPreviousSecretKey, CreatedAt: time.Now()},
	})

	verifier, err := etcdproxy.NewVerifier(newVerifierTestClient(t, secret), verifierTestNamespace)
	require.NoError(t, err)

	_, err = verifier.Authenticate(t.Context(), etcdproxy.EncodeToken(verifierTestZone, "not-the-right-secret"))
	require.ErrorIs(t, err, etcdproxy.ErrUnauthenticated)
}

func TestVerifierAuthenticateRejectsUnknownZone(t *testing.T) {
	t.Parallel()

	verifier, err := etcdproxy.NewVerifier(newVerifierTestClient(t), verifierTestNamespace)
	require.NoError(t, err)

	_, err = verifier.Authenticate(t.Context(), etcdproxy.EncodeToken("no-such-zone", "whatever"))
	require.ErrorIs(t, err, etcdproxy.ErrUnauthenticated)
}

func TestVerifierAuthenticateRejectsMalformedToken(t *testing.T) {
	t.Parallel()

	verifier, err := etcdproxy.NewVerifier(newVerifierTestClient(t), verifierTestNamespace)
	require.NoError(t, err)

	_, err = verifier.Authenticate(t.Context(), "not-valid-base64!!!")
	require.ErrorIs(t, err, etcdproxy.ErrInvalidToken)
}

// TestVerifierAuthenticateUsesCacheBetweenLookups covers the LRU cache
// itself: once a zone's key pair is fetched, a second Authenticate call
// for the same zone must not need the Secret to still exist — proving the
// second call was served from cache, not a fresh API read.
func TestVerifierAuthenticateUsesCacheBetweenLookups(t *testing.T) {
	t.Parallel()

	now := time.Now()
	secret := newVerifierTestSecret(t, etcdproxy.AuthKeyPair{
		Current:  etcdproxy.AuthKey{Value: testCurrentSecretKey, CreatedAt: now},
		Previous: etcdproxy.AuthKey{Value: testPreviousSecretKey, CreatedAt: now},
	})

	testClient := newVerifierTestClient(t, secret)
	verifier, err := etcdproxy.NewVerifier(testClient, verifierTestNamespace)
	require.NoError(t, err)

	_, err = verifier.Authenticate(t.Context(), etcdproxy.EncodeToken(verifierTestZone, testCurrentSecretKey))
	require.NoError(t, err)

	require.NoError(t, testClient.Delete(t.Context(), secret))

	zone, err := verifier.Authenticate(t.Context(), etcdproxy.EncodeToken(verifierTestZone, testCurrentSecretKey))
	require.NoError(t, err, "the second lookup should be served from cache, not require the Secret to still exist")
	assert.Equal(t, verifierTestZone, zone)
}
