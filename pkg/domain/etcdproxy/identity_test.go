package etcdproxy_test

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

const identityTestZone = "eu-eu-1a"

func TestGenerateIdentityProducesMatchingKeypair(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM, err := etcdproxy.GenerateIdentity(identityTestZone)
	require.NoError(t, err)

	pub, err := etcdproxy.PublicKeyFromCert(certPEM)
	require.NoError(t, err)

	priv, err := etcdproxy.LoadPrivateKey(keyPEM)
	require.NoError(t, err)

	assert.Equal(t, priv.Public().(ed25519.PublicKey), pub, //nolint:forcetypeassert // known ed25519 key
		"the certificate's own public key must match the private key it was issued alongside")

	message := []byte("round-trip")
	assert.True(t, ed25519.Verify(pub, message, ed25519.Sign(priv, message)))
}

func TestGenerateIdentityProducesDistinctKeypairs(t *testing.T) {
	t.Parallel()

	firstCert, _, err := etcdproxy.GenerateIdentity(identityTestZone)
	require.NoError(t, err)

	secondCert, _, err := etcdproxy.GenerateIdentity(identityTestZone)
	require.NoError(t, err)

	assert.NotEqual(t, firstCert, secondCert)
}

func TestThumbprintIsStableAndDistinct(t *testing.T) {
	t.Parallel()

	certPEM, _, err := etcdproxy.GenerateIdentity(identityTestZone)
	require.NoError(t, err)

	first, err := etcdproxy.Thumbprint(certPEM)
	require.NoError(t, err)

	second, err := etcdproxy.Thumbprint(certPEM)
	require.NoError(t, err)
	assert.Equal(t, first, second, "the same certificate must always hash to the same thumbprint")

	otherCert, _, err := etcdproxy.GenerateIdentity(identityTestZone)
	require.NoError(t, err)

	otherThumbprint, err := etcdproxy.Thumbprint(otherCert)
	require.NoError(t, err)
	assert.NotEqual(t, first, otherThumbprint)
}

func TestPublicKeyFromCertRejectsMalformedPEM(t *testing.T) {
	t.Parallel()

	_, err := etcdproxy.PublicKeyFromCert([]byte("not a certificate"))
	require.ErrorIs(t, err, etcdproxy.ErrInvalidCertificate)
}

func TestIdentityDueAtExpiresAtAndValid(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	identity := etcdproxy.Identity{CertPEM: []byte("cert"), IssuedAt: now}

	dueAt := now.Add(etcdproxy.IdentityRotationInterval)
	expiresAt := dueAt.Add(etcdproxy.IdentityOverlapWindow)

	assert.Equal(t, dueAt, identity.DueAt())
	assert.Equal(t, expiresAt, identity.ExpiresAt())

	assert.True(t, identity.Valid(dueAt), "still valid right at its own rotation-due time — demoted, not expired, then")
	assert.True(t, identity.Valid(expiresAt.Add(-time.Second)))
	assert.False(t, identity.Valid(expiresAt))
	assert.False(t, identity.Valid(expiresAt.Add(time.Second)))

	assert.False(t, etcdproxy.Identity{IssuedAt: now}.Valid(now), "an identity with no certificate is never valid")
}

func TestBuildAndParsePublicSecretRoundTrip(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pair := etcdproxy.IdentityPair{
		Current: etcdproxy.Identity{CertPEM: []byte("current-cert"), IssuedAt: now},
		Previous: etcdproxy.Identity{
			CertPEM: []byte("previous-cert"), IssuedAt: now.Add(-etcdproxy.IdentityRotationInterval),
		},
	}

	secret := etcdproxy.BuildPublicSecret(identityTestZone, "kontinuum-system", pair)
	assert.Equal(t, etcdproxy.AuthSecretName(identityTestZone), secret.Name)
	assert.Equal(t, "kontinuum-system", secret.Namespace)

	// A real apiserver converts StringData into the base64-encoded Data via
	// admission logic a fake client wouldn't replicate — ParsePublicSecret
	// reads Data, so mirror that conversion here directly rather than going
	// through a fake client just to round-trip this.
	secret.Data = map[string][]byte{}
	for k, v := range secret.StringData {
		secret.Data[k] = []byte(v)
	}

	got, ok := etcdproxy.ParsePublicSecret(secret)
	require.True(t, ok)
	assert.Equal(t, pair.Current.CertPEM, got.Current.CertPEM)
	assert.WithinDuration(t, pair.Current.IssuedAt, got.Current.IssuedAt, 0)
	assert.Equal(t, pair.Previous.CertPEM, got.Previous.CertPEM)
	assert.WithinDuration(t, pair.Previous.IssuedAt, got.Previous.IssuedAt, 0)
}

func TestParsePublicSecretRejectsMissingOrMalformedFields(t *testing.T) {
	t.Parallel()

	const (
		currentCertField     = "current-tls.crt"
		currentIssuedAtField = "current-issued-at"
		previousCertField    = "previous-tls.crt"
	)

	valid := map[string][]byte{
		currentCertField:     []byte("v"),
		currentIssuedAtField: []byte(time.Now().Format(time.RFC3339Nano)),
		previousCertField:    []byte("v"),
	}

	cases := map[string]map[string][]byte{
		"empty": {},
		"missing previous": {
			currentCertField: valid[currentCertField], currentIssuedAtField: valid[currentIssuedAtField],
		},
		"unparseable timestamp": {
			currentCertField: valid[currentCertField], currentIssuedAtField: []byte("not-a-time"),
		},
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			secret := &corev1.Secret{Data: data}
			_, ok := etcdproxy.ParsePublicSecret(secret)
			assert.False(t, ok)
		})
	}
}
