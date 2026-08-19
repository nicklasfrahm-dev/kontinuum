package etcdproxy_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

const jwtTestZone = "eu-eu-1a"

// errUnknownZoneFixture is TestVerifyTokenPropagatesLookupFailure's own
// fixture lookupKeys error.
var errUnknownZoneFixture = errors.New("unknown zone")

func generateTestKeypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	return pub, priv
}

func lookupSingleKey(key ed25519.PublicKey) func(string) ([]ed25519.PublicKey, error) {
	return func(string) ([]ed25519.PublicKey, error) {
		return []ed25519.PublicKey{key}, nil
	}
}

func TestSignAndVerifyTokenRoundTrip(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestKeypair(t)

	token, err := etcdproxy.SignToken(jwtTestZone, priv)
	require.NoError(t, err)

	zone, err := etcdproxy.VerifyToken(token, lookupSingleKey(pub))
	require.NoError(t, err)
	assert.Equal(t, jwtTestZone, zone)
}

func TestVerifyTokenTriesEachCandidateKey(t *testing.T) {
	t.Parallel()

	currentPub, _ := generateTestKeypair(t)
	previousPub, previousPriv := generateTestKeypair(t)

	// The token was signed with the "previous" key — VerifyToken must fall
	// back to it after the "current" candidate fails, exactly as it needs
	// to during a rotation's overlap window (see Verifier's own lookup).
	token, err := etcdproxy.SignToken(jwtTestZone, previousPriv)
	require.NoError(t, err)

	zone, err := etcdproxy.VerifyToken(token, func(string) ([]ed25519.PublicKey, error) {
		return []ed25519.PublicKey{currentPub, previousPub}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, jwtTestZone, zone)
}

func TestVerifyTokenRejectsWrongKey(t *testing.T) {
	t.Parallel()

	_, priv := generateTestKeypair(t)
	wrongPub, _ := generateTestKeypair(t)

	token, err := etcdproxy.SignToken(jwtTestZone, priv)
	require.NoError(t, err)

	_, err = etcdproxy.VerifyToken(token, lookupSingleKey(wrongPub))
	require.ErrorIs(t, err, etcdproxy.ErrInvalidToken)
}

func TestVerifyTokenRejectsExpiredToken(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestKeypair(t)

	claims := jwt.RegisteredClaims{
		Issuer:    jwtTestZone,
		Subject:   jwtTestZone,
		Audience:  jwt.ClaimStrings{"kontinuum-etcd-proxy"},
		IssuedAt:  jwt.NewNumericDate(time.Now().Add(-time.Hour)),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute)),
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(priv)
	require.NoError(t, err)

	_, err = etcdproxy.VerifyToken(token, lookupSingleKey(pub))
	require.ErrorIs(t, err, etcdproxy.ErrInvalidToken)
}

func TestVerifyTokenRejectsWrongAudience(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestKeypair(t)

	claims := jwt.RegisteredClaims{
		Issuer:    jwtTestZone,
		Subject:   jwtTestZone,
		Audience:  jwt.ClaimStrings{"some-other-audience"},
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(priv)
	require.NoError(t, err)

	_, err = etcdproxy.VerifyToken(token, lookupSingleKey(pub))
	require.ErrorIs(t, err, etcdproxy.ErrInvalidToken)
}

func TestVerifyTokenRejectsIssuerSubjectMismatch(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestKeypair(t)

	claims := jwt.RegisteredClaims{
		Issuer:    jwtTestZone,
		Subject:   "some-other-zone",
		Audience:  jwt.ClaimStrings{"kontinuum-etcd-proxy"},
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(priv)
	require.NoError(t, err)

	_, err = etcdproxy.VerifyToken(token, lookupSingleKey(pub))
	require.ErrorIs(t, err, etcdproxy.ErrInvalidToken)
}

func TestVerifyTokenPropagatesLookupFailure(t *testing.T) {
	t.Parallel()

	_, priv := generateTestKeypair(t)

	token, err := etcdproxy.SignToken(jwtTestZone, priv)
	require.NoError(t, err)

	_, err = etcdproxy.VerifyToken(token, func(string) ([]ed25519.PublicKey, error) {
		return nil, errUnknownZoneFixture
	})
	require.ErrorIs(t, err, etcdproxy.ErrInvalidToken)
	require.ErrorIs(t, err, errUnknownZoneFixture)
}

func TestVerifyTokenRejectsMalformedToken(t *testing.T) {
	t.Parallel()

	_, err := etcdproxy.VerifyToken("not-a-jwt", lookupSingleKey(nil))
	require.ErrorIs(t, err, etcdproxy.ErrInvalidToken)
}
