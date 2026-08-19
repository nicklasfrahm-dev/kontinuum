package etcdproxy

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// tokenAudience is the fixed "aud" claim every zone-signed JWT carries, and
// every VerifyToken check requires — scopes these tokens to this one proxy
// so a JWT minted for some other purpose (however signed) is never
// mistakenly accepted here.
//
//nolint:gosec // false positive: an audience string, not a credential value
const tokenAudience = "kontinuum-etcd-proxy"

// tokenTTL bounds how long a freshly minted JWT is valid for — short,
// since SignToken mints a fresh one on every outbound RPC (see relay.go's
// own jwtCredentials) rather than caching and reusing one, so there's no
// need for it to outlive a single call by much. This, not any certificate
// expiry, is what actually bounds this credential's freshness — see
// identity.go's own doc for why the certificate itself is long-lived.
const tokenTTL = 5 * time.Minute

// tokenClockSkew is how much clock drift between a zone and the hub
// VerifyToken tolerates on exp/iat.
const tokenClockSkew = 10 * time.Second

// ErrInvalidToken is VerifyToken's own error for anything that doesn't
// parse, verify, or claim the audience this package expects — including a
// lookupKey failure (e.g. an unknown zone), which VerifyToken can't
// distinguish from a forged "iss" claim without trusting lookupKey's own
// judgment.
var ErrInvalidToken = errors.New("invalid etcd proxy token")

// SignToken mints a fresh, short-lived JWT identifying zone, signed with
// key (see GenerateIdentity) — presented as the "Authorization: Bearer"
// value on every outbound RPC (see relay.go).
func SignToken(zone string, key ed25519.PrivateKey) (string, error) {
	now := time.Now()

	claims := jwt.RegisteredClaims{
		Issuer:    zone,
		Subject:   zone,
		Audience:  jwt.ClaimStrings{tokenAudience},
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(tokenTTL)),
	}

	signed, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(key)
	if err != nil {
		return "", fmt.Errorf("failed to sign etcd proxy token: %w", err)
	}

	return signed, nil
}

// VerifyToken verifies token's signature and claims, using lookupKeys to
// resolve the presenting zone's own currently-acceptable public keys (see
// pkg/domain/zone's own ensureEtcdIdentity — during a rotation's overlap
// window, this is briefly two: the just-issued and the just-superseded
// key, either of which a still-restarting zone might present) from the
// token's own "iss" claim, read via an unverified parse first, since which
// keys to even try depends on it. Tries each candidate key in turn, most
// likely (Current) first, returning the zone name for the first that
// genuinely verifies: signature, exp, and aud all check out, and iss
// matches sub, so a token issued for one zone can't be replayed as
// another.
func VerifyToken(token string, lookupKeys func(zone string) ([]ed25519.PublicKey, error)) (string, error) {
	var unverified jwt.RegisteredClaims

	_, _, err := jwt.NewParser().ParseUnverified(token, &unverified)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	if unverified.Issuer == "" {
		return "", ErrInvalidToken
	}

	keys, err := lookupKeys(unverified.Issuer)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	if len(keys) == 0 {
		return "", ErrInvalidToken
	}

	return verifyWithAnyKey(token, unverified.Issuer, keys)
}

// verifyWithAnyKey tries each of keys in turn against token, returning the
// zone name for the first that genuinely verifies — see VerifyToken's own
// doc for what "genuinely" requires.
func verifyWithAnyKey(token, issuer string, keys []ed25519.PublicKey) (string, error) {
	opts := []jwt.ParserOption{
		jwt.WithValidMethods([]string{jwt.SigningMethodEdDSA.Alg()}),
		jwt.WithAudience(tokenAudience),
		jwt.WithLeeway(tokenClockSkew),
	}

	var lastErr error

	for _, key := range keys {
		var claims jwt.RegisteredClaims

		parsed, verifyErr := jwt.ParseWithClaims(token, &claims,
			func(*jwt.Token) (any, error) { return key, nil }, opts...)
		if verifyErr == nil && parsed.Valid && claims.Issuer != "" &&
			claims.Issuer == claims.Subject && claims.Issuer == issuer {
			return claims.Issuer, nil
		}

		lastErr = verifyErr
	}

	return "", fmt.Errorf("%w: %w", ErrInvalidToken, lastErr)
}
