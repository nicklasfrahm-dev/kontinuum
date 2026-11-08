package identityprovider

import (
	"context"
	"fmt"

	"github.com/go-jose/go-jose/v4"
)

// signatureAlgorithms returns the asymmetric JOSE algorithms accepted for
// tokens. Symmetric (HS*) algorithms are never accepted: the verification
// keys are public.
func signatureAlgorithms() []jose.SignatureAlgorithm {
	return []jose.SignatureAlgorithm{
		jose.RS256, jose.RS384, jose.RS512,
		jose.ES256, jose.ES384, jose.ES512,
		jose.PS256, jose.PS384, jose.PS512,
	}
}

// SupportedSigningAlgs returns signatureAlgorithms as plain strings, the
// form k8s.io/apiserver's OIDC authenticator expects.
func SupportedSigningAlgs() []string {
	algorithms := signatureAlgorithms()

	algs := make([]string, 0, len(algorithms))
	for _, alg := range algorithms {
		algs = append(algs, string(alg))
	}

	return algs
}

// KeySet verifies token signatures against one source's keys in a
// KeyCache. It satisfies the KeySet interface of the go-oidc version
// k8s.io/apiserver's OIDC authenticator uses, so that authenticator reuses
// the shared, stale-while-revalidate cache instead of running its own
// discovery and key fetching per identity provider.
type KeySet struct {
	Cache  *KeyCache
	Source KeySource
}

// VerifySignature verifies jwt's signature and returns its payload. A token
// signed by a key ID the cache doesn't know triggers one rate-limited
// refresh, so issuer key rotation is picked up without waiting for the
// cached keys to go stale.
func (k *KeySet) VerifySignature(ctx context.Context, jwt string) ([]byte, error) {
	signed, err := jose.ParseSigned(jwt, signatureAlgorithms())
	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if len(signed.Signatures) != 1 {
		return nil, ErrMultipleSignatures
	}

	keyID := signed.Signatures[0].Header.KeyID

	keys, err := k.Cache.Keys(ctx, k.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to get signing keys: %w", err)
	}

	payload, verified := verifyWithKeys(signed, keys, keyID)
	if verified {
		return payload, nil
	}

	snapshot, refreshed, err := k.Cache.RefreshIfDue(ctx, k.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to refresh signing keys: %w", err)
	}

	if refreshed {
		payload, verified = verifyWithKeys(signed, snapshot.Keys, keyID)
		if verified {
			return payload, nil
		}
	}

	return nil, ErrNoMatchingKey
}

// verifyWithKeys tries every key matching keyID — or every key, when the
// token names none — and returns the payload of the first that verifies.
func verifyWithKeys(signed *jose.JSONWebSignature, keys []jose.JSONWebKey, keyID string) ([]byte, bool) {
	for _, key := range keys {
		if keyID != "" && key.KeyID != keyID {
			continue
		}

		payload, err := signed.Verify(key)
		if err == nil {
			return payload, true
		}
	}

	return nil, false
}
