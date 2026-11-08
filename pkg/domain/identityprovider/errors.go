package identityprovider

import "errors"

var (
	// ErrIssuerMismatch is returned when a discovery document's "issuer"
	// doesn't equal the configured issuer URL.
	ErrIssuerMismatch = errors.New("discovery document issuer does not match")
	// ErrMissingJWKSURI is returned when a discovery document has no
	// "jwks_uri".
	ErrMissingJWKSURI = errors.New("discovery document has no jwks_uri")
	// ErrNoSigningKeys is returned when a JWKS holds no usable public
	// signing keys.
	ErrNoSigningKeys = errors.New("no usable signing keys")
	// ErrUnexpectedStatus is returned when a discovery or JWKS request
	// doesn't return 200 OK.
	ErrUnexpectedStatus = errors.New("unexpected http status")
	// ErrInvalidCertificateAuthority is returned when a configured CA
	// bundle holds no parsable PEM certificate.
	ErrInvalidCertificateAuthority = errors.New("certificate authority contains no valid pem certificates")
	// ErrMultipleSignatures is returned for a JWS carrying more than one
	// signature, which OIDC ID tokens never do.
	ErrMultipleSignatures = errors.New("token must carry exactly one signature")
	// ErrNoMatchingKey is returned when no cached signing key verifies a
	// token's signature, even after refreshing the keys.
	ErrNoMatchingKey = errors.New("no signing key verified the token")
	// ErrInvalidConfiguration is returned when an identity provider's spec
	// fails validation or CEL compilation.
	ErrInvalidConfiguration = errors.New("invalid identity provider configuration")
)
