package auth

import "errors"

var (
	// ErrIssuerURLRequired is returned when Config.IssuerURL is empty.
	ErrIssuerURLRequired = errors.New("issuer url is required")
	// ErrClientIDRequired is returned when Config.ClientID is empty.
	ErrClientIDRequired = errors.New("client id is required")
	// ErrRedirectURLRequired is returned when Config.RedirectURL is empty.
	ErrRedirectURLRequired = errors.New("redirect url is required")
	// ErrLoginExpired is returned when an OIDC callback arrives without a
	// valid matching flow cookie (missing, expired, or already used).
	ErrLoginExpired = errors.New("login attempt expired or is invalid, please try again")
	// ErrStateMismatch is returned when the callback's state parameter does
	// not match the value stored in the flow cookie.
	ErrStateMismatch = errors.New("state parameter does not match")
	// ErrNonceMismatch is returned when the ID token's nonce claim does not
	// match the value stored in the flow cookie.
	ErrNonceMismatch = errors.New("nonce claim does not match")
	// ErrMissingIDToken is returned when the token response has no id_token field.
	ErrMissingIDToken = errors.New("token response did not include an id_token")
)
