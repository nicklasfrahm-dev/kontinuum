package auth

import (
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

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

// MapError converts a technical error into a human-readable message.
// If the error is not recognized, it returns the error's original message.
func MapError(err error) string {
	sentinelMessage := mapSentinelError(err)

	switch {
	case err == nil:
		return ""
	case sentinelMessage != "":
		return sentinelMessage
	// Forbidden means the Kubernetes API server's authorizer (see libkapi's
	// AdminAuthorizer) rejected the request — the caller authenticated fine
	// but isn't in the configured admin group, system:masters, or a service
	// account. That RBAC-style reason is meant for kubectl output, not this
	// UI, so swap it for something a browser user can act on.
	case apierrors.IsForbidden(err):
		return "You're signed in, but your account isn't authorized to access this. " +
			"Ask an administrator to grant you the necessary permissions."
	default:
		return err.Error()
	}
}

// mapSentinelError returns the human-readable message for one of this
// package's own sentinel errors, or "" if err doesn't match any of them.
// Split out of MapError purely to keep each function's branching under the
// cyclomatic-complexity limit — it's still a plain switch, just a second one.
func mapSentinelError(err error) string {
	switch {
	case errors.Is(err, ErrIssuerURLRequired):
		return "The OIDC issuer URL is missing from the configuration."
	case errors.Is(err, ErrClientIDRequired):
		return "The OIDC Client ID is missing from the configuration."
	case errors.Is(err, ErrRedirectURLRequired):
		return "The OIDC Redirect URL is missing from the configuration."
	case errors.Is(err, ErrLoginExpired):
		return "Your login session has expired. Please try signing in again."
	case errors.Is(err, ErrStateMismatch):
		return "Security validation failed (state mismatch). Please try signing in again."
	case errors.Is(err, ErrNonceMismatch):
		return "Security validation failed (nonce mismatch). Please try signing in again."
	case errors.Is(err, ErrMissingIDToken):
		return "The server failed to receive a valid identity token. Please try signing in again."
	default:
		return ""
	}
}
