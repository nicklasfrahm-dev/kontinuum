package config

import (
	"errors"
	"fmt"
	"strconv"
)

var (
	// ErrAuthenticationNotConfigured is returned when neither
	// OIDC.IssuerURL nor InsecureAllowAnonymous is set — kontinuum refuses
	// to start without a deliberate choice about authentication.
	ErrAuthenticationNotConfigured = errors.New(
		"neither KONTINUUM_OIDC_ISSUER_URL nor KONTINUUM_INSECURE_ALLOW_ANONYMOUS=true is set")
	// ErrAnonymousAccessWithOIDC is returned when both InsecureAllowAnonymous
	// and OIDC.IssuerURL are set — the two are mutually exclusive.
	ErrAnonymousAccessWithOIDC = errors.New(
		"KONTINUUM_INSECURE_ALLOW_ANONYMOUS=true is incompatible with KONTINUUM_OIDC_ISSUER_URL being set")
	// ErrInvalidInsecureAllowAnonymous is returned when
	// InsecureAllowAnonymous holds a value strconv.ParseBool can't parse.
	ErrInvalidInsecureAllowAnonymous = errors.New("KONTINUUM_INSECURE_ALLOW_ANONYMOUS must be a boolean")
)

// ValidateAuthentication enforces that c carries a deliberate choice about
// authentication: exactly one of OIDC.IssuerURL or InsecureAllowAnonymous
// may be set, never neither (ErrAuthenticationNotConfigured) and never both
// (ErrAnonymousAccessWithOIDC). anonymous is true only in the one case
// callers should surface as a startup warning rather than silence: an
// explicit opt-in to anonymous access with no OIDC issuer configured.
func (c *Config) ValidateAuthentication() (bool, error) {
	anonymous, err := strconv.ParseBool(c.InsecureAllowAnonymous)
	if err != nil {
		return false, fmt.Errorf("%w: %q", ErrInvalidInsecureAllowAnonymous, c.InsecureAllowAnonymous)
	}

	hasIssuerURL := c.OIDC.IssuerURL != ""

	switch {
	case !anonymous && !hasIssuerURL:
		return false, ErrAuthenticationNotConfigured
	case anonymous && hasIssuerURL:
		return false, ErrAnonymousAccessWithOIDC
	default:
		return anonymous, nil
	}
}
