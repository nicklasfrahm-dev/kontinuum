package auth

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const (
	// sessionCookieName holds the verified ID token for an authenticated
	// browser session.
	sessionCookieName = "kontinuum_session"
	// flowCookieName holds the state, nonce, and PKCE verifier for a login
	// attempt in progress.
	flowCookieName = "kontinuum_oidc_flow"
	// flowCookieMaxAge bounds how long a login attempt has to complete
	// before the flow cookie expires.
	flowCookieMaxAge = 5 * time.Minute
	// cookiePath scopes kontinuum's cookies to the /app UI.
	cookiePath = "/app"
	// randomTokenBytes is the size of the random state and nonce values
	// generated for each login attempt.
	randomTokenBytes = 32
)

// randomToken returns a URL-safe, base64-encoded random value suitable for
// use as an OAuth 2.0 state or OIDC nonce.
func randomToken() (string, error) {
	buf := make([]byte, randomTokenBytes)

	_, err := rand.Read(buf)
	if err != nil {
		return "", fmt.Errorf("failed to generate random token: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// encodeFlowCookie packs state, nonce, and the PKCE verifier into a single
// cookie value.
func encodeFlowCookie(state, nonce, pkceVerifier string) string {
	values := url.Values{
		"state":    {state},
		"nonce":    {nonce},
		"verifier": {pkceVerifier},
	}

	return base64.RawURLEncoding.EncodeToString([]byte(values.Encode()))
}

// decodeFlowCookie reverses encodeFlowCookie. It returns ErrLoginExpired if
// raw is malformed or missing any of the three values. The return order is
// state, nonce, PKCE verifier.
func decodeFlowCookie(raw string) (string, string, string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return "", "", "", ErrLoginExpired
	}

	values, err := url.ParseQuery(string(decoded))
	if err != nil {
		return "", "", "", ErrLoginExpired
	}

	state := values.Get("state")
	nonce := values.Get("nonce")
	pkceVerifier := values.Get("verifier")

	if state == "" || nonce == "" || pkceVerifier == "" {
		return "", "", "", ErrLoginExpired
	}

	return state, nonce, pkceVerifier, nil
}

// setCookie sets an HttpOnly, Secure, SameSite=Lax cookie scoped to
// cookiePath. Modern browsers treat localhost as a secure context, so
// Secure cookies still work over plain http during local development.
func setCookie(w http.ResponseWriter, name, value string, expires time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     cookiePath,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		Expires:  expires,
	})
}

// clearCookie expires a previously set cookie.
func clearCookie(w http.ResponseWriter, name string) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    "",
		Path:     cookiePath,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
}
