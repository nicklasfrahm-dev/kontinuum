// Package auth implements a PKCE-based OpenID Connect browser login flow for
// kontinuum's /app UI. It authenticates against a public OAuth 2.0 client
// (no client secret) by exchanging an authorization code for an ID token,
// verifying it, and storing it in an HttpOnly session cookie.
package auth
