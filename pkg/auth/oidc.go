package auth

import (
	"context"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// discoveryTimeout bounds how long NewHandler waits for the issuer's
// discovery document during startup.
const discoveryTimeout = 10 * time.Second

// Config configures the PKCE browser login flow.
type Config struct {
	// IssuerURL is the OIDC issuer URL (e.g. Dex). The discovery document is
	// fetched from {IssuerURL}/.well-known/openid-configuration during
	// NewHandler.
	IssuerURL string
	// ClientID is the OAuth 2.0 public client ID registered with the issuer.
	// No client secret is used — authentication relies entirely on PKCE.
	ClientID string
	// RedirectURL is the callback URL registered with the issuer. It is
	// reused as both the login-initiation and callback endpoint, since the
	// registered redirect URI has no dedicated /callback path.
	RedirectURL string
	// Scopes are the OAuth 2.0 scopes requested. Defaults to
	// ["openid", "profile", "email", "groups"] when empty.
	Scopes []string
}

// Handler drives the PKCE authorization code flow against a single OIDC
// issuer and gates access to protected routes with a session cookie holding
// the verified ID token.
type Handler struct {
	oauth2Config    oauth2.Config
	idTokenVerifier *oidc.IDTokenVerifier
	loginPage       *template.Template
	logger          *slog.Logger
}

// NewHandler fetches the issuer's discovery document and builds a Handler.
func NewHandler(ctx context.Context, cfg Config, logger *slog.Logger) (*Handler, error) {
	if cfg.IssuerURL == "" {
		return nil, ErrIssuerURLRequired
	}

	if cfg.ClientID == "" {
		return nil, ErrClientIDRequired
	}

	if cfg.RedirectURL == "" {
		return nil, ErrRedirectURLRequired
	}

	scopes := cfg.Scopes
	if len(scopes) == 0 {
		scopes = []string{oidc.ScopeOpenID, "profile", "email", "groups"}
	}

	discoveryCtx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()

	provider, err := oidc.NewProvider(discoveryCtx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("failed to discover oidc issuer: %w", err)
	}

	return &Handler{
		oauth2Config: oauth2.Config{
			ClientID:    cfg.ClientID,
			Endpoint:    provider.Endpoint(),
			RedirectURL: cfg.RedirectURL,
			Scopes:      scopes,
		},
		idTokenVerifier: provider.Verifier(&oidc.Config{ClientID: cfg.ClientID}),
		loginPage:       parseLoginPage(),
		logger:          logger,
	}, nil
}

// HandleApp is mounted at GET /app — the registered redirect URI has no
// dedicated /callback path, so this handler completes the OIDC callback
// when a code is present, sends an already-authenticated browser on to
// /app/home, or renders a login landing page with a "Login via SSO" button
// otherwise. It never redirects into the OIDC flow on its own; that only
// happens once the user clicks through to /app/login.
func (h *Handler) HandleApp(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Query().Get("code") != "" {
		h.handleCallback(writer, request)

		return
	}

	if _, ok := h.validSessionToken(request); ok {
		http.Redirect(writer, request, "/app/home", http.StatusFound)

		return
	}

	h.renderLoginPage(writer, request.URL.Query().Get("error"))
}

// HandleLogin begins a new PKCE login attempt, redirecting the browser to
// the issuer's authorization endpoint. Mounted at GET /app/login, reached
// by the "Login via SSO" button rendered by HandleApp and Protect.
func (h *Handler) HandleLogin(writer http.ResponseWriter, request *http.Request) {
	h.beginLogin(writer, request)
}

// Protect wraps next so it only runs when the request carries a valid
// session cookie; otherwise it redirects to /app's login landing page,
// without itself starting the OIDC flow. next receives the signed-in
// user's raw ID token via WithToken/TokenFromContext, so it can act as that
// user on further API calls.
func (h *Handler) Protect(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		rawIDToken, ok := h.validSessionToken(request)
		if !ok {
			http.Redirect(writer, request, "/app", http.StatusFound)

			return
		}

		next(writer, request.WithContext(WithToken(request.Context(), rawIDToken)))
	}
}

// HandleLogout clears the local session cookie and redirects to /app. This
// only ends kontinuum's local session — it does not end the issuer's own
// SSO session, so the browser may be transparently re-authenticated if one
// is still active there.
func (h *Handler) HandleLogout(writer http.ResponseWriter, request *http.Request) {
	clearCookie(writer, sessionCookieName)
	http.Redirect(writer, request, "/app", http.StatusFound)
}

// InvalidateSession clears the session cookie and redirects to the login
// page with reason shown as a human-readable error — the same treatment a
// failed login gets (see redirectLoginError, which this wraps). Exported
// for callers outside this package that determine, on their own, that a
// session already past HandleApp/Protect should no longer be trusted — for
// example pkg/ui, when the Kubernetes API rejects an otherwise
// authenticated request as Forbidden. A valid session cookie only proves
// who the caller is, not what they're allowed to do, so that kind of
// rejection is a real reason to send them back to sign in.
func (h *Handler) InvalidateSession(writer http.ResponseWriter, request *http.Request, reason string) {
	h.redirectLoginError(writer, request, reason)
}

// validSessionToken returns the request's session cookie value and true if
// it holds a signature-valid, unexpired ID token for this client.
func (h *Handler) validSessionToken(request *http.Request) (string, bool) {
	cookie, err := request.Cookie(sessionCookieName)
	if err != nil || cookie.Value == "" {
		return "", false
	}

	_, err = h.idTokenVerifier.Verify(request.Context(), cookie.Value)
	if err != nil {
		return "", false
	}

	return cookie.Value, true
}

// beginLogin generates a PKCE verifier, state, and nonce, stores them in a
// short-lived flow cookie, and redirects the browser to the issuer's
// authorization endpoint.
func (h *Handler) beginLogin(writer http.ResponseWriter, request *http.Request) {
	pkceVerifier := oauth2.GenerateVerifier()

	state, err := randomToken()
	if err != nil {
		http.Error(writer, "failed to start login: "+err.Error(), http.StatusInternalServerError)

		return
	}

	nonce, err := randomToken()
	if err != nil {
		http.Error(writer, "failed to start login: "+err.Error(), http.StatusInternalServerError)

		return
	}

	setCookie(writer, flowCookieName, encodeFlowCookie(state, nonce, pkceVerifier), time.Now().Add(flowCookieMaxAge))

	authURL := h.oauth2Config.AuthCodeURL(state,
		oauth2.S256ChallengeOption(pkceVerifier),
		oidc.Nonce(nonce),
	)

	http.Redirect(writer, request, authURL, http.StatusFound)
}

// handleCallback completes the authorization code exchange, verifies the
// returned ID token, and sets the session cookie. Any failure sends the
// browser back to the login page via redirectLoginError instead of
// rendering a raw error response, since this endpoint only ever receives
// browser navigations (the OIDC issuer's redirect back from its
// authorization endpoint), never an API client.
func (h *Handler) handleCallback(writer http.ResponseWriter, request *http.Request) {
	flowCookie, err := request.Cookie(flowCookieName)
	if err != nil {
		h.redirectLoginError(writer, request, MapError(ErrLoginExpired))

		return
	}

	// The flow cookie is single-use: clear it immediately so a replayed or
	// reloaded callback can't be completed twice.
	clearCookie(writer, flowCookieName)

	wantState, wantNonce, pkceVerifier, err := decodeFlowCookie(flowCookie.Value)
	if err != nil {
		h.redirectLoginError(writer, request, MapError(err))

		return
	}

	if request.URL.Query().Get("state") != wantState {
		h.redirectLoginError(writer, request, MapError(ErrStateMismatch))

		return
	}

	code := request.URL.Query().Get("code")

	token, err := h.oauth2Config.Exchange(request.Context(), code, oauth2.VerifierOption(pkceVerifier))
	if err != nil {
		h.logger.Warn("Failed to exchange oidc authorization code", "error", err)
		h.redirectLoginError(writer, request, MapError(err))

		return
	}

	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		h.redirectLoginError(writer, request, MapError(ErrMissingIDToken))

		return
	}

	idToken, err := h.idTokenVerifier.Verify(request.Context(), rawIDToken)
	if err != nil {
		h.logger.Warn("Failed to verify oidc id token", "error", err)
		h.redirectLoginError(writer, request, MapError(err))

		return
	}

	if idToken.Nonce != wantNonce {
		h.redirectLoginError(writer, request, MapError(ErrNonceMismatch))

		return
	}

	setCookie(writer, sessionCookieName, rawIDToken, idToken.Expiry)

	http.Redirect(writer, request, "/app/home", http.StatusFound)
}

// redirectLoginError clears any session cookie — a failed login attempt
// shouldn't leave a stale one behind — and sends the browser back to /app
// with message in an "error" query parameter, which HandleApp reads and
// hands to renderLoginPage to show as a human-readable error box.
func (h *Handler) redirectLoginError(writer http.ResponseWriter, request *http.Request, message string) {
	clearCookie(writer, sessionCookieName)

	target := url.URL{Path: "/app", RawQuery: url.Values{"error": {message}}.Encode()}
	http.Redirect(writer, request, target.String(), http.StatusFound)
}
