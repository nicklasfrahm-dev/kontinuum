package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/auth"
)

const (
	testClientID         = "kontinuum"
	maxTokenRequestBytes = 1 << 20
	testOrigin           = "http://kontinuum.local"
)

// testProvider is a minimal OIDC issuer for exercising the PKCE flow
// end-to-end without a real Dex instance.
type testProvider struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	// nonce is embedded in the next id_token minted by handleToken. Tests
	// set it right before driving the callback request.
	nonce string
}

func newTestProvider(t *testing.T) *testProvider {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	provider := &testProvider{key: key}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", provider.handleDiscovery)
	mux.HandleFunc("GET /keys", provider.handleKeys)
	mux.HandleFunc("POST /token", provider.handleToken)
	mux.HandleFunc("GET /auth", func(http.ResponseWriter, *http.Request) {})

	provider.server = httptest.NewServer(mux)
	t.Cleanup(provider.server.Close)

	return provider
}

func writeTestJSON(writer http.ResponseWriter, body map[string]any) {
	writer.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(writer).Encode(body)
	if err != nil {
		http.Error(writer, "failed to encode test response: "+err.Error(), http.StatusInternalServerError)
	}
}

func (provider *testProvider) handleDiscovery(writer http.ResponseWriter, _ *http.Request) {
	writeTestJSON(writer, map[string]any{
		"issuer":                                provider.server.URL,
		"authorization_endpoint":                provider.server.URL + "/auth",
		"token_endpoint":                        provider.server.URL + "/token",
		"jwks_uri":                              provider.server.URL + "/keys",
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
	})
}

func (provider *testProvider) handleKeys(writer http.ResponseWriter, _ *http.Request) {
	pub := provider.key.PublicKey

	writeTestJSON(writer, map[string]any{
		"keys": []map[string]any{
			{
				"kty": "RSA",
				"kid": "test-key",
				"use": "sig",
				"alg": "RS256",
				"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
			},
		},
	})
}

// handleToken is a stub token endpoint: it ignores the authorization code
// and client authentication entirely (this is a test double, not a
// spec-complete server) but does assert a PKCE code_verifier was sent, then
// mints an ID token carrying provider.nonce.
func (provider *testProvider) handleToken(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxTokenRequestBytes)

	err := request.ParseForm()
	if err != nil || request.PostForm.Get("code_verifier") == "" {
		http.Error(writer, "missing code_verifier", http.StatusBadRequest)

		return
	}

	now := time.Now()
	claims := jwt.MapClaims{
		"iss":   provider.server.URL,
		"sub":   "test-user",
		"aud":   testClientID,
		"exp":   now.Add(time.Hour).Unix(),
		"iat":   now.Unix(),
		"nonce": provider.nonce,
		"email": "test@example.com",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key"

	rawIDToken, err := token.SignedString(provider.key)
	if err != nil {
		http.Error(writer, "failed to sign test id token: "+err.Error(), http.StatusInternalServerError)

		return
	}

	writeTestJSON(writer, map[string]any{
		"access_token": "test-access-token",
		"token_type":   "Bearer",
		"expires_in":   3600,
		"id_token":     rawIDToken,
	})
}

func newHandler(t *testing.T, provider *testProvider) *auth.Handler {
	t.Helper()

	handler, err := auth.NewHandler(context.Background(), auth.Config{
		IssuerURL:   provider.server.URL,
		ClientID:    testClientID,
		RedirectURL: "http://localhost:8080/app",
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	return handler
}

func newTestRequest(t *testing.T, target string) *http.Request {
	t.Helper()

	return httptest.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
}

func TestNewHandlerRequiresConfig(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)

	_, err := auth.NewHandler(context.Background(), auth.Config{}, logger)
	require.ErrorIs(t, err, auth.ErrIssuerURLRequired)

	_, err = auth.NewHandler(context.Background(), auth.Config{IssuerURL: "https://example.com"}, logger)
	require.ErrorIs(t, err, auth.ErrClientIDRequired)

	_, err = auth.NewHandler(context.Background(), auth.Config{
		IssuerURL: "https://example.com",
		ClientID:  "kontinuum",
	}, logger)
	require.ErrorIs(t, err, auth.ErrRedirectURLRequired)
}

func TestHandleAppRendersLoginPageWhenUnauthenticated(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t)
	handler := newHandler(t, provider)

	recorder := httptest.NewRecorder()
	handler.HandleApp(recorder, newTestRequest(t, testOrigin+"/app"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Location"), "HandleApp must not redirect on its own")
	assert.Empty(t, resp.Cookies(), "HandleApp must not start the PKCE flow on its own")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Login via SSO")
	assert.Contains(t, string(body), `href="/app/login"`)
}

func TestHandleLoginRedirectsToProvider(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t)
	handler := newHandler(t, provider)

	recorder := httptest.NewRecorder()
	handler.HandleLogin(recorder, newTestRequest(t, testOrigin+"/app/login"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusFound, resp.StatusCode)

	location, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	assert.Equal(t, provider.server.URL+"/auth", location.Scheme+"://"+location.Host+location.Path)
	assert.Equal(t, testClientID, location.Query().Get("client_id"))
	assert.Equal(t, "S256", location.Query().Get("code_challenge_method"))
	assert.NotEmpty(t, location.Query().Get("code_challenge"))
	assert.NotEmpty(t, location.Query().Get("state"))
	assert.NotEmpty(t, location.Query().Get("nonce"))

	cookies := resp.Cookies()
	require.Len(t, cookies, 1)
	assert.Equal(t, "kontinuum_oidc_flow", cookies[0].Name)
	assert.True(t, cookies[0].HttpOnly)
}

func TestHandleAppRedirectsHomeWhenAuthenticated(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t)
	handler := newHandler(t, provider)

	sessionCookie := loginAndGetSessionCookie(t, handler, provider)

	req := newTestRequest(t, testOrigin+"/app")
	req.AddCookie(sessionCookie)

	recorder := httptest.NewRecorder()
	handler.HandleApp(recorder, req)

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, "/app/home", resp.Header.Get("Location"))
}

// beginLoginAndSetNonce drives HandleLogin, primes the fake provider with
// the nonce it generated, and returns the state and the flow cookie a real
// browser would carry into the callback.
func beginLoginAndSetNonce(t *testing.T, handler *auth.Handler, provider *testProvider) (string, *http.Cookie) {
	t.Helper()

	recorder := httptest.NewRecorder()
	handler.HandleLogin(recorder, newTestRequest(t, testOrigin+"/app/login"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	location, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)

	provider.nonce = location.Query().Get("nonce")

	flowCookies := resp.Cookies()
	require.Len(t, flowCookies, 1)

	return location.Query().Get("state"), flowCookies[0]
}

// loginAndGetSessionCookie drives a full login (HandleLogin, then the /app
// callback) against provider and returns the resulting session cookie.
func loginAndGetSessionCookie(t *testing.T, handler *auth.Handler, provider *testProvider) *http.Cookie {
	t.Helper()

	state, flowCookie := beginLoginAndSetNonce(t, handler, provider)

	callbackReq := newTestRequest(t, testOrigin+"/app?code=test-code&state="+state)
	callbackReq.AddCookie(flowCookie)

	callbackRecorder := httptest.NewRecorder()
	handler.HandleApp(callbackRecorder, callbackReq)

	callbackResp := callbackRecorder.Result()

	defer func() { _ = callbackResp.Body.Close() }()

	sessionCookie := findCookie(callbackResp.Cookies(), "kontinuum_session")
	require.NotNil(t, sessionCookie)

	return sessionCookie
}

func TestCallbackCompletesLoginAndSetsSessionCookie(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t)
	handler := newHandler(t, provider)

	state, flowCookie := beginLoginAndSetNonce(t, handler, provider)

	callbackReq := newTestRequest(t, testOrigin+"/app?code=test-code&state="+state)
	callbackReq.AddCookie(flowCookie)

	callbackRecorder := httptest.NewRecorder()
	handler.HandleApp(callbackRecorder, callbackReq)

	callbackResp := callbackRecorder.Result()

	defer func() { _ = callbackResp.Body.Close() }()

	body, _ := io.ReadAll(callbackResp.Body)
	require.Equalf(t, http.StatusFound, callbackResp.StatusCode, "callback body: %s", body)
	assert.Equal(t, "/app/home", callbackResp.Header.Get("Location"))

	sessionCookie := findCookie(callbackResp.Cookies(), "kontinuum_session")
	require.NotNil(t, sessionCookie)
	assert.NotEmpty(t, sessionCookie.Value)
}

func TestProtectAllowsValidSessionAndForwardsToken(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t)
	handler := newHandler(t, provider)

	sessionCookie := loginAndGetSessionCookie(t, handler, provider)

	var tokenSeenByNext string

	protected := handler.Protect(func(_ http.ResponseWriter, request *http.Request) {
		tokenSeenByNext = auth.TokenFromContext(request.Context())
	})

	homeReq := newTestRequest(t, testOrigin+"/app/home")
	homeReq.AddCookie(sessionCookie)

	homeRecorder := httptest.NewRecorder()
	protected(homeRecorder, homeReq)

	homeResp := homeRecorder.Result()

	defer func() { _ = homeResp.Body.Close() }()

	assert.Equal(t, sessionCookie.Value, tokenSeenByNext,
		"Protect should forward the session's raw ID token via WithToken")
}

func TestProtectRedirectsToAppWithoutSession(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t)
	handler := newHandler(t, provider)

	called := false
	protected := handler.Protect(func(http.ResponseWriter, *http.Request) { called = true })

	unauthedRecorder := httptest.NewRecorder()
	protected(unauthedRecorder, newTestRequest(t, testOrigin+"/app/home"))

	unauthedResp := unauthedRecorder.Result()

	defer func() { _ = unauthedResp.Body.Close() }()

	assert.False(t, called, "protected handler should not run without a session cookie")
	assert.Equal(t, http.StatusFound, unauthedResp.StatusCode)
	assert.Equal(t, "/app", unauthedResp.Header.Get("Location"),
		"Protect should send unauthenticated visitors to the local login page, not the provider directly")
}

func TestHandleLogoutClearsSessionCookie(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t)
	handler := newHandler(t, provider)

	recorder := httptest.NewRecorder()
	handler.HandleLogout(recorder, newTestRequest(t, testOrigin+"/app/logout"))

	resp := recorder.Result()

	defer func() { _ = resp.Body.Close() }()

	require.Len(t, resp.Cookies(), 1)
	assert.Equal(t, "kontinuum_session", resp.Cookies()[0].Name)
	assert.Negative(t, resp.Cookies()[0].MaxAge)
	assert.Equal(t, "/app", resp.Header.Get("Location"))
}

func TestHandleAppRejectsStateMismatch(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t)
	handler := newHandler(t, provider)

	_, flowCookie := beginLoginAndSetNonce(t, handler, provider)

	callbackReq := newTestRequest(t, testOrigin+"/app?code=test-code&state=wrong-state")
	callbackReq.AddCookie(flowCookie)

	callbackRecorder := httptest.NewRecorder()
	handler.HandleApp(callbackRecorder, callbackReq)

	callbackResp := callbackRecorder.Result()

	defer func() { _ = callbackResp.Body.Close() }()

	assert.Equal(t, http.StatusBadRequest, callbackResp.StatusCode)
}

func TestHandleAppRejectsMissingFlowCookie(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t)
	handler := newHandler(t, provider)

	callbackRecorder := httptest.NewRecorder()
	handler.HandleApp(callbackRecorder, newTestRequest(t, testOrigin+"/app?code=test-code&state=whatever"))

	callbackResp := callbackRecorder.Result()

	defer func() { _ = callbackResp.Body.Close() }()

	assert.Equal(t, http.StatusBadRequest, callbackResp.StatusCode)
}

func findCookie(cookies []*http.Cookie, name string) *http.Cookie {
	for _, cookie := range cookies {
		if cookie.Name == name {
			return cookie
		}
	}

	return nil
}
