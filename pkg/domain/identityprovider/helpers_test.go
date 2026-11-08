package identityprovider_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"encoding/pem"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/identityprovider"
)

const (
	testAudience       = "kontinuum"
	testKeyBits        = 2048
	clusterName        = "corp"
	tenantName         = "acme"
	tenantProviderName = "sso"
	groupsClaim        = "groups"
	firstKeyID         = "key-1"
	rotatedKeyID       = "key-2"
	aliceEmail         = "alice@example.com"
)

// testIssuer is a TLS OIDC issuer serving a discovery document and a JWKS,
// counting requests and able to sign tokens with any of its keys.
type testIssuer struct {
	server *httptest.Server

	mu          sync.Mutex
	privateKeys map[string]*rsa.PrivateKey
	publicKeys  []jose.JSONWebKey

	jwksHits      atomic.Int32
	discoveryHits atomic.Int32
	failing       atomic.Bool
	jwksGate      chan struct{}
	issuerClaim   atomic.Pointer[string]
}

// newTestIssuer starts a testIssuer with one signing key, "key-1".
func newTestIssuer(t *testing.T) *testIssuer {
	t.Helper()

	issuer := &testIssuer{privateKeys: map[string]*rsa.PrivateKey{}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", issuer.serveDiscovery)
	mux.HandleFunc("GET /jwks", issuer.serveJWKS)

	issuer.server = httptest.NewTLSServer(mux)
	t.Cleanup(issuer.server.Close)

	issuer.addKey(t, firstKeyID)

	return issuer
}

// URL is the issuer URL, which every token's "iss" claim carries.
func (i *testIssuer) URL() string {
	return i.server.URL
}

// CA returns the PEM CA bundle trusting the issuer's TLS certificate.
func (i *testIssuer) CA() string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: i.server.Certificate().Raw}))
}

// Source returns the KeySource for this issuer.
func (i *testIssuer) Source() identityprovider.KeySource {
	return identityprovider.KeySource{IssuerURL: i.URL(), CertificateAuthority: i.CA()}
}

func (i *testIssuer) serveDiscovery(writer http.ResponseWriter, _ *http.Request) {
	i.discoveryHits.Add(1)

	if i.failing.Load() {
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)

		return
	}

	issuerClaim := i.server.URL
	if override := i.issuerClaim.Load(); override != nil {
		issuerClaim = *override
	}

	writeJSON(writer, map[string]string{"issuer": issuerClaim, "jwks_uri": i.server.URL + "/jwks"})
}

func (i *testIssuer) serveJWKS(writer http.ResponseWriter, _ *http.Request) {
	i.jwksHits.Add(1)

	i.mu.Lock()
	gate := i.jwksGate
	keys := jose.JSONWebKeySet{Keys: append([]jose.JSONWebKey(nil), i.publicKeys...)}
	i.mu.Unlock()

	if gate != nil {
		<-gate
	}

	writeJSON(writer, keys)
}

func writeJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(writer).Encode(value)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

// addKey generates and publishes a new RSA signing key under keyID.
func (i *testIssuer) addKey(t *testing.T, keyID string) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, testKeyBits)
	require.NoError(t, err)

	i.mu.Lock()
	defer i.mu.Unlock()

	i.privateKeys[keyID] = privateKey
	i.publicKeys = append(i.publicKeys, jose.JSONWebKey{
		Key: &privateKey.PublicKey, KeyID: keyID, Algorithm: string(jose.RS256), Use: "sig",
	})
}

// gateJWKS makes every JWKS request block until the returned func is
// called.
func (i *testIssuer) gateJWKS() func() {
	gate := make(chan struct{})

	i.mu.Lock()
	i.jwksGate = gate
	i.mu.Unlock()

	return func() {
		i.mu.Lock()
		i.jwksGate = nil
		i.mu.Unlock()

		close(gate)
	}
}

// sign returns a compact JWT signed with keyID, carrying the standard
// claims for this issuer plus extra.
func (i *testIssuer) sign(t *testing.T, keyID string, extra map[string]any) string {
	t.Helper()

	i.mu.Lock()
	privateKey := i.privateKeys[keyID]
	i.mu.Unlock()

	require.NotNil(t, privateKey, "unknown key %q", keyID)

	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: privateKey},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", keyID),
	)
	require.NoError(t, err)

	now := time.Now()
	claims := map[string]any{
		"iss":            i.URL(),
		"aud":            testAudience,
		"sub":            "user-123",
		"email":          aliceEmail,
		"email_verified": true,
		groupsClaim:      []string{"engineering"},
		"iat":            now.Unix(),
		"exp":            now.Add(time.Hour).Unix(),
	}

	maps.Copy(claims, extra)

	token, err := jwt.Signed(signer).Claims(claims).Serialize()
	require.NoError(t, err)

	return token
}

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(duration)
}
