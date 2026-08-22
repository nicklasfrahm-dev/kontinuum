package etcdproxy

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// identityValidity is how long a freshly issued identity certificate's own
// X.509 NotAfter is set to — generously long, since it's IdentityPair's own
// app-level Current/Previous bookkeeping (see IdentityRotationInterval and
// Identity.ExpiresAt), not this field, that actually governs when a
// certificate stops being accepted.
const identityValidity = 100 * 365 * 24 * time.Hour

// IdentityRotationInterval is how long a freshly issued identity stays
// "current" before pkg/domain/zone's own ensureEtcdIdentity mints a fresh
// one to replace it.
const IdentityRotationInterval = 6 * time.Hour

// IdentityOverlapWindow is how much longer a just-superseded identity
// keeps being accepted (see Identity.ExpiresAt) after rotation — long
// enough to cover the delay between the hub delivering a fresh keypair to
// a zone's own downstream identity Secret and that zone's own Relay
// actually observing the change through its watch on it (see
// WatchIdentity), during which it may still sign outbound RPCs with the
// key it had cached from before.
const IdentityOverlapWindow = 5 * time.Minute

// certSecretField/issuedAtSecretField name a single identity's own fields
// within a Secret — certSecretField reuses corev1.TLSCertKey rather than
// inventing a new name, so PublicKeyFromCert/Thumbprint work unchanged
// whether they're reading a zone's own full kubernetes.io/tls Secret (see
// BuildDownstreamIdentitySecret, which only ever carries one identity — the
// currently active one) or one half of the hub's own Current/Previous pair
// (see BuildPublicSecret).
const (
	certSecretField     = corev1.TLSCertKey
	issuedAtSecretField = "issued-at"
)

// currentCertField/currentIssuedAtField/previousCertField/previousIssuedAtField
// are the field names the hub's own public identity Secret (see
// BuildPublicSecret) stores its Current/Previous pair under.
const (
	currentCertField      = "current-" + certSecretField
	currentIssuedAtField  = "current-" + issuedAtSecretField
	previousCertField     = "previous-" + certSecretField
	previousIssuedAtField = "previous-" + issuedAtSecretField
)

// AuthSecretName is the name of the Secret carrying zoneName's own etcd
// gRPC proxy identity: the hub's own trimmed, public-key-only Current/
// Previous pair (see BuildPublicSecret) here, or the full kubernetes.io/tls
// keypair for whichever identity is currently active (see
// BuildDownstreamIdentitySecret) on the zone's own downstream cluster.
func AuthSecretName(zoneName string) string {
	return zoneName + "-etcd-auth"
}

// certSerialBits bounds GenerateIdentity's own randomly generated
// certificate serial number — RFC 5280 caps a serial at 20 octets; 128
// bits (16 octets) leaves headroom while still making a collision
// astronomically unlikely.
const certSerialBits = 128

// GenerateIdentity issues a fresh ed25519 keypair for zoneName, wrapped in
// a long-lived, self-signed X.509 certificate (see identityValidity's own
// doc for why its own expiry isn't what actually bounds this identity's
// use). Returns both PEM-encoded: the certificate alone is enough for one
// half of an IdentityPair, both together for BuildDownstreamIdentitySecret.
func GenerateIdentity(zoneName string) ([]byte, []byte, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate ed25519 key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), certSerialBits))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate certificate serial: %w", err)
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: zoneName},
		// Valid an hour before now, same reasoning as
		// k8s.io/client-go/util/cert.GenerateSelfSignedCertKeyWithFixtures:
		// avoids flakes from clock skew between whichever process verifies
		// it first and this one's own clock at issuance time.
		NotBefore: now.Add(-time.Hour),
		NotAfter:  now.Add(identityValidity),
		KeyUsage:  x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create self-signed certificate: %w", err)
	}

	keyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})

	return certPEM, keyPEM, nil
}

// ErrInvalidCertificate is PublicKeyFromCert/Thumbprint's own error for
// certPEM content that isn't a well-formed PEM-encoded X.509 certificate.
var ErrInvalidCertificate = errors.New("invalid identity certificate")

// ErrNotEd25519Key is PublicKeyFromCert's own error for an otherwise
// well-formed certificate whose public key isn't ed25519 — never expected
// from a certificate GenerateIdentity itself issued, but guards against a
// hand-edited or future/older-version Secret with a different shape.
var ErrNotEd25519Key = errors.New("certificate does not hold an ed25519 public key")

// parseCert decodes and parses a single PEM-encoded X.509 certificate —
// shared by PublicKeyFromCert and Thumbprint.
func parseCert(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, ErrInvalidCertificate
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCertificate, err)
	}

	return cert, nil
}

// PublicKeyFromCert extracts the ed25519 public key certPEM's own
// certificate holds — see GenerateIdentity for how it was minted.
func PublicKeyFromCert(certPEM []byte) (ed25519.PublicKey, error) {
	cert, err := parseCert(certPEM)
	if err != nil {
		return nil, err
	}

	pub, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, ErrNotEd25519Key
	}

	return pub, nil
}

// Thumbprint returns certPEM's own SHA-256 certificate fingerprint,
// colon-separated uppercase hex — the same value `openssl x509
// -fingerprint -sha256` or kubectl tooling would report, shown on the zone
// detail page so an operator can independently verify which identity a
// zone presents.
func Thumbprint(certPEM []byte) (string, error) {
	cert, err := parseCert(certPEM)
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(cert.Raw)

	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = fmt.Sprintf("%02X", b)
	}

	return strings.Join(parts, ":"), nil
}

// Identity is one issued certificate and when it was issued.
type Identity struct {
	CertPEM  []byte
	IssuedAt time.Time
}

// DueAt is when this Identity stops being the "current" one — see
// pkg/domain/zone's own ensureEtcdIdentity.
func (i Identity) DueAt() time.Time {
	return i.IssuedAt.Add(IdentityRotationInterval)
}

// ExpiresAt is when this Identity stops being accepted at all, whether
// it's still "current" or has already been demoted to "previous" —
// IdentityOverlapWindow past DueAt, not past whenever it happened to be
// demoted, so an identity's own total acceptance window is fixed
// (IdentityRotationInterval+IdentityOverlapWindow) regardless of exactly
// when a reconcile pass actually notices it's due.
func (i Identity) ExpiresAt() time.Time {
	return i.DueAt().Add(IdentityOverlapWindow)
}

// Valid reports whether now falls within this Identity's own acceptance
// window — i.e. before ExpiresAt, and it actually carries a certificate.
// Applied identically to both a pair's Current and Previous by Verifier —
// see IdentityPair's own doc for why that gives this scheme a fail-closed
// property if rotation ever silently stops happening.
func (i Identity) Valid(now time.Time) bool {
	return len(i.CertPEM) > 0 && now.Before(i.ExpiresAt())
}

// IdentityPair is the parsed contents of the hub's own public identity
// Secret (see AuthSecretName) — Current and Previous are both checked
// identically by Verifier (via Identity.Valid), so a zone that's never had
// its rotation reconciled for longer than
// IdentityRotationInterval+IdentityOverlapWindow eventually loses even its
// own "current" identity's trust, rather than staying valid forever by
// construction.
type IdentityPair struct {
	Current  Identity
	Previous Identity
}

// BuildPublicSecret builds the Opaque Secret AuthSecretName(zoneName)
// should hold on the hub: pair's own two certificates, never either
// private key (see this package's own doc). The caller is expected to set
// its own owner reference (e.g. via controllerutil.SetControllerReference)
// — that needs the owning Zone's own concrete type and scheme, both of
// which live in pkg/domain/zone, not here.
func BuildPublicSecret(zoneName, namespace string, pair IdentityPair) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: AuthSecretName(zoneName), Namespace: namespace},
		StringData: map[string]string{
			currentCertField:      string(pair.Current.CertPEM),
			currentIssuedAtField:  pair.Current.IssuedAt.Format(time.RFC3339Nano),
			previousCertField:     string(pair.Previous.CertPEM),
			previousIssuedAtField: pair.Previous.IssuedAt.Format(time.RFC3339Nano),
		},
	}
}

// ParsePublicSecret reads back the Current/Previous pair BuildPublicSecret
// wrote. A malformed value (hand-edited, or written by some future/older
// version with a different shape) returns ok=false rather than an error,
// so a caller reconciling this Secret can treat that exactly like the
// Secret not existing yet, and a caller only reading it (see Verifier) can
// treat it as "no valid identity here" rather than panicking on it.
func ParsePublicSecret(secret *corev1.Secret) (IdentityPair, bool) {
	current, currentOK := parseIdentity(secret, currentCertField, currentIssuedAtField)
	if !currentOK {
		return IdentityPair{}, false
	}

	previous, previousOK := parseIdentity(secret, previousCertField, previousIssuedAtField)
	if !previousOK {
		return IdentityPair{}, false
	}

	return IdentityPair{Current: current, Previous: previous}, true
}

func parseIdentity(secret *corev1.Secret, certField, issuedAtField string) (Identity, bool) {
	cert, exists := secret.Data[certField]
	if !exists || len(cert) == 0 {
		return Identity{}, false
	}

	issuedAt, err := time.Parse(time.RFC3339Nano, string(secret.Data[issuedAtField]))
	if err != nil {
		return Identity{}, false
	}

	return Identity{CertPEM: cert, IssuedAt: issuedAt}, true
}
