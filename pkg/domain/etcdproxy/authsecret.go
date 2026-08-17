package etcdproxy

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// KeyLength is the length, in characters, of a freshly generated
	// bearer credential (see GenerateAuthKey) — not a byte count, since
	// KeyCharset is drawn one character at a time.
	KeyLength = 128
	// KeyCharset excludes ':' deliberately: DecodeToken splits a bearer
	// token on the first ':' ("<zone>:<key>"), so a key that could itself
	// contain one would make that split ambiguous. Otherwise a plain
	// alphanumeric charset — no characters that would need escaping in an
	// HTTP header value.
	KeyCharset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

	// RotationInterval is how long a freshly issued key is the "current"
	// one before a fresh one replaces it — see pkg/domain/zone's own
	// reconcileAuthKeys, which drives rotation on this schedule.
	RotationInterval = time.Hour
	// OverlapWindow is how much longer a just-superseded key keeps being
	// accepted (see AuthKey.ExpiresAt) after rotation — long enough for a
	// zone's own kontinuum-server, which only re-reads its bearer
	// credential when its own kontinuum-env Secret is next updated (not
	// continuously), to pick up the new key before the old one stops
	// working.
	OverlapWindow = 5 * time.Minute

	// authSecretKeyField/authSecretKeyCreatedAtField and
	// authSecretPreviousKeyField/authSecretPreviousKeyCreatedAtField are
	// the field names a zone's own auth Secret (see AuthSecretName) stores
	// its current/previous key pair under — field names, not credential
	// values, so gosec's G101 (triggered purely by "key"/"secret"
	// appearing in the identifiers) is a false positive here.
	authSecretKeyField          = "key"
	authSecretKeyCreatedAtField = "key-created-at"
	authSecretPreviousKeyField  = "previous-key"
	//nolint:gosec // false positive: field name, not a credential value
	authSecretPreviousKeyCreatedAtField = "previous-key-created-at"
)

// AuthSecretName is the name of the Secret carrying zoneName's own etcd
// gRPC proxy bearer credentials.
func AuthSecretName(zoneName string) string {
	return zoneName + "-etcd-auth"
}

// GenerateAuthKey returns a fresh, random KeyLength-character key drawn
// from KeyCharset.
func GenerateAuthKey() (string, error) {
	charsetSize := big.NewInt(int64(len(KeyCharset)))

	key := make([]byte, KeyLength)

	for index := range key {
		charIndex, err := rand.Int(rand.Reader, charsetSize)
		if err != nil {
			return "", fmt.Errorf("failed to generate auth key: %w", err)
		}

		key[index] = KeyCharset[charIndex.Int64()]
	}

	return string(key), nil
}

// AuthKey is one issued bearer credential and when it was issued.
type AuthKey struct {
	Value     string
	CreatedAt time.Time
}

// DueAt is when Key stops being the "current" one — see
// pkg/domain/zone's own reconcileAuthKeys.
func (k AuthKey) DueAt() time.Time {
	return k.CreatedAt.Add(RotationInterval)
}

// ExpiresAt is when Key stops being accepted at all, whether it's still
// "current" or has already been demoted to "previous" — OverlapWindow past
// DueAt, not past whenever it happened to be demoted, so a key's own total
// acceptance window is fixed (RotationInterval+OverlapWindow) regardless of
// exactly when a reconcile pass actually notices it's due.
func (k AuthKey) ExpiresAt() time.Time {
	return k.DueAt().Add(OverlapWindow)
}

// Valid reports whether now falls within k's own acceptance window — i.e.
// before ExpiresAt. Unlike DueAt, this doesn't distinguish "current" from
// "previous": Verifier calls it on both of a zone's keys identically.
func (k AuthKey) Valid(now time.Time) bool {
	return now.Before(k.ExpiresAt())
}

// AuthKeyPair is the parsed contents of a zone's own auth Secret — see
// AuthSecretName.
type AuthKeyPair struct {
	Current  AuthKey
	Previous AuthKey
}

// BuildAuthSecret builds the Secret AuthSecretName(zoneName) should hold
// for pair, in namespace. The caller is expected to set its own owner
// reference (e.g. via controllerutil.SetControllerReference) — that needs
// the owning Zone's own concrete type and scheme, both of which live in
// pkg/domain/zone, not here.
func BuildAuthSecret(zoneName, namespace string, pair AuthKeyPair) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: AuthSecretName(zoneName), Namespace: namespace},
		StringData: map[string]string{
			authSecretKeyField:                  pair.Current.Value,
			authSecretKeyCreatedAtField:         pair.Current.CreatedAt.Format(time.RFC3339Nano),
			authSecretPreviousKeyField:          pair.Previous.Value,
			authSecretPreviousKeyCreatedAtField: pair.Previous.CreatedAt.Format(time.RFC3339Nano),
		},
	}
}

// ParseAuthSecret reads back the key pair BuildAuthSecret wrote. A
// malformed value (hand-edited, or written by some future/older version
// with a different shape) returns ok=false rather than an error, so a
// caller reconciling this Secret (see pkg/domain/zone's reconcileAuthKeys)
// can treat that exactly like the Secret not existing yet, and a caller
// only reading it (see Verifier) can treat it as "no valid credential
// here" rather than panicking on it.
func ParseAuthSecret(secret *corev1.Secret) (AuthKeyPair, bool) {
	current, currentOK := parseAuthKey(secret, authSecretKeyField, authSecretKeyCreatedAtField)
	if !currentOK {
		return AuthKeyPair{}, false
	}

	previous, previousOK := parseAuthKey(secret, authSecretPreviousKeyField, authSecretPreviousKeyCreatedAtField)
	if !previousOK {
		return AuthKeyPair{}, false
	}

	return AuthKeyPair{Current: current, Previous: previous}, true
}

func parseAuthKey(secret *corev1.Secret, valueField, createdAtField string) (AuthKey, bool) {
	value, ok := secret.Data[valueField]
	if !ok || len(value) == 0 {
		return AuthKey{}, false
	}

	createdAt, err := time.Parse(time.RFC3339Nano, string(secret.Data[createdAtField]))
	if err != nil {
		return AuthKey{}, false
	}

	return AuthKey{Value: string(value), CreatedAt: createdAt}, true
}
