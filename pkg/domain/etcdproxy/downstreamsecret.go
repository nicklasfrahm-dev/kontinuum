package etcdproxy

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// IdentitySecretName is the fixed name a zone's own downstream cluster
// stores its etcd gRPC proxy identity keypair under — fixed, not
// per-zone-suffixed like AuthSecretName, since exactly one zone's worth of
// identity ever lives on a given downstream cluster (mirrors
// pkg/domain/zone/workload.go's own envSecretName/envConfigMapName
// convention).
//
//nolint:gosec // false positive: an object name, not a credential value
const IdentitySecretName = "kontinuum-etcd-identity"

// BuildDownstreamIdentitySecret builds the kubernetes.io/tls Secret a
// zone's own downstream cluster holds its full ed25519 identity keypair
// under (see GenerateIdentity) — the private half never leaves this
// cluster; the hub only ever keeps certPEM (see BuildPublicSecret).
func BuildDownstreamIdentitySecret(namespace string, certPEM, keyPEM []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: IdentitySecretName, Namespace: namespace},
		Type:       corev1.SecretTypeTLS,
		StringData: map[string]string{
			corev1.TLSCertKey:       string(certPEM),
			corev1.TLSPrivateKeyKey: string(keyPEM),
		},
	}
}

// ErrNoPrivateKey is LoadPrivateKey's own error for PEM content with no
// parseable block at all.
var ErrNoPrivateKey = errors.New("no private key found in PEM data")

// LoadPrivateKey parses the ed25519 private key
// BuildDownstreamIdentitySecret stored PEM(PKCS8)-encoded — used by
// WatchIdentity to load a zone's own identity Secret, fetched live off its
// own downstream cluster's API rather than a mounted file.
func LoadPrivateKey(keyPEM []byte) (ed25519.PrivateKey, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, ErrNoPrivateKey
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pkcs8 private key: %w", err)
	}

	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, ErrNotEd25519Key
	}

	return priv, nil
}
