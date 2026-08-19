package etcdproxy_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

func TestBuildDownstreamIdentitySecretShape(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM, err := etcdproxy.GenerateIdentity(identityTestZone)
	require.NoError(t, err)

	secret := etcdproxy.BuildDownstreamIdentitySecret("kontinuum-system", certPEM, keyPEM)

	assert.Equal(t, etcdproxy.IdentitySecretName, secret.Name)
	assert.Equal(t, "kontinuum-system", secret.Namespace)
	assert.Equal(t, corev1.SecretTypeTLS, secret.Type)
	assert.Equal(t, string(certPEM), secret.StringData[corev1.TLSCertKey])
	assert.Equal(t, string(keyPEM), secret.StringData[corev1.TLSPrivateKeyKey])
}

func TestLoadPrivateKeyRejectsMalformedPEM(t *testing.T) {
	t.Parallel()

	_, err := etcdproxy.LoadPrivateKey([]byte("not a key"))
	require.ErrorIs(t, err, etcdproxy.ErrNoPrivateKey)
}
