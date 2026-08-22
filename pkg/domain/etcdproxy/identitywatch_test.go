package etcdproxy_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

const identityWatchTestNamespace = "kontinuum-system"

//nolint:ireturn // client.WithWatch is controller-runtime's own seam, mirrors fake.NewClientBuilder().Build()'s return
func newIdentityWatchFakeClient(t *testing.T, objs ...client.Object) client.WithWatch {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func identitySecretWithKey(t *testing.T, keyPEM []byte) *corev1.Secret {
	t.Helper()

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: etcdproxy.IdentitySecretName, Namespace: identityWatchTestNamespace},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}
}

// TestWatchIdentityLoadsInitialKey covers WatchIdentity's synchronous
// initial load: a returned nil error means the caller already has the
// Secret's own current key in hand, with no need to wait on the
// background watch to populate it.
func TestWatchIdentityLoadsInitialKey(t *testing.T) {
	t.Parallel()

	_, keyPEM, err := etcdproxy.GenerateIdentity(identityTestZone)
	require.NoError(t, err)

	fakeClient := newIdentityWatchFakeClient(t, identitySecretWithKey(t, keyPEM))

	source, err := etcdproxy.WatchIdentity(t.Context(), fakeClient, identityWatchTestNamespace, testLogger())
	require.NoError(t, err)

	wantKey, err := etcdproxy.LoadPrivateKey(keyPEM)
	require.NoError(t, err)

	assert.Equal(t, wantKey, source.Current())
}

// TestWatchIdentityMissingSecretFails covers WatchIdentity's own initial
// load failing fast when the Secret doesn't exist yet — a caller should
// never end up with a KeySource that silently has no key at all.
func TestWatchIdentityMissingSecretFails(t *testing.T) {
	t.Parallel()

	fakeClient := newIdentityWatchFakeClient(t)

	_, err := etcdproxy.WatchIdentity(t.Context(), fakeClient, identityWatchTestNamespace, testLogger())
	require.Error(t, err)
}

// TestWatchIdentityPicksUpRotation covers the whole point of WatchIdentity
// over the mounted-file approach it replaced: once the caller's own ctx
// stays alive, updating the Secret's own contents (as
// pkg/domain/zone.ensureEtcdIdentity does on rotation) is reflected in
// Current with no restart of anything.
func TestWatchIdentityPicksUpRotation(t *testing.T) {
	t.Parallel()

	_, firstKeyPEM, err := etcdproxy.GenerateIdentity(identityTestZone)
	require.NoError(t, err)

	secret := identitySecretWithKey(t, firstKeyPEM)
	fakeClient := newIdentityWatchFakeClient(t, secret)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	source, err := etcdproxy.WatchIdentity(ctx, fakeClient, identityWatchTestNamespace, testLogger())
	require.NoError(t, err)

	_, secondKeyPEM, err := etcdproxy.GenerateIdentity(identityTestZone)
	require.NoError(t, err)

	var current corev1.Secret
	require.NoError(t, fakeClient.Get(ctx,
		client.ObjectKey{Name: etcdproxy.IdentitySecretName, Namespace: identityWatchTestNamespace}, &current))

	current.Data = map[string][]byte{corev1.TLSPrivateKeyKey: secondKeyPEM}
	require.NoError(t, fakeClient.Update(ctx, &current))

	wantKey, err := etcdproxy.LoadPrivateKey(secondKeyPEM)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got := source.Current()

		return got != nil && string(got) == string(wantKey)
	}, time.Second, 10*time.Millisecond, "Current must observe the rotated key without any restart")
}

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}
