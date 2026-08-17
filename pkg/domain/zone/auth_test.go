package zone //nolint:testpackage // exercises unexported reconcileAuthKeys directly

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
)

const authTestZoneName = "eu-eu-1a"

func newAuthTestClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func authTestZoneObject() *v1alpha2.Zone {
	return &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: authTestZoneName, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: "eu", Zone: "eu-1a", Domain: "example.com"},
	}
}

// simulateAdmission copies StringData into Data (base64-decoded, as a real
// apiserver's admission would) and persists the result — the fake client
// doesn't run that conversion itself, so a test that writes via StringData
// (as reconcileAuthKeys does) and then wants to re-read the same object
// through ParseAuthSecret (which reads Data) needs this in between, exactly
// as pkg/domain/registry/heartbeat_test.go's own tests already do for the
// same reason.
func simulateAdmission(t *testing.T, hubClient client.Client, secret *corev1.Secret) {
	t.Helper()

	secret.Data = map[string][]byte{}
	for k, v := range secret.StringData {
		secret.Data[k] = []byte(v)
	}

	secret.StringData = nil
	require.NoError(t, hubClient.Update(t.Context(), secret))
}

func getAuthSecret(t *testing.T, c client.Client) *corev1.Secret {
	t.Helper()

	var secret corev1.Secret
	require.NoError(t, c.Get(t.Context(),
		client.ObjectKey{Name: etcdproxy.AuthSecretName(authTestZoneName), Namespace: v1alpha2.KontinuumSystemNamespace},
		&secret))

	return &secret
}

func TestReconcileAuthKeysIssuesFreshPairWhenSecretMissing(t *testing.T) {
	t.Parallel()

	zoneObj := authTestZoneObject()
	hubClient := newAuthTestClient(t, zoneObj)
	reconciler := &Reconciler{Client: hubClient}

	requeue, err := reconciler.reconcileAuthKeys(t.Context(), zoneObj)
	require.NoError(t, err)
	assert.Equal(t, authKeyCheckInterval, requeue)

	secret := getAuthSecret(t, hubClient)

	require.Len(t, secret.OwnerReferences, 1)
	assert.Equal(t, "Zone", secret.OwnerReferences[0].Kind)
	assert.Equal(t, authTestZoneName, secret.OwnerReferences[0].Name)
	assert.True(t, *secret.OwnerReferences[0].Controller)

	assert.Len(t, secret.StringData["key"], etcdproxy.KeyLength)
	assert.Len(t, secret.StringData["previous-key"], etcdproxy.KeyLength)
	// Freshly issued: both keys start out identical, since there's no real
	// "previous" one yet — see reconcileAuthKeys' own doc.
	assert.Equal(t, secret.StringData["key"], secret.StringData["previous-key"])
}

func TestReconcileAuthKeysLeavesFreshKeyUntouched(t *testing.T) {
	t.Parallel()

	zoneObj := authTestZoneObject()
	hubClient := newAuthTestClient(t, zoneObj)
	reconciler := &Reconciler{Client: hubClient}

	_, err := reconciler.reconcileAuthKeys(t.Context(), zoneObj)
	require.NoError(t, err)

	secret := getAuthSecret(t, hubClient)
	simulateAdmission(t, hubClient, secret)
	beforeResourceVersion := secret.ResourceVersion

	requeue, err := reconciler.reconcileAuthKeys(t.Context(), zoneObj)
	require.NoError(t, err)
	assert.Positive(t, requeue)
	assert.LessOrEqual(t, requeue, authKeyCheckInterval)

	after := getAuthSecret(t, hubClient)
	assert.Equal(t, beforeResourceVersion, after.ResourceVersion, "an unrotated key should not trigger a write")
}

// TestReconcileAuthKeysRotatesDueKeyAndKeepsPreviousInOverlap covers the
// core rotation contract: once the current key is due, it's demoted into
// the previous slot (keeping its own original CreatedAt) and a fresh key
// takes over as current.
func TestReconcileAuthKeysRotatesDueKeyAndKeepsPreviousInOverlap(t *testing.T) {
	t.Parallel()

	zoneObj := authTestZoneObject()

	staleCreatedAt := time.Now().Add(-etcdproxy.RotationInterval - time.Minute)
	existing := etcdproxy.BuildAuthSecret(authTestZoneName, v1alpha2.KontinuumSystemNamespace, etcdproxy.AuthKeyPair{
		Current:  etcdproxy.AuthKey{Value: "old-current", CreatedAt: staleCreatedAt},
		Previous: etcdproxy.AuthKey{Value: "old-previous", CreatedAt: staleCreatedAt.Add(-etcdproxy.RotationInterval)},
	})
	existing.Data = map[string][]byte{}

	for k, v := range existing.StringData {
		existing.Data[k] = []byte(v)
	}

	existing.StringData = nil

	hubClient := newAuthTestClient(t, zoneObj, existing)
	reconciler := &Reconciler{Client: hubClient}

	requeue, err := reconciler.reconcileAuthKeys(t.Context(), zoneObj)
	require.NoError(t, err)
	assert.Equal(t, authKeyCheckInterval, requeue)

	got := getAuthSecret(t, hubClient)

	// The just-superseded "old-current" is now the previous key — its own
	// value carries forward unchanged, still within its overlap window
	// (only just past its rotation due time, well before its ExpiresAt).
	assert.Equal(t, "old-current", got.StringData["previous-key"])
	assert.NotEqual(t, "old-previous", got.StringData["previous-key"])
	assert.NotEqual(t, "old-current", got.StringData["key"])
	assert.Len(t, got.StringData["key"], etcdproxy.KeyLength)
}
