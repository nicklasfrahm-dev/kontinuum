package registry_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
)

const testHeartbeatInterval = 10 * time.Millisecond

// testServerKey() is "test-server"'s own NamespacedName — every Heartbeat
// fixture in this file registers as v1alpha2.DefaultSecretNamespace (see
// Heartbeat.Start's own doc), so every Get/Request below needs both, not
// just Name.
func testServerKey() types.NamespacedName {
	return types.NamespacedName{Name: "test-server", Namespace: v1alpha2.DefaultSecretNamespace}
}

// testSecretData is a placeholder value for Heartbeat.SecretData in tests —
// its content is irrelevant, only that it round-trips into the Secret
// ensureSecret creates.
const testSecretDataValue = "test-storage-dsn"

// assertConfigSecret asserts that ensureSecret created ref's namespace and
// upserted a Secret there containing want.
func assertConfigSecret(
	t *testing.T, fakeClient client.Client, ref v1alpha2.KontinuumSecretReference, want map[string]string,
) {
	t.Helper()

	var namespace corev1.Namespace

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: ref.Namespace}, &namespace),
		"heartbeat should have created the secret's namespace")

	var secret corev1.Secret

	secretKey := types.NamespacedName{Name: ref.Name, Namespace: ref.Namespace}
	require.NoError(t, fakeClient.Get(context.Background(), secretKey, &secret))
	// A real apiserver converts StringData into the base64-encoded Data
	// field on write; the fake client used in these tests doesn't run that
	// admission logic, so StringData is what's actually observable here.
	assert.Equal(t, want, secret.StringData)
}

func TestHeartbeatRegistersAndUpdatesStatus(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)

	testConfig := v1alpha2.KontinuumConfigStatus{
		Server: v1alpha2.KontinuumServerConfigStatus{Addr: ":8080", Storage: testSecretDataValue},
		Log:    v1alpha2.KontinuumLogConfigStatus{Level: "info", Format: "json"},
		OIDC:   v1alpha2.KontinuumOIDCConfigStatus{Enabled: true, IssuerURL: "https://auth.example.com"},
	}

	heartbeat := &registry.Heartbeat{
		Client:     fakeClient,
		Name:       "test-server",
		Role:       v1alpha2.RoleWorker,
		Spec:       v1alpha2.KontinuumSpec{Region: "eu", Zone: "eu-1a"},
		Interval:   testHeartbeatInterval,
		Logger:     slog.Default(),
		Version:    "v1.2.3",
		SecretData: map[string]string{"KONTINUUM_SERVER_STORAGE": testSecretDataValue},
		Config:     testConfig,
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)

	go func() {
		done <- heartbeat.Start(ctx)
	}()

	var server v1alpha2.Kontinuum

	require.Eventually(t, func() bool {
		err := fakeClient.Get(context.Background(), testServerKey(), &server)

		return err == nil && !server.Status.LastHeartbeatTime.IsZero()
	}, time.Second, time.Millisecond, "server was never created with a heartbeat")

	assert.Equal(t, v1alpha2.RoleWorker, server.Status.Role)
	assert.Equal(t, "eu", server.Spec.Region)
	assert.Equal(t, "eu-1a", server.Spec.Zone)
	assert.Equal(t, "v1.2.3", server.Status.Version)
	assert.Equal(t, v1alpha2.KontinuumSecretReference{
		Name:      "kontinuum-test-server",
		Namespace: v1alpha2.DefaultSecretNamespace,
	}, server.Status.SecretRef)
	assertConfigSecret(t, fakeClient, server.Status.SecretRef, heartbeat.SecretData)
	assert.Equal(t, testConfig, server.Status.Config)

	firstHeartbeat := server.Status.LastHeartbeatTime.Time
	assertHeartbeatAdvancesThenDeregisters(t, fakeClient, "test-server", cancel, done, firstHeartbeat)
}

// assertHeartbeatAdvancesThenDeregisters waits for name's Kontinuum object
// to get a heartbeat later than since, cancels the Heartbeat.Start run
// behind done, waits for it to return, then asserts the object was
// deregistered — the common tail shared by every test that drives a full
// Heartbeat.Start lifecycle.
func assertHeartbeatAdvancesThenDeregisters(
	t *testing.T, fakeClient client.Client, name string, cancel context.CancelFunc, done <-chan error, since time.Time,
) {
	t.Helper()

	var server v1alpha2.Kontinuum

	key := types.NamespacedName{Name: name, Namespace: v1alpha2.DefaultSecretNamespace}

	require.Eventually(t, func() bool {
		err := fakeClient.Get(context.Background(), key, &server)

		return err == nil && server.Status.LastHeartbeatTime.After(since)
	}, time.Second, time.Millisecond, "heartbeat never advanced")

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Start did not return after ctx was canceled")
	}

	err := fakeClient.Get(context.Background(), key, &server)
	assert.True(t, apierrors.IsNotFound(err), "server should be deregistered after Start returns")
}

func TestHeartbeatStartAdoptsPreexistingRegistration(t *testing.T) {
	t.Parallel()

	// Simulates a hot-reload restart: the previous process's own object
	// (same hostname-derived name) is still around, either because its
	// graceful deregistration lost the race with the new process starting,
	// or the TTL reconciler hasn't expired it yet.
	fakeClient := newFakeClient(t, &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "test-server", Namespace: v1alpha2.DefaultSecretNamespace},
		Spec:       v1alpha2.KontinuumSpec{Region: "eu", Zone: "eu-1a"},
		Status:     v1alpha2.KontinuumStatus{Role: v1alpha2.RoleWorker},
	})

	heartbeat := &registry.Heartbeat{
		Client:   fakeClient,
		Name:     "test-server",
		Role:     v1alpha2.RoleWorker,
		Spec:     v1alpha2.KontinuumSpec{Region: "eu", Zone: "eu-1a"},
		Interval: testHeartbeatInterval,
		Logger:   slog.Default(),
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)

	go func() {
		done <- heartbeat.Start(ctx)
	}()

	var server v1alpha2.Kontinuum

	require.Eventually(t, func() bool {
		err := fakeClient.Get(context.Background(), testServerKey(), &server)

		return err == nil && !server.Status.LastHeartbeatTime.IsZero()
	}, time.Second, time.Millisecond, "Start should adopt the pre-existing registration and heartbeat it, not fail")

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err, "Start must not treat an already-registered object as fatal")
	case <-time.After(time.Second):
		t.Fatal("Start did not return after ctx was canceled")
	}
}

func TestHeartbeatReregistersIfDeletedExternally(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)

	heartbeat := &registry.Heartbeat{
		Client:   fakeClient,
		Name:     "test-server",
		Role:     v1alpha2.RoleWorker,
		Spec:     v1alpha2.KontinuumSpec{Region: "eu", Zone: "eu-1a"},
		Interval: testHeartbeatInterval,
		Logger:   slog.Default(),
	}

	ctx := t.Context()

	done := make(chan error, 1)

	go func() {
		done <- heartbeat.Start(ctx)
	}()

	var server v1alpha2.Kontinuum

	require.Eventually(t, func() bool {
		err := fakeClient.Get(context.Background(), testServerKey(), &server)

		return err == nil && !server.Status.LastHeartbeatTime.IsZero()
	}, time.Second, time.Millisecond, "server was never created with a heartbeat")

	// Simulate a manual deletion — via the UI's delete button, or kubectl —
	// out from under the running process.
	require.NoError(t, fakeClient.Delete(context.Background(), &server))

	require.Eventually(t, func() bool {
		var recreated v1alpha2.Kontinuum

		err := fakeClient.Get(context.Background(), testServerKey(), &recreated)

		return err == nil && !recreated.Status.LastHeartbeatTime.IsZero()
	}, time.Second, time.Millisecond, "server was never re-registered after external deletion")

	var recreated v1alpha2.Kontinuum

	require.NoError(t, fakeClient.Get(context.Background(), testServerKey(), &recreated))
	assert.Equal(t, v1alpha2.RoleWorker, recreated.Status.Role)
	assert.Equal(t, "eu", recreated.Spec.Region)
	assert.Equal(t, "eu-1a", recreated.Spec.Zone)
}

func TestHeartbeatReconcileReregistersOnDelete(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)

	heartbeat := &registry.Heartbeat{
		Client: fakeClient,
		Name:   "test-server",
		Role:   v1alpha2.RoleControlPlane,
		Logger: slog.Default(),
	}

	req := ctrl.Request{NamespacedName: testServerKey()}

	_, err := heartbeat.Reconcile(context.Background(), req)
	require.NoError(t, err)

	var server v1alpha2.Kontinuum

	require.NoError(t, fakeClient.Get(context.Background(), testServerKey(), &server))
	assert.Equal(t, v1alpha2.RoleControlPlane, server.Status.Role)
	assert.False(t, server.Status.LastHeartbeatTime.IsZero())
}

func TestHeartbeatReconcileNoOpsWhenServerExists(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "test-server", Namespace: v1alpha2.DefaultSecretNamespace},
		Status:     v1alpha2.KontinuumStatus{Role: v1alpha2.RoleWorker},
	})

	heartbeat := &registry.Heartbeat{
		Client: fakeClient,
		Name:   "test-server",
		Logger: slog.Default(),
	}

	req := ctrl.Request{NamespacedName: testServerKey()}

	result, err := heartbeat.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)
}

func TestHeartbeatDeregisterIsIdempotent(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "test-server", Namespace: v1alpha2.DefaultSecretNamespace},
	})

	heartbeat := &registry.Heartbeat{
		Client: fakeClient,
		Name:   "test-server",
		Logger: slog.Default(),
	}

	require.NoError(t, heartbeat.Deregister(context.Background()))

	var server v1alpha2.Kontinuum

	err := fakeClient.Get(context.Background(), testServerKey(), &server)
	assert.True(t, apierrors.IsNotFound(err), "Deregister should have deleted the object")

	// A second call finds the object already gone — a NotFound from an
	// already-completed delete is tolerated, not an error.
	require.NoError(t, heartbeat.Deregister(context.Background()))
}

// TestHeartbeatDeregisterPreventsReconcileFromReregistering is the
// regression test for the race Deregister's shuttingDown flag closes: before
// it existed, Reconcile treated any NotFound — including one caused by
// Deregister's own DELETE — as an external deletion to immediately undo.
func TestHeartbeatDeregisterPreventsReconcileFromReregistering(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t, &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "test-server", Namespace: v1alpha2.DefaultSecretNamespace},
	})

	heartbeat := &registry.Heartbeat{
		Client: fakeClient,
		Name:   "test-server",
		Role:   v1alpha2.RoleWorker,
		Logger: slog.Default(),
	}

	require.NoError(t, heartbeat.Deregister(context.Background()))

	req := ctrl.Request{NamespacedName: testServerKey()}

	_, err := heartbeat.Reconcile(context.Background(), req)
	require.NoError(t, err)

	var server v1alpha2.Kontinuum

	err = fakeClient.Get(context.Background(), testServerKey(), &server)
	assert.True(t, apierrors.IsNotFound(err), "Reconcile must not recreate an object Deregister just deleted")
}

// TestHeartbeatDeregisterStopsBeatFromReregistering is beat's side of the
// same race TestHeartbeatDeregisterPreventsReconcileFromReregistering
// covers for Reconcile: Deregister is called directly, the way
// Controller.Deregister calls it — before Start's own ctx.Done() case ever
// fires, i.e. while Start's ticker is still live and could otherwise race
// to self-heal the deletion on its next tick.
func TestHeartbeatDeregisterStopsBeatFromReregistering(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)

	heartbeat := &registry.Heartbeat{
		Client:   fakeClient,
		Name:     "test-server",
		Role:     v1alpha2.RoleWorker,
		Interval: testHeartbeatInterval,
		Logger:   slog.Default(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() { done <- heartbeat.Start(ctx) }()

	var server v1alpha2.Kontinuum

	require.Eventually(t, func() bool {
		err := fakeClient.Get(context.Background(), testServerKey(), &server)

		return err == nil && !server.Status.LastHeartbeatTime.IsZero()
	}, time.Second, time.Millisecond, "server was never created with a heartbeat")

	require.NoError(t, heartbeat.Deregister(context.Background()))

	// Give the still-running ticker several chances to fire and
	// (incorrectly) recreate the object.
	assert.Never(t, func() bool {
		err := fakeClient.Get(context.Background(), testServerKey(), &server)

		return err == nil
	}, 5*testHeartbeatInterval, testHeartbeatInterval, "beat must not recreate an object Deregister already deleted")

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Start did not return after ctx was canceled")
	}
}
