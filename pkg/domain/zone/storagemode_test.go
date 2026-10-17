package zone_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

// storageEnvKey is the kontinuum-env key a zone's storage DSN lands under.
const storageEnvKey = "KONTINUUM_SERVER_STORAGE"

// testDirectStorage is the shared, publicly reachable database a
// direct-mode fleet's every process connects to.
const testDirectStorage = "postgres://kontinuum@db.example.com:5432/kontinuum"

// newDirectModeReconciler builds a Reconciler for a direct-mode fleet: no
// gRPC endpoint configured at all, since direct mode never needs one.
func newDirectModeReconciler(t *testing.T, hubClient, downstream client.Client) *zone.Reconciler {
	t.Helper()

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})
	reconciler.StorageMode = config.StorageModeDirect
	reconciler.HubConfig.Server.Storage = testDirectStorage
	reconciler.HubConfig.Server.GRPC.Endpoint = ""

	return reconciler
}

func getTestZone(t *testing.T, hubClient client.Client) v1alpha2.Zone {
	t.Helper()

	var got v1alpha2.Zone
	require.NoError(t, hubClient.Get(t.Context(), testZoneKey(), &got))

	return got
}

func TestReconcileDirectModeHandsZoneTheSharedDSN(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newDirectModeReconciler(t, hubClient, downstream)

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	got := getTestZone(t, hubClient)
	assert.Equal(t, string(config.StorageModeDirect), got.Status.StorageMode)

	// Past storage preparation: no gRPC endpoint was needed.
	cond := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, "WaitingForCertificate", cond.Reason)

	var secret corev1.Secret
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &secret))
	assert.Equal(t, testDirectStorage, string(secret.Data[storageEnvKey]))

	// The DSN carries the database credential, so it must never reach the
	// broadly-readable ConfigMap.
	var configMap corev1.ConfigMap
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &configMap))
	assert.NotContains(t, configMap.Data, storageEnvKey)

	// No etcd proxy identity, on either side: it only exists to
	// authenticate against the hub's etcd proxy.
	err = downstream.Get(t.Context(),
		client.ObjectKey{Name: etcdproxy.IdentitySecretName, Namespace: testDownstreamNamespace}, &corev1.Secret{})
	assert.True(t, apierrors.IsNotFound(err), "direct mode must not issue a downstream identity")

	err = hubClient.Get(t.Context(),
		client.ObjectKey{Name: etcdproxy.AuthSecretName(testZoneName), Namespace: v1alpha2.KontinuumSystemNamespace},
		&corev1.Secret{})
	assert.True(t, apierrors.IsNotFound(err), "direct mode must not issue a hub-side identity")
}

func TestReconcileProxyModeRecordsStorageMode(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	got := getTestZone(t, hubClient)
	assert.Equal(t, string(config.StorageModeProxy), got.Status.StorageMode)

	assertDownstreamFootprintInstalled(t, downstream)
}

func TestReconcileRejectsRecordedStorageModeChange(t *testing.T) {
	t.Parallel()

	zoneObj := testZoneObject()
	zoneObj.Status.StorageMode = string(config.StorageModeProxy)

	hubClient := newHubFakeClient(t, zoneObj, readyTalosCluster(), kubeconfigSecret())
	downstream := newDownstreamFakeClient(t)
	reconciler := newDirectModeReconciler(t, hubClient, downstream)

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	got := getTestZone(t, hubClient)

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "StorageModeChanged", cond.Reason)
	assert.Contains(t, cond.Message, "KONTINUUM_SERVER_STORAGE_MODE=Proxy")
	assert.Equal(t, string(config.StorageModeProxy), got.Status.StorageMode)

	// Nothing downstream is rewritten.
	err = downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &corev1.Secret{})
	assert.True(t, apierrors.IsNotFound(err), "a rejected mode change must not write the zone's env")
}

// TestReconcileRejectsModeChangeOnZoneInstalledBeforeModesExisted covers
// the upgrade path: a zone installed before Zone.status.storageMode
// existed has no recorded mode, but its kontinuum-env already carries a
// relay DSN. A hub that now infers Direct (e.g. its database is public)
// must not move it.
func TestReconcileRejectsModeChangeOnZoneInstalledBeforeModesExisted(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret())
	downstream := newDownstreamFakeClient(t)

	legacyEnv := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace},
		Data: map[string][]byte{
			storageEnvKey: []byte(etcdproxy.BuildRelayDSN(testZoneName, testGRPCEndpoint)),
		},
	}
	require.NoError(t, downstream.Create(t.Context(), legacyEnv))

	reconciler := newDirectModeReconciler(t, hubClient, downstream)

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	got := getTestZone(t, hubClient)
	assert.Equal(t, string(config.StorageModeProxy), got.Status.StorageMode)

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, "StorageModeChanged", cond.Reason)

	var secret corev1.Secret
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &secret))
	assert.Equal(t, legacyEnv.Data[storageEnvKey], secret.Data[storageEnvKey])
}

func TestReconcileAdoptsLegacyProxyZoneInProxyFleet(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)

	require.NoError(t, downstream.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace},
		Data: map[string][]byte{
			storageEnvKey: []byte(etcdproxy.BuildRelayDSN(testZoneName, testGRPCEndpoint)),
		},
	}))

	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	got := getTestZone(t, hubClient)
	assert.Equal(t, string(config.StorageModeProxy), got.Status.StorageMode)

	cond := meta.FindStatusCondition(got.Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, "WaitingForCertificate", cond.Reason)
}

func TestReconcileReportsUnsupportedNATSMode(t *testing.T) {
	t.Parallel()

	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret())
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: newDownstreamFakeClient(t)})
	reconciler.StorageMode = config.StorageModeNATS

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	cond := meta.FindStatusCondition(getTestZone(t, hubClient).Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, "NoStorageSecretFound", cond.Reason)
	assert.Contains(t, cond.Message, "not supported")
}

func TestStorageModeOfDSN(t *testing.T) {
	t.Parallel()

	for dsn, want := range map[string]config.StorageMode{
		etcdproxy.BuildRelayDSN(testZoneName, testGRPCEndpoint): config.StorageModeProxy,
		testDirectStorage:               config.StorageModeDirect,
		"mysql://kine@tcp(db:3306)/db":  config.StorageModeDirect,
		"nats://nats.example.com:4222":  config.StorageModeNATS,
		"sqlite://kontinuum.db":         config.StorageModeDirect,
		"etcdproxy://missing-user-host": config.StorageModeDirect,
	} {
		assert.Equal(t, want, zone.StorageModeOfDSN(dsn), dsn)
	}
}
