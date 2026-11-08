package zone_test

import (
	"testing"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// testEABKeyID and testEABHMACKey are a fake external account
	// binding, not a real one.
	testEABKeyID   = "kid-1"
	testEABHMACKey = "example-hmac-key"
	// testEABObjectName is the downstream Secret the binding's MAC key
	// lands in.
	testEABObjectName = "kontinuum-acme-eab"
)

func reconcileWithEAB(t *testing.T, keyID, hmacKey string) client.Client {
	t.Helper()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})
	reconciler.HubConfig.ACME.ExternalAccountBinding.KeyID = keyID
	reconciler.HubConfig.ACME.ExternalAccountBinding.HMACKey = hmacKey

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	return downstream
}

func TestReconcileClusterIssuerUsesExternalAccountBinding(t *testing.T) {
	t.Parallel()

	downstream := reconcileWithEAB(t, testEABKeyID, testEABHMACKey)

	var issuer certmanagerv1.ClusterIssuer
	require.NoError(t, downstream.Get(t.Context(), client.ObjectKey{Name: testDownstreamResourceName}, &issuer))
	require.NotNil(t, issuer.Spec.ACME.ExternalAccountBinding)
	assert.Equal(t, testEABKeyID, issuer.Spec.ACME.ExternalAccountBinding.KeyID)
	assert.Equal(t, testEABObjectName, issuer.Spec.ACME.ExternalAccountBinding.Key.Name)

	var secret corev1.Secret
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testEABObjectName, Namespace: testDownstreamNamespace}, &secret))
	assert.Equal(t, testEABHMACKey, string(secret.Data[issuer.Spec.ACME.ExternalAccountBinding.Key.Key]))

	// The MAC key reaches the zone's own env through its Secret only.
	var env corev1.Secret
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &env))
	assert.Equal(t, testEABHMACKey, string(env.Data["KONTINUUM_ACME_EXTERNAL_ACCOUNT_BINDING_HMAC_KEY"]))

	var configMap corev1.ConfigMap
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &configMap))
	assert.NotContains(t, configMap.Data, "KONTINUUM_ACME_EXTERNAL_ACCOUNT_BINDING_HMAC_KEY")
	assert.Equal(t, testEABKeyID, configMap.Data["KONTINUUM_ACME_EXTERNAL_ACCOUNT_BINDING_KEY_ID"])
}

func TestReconcileClusterIssuerWithoutExternalAccountBinding(t *testing.T) {
	t.Parallel()

	downstream := reconcileWithEAB(t, "", "")

	var issuer certmanagerv1.ClusterIssuer
	require.NoError(t, downstream.Get(t.Context(), client.ObjectKey{Name: testDownstreamResourceName}, &issuer))
	assert.Nil(t, issuer.Spec.ACME.ExternalAccountBinding)

	err := downstream.Get(t.Context(),
		client.ObjectKey{Name: testEABObjectName, Namespace: testDownstreamNamespace}, &corev1.Secret{})
	assert.True(t, apierrors.IsNotFound(err))
}
