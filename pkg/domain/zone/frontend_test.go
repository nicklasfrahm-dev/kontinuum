package zone_test

import (
	"testing"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

const (
	// testFrontendHost is the fleet's frontend host — within testDomain.
	testFrontendHost = "app." + testDomain
	// testDNS01ObjectName is the downstream Secret the DNS-01 solver reads
	// the provider credential from.
	testDNS01ObjectName = "kontinuum-dns01-credentials"
)

// reconcileWithFrontendHost reconciles a zone whose fleet serves the UI on
// frontendHost (none when empty) through Cloudflare, returning the zone's
// own cluster.
func reconcileWithFrontendHost(t *testing.T, frontendHost string) client.Client {
	t.Helper()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})
	reconciler.HubConfig.Server.DNS.Provider = "Cloudflare"
	reconciler.HubConfig.Server.Frontend.Host = frontendHost

	enableOIDCForTest(reconciler.HubConfig)

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	return downstream
}

func TestReconcileServesFrontendHostViaDNS01(t *testing.T) {
	t.Parallel()

	downstream := reconcileWithFrontendHost(t, testFrontendHost)

	var issuer certmanagerv1.ClusterIssuer
	require.NoError(t, downstream.Get(t.Context(), client.ObjectKey{Name: testDownstreamResourceName}, &issuer))
	require.Len(t, issuer.Spec.ACME.Solvers, 1)
	assert.Nil(t, issuer.Spec.ACME.Solvers[0].HTTP01, "a name several zones serve can't use HTTP-01")
	require.NotNil(t, issuer.Spec.ACME.Solvers[0].DNS01)
	require.NotNil(t, issuer.Spec.ACME.Solvers[0].DNS01.Cloudflare)
	assert.Equal(t, testDNS01ObjectName, issuer.Spec.ACME.Solvers[0].DNS01.Cloudflare.APIToken.Name)

	var credential corev1.Secret
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDNS01ObjectName, Namespace: testDownstreamNamespace}, &credential))
	assert.Equal(t, testDNSCredentialValue, string(credential.Data["CF_API_TOKEN"]))

	var cert certmanagerv1.Certificate
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &cert))
	assert.Equal(t, []string{testHostname, testFrontendHost}, cert.Spec.DNSNames)

	var gateway gatewayv1.Gateway
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &gateway))
	require.Len(t, gateway.Spec.Listeners, 3)
	assert.Equal(t, gatewayv1.Hostname(testFrontendHost), *gateway.Spec.Listeners[2].Hostname)

	var route gatewayv1.HTTPRoute
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &route))
	assert.Equal(t, []gatewayv1.Hostname{testHostname, testFrontendHost}, route.Spec.Hostnames)
	assert.Len(t, route.Spec.ParentRefs, 2)

	var configMap corev1.ConfigMap
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &configMap))
	assert.Equal(t, "https://"+testFrontendHost+"/app", configMap.Data["KONTINUUM_OIDC_REDIRECT_URL"],
		"every zone registers the one frontend redirect URL")
}

func TestReconcileKeepsHTTP01WithoutFrontendHost(t *testing.T) {
	t.Parallel()

	downstream := reconcileWithFrontendHost(t, "")

	var issuer certmanagerv1.ClusterIssuer
	require.NoError(t, downstream.Get(t.Context(), client.ObjectKey{Name: testDownstreamResourceName}, &issuer))
	require.Len(t, issuer.Spec.ACME.Solvers, 1)
	assert.NotNil(t, issuer.Spec.ACME.Solvers[0].HTTP01)
	assert.Nil(t, issuer.Spec.ACME.Solvers[0].DNS01)

	err := downstream.Get(t.Context(),
		client.ObjectKey{Name: testDNS01ObjectName, Namespace: testDownstreamNamespace}, &corev1.Secret{})
	assert.True(t, apierrors.IsNotFound(err))

	var cert certmanagerv1.Certificate
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamResourceName, Namespace: testDownstreamNamespace}, &cert))
	assert.Equal(t, []string{testHostname}, cert.Spec.DNSNames)

	var configMap corev1.ConfigMap
	require.NoError(t, downstream.Get(t.Context(),
		client.ObjectKey{Name: testDownstreamEnvName, Namespace: testDownstreamNamespace}, &configMap))
	assert.Equal(t, "https://"+testHostname+"/app", configMap.Data["KONTINUUM_OIDC_REDIRECT_URL"])
}

func TestReconcileReportsMissingDNS01CredentialKey(t *testing.T) {
	t.Parallel()

	kontinuum, kontinuumSecret := registeredKontinuum("hub")
	hubClient := newHubFakeClient(t, testZoneObject(), readyTalosCluster(), kubeconfigSecret(),
		kontinuum, kontinuumSecret)
	downstream := newDownstreamFakeClient(t)
	reconciler := newReconciler(hubClient, fakeDownstreamClientBuilder{client: downstream})
	reconciler.HubConfig.Server.DNS.Provider = "AWS"
	reconciler.HubConfig.Server.Frontend.Host = testFrontendHost

	_, err := reconciler.Reconcile(t.Context(), reconcileRequest())
	require.NoError(t, err)

	cond := getInstalledCondition(t, hubClient)
	assert.Equal(t, "InstallFailed", cond.Reason)
	assert.Contains(t, cond.Message, "AWS_ACCESS_KEY_ID")
}

// getInstalledCondition reads the test zone's Installed condition.
func getInstalledCondition(t *testing.T, hubClient client.Client) metav1.Condition {
	t.Helper()

	cond := meta.FindStatusCondition(getTestZone(t, hubClient).Status.Conditions, zone.InstalledConditionType)
	require.NotNil(t, cond)

	return *cond
}
