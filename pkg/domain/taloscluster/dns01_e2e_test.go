package taloscluster_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/addon"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

// This file is the frontend host's DNS-01 end-to-end test. It lives here,
// rather than in pkg/domain/zone, because it builds on this package's own
// Talos cluster bootstrap harness (startTalosContainer, reconcileAddonsOnce)
// to get a real cluster running cert-manager.

const (
	// e2eBindImage and e2ePebbleImage are pinned like e2eTalosImage.
	e2eBindImage   = "internetsystemsconsortium/bind9:9.20"
	e2ePebbleImage = "ghcr.io/letsencrypt/pebble:2.10.1"
	// e2eDNSDomain is the zone BIND serves and accepts dynamic updates for.
	e2eDNSDomain = "e2e.test"
	// e2eTSIGKeyName and e2eTSIGKey sign RFC2136 updates. A throwaway test
	// key, valid only for this test's own BIND container.
	e2eTSIGKeyName = "kontinuum."
	e2eTSIGKey     = "a29udGludXVtLWUyZS10c2lnLWtleS1mb3ItdGVzdHM="
	// e2eCertificateTimeout bounds how long cert-manager has to issue the
	// zone's certificate once the cluster is Ready.
	e2eCertificateTimeout = 6 * time.Minute
	// e2eZoneName and e2eRegion name the zone whose certificate is issued.
	e2eZoneName = "1a"
	e2eRegion   = "eu"
)

// TestE2EFrontendHostCertificateIsIssuedViaDNS01 proves a zone with a
// frontend host gets one certificate covering both its own hostname and the
// frontend host, ordered through DNS-01 with the RFC2136 solver built from
// KONTINUUM_SERVER_DNS_CREDENTIAL: BIND accepts the TSIG-signed TXT
// updates, Pebble validates against BIND, and cert-manager's own
// propagation check is pointed at BIND too.
func TestE2EFrontendHostCertificateIsIssuedViaDNS01(t *testing.T) {
	t.Parallel()

	if os.Getenv("KONTINUUM_TEST_E2E") == "" {
		t.Skip("set KONTINUUM_TEST_E2E=1 to run this test — it needs Docker and boots real Talos containers")
	}

	ctx, cancel := context.WithTimeout(context.Background(), e2eOverallTimeout)
	defer cancel()

	bindIP := startBindContainer(ctx, t)
	pebbleIP := startPebbleContainer(ctx, t, bindIP)
	controlPlaneIP := startTalosContainer(ctx, t)

	hubClient, talosReconciler, addonReconciler, req := setupDNS01Cluster(t, controlPlaneIP, bindIP)

	reconcileUntilReady(ctx, t, hubClient, talosReconciler, addonReconciler, req,
		e2eOverallTimeout-e2eCertificateTimeout)

	zoneReconciler := newDNS01ZoneReconciler(hubClient, talosReconciler.Logger, bindIP, pebbleIP)

	cert := waitForZoneCertificate(ctx, t, hubClient, zoneReconciler)

	hostname := e2eZoneName + "." + e2eRegion + "." + e2eDNSDomain
	assert.Equal(t, []string{hostname, "app." + e2eDNSDomain}, cert.Spec.DNSNames)
}

// startBindContainer runs BIND as the authoritative server for
// e2eDNSDomain, accepting dynamic updates signed with e2eTSIGKey, and
// returns its address.
func startBindContainer(ctx context.Context, t *testing.T) string {
	t.Helper()

	namedConf := fmt.Sprintf(`key %q { algorithm hmac-sha256; secret %q; };
options {
	directory "/var/cache/bind";
	listen-on { any; };
	listen-on-v6 { none; };
	allow-query { any; };
	recursion no;
	dnssec-validation no;
};
zone %q {
	type primary;
	file "/var/lib/bind/db.zone";
	update-policy { grant %s zonesub ANY; };
};
`, e2eTSIGKeyName, e2eTSIGKey, e2eDNSDomain, e2eTSIGKeyName)

	zoneFile := fmt.Sprintf(`$TTL 30
@ IN SOA ns.%[1]s. admin.%[1]s. 1 30 30 300 30
@ IN NS ns.%[1]s.
ns IN A 127.0.0.1
`, e2eDNSDomain)

	bind, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image: e2eBindImage,
			Cmd:   []string{"-g", "-c", "/etc/bind/named.conf"},
			Files: []testcontainers.ContainerFile{
				{Reader: strings.NewReader(namedConf), ContainerFilePath: "/etc/bind/named.conf", FileMode: 0o644},
				{Reader: strings.NewReader(zoneFile), ContainerFilePath: "/var/lib/bind/db.zone", FileMode: 0o666},
			},
			WaitingFor: wait.ForLog("running").WithStartupTimeout(e2eContainerStartTimeout),
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() { terminateContainer(ctx, t, bind) })

	bindIP, err := bind.ContainerIP(ctx)
	require.NoError(t, err)

	return bindIP
}

// startPebbleContainer runs Pebble, validating challenges against the DNS
// server at bindIP, and returns its address.
func startPebbleContainer(ctx context.Context, t *testing.T, bindIP string) string {
	t.Helper()

	pebble, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image: e2ePebbleImage,
			Cmd:   []string{"-config", "test/config/pebble-config.json", "-dnsserver", bindIP + ":53"},
			Env:   map[string]string{"PEBBLE_VA_NOSLEEP": "1", "PEBBLE_WFE_NONCEREJECT": "0"},
			WaitingFor: wait.ForLog("ACME directory available").
				WithStartupTimeout(e2eContainerStartTimeout),
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() { terminateContainer(ctx, t, pebble) })

	pebbleIP, err := pebble.ContainerIP(ctx)
	require.NoError(t, err)

	return pebbleIP
}

// terminateContainer stops container at the end of the test — see
// isContainerAlreadyRemoving for the one error it tolerates.
func terminateContainer(ctx context.Context, t *testing.T, container testcontainers.Container) {
	t.Helper()

	err := container.Terminate(context.WithoutCancel(ctx))
	if err != nil && !isContainerAlreadyRemoving(err) {
		require.NoError(t, err)
	}
}

// setupDNS01Cluster seeds a hub client with a single-node TalosCluster for
// controlPlaneIP, its Zone, and a cert-manager Addon whose own DNS-01
// propagation check only asks bindIP — e2eDNSDomain exists nowhere else.
func setupDNS01Cluster(
	t *testing.T, controlPlaneIP, bindIP string,
) (client.Client, *taloscluster.Reconciler, *addon.Reconciler, ctrl.Request) {
	t.Helper()

	name := e2eRegion + "-" + e2eZoneName

	cluster := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.TalosClusterSpec{
			ControlPlane: v1alpha2.TalosClusterMemberSpec{PoolRef: v1alpha2.InstancePoolReference{Name: testControlPlanePool}},
		},
	}

	cpInstance := claimedDiscoveredInstance("cp-node", testControlPlanePool, controlPlaneIP)
	cpInstance.Namespace = v1alpha2.KontinuumSystemNamespace

	certManagerValues, err := json.Marshal(map[string]any{"extraArgs": []string{
		"--dns01-recursive-nameservers-only", "--dns01-recursive-nameservers=" + bindIP + ":53",
	}})
	require.NoError(t, err)

	certManager := &v1alpha2.Addon{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-cert-manager", Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec: v1alpha2.AddonSpec{
			TalosClusterRef: v1alpha2.TalosClusterReference{Name: name},
			ReleaseName:     "cert-manager",
			Values:          &apiextensionsv1.JSON{Raw: certManagerValues},
		},
	}

	zoneObj := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
		Spec:       v1alpha2.ZoneSpec{Region: e2eRegion, Zone: e2eZoneName, Domain: e2eDNSDomain},
	}

	// resolveImage needs some registered version to deploy; the zone's own
	// kontinuum-server never has to run for its certificate to be issued.
	kontinuum := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: "hub", Namespace: v1alpha2.KontinuumSystemNamespace},
		Status:     v1alpha2.KontinuumStatus{Version: "v0.37.0"},
	}

	hubClient := newDNS01HubClient(t, cluster, cpInstance, certManager, zoneObj, kontinuum)

	talosReconciler, addonReconciler := newDNS01Reconcilers(hubClient)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace}}

	return hubClient, talosReconciler, addonReconciler, req
}

// newDNS01Reconcilers builds the real TalosCluster and Addon reconcilers
// that bootstrap the test's cluster.
func newDNS01Reconcilers(hubClient client.Client) (*taloscluster.Reconciler, *addon.Reconciler) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	talosReconciler := &taloscluster.Reconciler{
		Client:             hubClient,
		Bootstrapper:       taloscluster.NewTalosBootstrapper(logger),
		HealthCheckTimeout: e2eHealthCheckTimeout,
		RetryInterval:      e2eRetryInterval,
		Locker:             zonelease.NewLocker(hubClient, hubClient, "test-hub", "", 0),
		Logger:             logger,
	}

	addonReconciler := &addon.Reconciler{
		Client:        hubClient,
		Installer:     addon.NewHelmInstaller(),
		PodProber:     addon.NewPodProber(),
		CRDChecker:    addon.NewCRDChecker(),
		RetryInterval: e2eRetryInterval,
		Locker:        zonelease.NewLocker(hubClient, hubClient, "test-hub", "", 0),
		Logger:        logger,
	}

	return talosReconciler, addonReconciler
}

// newDNS01HubClient is newFakeClient plus the Zone and Kontinuum status
// subresources the zone controller writes.
func newDNS01HubClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, v1alpha2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, coordinationv1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.TalosCluster{}, &v1alpha2.Addon{}, &v1alpha2.Instance{}, &v1alpha2.Zone{}).
		WithObjects(objects...).
		Build()
}

// newDNS01ZoneReconciler builds a zone controller for a fleet serving its
// UI on app.<e2eDNSDomain>, with an RFC2136 DNS provider at bindIP and
// Pebble at pebbleIP as its CA.
func newDNS01ZoneReconciler(hubClient client.Client, logger *slog.Logger, bindIP, pebbleIP string) *zone.Reconciler {
	hubConfig := &config.Config{}
	hubConfig.Defaults()
	hubConfig.InsecureAllowAnonymous = "true"
	hubConfig.Server.Storage = "postgres://kontinuum@db.example.com/kontinuum"
	hubConfig.Server.DNS.Domain = e2eDNSDomain
	hubConfig.Server.DNS.Provider = string(config.DNSProviderRFC2136)
	hubConfig.Server.DNS.Credential = fmt.Sprintf(
		"EXTERNAL_DNS_RFC2136_HOST: %s\nEXTERNAL_DNS_RFC2136_TSIG_KEYNAME: %s\nEXTERNAL_DNS_RFC2136_TSIG_SECRET: %s\n",
		bindIP, e2eTSIGKeyName, e2eTSIGKey)
	hubConfig.Server.Frontend.Host = "app." + e2eDNSDomain
	hubConfig.ACME.Email = "ops@" + e2eDNSDomain
	hubConfig.ACME.Server = "https://" + pebbleIP + ":14000/dir"
	hubConfig.ACME.InsecureSkipVerify = "true"

	return &zone.Reconciler{
		Client:                  hubClient,
		DownstreamClientBuilder: zone.NewDownstreamClientBuilder(),
		Digests:                 zone.NewHelmDigestResolver(),
		HubConfig:               hubConfig,
		ImageRepo:               "ghcr.io/nicklasfrahm-dev/kontinuum",
		RetryInterval:           e2eRetryInterval,
		StorageMode:             config.StorageModeDirect,
		Locker:                  zonelease.NewLocker(hubClient, hubClient, "test-hub", "", 0),
		Logger:                  logger,
	}
}

// waitForZoneCertificate reconciles the zone until its own cluster's
// Certificate reports Ready, returning it. Installed itself may stay False
// throughout: this cluster has no external-dns to serve the zone's
// DNSEndpoint, and the zone's own kontinuum-server never needs to start.
// Only the certificate is under test.
func waitForZoneCertificate(
	ctx context.Context, t *testing.T, hubClient client.Client, reconciler *zone.Reconciler,
) certmanagerv1.Certificate {
	t.Helper()

	req := ctrl.Request{NamespacedName: types.NamespacedName{
		Name: e2eRegion + "-" + e2eZoneName, Namespace: v1alpha2.KontinuumSystemNamespace,
	}}
	deadline := time.Now().Add(e2eCertificateTimeout)

	var zoneObj v1alpha2.Zone

	for {
		_, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			reconciler.Logger.Warn("zone reconcile error (may be transient)", "error", err)
		}

		require.NoError(t, hubClient.Get(ctx, req.NamespacedName, &zoneObj))

		downstream, err := zone.DownstreamClient(ctx, hubClient, reconciler.DownstreamClientBuilder, &zoneObj)
		require.NoError(t, err)

		var cert certmanagerv1.Certificate

		err = downstream.Get(ctx, client.ObjectKey{Name: "kontinuum", Namespace: "kontinuum-system"}, &cert)
		if err == nil && certificateIsReady(cert) {
			reconciler.Logger.Info("zone certificate issued", "dnsNames", cert.Spec.DNSNames)

			return cert
		}

		reconciler.Logger.Info("progress: waiting for zone certificate",
			"zone", conditionsSummary(zoneObj.Status.Conditions), "certificate", conditionsSummary(certConditions(cert)))

		if time.Now().After(deadline) {
			require.Fail(t, "zone certificate was never issued")
		}

		select {
		case <-ctx.Done():
			require.Fail(t, "context canceled while waiting for the zone certificate")
		case <-time.After(e2eReconcileTick):
		}
	}
}

// certificateIsReady reports whether cert's Ready condition is True.
func certificateIsReady(cert certmanagerv1.Certificate) bool {
	for _, cond := range cert.Status.Conditions {
		if cond.Type == certmanagerv1.CertificateConditionReady {
			return cond.Status == "True"
		}
	}

	return false
}

// certConditions converts cert's conditions for conditionsSummary.
func certConditions(cert certmanagerv1.Certificate) []metav1.Condition {
	conds := make([]metav1.Condition, 0, len(cert.Status.Conditions))
	for _, cond := range cert.Status.Conditions {
		conds = append(conds, metav1.Condition{
			Type: string(cond.Type), Status: metav1.ConditionStatus(cond.Status), Reason: cond.Reason,
			Message: cond.Message,
		})
	}

	return conds
}
