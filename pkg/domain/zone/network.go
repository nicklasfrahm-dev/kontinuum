package zone

import (
	"context"
	"fmt"
	"net"
	"strconv"

	acmev1 "github.com/cert-manager/cert-manager/pkg/apis/acme/v1"
	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/addon"
)

// gatewayClassName is the GatewayClass every joined zone's Gateway
// references — assumed pre-existing on the downstream cluster, provided by
// Cilium's own Gateway API support (already installed as a built-in addon
// with gatewayAPI.enabled: true — see pkg/domain/addon/values/cilium.yaml).
// Zone does not create this GatewayClass itself.
const gatewayClassName = "cilium"

// clusterIssuerName, gatewayName, certificateName, httpRouteName, and
// certSecretName are all fixed, not per-zone-derived: each joined zone gets
// its own downstream cluster, so there's exactly one of each per cluster —
// same reasoning as workload.go's deploymentName/serviceName.
//
//nolint:gosec // false positive: certSecretName is a Secret's name, not a credential value
const (
	clusterIssuerName = "kontinuum"
	// eabSecretName holds the ACME external account binding's MAC key
	// under eabSecretKey — in downstreamNamespace, which is where
	// cert-manager itself is installed and so where it resolves a
	// ClusterIssuer's Secret references.
	eabSecretName = "kontinuum-acme-eab"
	eabSecretKey  = "hmacKey"
	// dns01SecretName holds the DNS provider credential cert-manager's
	// DNS-01 solver reads, in downstreamNamespace for the same reason as
	// eabSecretName. Only present while a frontend host is set.
	dns01SecretName = "kontinuum-dns01-credentials"
	gatewayName     = "kontinuum"
	certificateName = "kontinuum"
	httpRouteName   = "kontinuum"
	certSecretName  = "kontinuum-tls"
	dnsEndpointName = "kontinuum"

	httpListenerName  = "http"
	httpsListenerName = "https"
	// frontendListenerName is the HTTPS listener serving the fleet's
	// frontend host — see v1alpha2.KontinuumFrontendConfigStatus.
	frontendListenerName = "https-frontend"
	httpPort             = 80
	httpsPort            = 443

	// dnsRecordTypeA, dnsRecordTypeAAAA, and dnsRecordTypeCNAME are the
	// external-dns record types ensureDNSEndpoint chooses between, based on
	// gatewayAddress' own resolved address shape — an IPv4 literal, an IPv6
	// literal, or a Hostname-type Gateway address respectively.
	dnsRecordTypeA     = "A"
	dnsRecordTypeAAAA  = "AAAA"
	dnsRecordTypeCNAME = "CNAME"
)

// installNetwork ensures the ClusterIssuer, Gateway, Certificate, and
// HTTPRoute exposing this zone's own kontinuum-server at hostname — and, when
// the fleet has a frontend host (see v1alpha2.KontinuumFrontendConfigStatus),
// at that host too — and reports whether the Certificate has itself finished
// issuing — see certificateReady's own doc for why that, not just object
// creation, is what Installed waits on.
func (r *Reconciler) installNetwork(
	ctx context.Context, downstream client.Client, hostname string,
) (bool, error) {
	frontendHost := r.HubConfig.Server.Frontend.Host
	hostnames := []string{hostname}

	if frontendHost != "" {
		hostnames = append(hostnames, frontendHost)
	}

	solver, err := ensureChallengeSolver(ctx, downstream, r.HubConfig.Server.DNS, frontendHost)
	if err != nil {
		return false, err
	}

	err = ensureExternalAccountBindingSecret(ctx, downstream, r.HubConfig.ACME.ExternalAccountBinding)
	if err != nil {
		return false, err
	}

	err = ensureClusterIssuer(ctx, downstream, clusterIssuerName, r.HubConfig.ACME, solver)
	if err != nil {
		return false, err
	}

	err = ensureGateway(ctx, downstream, downstreamNamespace, gatewayName, hostname, frontendHost, certSecretName)
	if err != nil {
		return false, err
	}

	err = ensureCertificate(ctx, downstream, downstreamNamespace, certificateName, hostnames, certSecretName,
		clusterIssuerName)
	if err != nil {
		return false, err
	}

	err = ensureHTTPRoute(ctx, downstream, downstreamNamespace, httpRouteName, hostnames, gatewayName, serviceName)
	if err != nil {
		return false, err
	}

	return certificateReady(ctx, downstream, downstreamNamespace, certificateName)
}

// ensureChallengeSolver returns the ACME challenge solver this zone's
// ClusterIssuer orders certificates with. The challenge is never
// configured, it follows from frontendHost:
//
//   - Without one, HTTP-01 through the Gateway API solver, attached to
//     gatewayName's own HTTP listener (see ensureGateway) — cert-manager
//     creates an ephemeral HTTPRoute there for each challenge.
//   - With one, DNS-01 through dns's provider (see addon.DNS01Solver),
//     because HTTP-01 fails at random for a name several zones serve: the
//     CA may validate against a zone other than the one that ordered. The
//     provider credential external-dns already uses is copied into
//     dns01SecretName for the solver to read, so operators supply one set
//     of DNS credentials.
//
// dns01SecretName is deleted whenever HTTP-01 is used, so a credential
// doesn't linger on a zone once the frontend host is unset.
func ensureChallengeSolver(
	ctx context.Context, downstream client.Client, dns v1alpha2.KontinuumDNSConfigStatus, frontendHost string,
) (acmev1.ACMEChallengeSolver, error) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: dns01SecretName, Namespace: downstreamNamespace}}

	if frontendHost == "" {
		err := client.IgnoreNotFound(downstream.Delete(ctx, secret))
		if err != nil {
			return acmev1.ACMEChallengeSolver{}, fmt.Errorf("failed to delete %q secret: %w", dns01SecretName, err)
		}

		return acmev1.ACMEChallengeSolver{
			HTTP01: &acmev1.ACMEChallengeSolverHTTP01{
				GatewayHTTPRoute: &acmev1.ACMEChallengeSolverHTTP01GatewayHTTPRoute{
					ParentRefs: []gatewayv1.ParentReference{{Name: gatewayv1.ObjectName(gatewayName)}},
				},
			},
		}, nil
	}

	keys, err := addon.ParseDNSCredentialKeys(dns.Credential)
	if err != nil {
		return acmev1.ACMEChallengeSolver{}, fmt.Errorf("failed to parse dns credential: %w", err)
	}

	solver, err := addon.DNS01Solver(dns.Provider, dns01SecretName, keys)
	if err != nil {
		return acmev1.ACMEChallengeSolver{}, fmt.Errorf("failed to build dns-01 solver: %w", err)
	}

	_, err = controllerutil.CreateOrUpdate(ctx, downstream, secret, func() error {
		secret.Data = make(map[string][]byte, len(keys))
		for key, value := range keys {
			secret.Data[key] = []byte(value)
		}

		return nil
	})
	if err != nil {
		return acmev1.ACMEChallengeSolver{}, fmt.Errorf("failed to ensure %q secret: %w", dns01SecretName, err)
	}

	return acmev1.ACMEChallengeSolver{DNS01: solver}, nil
}

// ensureClusterIssuer upserts a cluster-scoped ACME ClusterIssuer solving
// challenges with solver — see ensureChallengeSolver.
func ensureClusterIssuer(
	ctx context.Context, downstream client.Client, name string, acme v1alpha2.KontinuumACMEConfigStatus,
	solver acmev1.ACMEChallengeSolver,
) error {
	skipTLSVerify, err := strconv.ParseBool(acme.InsecureSkipVerify)
	if err != nil {
		return fmt.Errorf("failed to parse KONTINUUM_ACME_INSECURE_SKIP_VERIFY: %w", err)
	}

	issuer := &certmanagerv1.ClusterIssuer{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: certmanagerv1.IssuerSpec{
			IssuerConfig: certmanagerv1.IssuerConfig{
				ACME: &acmev1.ACMEIssuer{
					Email:         acme.Email,
					Server:        acme.Server,
					SkipTLSVerify: skipTLSVerify,
					// Set for CAs that require binding to an existing
					// account — see ensureExternalAccountBindingSecret.
					ExternalAccountBinding: externalAccountBinding(acme.ExternalAccountBinding),
					PrivateKey: cmmeta.SecretKeySelector{
						LocalObjectReference: cmmeta.LocalObjectReference{Name: name + "-account-key"},
					},
					Solvers: []acmev1.ACMEChallengeSolver{solver},
				},
			},
		},
	}

	err = downstream.Create(ctx, issuer)
	if apierrors.IsAlreadyExists(err) {
		var existing certmanagerv1.ClusterIssuer

		err = downstream.Get(ctx, client.ObjectKeyFromObject(issuer), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q clusterissuer: %w", name, err)
		}

		existing.Spec = issuer.Spec

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q clusterissuer: %w", name, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q clusterissuer: %w", name, err)
	}

	return nil
}

// externalAccountBinding builds the ClusterIssuer's external account
// binding from binding, pointing at the Secret
// ensureExternalAccountBindingSecret maintains — nil when no binding is
// configured.
func externalAccountBinding(
	binding v1alpha2.KontinuumACMEExternalAccountBindingConfigStatus,
) *acmev1.ACMEExternalAccountBinding {
	if binding.KeyID == "" {
		return nil
	}

	return &acmev1.ACMEExternalAccountBinding{
		KeyID: binding.KeyID,
		Key: cmmeta.SecretKeySelector{
			LocalObjectReference: cmmeta.LocalObjectReference{Name: eabSecretName}, Key: eabSecretKey,
		},
	}
}

// ensureExternalAccountBindingSecret upserts the Secret holding the ACME
// external account binding's MAC key that ensureClusterIssuer references,
// or deletes it when no binding is configured, so a key removed from the
// hub's config doesn't linger on every zone.
func ensureExternalAccountBindingSecret(
	ctx context.Context, downstream client.Client, binding v1alpha2.KontinuumACMEExternalAccountBindingConfigStatus,
) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: eabSecretName, Namespace: downstreamNamespace}}

	if binding.KeyID == "" {
		err := downstream.Delete(ctx, secret)
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete %q secret: %w", eabSecretName, err)
		}

		return nil
	}

	_, err := controllerutil.CreateOrUpdate(ctx, downstream, secret, func() error {
		secret.Data = map[string][]byte{eabSecretKey: []byte(binding.HMACKey)}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to ensure %q secret: %w", eabSecretName, err)
	}

	return nil
}

// ensureGateway upserts a Gateway with a plain HTTP listener (no hostname
// restriction) for cert-manager's HTTP-01 solver to attach its ephemeral
// challenge HTTPRoute to, and an HTTPS listener per hostname this zone
// serves — its own, and frontendHost when set — each terminating TLS from
// certSecretName, the Secret ensureCertificate's Certificate populates.
func ensureGateway(
	ctx context.Context, downstream client.Client, namespace, name, hostname, frontendHost, certSecretName string,
) error {
	listeners := []gatewayv1.Listener{
		{
			Name:     httpListenerName,
			Port:     httpPort,
			Protocol: gatewayv1.HTTPProtocolType,
		},
		httpsListener(httpsListenerName, hostname, certSecretName),
	}

	if frontendHost != "" {
		listeners = append(listeners, httpsListener(frontendListenerName, frontendHost, certSecretName))
	}

	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: gatewayClassName,
			Listeners:        listeners,
		},
	}

	err := downstream.Create(ctx, gateway)
	if apierrors.IsAlreadyExists(err) {
		var existing gatewayv1.Gateway

		err = downstream.Get(ctx, client.ObjectKeyFromObject(gateway), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q gateway: %w", name, err)
		}

		existing.Spec = gateway.Spec

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q gateway: %w", name, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q gateway: %w", name, err)
	}

	return nil
}

// httpsListener builds an HTTPS listener named name for hostname,
// terminating TLS from certSecretName.
func httpsListener(name, hostname, certSecretName string) gatewayv1.Listener {
	return gatewayv1.Listener{
		Name:     gatewayv1.SectionName(name),
		Port:     httpsPort,
		Protocol: gatewayv1.HTTPSProtocolType,
		Hostname: new(gatewayv1.Hostname(hostname)),
		TLS: &gatewayv1.GatewayTLSConfig{
			Mode: new(gatewayv1.TLSModeTerminate),
			CertificateRefs: []gatewayv1.SecretObjectReference{
				{Name: gatewayv1.ObjectName(certSecretName)},
			},
		},
	}
}

// ensureCertificate upserts one Certificate covering every one of
// hostnames — the zone's own, plus the frontend host when set — issued by
// issuerName (a ClusterIssuer), storing its key pair in secretName — the
// same Secret every one of ensureGateway's HTTPS listeners references.
// Each zone keeps a certificate of its own rather than sharing one
// fleet-wide: every zone's name set differs, so the CA's duplicate
// certificate limit never applies and no Secret is copied between zones.
func ensureCertificate(
	ctx context.Context, downstream client.Client, namespace, name string, hostnames []string,
	secretName, issuerName string,
) error {
	cert := &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: certmanagerv1.CertificateSpec{
			DNSNames:   hostnames,
			SecretName: secretName,
			IssuerRef: cmmeta.ObjectReference{
				Name:  issuerName,
				Kind:  "ClusterIssuer",
				Group: certmanagerv1.SchemeGroupVersion.Group,
			},
		},
	}

	err := downstream.Create(ctx, cert)
	if apierrors.IsAlreadyExists(err) {
		var existing certmanagerv1.Certificate

		err = downstream.Get(ctx, client.ObjectKeyFromObject(cert), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q certificate: %w", name, err)
		}

		existing.Spec = cert.Spec

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q certificate: %w", name, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q certificate: %w", name, err)
	}

	return nil
}

// routeAttachments returns the HTTPRoute's parent references and hostnames
// for hostnames: the zone's own hostname on gatewayName's HTTPS listener,
// and the frontend host, when present, on its own listener.
func routeAttachments(hostnames []string, gatewayName string) ([]gatewayv1.ParentReference, []gatewayv1.Hostname) {
	parentRefs := []gatewayv1.ParentReference{{
		Name:        gatewayv1.ObjectName(gatewayName),
		SectionName: new(gatewayv1.SectionName(httpsListenerName)),
	}}

	routeHostnames := make([]gatewayv1.Hostname, 0, len(hostnames))
	for index, hostname := range hostnames {
		routeHostnames = append(routeHostnames, gatewayv1.Hostname(hostname))

		if index > 0 {
			parentRefs = append(parentRefs, gatewayv1.ParentReference{
				Name:        gatewayv1.ObjectName(gatewayName),
				SectionName: new(gatewayv1.SectionName(frontendListenerName)),
			})
		}
	}

	return parentRefs, routeHostnames
}

// ensureHTTPRoute upserts an HTTPRoute attaching to gatewayName's HTTPS
// listeners and routing every one of hostnames to serviceName. Redirecting
// the UI from a zone's own hostname to the frontend host is
// kontinuum-server's own concern, not this route's — see
// pkg/ui.FrontendRedirect.
func ensureHTTPRoute(
	ctx context.Context, downstream client.Client, namespace, name string, hostnames []string,
	gatewayName, serviceName string,
) error {
	parentRefs, routeHostnames := routeAttachments(hostnames, gatewayName)

	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: parentRefs},
			Hostnames:       routeHostnames,
			Rules: []gatewayv1.HTTPRouteRule{{
				BackendRefs: []gatewayv1.HTTPBackendRef{{
					BackendRef: gatewayv1.BackendRef{
						BackendObjectReference: gatewayv1.BackendObjectReference{
							Name: gatewayv1.ObjectName(serviceName),
							Port: new(gatewayv1.PortNumber(servicePort)),
						},
					},
				}},
			}},
		},
	}

	err := downstream.Create(ctx, route)
	if apierrors.IsAlreadyExists(err) {
		var existing gatewayv1.HTTPRoute

		err = downstream.Get(ctx, client.ObjectKeyFromObject(route), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q httproute: %w", name, err)
		}

		existing.Spec = route.Spec

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q httproute: %w", name, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q httproute: %w", name, err)
	}

	return nil
}

// IgnoreNotFoundOrNoMatch tolerates both NotFound (the object itself is
// already gone) and meta.IsNoMatchError (the object's whole Kind isn't even
// registered on the downstream cluster, e.g. the gateway-api/cert-manager
// CRDs that back these four types were never actually installed there —
// addon install failing, or genuinely never getting that far before the
// Zone was deleted, are both real, observed cases, not hypothetical). The
// second case matters distinctly from NotFound: a plain 404 still means
// controller-runtime's RESTMapper *found* the resource type and asked the
// server, which said "no such object"; a NoMatchError means the client
// couldn't even resolve the type to ask about, so there is categorically
// nothing installed of this kind to have deleted in the first place —
// exactly as safe to treat as "already gone" as NotFound itself, rather
// than a real failure worth retrying until Zone's own TeardownTimeout.
func IgnoreNotFoundOrNoMatch(err error) error {
	if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
		return nil
	}

	return err
}

// deleteHTTPRoute deletes the HTTPRoute ensureHTTPRoute upserts, tolerating
// NotFound and a not-installed Gateway API — see teardown.go's own doc for
// why every deleteX helper is idempotent the same way its ensureX
// counterpart already is, and IgnoreNotFoundOrNoMatch's own doc for why a
// missing Kind is tolerated the same way as a missing object.
func deleteHTTPRoute(ctx context.Context, downstream client.Client, namespace, name string) error {
	route := &gatewayv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}

	err := IgnoreNotFoundOrNoMatch(downstream.Delete(ctx, route))
	if err != nil {
		return fmt.Errorf("failed to delete %q httproute: %w", name, err)
	}

	return nil
}

// deleteCertificate deletes the Certificate ensureCertificate upserts,
// tolerating NotFound and a not-installed cert-manager (see
// IgnoreNotFoundOrNoMatch's own doc).
func deleteCertificate(ctx context.Context, downstream client.Client, namespace, name string) error {
	cert := &certmanagerv1.Certificate{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}

	err := IgnoreNotFoundOrNoMatch(downstream.Delete(ctx, cert))
	if err != nil {
		return fmt.Errorf("failed to delete %q certificate: %w", name, err)
	}

	return nil
}

// deleteGateway deletes the Gateway ensureGateway upserts, tolerating
// NotFound and a not-installed Gateway API (see IgnoreNotFoundOrNoMatch's
// own doc).
func deleteGateway(ctx context.Context, downstream client.Client, namespace, name string) error {
	gateway := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}

	err := IgnoreNotFoundOrNoMatch(downstream.Delete(ctx, gateway))
	if err != nil {
		return fmt.Errorf("failed to delete %q gateway: %w", name, err)
	}

	return nil
}

// deleteClusterIssuer deletes the cluster-scoped ClusterIssuer
// ensureClusterIssuer upserts, tolerating NotFound and a not-installed
// cert-manager (see IgnoreNotFoundOrNoMatch's own doc).
func deleteClusterIssuer(ctx context.Context, downstream client.Client, name string) error {
	issuer := &certmanagerv1.ClusterIssuer{ObjectMeta: metav1.ObjectMeta{Name: name}}

	err := IgnoreNotFoundOrNoMatch(downstream.Delete(ctx, issuer))
	if err != nil {
		return fmt.Errorf("failed to delete %q clusterissuer: %w", name, err)
	}

	return nil
}

// certificateReady reports whether name's Certificate has itself reported
// Ready — cert-manager's own real signal that issuance (including waiting
// on the ACME HTTP-01 challenge to be validated) has actually succeeded,
// not just that the Certificate object exists. A NotFound Certificate
// (shouldn't happen — ensureCertificate always creates it first) is
// reported as not-ready, not an error, so a transient read-after-write gap
// just requeues.
func certificateReady(ctx context.Context, downstream client.Client, namespace, name string) (bool, error) {
	var cert certmanagerv1.Certificate

	err := downstream.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, &cert)
	if apierrors.IsNotFound(err) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("failed to fetch %q certificate: %w", name, err)
	}

	for _, cond := range cert.Status.Conditions {
		if cond.Type == certmanagerv1.CertificateConditionReady {
			return cond.Status == cmmeta.ConditionTrue, nil
		}
	}

	return false, nil
}

// gatewayAddress reads name's Gateway status.addresses[0] on downstream and
// reports the DNS record shape ensureDNSEndpoint needs to point at it: the
// address value itself, and which record type it needs (dnsRecordTypeCNAME
// for a Hostname-type address, dnsRecordTypeA/AAAA for an IPv4/IPv6 literal
// — see this package's own record type constants). ready is false, not an
// error, both when the Gateway itself doesn't exist yet and when it exists
// but its own LoadBalancer implementation (e.g. Cilium's) hasn't assigned it
// an address yet — either way there's nothing to point a DNS record at, and
// the caller just requeues.
func gatewayAddress(
	ctx context.Context, downstream client.Client, namespace, name string,
) (string, string, bool, error) {
	var gateway gatewayv1.Gateway

	err := downstream.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, &gateway)
	if apierrors.IsNotFound(err) {
		return "", "", false, nil
	}

	if err != nil {
		return "", "", false, fmt.Errorf("failed to fetch %q gateway: %w", name, err)
	}

	if len(gateway.Status.Addresses) == 0 {
		return "", "", false, nil
	}

	address := gateway.Status.Addresses[0]
	if address.Type != nil && *address.Type == gatewayv1.HostnameAddressType {
		return address.Value, dnsRecordTypeCNAME, true, nil
	}

	if ip := net.ParseIP(address.Value); ip != nil && ip.To4() == nil {
		return address.Value, dnsRecordTypeAAAA, true, nil
	}

	return address.Value, dnsRecordTypeA, true, nil
}

// ensureDNSEndpoint upserts a DNSEndpoint requesting a single record —
// hostname pointing at target, as recordType (see gatewayAddress) — for
// whatever external-dns instance is watching this cluster's own crd source
// to pick up and reconcile against its own provider (see
// pkg/domain/addon/values/external-dns.yaml's own doc for why kontinuum
// itself never talks to a DNS provider directly).
func ensureDNSEndpoint(
	ctx context.Context, downstream client.Client, namespace, name, hostname, target, recordType string,
) error {
	endpoint := &DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: DNSEndpointSpec{
			Endpoints: []Endpoint{{DNSName: hostname, Targets: []string{target}, RecordType: recordType}},
		},
	}

	err := downstream.Create(ctx, endpoint)
	if apierrors.IsAlreadyExists(err) {
		var existing DNSEndpoint

		err = downstream.Get(ctx, client.ObjectKeyFromObject(endpoint), &existing)
		if err != nil {
			return fmt.Errorf("failed to fetch existing %q dnsendpoint: %w", name, err)
		}

		existing.Spec = endpoint.Spec

		err = downstream.Update(ctx, &existing)
		if err != nil {
			return fmt.Errorf("failed to update %q dnsendpoint: %w", name, err)
		}

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to create %q dnsendpoint: %w", name, err)
	}

	return nil
}

// deleteDNSEndpoint deletes the DNSEndpoint ensureDNSEndpoint upserts,
// tolerating NotFound.
func deleteDNSEndpoint(ctx context.Context, downstream client.Client, namespace, name string) error {
	endpoint := &DNSEndpoint{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}

	err := client.IgnoreNotFound(downstream.Delete(ctx, endpoint))
	if err != nil {
		return fmt.Errorf("failed to delete %q dnsendpoint: %w", name, err)
	}

	return nil
}

// reconcileDNS sets DNSRecordConditionType on zoneObj, in memory — the
// caller (reconcileInstall) persists it together with InstalledConditionType
// in a single Status().Update, same as installWorkload/installNetwork's own
// errors already share that call. Only ever reached once
// finishInstallWithDomain's own DNS config check has already
// confirmed a DNS provider and credential are configured — see that
// function's own doc — so unlike before that gate existed, there's no
// "credentials not configured" outcome to record here. Returns a non-nil
// error only for a real downstream API failure, so it's handled exactly
// like an installWorkload/installNetwork failure (Installed flips
// False/InstallFailed, forcing a retry) — every other outcome here is
// expected, not a failure, and is recorded on the condition directly
// instead:
//
//   - hostname's own downstream Gateway has no address yet: False/
//     reasonWaitingForGatewayAddress. Installed already can't reach True
//     yet either in this case — the same ACME HTTP-01 challenge that gates
//     certReady needs the hostname to resolve, which needs this very
//     DNSEndpoint to exist and propagate first — so no separate retry
//     wiring is needed here: reconcileInstall's own certReady-driven
//     requeue already covers it.
//   - DNSEndpoint upserted successfully: True/reasonDNSRecordCreated.
//
// The returned bool is whatever the meta.SetStatusCondition call below
// reported (see setInstalledCondition's own otherChanged doc for why the
// caller needs it): DNSRecordConditionType is written here, not through
// its own persistStatus call, so without threading this back, a pass where
// DNSRecordConditionType changes but InstalledConditionType's own
// Status/Reason/Message happen not to would never actually get persisted.
func (r *Reconciler) reconcileDNS(
	ctx context.Context, zoneObj *v1alpha2.Zone, downstream client.Client, hostname string,
) (bool, error) {
	address, recordType, ready, err := gatewayAddress(ctx, downstream, downstreamNamespace, gatewayName)
	if err != nil {
		return false, err
	}

	if !ready {
		changed := meta.SetStatusCondition(&zoneObj.Status.Conditions, metav1.Condition{
			Type: DNSRecordConditionType, Status: metav1.ConditionFalse, Reason: reasonWaitingForGatewayAddress,
			Message: "waiting for the downstream gateway to be assigned an address",
		})

		return changed, nil
	}

	err = ensureDNSEndpoint(ctx, downstream, downstreamNamespace, dnsEndpointName, hostname, address, recordType)
	if err != nil {
		return false, err
	}

	changed := meta.SetStatusCondition(&zoneObj.Status.Conditions, metav1.Condition{
		Type: DNSRecordConditionType, Status: metav1.ConditionTrue, Reason: reasonDNSRecordCreated,
		Message: "created dns record for " + hostname,
	})

	return changed, nil
}
