package zone

import (
	"context"
	"fmt"

	acmev1 "github.com/cert-manager/cert-manager/pkg/apis/acme/v1"
	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
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
	gatewayName       = "kontinuum"
	certificateName   = "kontinuum"
	httpRouteName     = "kontinuum"
	certSecretName    = "kontinuum-tls"

	httpListenerName  = "http"
	httpsListenerName = "https"
	httpPort          = 80
	httpsPort         = 443
)

// installNetwork ensures the ClusterIssuer, Gateway, Certificate, and
// HTTPRoute exposing this zone's own kontinuum-server at hostname, and
// reports whether the Certificate has itself finished issuing — see
// certificateReady's own doc for why that, not just object creation, is
// what Installed waits on.
func (r *Reconciler) installNetwork(
	ctx context.Context, downstream client.Client, hostname string,
) (bool, error) {
	err := ensureClusterIssuer(ctx, downstream, clusterIssuerName, r.ACMEEmail, r.ACMEServer)
	if err != nil {
		return false, err
	}

	err = ensureGateway(ctx, downstream, downstreamNamespace, gatewayName, hostname, certSecretName)
	if err != nil {
		return false, err
	}

	err = ensureCertificate(ctx, downstream, downstreamNamespace, certificateName, hostname, certSecretName,
		clusterIssuerName)
	if err != nil {
		return false, err
	}

	err = ensureHTTPRoute(ctx, downstream, downstreamNamespace, httpRouteName, hostname, gatewayName, serviceName)
	if err != nil {
		return false, err
	}

	return certificateReady(ctx, downstream, downstreamNamespace, certificateName)
}

// ensureClusterIssuer upserts a cluster-scoped ACME ClusterIssuer solving
// HTTP-01 challenges via the Gateway API solver, attached to gatewayName's
// own HTTP listener (see ensureGateway) — cert-manager creates an ephemeral
// HTTPRoute there for each challenge.
func ensureClusterIssuer(ctx context.Context, downstream client.Client, name, email, server string) error {
	issuer := &certmanagerv1.ClusterIssuer{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: certmanagerv1.IssuerSpec{
			IssuerConfig: certmanagerv1.IssuerConfig{
				ACME: &acmev1.ACMEIssuer{
					Email:  email,
					Server: server,
					PrivateKey: cmmeta.SecretKeySelector{
						LocalObjectReference: cmmeta.LocalObjectReference{Name: name + "-account-key"},
					},
					Solvers: []acmev1.ACMEChallengeSolver{{
						HTTP01: &acmev1.ACMEChallengeSolverHTTP01{
							GatewayHTTPRoute: &acmev1.ACMEChallengeSolverHTTP01GatewayHTTPRoute{
								ParentRefs: []gatewayv1.ParentReference{{Name: gatewayv1.ObjectName(gatewayName)}},
							},
						},
					}},
				},
			},
		},
	}

	err := downstream.Create(ctx, issuer)
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

// ensureGateway upserts a Gateway with two listeners: a plain HTTP one (no
// hostname restriction) for cert-manager's HTTP-01 solver to attach its
// ephemeral challenge HTTPRoute to, and an HTTPS one terminating TLS from
// certSecretName — the Secret ensureCertificate's Certificate populates.
func ensureGateway(
	ctx context.Context, downstream client.Client, namespace, name, hostname, certSecretName string,
) error {
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: gatewayClassName,
			Listeners: []gatewayv1.Listener{
				{
					Name:     httpListenerName,
					Port:     httpPort,
					Protocol: gatewayv1.HTTPProtocolType,
				},
				{
					Name:     httpsListenerName,
					Port:     httpsPort,
					Protocol: gatewayv1.HTTPSProtocolType,
					Hostname: new(gatewayv1.Hostname(hostname)),
					TLS: &gatewayv1.GatewayTLSConfig{
						Mode: new(gatewayv1.TLSModeTerminate),
						CertificateRefs: []gatewayv1.SecretObjectReference{
							{Name: gatewayv1.ObjectName(certSecretName)},
						},
					},
				},
			},
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

// ensureCertificate upserts a Certificate for hostname, issued by
// issuerName (a ClusterIssuer), storing its key pair in secretName — the
// same Secret ensureGateway's HTTPS listener references.
func ensureCertificate(
	ctx context.Context, downstream client.Client, namespace, name, hostname, secretName, issuerName string,
) error {
	cert := &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: certmanagerv1.CertificateSpec{
			DNSNames:   []string{hostname},
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

// ensureHTTPRoute upserts an HTTPRoute attaching to gatewayName's HTTPS
// listener and routing hostname to serviceName.
func ensureHTTPRoute(
	ctx context.Context, downstream client.Client, namespace, name, hostname, gatewayName, serviceName string,
) error {
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: gatewayv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{
				ParentRefs: []gatewayv1.ParentReference{{
					Name:        gatewayv1.ObjectName(gatewayName),
					SectionName: new(gatewayv1.SectionName(httpsListenerName)),
				}},
			},
			Hostnames: []gatewayv1.Hostname{gatewayv1.Hostname(hostname)},
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
