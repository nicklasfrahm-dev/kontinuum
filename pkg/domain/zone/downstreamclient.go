package zone

import (
	"context"
	"errors"
	"fmt"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// kubeconfigSecretKey is the key a TalosCluster's own kubeconfig is stored
// under in the Secret its status.secretRef points to — must match
// pkg/domain/taloscluster/secrets.go's own kubeconfigKey. Duplicated rather
// than imported, for the same import-cycle-avoidance reason
// pkg/domain/addon's own identical constant already is: taloscluster
// already imports this package's sibling (addon), and would cycle back
// through zone too if zone were imported the other way.
const kubeconfigSecretKey = "kubeconfig"

// errKubeconfigNotStored is a static sentinel — err113 flags a dynamically
// constructed errors.New/fmt.Errorf call without a wrapped static error.
var errKubeconfigNotStored = errors.New("secret has no stored kubeconfig yet")

// loadClusterKubeconfig fetches cluster's own stored kubeconfig — mirrors
// pkg/domain/addon's identical helper (see downstreamclient.go's own doc
// for why it's duplicated rather than imported).
func loadClusterKubeconfig(
	ctx context.Context, hubClient client.Client, cluster *v1alpha2.TalosCluster,
) ([]byte, error) {
	ref := cluster.Status.SecretRef

	var secret corev1.Secret

	err := hubClient.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}, &secret)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %q secret to load kubeconfig: %w", ref.Name, err)
	}

	kubeconfig, ok := secret.Data[kubeconfigSecretKey]
	if !ok {
		return nil, fmt.Errorf("%q %w", ref.Name, errKubeconfigNotStored)
	}

	return kubeconfig, nil
}

// DownstreamClientBuilder builds a typed client.Client against a zone's own
// cluster from its stored kubeconfig — this package's seam for injecting a
// fake in tests, the same role addon.Installer plays for Helm installs.
type DownstreamClientBuilder interface {
	Build(kubeconfig []byte) (client.Client, error)
}

// restDownstreamClientBuilder is DownstreamClientBuilder's production
// implementation.
type restDownstreamClientBuilder struct {
	scheme *runtime.Scheme
}

// NewDownstreamClientBuilder returns the production DownstreamClientBuilder,
// which builds a real client against a real cluster. DownstreamClientBuilder
// is this package's own seam for injecting a fake in tests.
//
//nolint:ireturn // see doc above
func NewDownstreamClientBuilder() DownstreamClientBuilder {
	return restDownstreamClientBuilder{scheme: downstreamScheme()}
}

// Build implements DownstreamClientBuilder.
func (b restDownstreamClientBuilder) Build(kubeconfig []byte) (client.Client, error) {
	restCfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build rest config from kubeconfig: %w", err)
	}

	downstream, err := client.New(restCfg, client.Options{Scheme: b.scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to build downstream client: %w", err)
	}

	return downstream, nil
}

// downstreamScheme registers every Kind Zone creates on a downstream
// cluster: core/v1 (Namespace/Secret/ConfigMap/Service), apps/v1
// (Deployment), gateway.networking.k8s.io/v1 (Gateway/HTTPRoute),
// cert-manager.io/v1 (Certificate/ClusterIssuer), and externaldns.k8s.io/
// v1alpha1 (DNSEndpoint — see dnsendpoint_types.go). This is deliberately
// separate from the hub's own scheme (see pkg/cli/serve.go's buildServer):
// the hub apiserver never serves any of these types itself — only a
// zone's own downstream cluster does, once its own addons install their
// CRDs (see pkg/domain/addon/values/cert-manager.yaml,
// gateway-api-crds.yaml, and external-dns.yaml).
func downstreamScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = gatewayv1.Install(scheme)
	_ = certmanagerv1.AddToScheme(scheme)
	AddExternalDNSToScheme(scheme)

	return scheme
}
