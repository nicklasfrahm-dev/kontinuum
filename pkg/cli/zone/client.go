package zone

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// buildHubClient resolves a kubeconfig — from kubeconfigPath when set,
// otherwise $KUBECONFIG or ~/.kube/config (clientcmd's own default loading
// rules, the same ones pkg/cli/config's own "config import" resolves
// against) — and builds a client.Client against whichever cluster
// contextOverride names, or the kubeconfig's own current-context when
// contextOverride is empty. This is new plumbing: unlike pkg/cli/config,
// which only ever reads/writes a kubeconfig file, "zone add" needs a live
// client against the hub apiserver that file points at.
func buildHubClient(kubeconfigPath, contextOverride string) (client.Client, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		loadingRules.ExplicitPath = kubeconfigPath
	}

	overrides := &clientcmd.ConfigOverrides{}
	if contextOverride != "" {
		overrides.CurrentContext = contextOverride
	}

	restCfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve hub kubeconfig: %w", err)
	}

	scheme := runtime.NewScheme()

	err = v1alpha2.AddToScheme(scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to register kontinuum.sh/v1alpha2 scheme: %w", err)
	}

	// corev1 is needed too — zonedomain.Add's own ensureNamespace creates a
	// plain corev1.Namespace as part of the fan-out.
	err = corev1.AddToScheme(scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to register core/v1 scheme: %w", err)
	}

	hubClient, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to build hub client: %w", err)
	}

	return hubClient, nil
}
