package addon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/registry"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// kubectlApplyFieldManager identifies this controller's own writes for
// server-side apply's field-ownership tracking — see installViaKubectlApply.
const kubectlApplyFieldManager = "kontinuum"

// yamlDecoderBufferSize is NewYAMLOrJSONDecoder's own read-buffer size —
// large enough for any single manifest document this package renders.
const yamlDecoderBufferSize = 4096

// installViaKubectlApply renders req's chart client-side (the
// `helm template` equivalent) and applies the resulting manifests
// directly via real Kubernetes server-side apply (the `kubectl apply -f -`
// equivalent) — no Helm release record is created, unlike installViaHelm,
// so the same manifests can later be adopted by a GitOps tool like ArgoCD
// without it needing to understand a pre-existing Helm release.
func (helmInstaller) installViaKubectlApply(ctx context.Context, kubeconfig []byte, req InstallRequest) error {
	manifest, err := renderChart(kubeconfig, req)
	if err != nil {
		return err
	}

	err = applyManifest(ctx, kubeconfig, req.Namespace, manifest)
	if err != nil {
		return fmt.Errorf("failed to apply rendered %q manifests: %w", req.ReleaseName, err)
	}

	return nil
}

// renderChart loads req's chart and renders it client-side (ClientOnly —
// the `helm template` equivalent, no create/apply/upgrade against the
// cluster) — but does look up the target cluster's own real Kubernetes
// version first (a single read-only discovery call) and passes it as
// KubeVersion: ClientOnly rendering otherwise falls back to Helm's own
// hardcoded chartutil.DefaultCapabilities.KubeVersion, which is old
// enough that a chart declaring a recent minimum kubeVersion (e.g.
// Cilium 1.20's own Chart.yaml) fails Helm's own compatibility check
// before a single template even renders. Hooks are deliberately excluded
// from the result: a GitOps-adoptable manifest set shouldn't include
// Helm hook Jobs meant to run once during an imperative install.
func renderChart(kubeconfig []byte, req InstallRequest) (string, error) {
	kubeVersion, err := targetKubeVersion(kubeconfig)
	if err != nil {
		return "", err
	}

	actionConfig := new(action.Configuration)

	err = actionConfig.Init(nil, req.Namespace, "memory", func(string, ...any) {})
	if err != nil {
		return "", fmt.Errorf("failed to init helm action configuration for %q: %w", req.ReleaseName, err)
	}

	actionConfig.RegistryClient, err = registry.NewClient()
	if err != nil {
		return "", fmt.Errorf("failed to create helm registry client: %w", err)
	}

	chrt, err := loadChart(actionConfig, req.ChartName, req.RepoURL, req.Version)
	if err != nil {
		return "", err
	}

	installAction := action.NewInstall(actionConfig)
	installAction.DryRun = true
	installAction.DryRunOption = "true"
	installAction.ClientOnly = true
	installAction.KubeVersion = kubeVersion
	installAction.ReleaseName = req.ReleaseName
	installAction.Replace = true
	installAction.Namespace = req.Namespace
	installAction.IncludeCRDs = true

	rel, err := installAction.Run(chrt, req.Values)
	if err != nil {
		return "", fmt.Errorf("failed to render %q: %w", req.ReleaseName, err)
	}

	return rel.Manifest, nil
}

// targetKubeVersion queries kubeconfig's own cluster for its real
// Kubernetes version — a single read-only discovery call, not a create/
// apply/upgrade — for renderChart's own ClientOnly render to pass as
// KubeVersion (see that function's own doc for why).
func targetKubeVersion(kubeconfig []byte) (*chartutil.KubeVersion, error) {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build rest config from kubeconfig: %w", err)
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build discovery client: %w", err)
	}

	serverVersion, err := discoveryClient.ServerVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to query cluster kubernetes version: %w", err)
	}

	return &chartutil.KubeVersion{
		Version: serverVersion.GitVersion, Major: serverVersion.Major, Minor: serverVersion.Minor,
	}, nil
}

// applyManifest splits manifest into individual documents and applies
// each via server-side apply — the `kubectl apply -f -` equivalent.
// Real SSA needs no prior-state bookkeeping (unlike a three-way strategic-
// merge patch, which needs a caller-supplied "original" state this
// no-Helm-release path deliberately has none of): the server computes the
// merge itself from each object's own field-manager history.
func applyManifest(ctx context.Context, kubeconfig []byte, defaultNamespace, manifest string) error {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to build rest config from kubeconfig: %w", err)
	}

	restMapper, dynamicClient, err := buildApplyClients(restConfig)
	if err != nil {
		return err
	}

	decoder := k8syaml.NewYAMLOrJSONDecoder(strings.NewReader(manifest), yamlDecoderBufferSize)

	for {
		obj := &unstructured.Unstructured{}

		err := decoder.Decode(obj)
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("failed to decode manifest document: %w", err)
		}

		if len(obj.Object) == 0 {
			continue // empty document between `---` separators
		}

		err = applyObject(ctx, restMapper, dynamicClient, defaultNamespace, obj)
		if err != nil {
			return err
		}
	}
}

// buildApplyClients builds the RESTMapper and dynamic client applyObject
// needs to resolve and apply arbitrary object kinds — same
// apiutil.NewDynamicRESTMapper pattern pkg/crd/crd.go's own
// waitForDiscoverable already uses.
//
//nolint:ireturn // both are the standard client-go/controller-runtime seams for this, not a leak
func buildApplyClients(restConfig *rest.Config) (meta.RESTMapper, dynamic.Interface, error) {
	httpClient, err := rest.HTTPClientFor(restConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build discovery http client: %w", err)
	}

	restMapper, err := apiutil.NewDynamicRESTMapper(restConfig, httpClient)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build rest mapper: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build dynamic client: %w", err)
	}

	return restMapper, dynamicClient, nil
}

// applyObject resolves obj's GVK to the right namespaced/cluster-scoped
// resource client and applies it via server-side apply, defaulting a
// namespaced object's empty metadata.namespace to defaultNamespace —
// chart output commonly omits it, relying on the release namespace.
func applyObject(
	ctx context.Context, restMapper meta.RESTMapper, dynamicClient dynamic.Interface,
	defaultNamespace string, obj *unstructured.Unstructured,
) error {
	gvk := obj.GroupVersionKind()

	mapping, err := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return fmt.Errorf("failed to resolve rest mapping for %s: %w", gvk, err)
	}

	var resourceClient dynamic.ResourceInterface

	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		namespace := obj.GetNamespace()
		if namespace == "" {
			namespace = defaultNamespace
		}

		resourceClient = dynamicClient.Resource(mapping.Resource).Namespace(namespace)
	} else {
		resourceClient = dynamicClient.Resource(mapping.Resource)
	}

	data, err := obj.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal %s %q: %w", gvk, obj.GetName(), err)
	}

	force := true

	_, err = resourceClient.Patch(ctx, obj.GetName(), types.ApplyPatchType, data,
		metav1.PatchOptions{FieldManager: kubectlApplyFieldManager, Force: &force})
	if err != nil {
		return fmt.Errorf("failed to apply %s %q: %w", gvk, obj.GetName(), err)
	}

	return nil
}
