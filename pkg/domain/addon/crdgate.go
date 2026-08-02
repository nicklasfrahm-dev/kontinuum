package addon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// CRDChecker reports whether every CRD an addon's own chart declares is
// Established and discoverable on the target cluster — the seam
// Reconciler gates an addon's Ready condition through, alongside
// PodProber, right after a successful install (see that interface's own
// doc for the identical rationale). crdChecker is the production
// implementation, rendering req's own chart client-side to discover its
// CRDs and then checking each against a real cluster; tests inject a
// fake to avoid both a real chart render and a real cluster dependency.
type CRDChecker interface {
	ChartCRDsReady(ctx context.Context, kubeconfig []byte, req InstallRequest) (bool, string, error)
}

// crdChecker is CRDChecker's production implementation.
type crdChecker struct{}

// NewCRDChecker returns the production CRDChecker. CRDChecker is this
// package's own seam for injecting a fake in tests.
//
//nolint:ireturn // see doc above
func NewCRDChecker() CRDChecker {
	return crdChecker{}
}

// ChartCRDsReady implements CRDChecker.
func (crdChecker) ChartCRDsReady(ctx context.Context, kubeconfig []byte, req InstallRequest) (bool, string, error) {
	refs, err := chartCRDs(kubeconfig, req)
	if err != nil {
		return false, "", err
	}

	return crdsReady(ctx, kubeconfig, refs)
}

// crdRef names one CRD an addon's own chart declares — its object name,
// plus every GVK it serves (one per spec.versions entry). Both are
// needed to check readiness: Established is a per-object status,
// discoverability is checked per served version.
type crdRef struct {
	name string
	gvks []schema.GroupVersionKind
}

// chartCRDs renders req's chart client-side (the same renderChart
// installViaKubectlApply's own install path uses, for its own different
// purpose) purely to discover which CRDs, if any, it declares —
// crdsReady then checks each is Established and discoverable on the
// target cluster before Reconcile considers this addon installed.
// Returns nil, not an error, for the overwhelmingly common case of a
// chart with no CRDs at all.
func chartCRDs(kubeconfig []byte, req InstallRequest) ([]crdRef, error) {
	manifest, err := renderChart(kubeconfig, req)
	if err != nil {
		return nil, err
	}

	return parseCRDRefs(manifest)
}

// parseCRDRefs extracts every CustomResourceDefinition object's own name
// and served GVKs out of manifest — the same per-document decode loop
// applyManifest uses, just collecting CRDs instead of applying them.
func parseCRDRefs(manifest string) ([]crdRef, error) {
	decoder := k8syaml.NewYAMLOrJSONDecoder(strings.NewReader(manifest), yamlDecoderBufferSize)

	var refs []crdRef

	for {
		obj := &unstructured.Unstructured{}

		err := decoder.Decode(obj)
		if errors.Is(err, io.EOF) {
			return refs, nil
		}

		if err != nil {
			return nil, fmt.Errorf("failed to decode manifest document: %w", err)
		}

		if len(obj.Object) == 0 || obj.GetKind() != "CustomResourceDefinition" {
			continue // empty document between `---` separators, or not a CRD
		}

		ref, err := crdRefFromUnstructured(obj)
		if err != nil {
			return nil, err
		}

		refs = append(refs, ref)
	}
}

// crdRefFromUnstructured converts obj (already known to be a
// CustomResourceDefinition) into a crdRef.
func crdRefFromUnstructured(obj *unstructured.Unstructured) (crdRef, error) {
	var crdObj apiextensionsv1.CustomResourceDefinition

	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &crdObj)
	if err != nil {
		return crdRef{}, fmt.Errorf("failed to decode crd %q: %w", obj.GetName(), err)
	}

	gvks := make([]schema.GroupVersionKind, 0, len(crdObj.Spec.Versions))
	for _, version := range crdObj.Spec.Versions {
		if !version.Served {
			continue // never discoverable — the apiserver doesn't serve it at all, checking it would wait forever
		}

		gvks = append(gvks, schema.GroupVersionKind{
			Group: crdObj.Spec.Group, Version: version.Name, Kind: crdObj.Spec.Names.Kind,
		})
	}

	return crdRef{name: crdObj.Name, gvks: gvks}, nil
}

// crdsReady reports whether every ref in refs is both Established and
// discoverable via a RESTMapper on the cluster kubeconfig reaches — the
// same two-step gate pkg/crd's own Ensure applies to kontinuum's own
// bootstrap CRDs at startup (see that package's own doc), just targeting
// an addon's remote cluster instead of the management plane's loopback
// apiserver, and checking CRDs Helm already applied rather than creating
// them here. A CRD that isn't found yet, isn't Established yet, or isn't
// discoverable yet all report not-ready with a reason — never an error,
// since all three are expected, temporary states while a chart converges.
func crdsReady(ctx context.Context, kubeconfig []byte, refs []crdRef) (bool, string, error) {
	if len(refs) == 0 {
		return true, "", nil
	}

	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return false, "", fmt.Errorf("failed to build rest config from kubeconfig: %w", err)
	}

	apiextensionsClient, err := apiextensionsclientset.NewForConfig(restConfig)
	if err != nil {
		return false, "", fmt.Errorf("failed to build apiextensions client: %w", err)
	}

	httpClient, err := restclient.HTTPClientFor(restConfig)
	if err != nil {
		return false, "", fmt.Errorf("failed to build discovery http client: %w", err)
	}

	restMapper, err := apiutil.NewDynamicRESTMapper(restConfig, httpClient)
	if err != nil {
		return false, "", fmt.Errorf("failed to build rest mapper: %w", err)
	}

	for _, ref := range refs {
		ready, reason := crdRefReady(ctx, apiextensionsClient, restMapper, ref)
		if !ready {
			return false, reason, nil
		}
	}

	return true, "", nil
}

// crdRefReady checks one crdRef against apiextensionsClient (Established)
// and restMapper (discoverable) — see crdsReady's own doc for why a CRD
// that isn't found, Established, or discoverable yet is reported via the
// returned reason, never an error.
func crdRefReady(
	ctx context.Context, apiextensionsClient apiextensionsclientset.Interface, restMapper meta.RESTMapper, ref crdRef,
) (bool, string) {
	crds := apiextensionsClient.ApiextensionsV1().CustomResourceDefinitions()

	crdObj, err := crds.Get(ctx, ref.name, metav1.GetOptions{})
	if err != nil {
		return false, ref.name + ": crd not found yet"
	}

	if !crdEstablished(crdObj) {
		return false, ref.name + ": crd not Established yet"
	}

	for _, gvk := range ref.gvks {
		_, err := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return false, fmt.Sprintf("%s: %s not discoverable yet", ref.name, gvk)
		}
	}

	return true, ""
}

// crdEstablished reports whether crdObj's Established condition is true
// — mirrors pkg/crd's own identically-named, unexported helper.
func crdEstablished(crdObj *apiextensionsv1.CustomResourceDefinition) bool {
	for _, cond := range crdObj.Status.Conditions {
		if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
			return true
		}
	}

	return false
}
