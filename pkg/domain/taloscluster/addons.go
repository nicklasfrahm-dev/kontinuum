// Package taloscluster implements TalosCluster's bootstrap/addons
// reconciler — see issue #24's architecture decision 3/5. It resolves a
// cluster's control-plane and worker InstancePools' claimed, Discovered
// members, generates and applies Talos machine configs, bootstraps etcd,
// waits for cluster health, then installs Cilium and cert-manager via the
// Helm SDK — control-plane-first: worker pools are only touched once the
// control plane reports healthy. talosclusters.kontinuum.sh's CRD is
// already ensured by pkg/domain/instance.EnsureCRDs — no separate ensure
// step lives here.
package taloscluster

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/kube"
	"helm.sh/helm/v3/pkg/storage/driver"
	"sigs.k8s.io/yaml"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// Addon names — each is also the embedded values/<name>.yaml filename, and
// the Helm release/chart name (see loadAddonDefaults and buildAddonRequest,
// which infer both from this same string, so there's exactly one place
// that spells an addon's name).
const (
	ciliumAddonName      = "cilium"
	certManagerAddonName = "cert-manager"
)

//go:embed values/*.yaml
var defaultValuesFS embed.FS

// addonDefaults is one embedded values/<name>.yaml file's shape — chart
// identity/version, install namespace, and Helm values all live together
// so there's a single place to update when e.g. bumping a chart version. A
// TalosCluster's own spec.addons.<name>.version/.namespace/.values
// override these — see AddonSpec's own doc.
type addonDefaults struct {
	Chart struct {
		Repo    string `json:"repo"`
		Version string `json:"version"`
	} `json:"chart"`
	Namespace string         `json:"namespace"`
	Values    map[string]any `json:"values"`
}

// loadAddonDefaults reads and parses name's embedded values/<name>.yaml —
// see that directory's own files for what each addon's required/functional
// defaults are and why. Kept as real YAML files rather than Go literals
// purely for readability/diffability; a corrupt or missing embedded file
// can only mean a build-time bug, not a condition callers could
// meaningfully recover from — hence the panic instead of a returned error,
// mirroring pkg/crd.Build's identical reasoning for its own embedded
// manifests.
func loadAddonDefaults(name string) addonDefaults {
	data, err := defaultValuesFS.ReadFile("values/" + name + ".yaml")
	if err != nil {
		panic(fmt.Sprintf("failed to read embedded %s.yaml: %v", name, err))
	}

	var defaults addonDefaults

	err = yaml.Unmarshal(data, &defaults)
	if err != nil {
		panic(fmt.Sprintf("failed to parse embedded %s.yaml: %v", name, err))
	}

	return defaults
}

// multiControlPlaneOperatorReplicas is cilium-operator's replica count once
// a cluster has more than one control-plane node — high-availability
// without over-provisioning, since the operator itself is a leader-elected
// singleton (more replicas past 2 add standby capacity, not throughput).
// values/cilium.yaml's own default (1) covers the single-control-plane
// case, where a second replica could never even schedule with
// hostNetwork: true (see this file's own history for the deadlock that
// caused).
const multiControlPlaneOperatorReplicas = 2

// ciliumValues builds the Helm values for the Cilium install — see
// values/cilium.yaml for the full set and why each diverges from the
// chart's own defaults; every value there follows Talos's own documented
// Cilium install (docs.siderolabs.com/kubernetes-guides/cni/deploying-cilium).
// k8sServicePort is overlaid at runtime from kubePrismPort (config.go),
// rather than living in the YAML file, since it's derived from a Go
// constant, not a static default. controlPlaneCount scales
// operator.replicas up once there's more than one control-plane node to
// spread across — see multiControlPlaneOperatorReplicas. userValues, when
// non-nil, are merged on top last — user values win on conflict, see
// mergeValues.
func ciliumValues(controlPlaneCount int, userValues map[string]any) map[string]any {
	values := loadAddonDefaults(ciliumAddonName).Values
	values["k8sServicePort"] = kubePrismPort

	if controlPlaneCount > 1 {
		values = mergeValues(values, map[string]any{
			"operator": map[string]any{"replicas": multiControlPlaneOperatorReplicas},
		})
	}

	return mergeValues(values, userValues)
}

// certManagerValues builds the Helm values for the cert-manager install —
// see values/cert-manager.yaml. userValues, when non-nil, are merged on
// top — user values win on conflict, see mergeValues.
func certManagerValues(userValues map[string]any) map[string]any {
	return mergeValues(loadAddonDefaults(certManagerAddonName).Values, userValues)
}

// mergeValues recursively merges overlay onto base, returning a new map —
// neither input is mutated. overlay wins whenever both sides set the same
// key, except when both sides' values are themselves maps, in which case
// they're merged recursively rather than one replacing the other whole.
// Used to apply a TalosCluster's own spec.addons.<name>.values on top of
// this package's required/functional defaults, per the implementation
// plan's "user values win on conflict" decision.
func mergeValues(base, overlay map[string]any) map[string]any {
	merged := maps.Clone(base)
	if merged == nil {
		merged = map[string]any{}
	}

	for key, overlayValue := range overlay {
		baseValue, ok := merged[key]
		if !ok {
			merged[key] = overlayValue

			continue
		}

		baseMap, baseIsMap := baseValue.(map[string]any)

		overlayMap, overlayIsMap := overlayValue.(map[string]any)
		if baseIsMap && overlayIsMap {
			merged[key] = mergeValues(baseMap, overlayMap)

			continue
		}

		merged[key] = overlayValue
	}

	return merged
}

// addonEnabled reports whether spec allows this addon to install — nil
// (the zero value, matching a hand-built Go value in a test that never set
// it) or an explicit true both mean enabled; only an explicit false
// disables it. See AddonSpec.Enabled's own doc for why this is a pointer.
func addonEnabled(spec v1alpha2.AddonSpec) bool {
	return spec.Enabled == nil || *spec.Enabled
}

// addonMethod returns spec's own provisioning method, defaulting to
// HelmUpgradeInstall when unset.
func addonMethod(spec v1alpha2.AddonSpec) v1alpha2.AddonProvisioningMethod {
	if spec.Lifecycle.Provisioning.Method == "" {
		return v1alpha2.AddonProvisioningMethodHelmUpgradeInstall
	}

	return spec.Lifecycle.Provisioning.Method
}

// addonUserValues parses spec.Values into a plain map — nil (not an
// error) when Values is unset, so callers can merge it unconditionally.
func addonUserValues(spec v1alpha2.AddonSpec) (map[string]any, error) {
	if spec.Values == nil || len(spec.Values.Raw) == 0 {
		return nil, nil //nolint:nilnil // absent Values is a valid, non-error case — see doc above
	}

	var values map[string]any

	err := json.Unmarshal(spec.Values.Raw, &values)
	if err != nil {
		return nil, fmt.Errorf("failed to parse addon values: %w", err)
	}

	return values, nil
}

// buildAddonRequest resolves spec's namespace/version/method/values
// overrides against name's own embedded defaults (see loadAddonDefaults)
// and merges baseValues' required/functional defaults with any
// user-supplied values — see ciliumValues/certManagerValues, and
// mergeValues's own "user values win on conflict" doc. name is both the
// Helm release/chart name and the embedded values/<name>.yaml this addon's
// defaults come from — see ciliumAddonName/certManagerAddonName's own doc.
func buildAddonRequest(
	name string, spec v1alpha2.AddonSpec,
	baseValues func(userValues map[string]any) map[string]any,
) (AddonInstallRequest, error) {
	defaults := loadAddonDefaults(name)

	userValues, err := addonUserValues(spec)
	if err != nil {
		return AddonInstallRequest{}, err
	}

	version := spec.Version
	if version == "" {
		version = defaults.Chart.Version
	}

	namespace := spec.Namespace
	if namespace == "" {
		namespace = defaults.Namespace
	}

	return AddonInstallRequest{
		ReleaseName: name,
		RepoURL:     defaults.Chart.Repo,
		ChartName:   name,
		Version:     version,
		Namespace:   namespace,
		Method:      addonMethod(spec),
		Values:      baseValues(userValues),
	}, nil
}

// AddonInstallRequest describes one addon install/upgrade — the seam
// AddonInstaller.Install acts on.
type AddonInstallRequest struct {
	ReleaseName string
	RepoURL     string
	ChartName   string
	Version     string
	Namespace   string
	Method      v1alpha2.AddonProvisioningMethod
	// Values are already fully merged (defaults + user overrides) — see
	// ciliumValues/certManagerValues, called by this package's own
	// controller.go before building a request.
	Values map[string]any
}

// AddonInstaller installs (or upgrades, idempotently) a chart into a real
// cluster reachable via kubeconfig, via whichever ProvisioningMethod
// req.Method selects — the seam this package's controller installs
// Cilium and cert-manager through. helmInstaller is the production
// implementation; tests inject a fake to avoid a real Helm install and
// the network dependency that comes with it.
type AddonInstaller interface {
	Install(ctx context.Context, kubeconfig []byte, req AddonInstallRequest) error
}

// helmInstaller is AddonInstaller's production implementation.
type helmInstaller struct{}

// NewHelmInstaller returns the production AddonInstaller, which performs
// real installs against a real cluster. AddonInstaller is this package's
// own seam for injecting a fake in tests — mirrors
// NewTalosBootstrapper's identical rationale.
//
//nolint:ireturn // see doc above
func NewHelmInstaller() AddonInstaller {
	return helmInstaller{}
}

// Install implements AddonInstaller, dispatching on req.Method.
func (h helmInstaller) Install(ctx context.Context, kubeconfig []byte, req AddonInstallRequest) error {
	if req.Method == v1alpha2.AddonProvisioningMethodKubectlApply {
		return h.installViaKubectlApply(ctx, kubeconfig, req)
	}

	return h.installViaHelm(ctx, kubeconfig, req)
}

// installViaHelm does the `helm upgrade --install` dance: install if the
// release doesn't exist yet, upgrade in place otherwise — idempotent
// either way. This creates a real Helm release record (a Secret in
// req.Namespace) — see installViaKubectlApply for the alternative that
// doesn't.
func (helmInstaller) installViaHelm(ctx context.Context, kubeconfig []byte, req AddonInstallRequest) error {
	kubeconfigPath, cleanup, err := writeTempKubeconfig(kubeconfig)
	if err != nil {
		return err
	}
	defer cleanup()

	actionConfig := new(action.Configuration)

	getter := kube.GetConfig(kubeconfigPath, "", req.Namespace)

	err = actionConfig.Init(getter, req.Namespace, "secret", func(string, ...any) {})
	if err != nil {
		return fmt.Errorf("failed to init helm action configuration for %q: %w", req.ReleaseName, err)
	}

	chrt, err := loadChart(req.ChartName, req.RepoURL, req.Version)
	if err != nil {
		return err
	}

	_, err = action.NewHistory(actionConfig).Run(req.ReleaseName)

	switch {
	case errors.Is(err, driver.ErrReleaseNotFound):
		return installRelease(ctx, actionConfig, req.ReleaseName, req.Namespace, chrt, req.Values)
	case err != nil:
		return fmt.Errorf("failed to check existing release %q: %w", req.ReleaseName, err)
	default:
		return upgradeRelease(ctx, actionConfig, req.ReleaseName, req.Namespace, chrt, req.Values)
	}
}

// loadChart locates and loads chartName@version from repoURL.
func loadChart(chartName, repoURL, version string) (*chart.Chart, error) {
	chartPathOptions := action.ChartPathOptions{RepoURL: repoURL, Version: version}

	chartPath, err := chartPathOptions.LocateChart(chartName, cli.New())
	if err != nil {
		return nil, fmt.Errorf("failed to locate chart %q version %q from %q: %w", chartName, version, repoURL, err)
	}

	chrt, err := loader.Load(chartPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load chart %q: %w", chartName, err)
	}

	return chrt, nil
}

// installRelease runs a fresh `helm install`, creating namespace if it
// doesn't already exist. Deliberately non-blocking (no Wait/WaitForJobs):
// this returns as soon as the manifests are applied, without waiting for
// any pod to actually start. Pod health is gated separately, by
// PodProber, on a later reconcile — matching this reconciler's existing
// self-healing, non-blocking pattern (ApplyConfiguration/Bootstrap don't
// block for completion either) rather than tying up the calling reconcile
// for however long a cold rollout takes.
func installRelease(
	ctx context.Context, actionConfig *action.Configuration, releaseName, namespace string,
	chrt *chart.Chart, values map[string]any,
) error {
	installAction := action.NewInstall(actionConfig)
	installAction.ReleaseName = releaseName
	installAction.Namespace = namespace
	installAction.CreateNamespace = true

	_, err := installAction.RunWithContext(ctx, chrt, values)
	if err != nil {
		return fmt.Errorf("failed to install release %q: %w", releaseName, err)
	}

	return nil
}

// upgradeRelease runs `helm upgrade` in place against an already-installed
// release — see installRelease's own doc for why this stays non-blocking.
func upgradeRelease(
	ctx context.Context, actionConfig *action.Configuration, releaseName, namespace string,
	chrt *chart.Chart, values map[string]any,
) error {
	upgradeAction := action.NewUpgrade(actionConfig)
	upgradeAction.Namespace = namespace

	_, err := upgradeAction.RunWithContext(ctx, releaseName, chrt, values)
	if err != nil {
		return fmt.Errorf("failed to upgrade release %q: %w", releaseName, err)
	}

	return nil
}

// writeTempKubeconfig writes kubeconfig to a temp file — the Helm SDK's
// own kube.GetConfig takes a kubeconfig file path, not raw bytes, so this
// avoids reimplementing genericclioptions.RESTClientGetter by hand. The
// returned cleanup func removes the file; always call it (e.g. via defer).
func writeTempKubeconfig(kubeconfig []byte) (string, func(), error) {
	file, err := os.CreateTemp("", "taloscluster-kubeconfig-*.yaml")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temp kubeconfig file: %w", err)
	}

	cleanup := func() { _ = os.Remove(file.Name()) }

	_, err = file.Write(kubeconfig)
	if err != nil {
		_ = file.Close()

		cleanup()

		return "", nil, fmt.Errorf("failed to write temp kubeconfig file: %w", err)
	}

	err = file.Close()
	if err != nil {
		cleanup()

		return "", nil, fmt.Errorf("failed to close temp kubeconfig file: %w", err)
	}

	return file.Name(), cleanup, nil
}
