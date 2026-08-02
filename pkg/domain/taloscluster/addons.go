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
// so there's a single place to update when e.g. bumping a chart version.
// The chart's own name is the embedded file's own name (see
// loadAddonDefaults) — only Repo/Version live here. A TalosCluster's own
// spec.addons[] entry can override each of these fields individually —
// see AddonSpec's own doc.
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
// purely for readability/diffability. Returns an error, not a panic: only
// the two built-in names (see resolveAddons) are ever expected to resolve
// to a real embedded file, but a generic addon system shouldn't have a
// function's safety depend on every call site getting that discipline
// right by convention.
func loadAddonDefaults(name string) (addonDefaults, error) {
	data, err := defaultValuesFS.ReadFile("values/" + name + ".yaml")
	if err != nil {
		return addonDefaults{}, fmt.Errorf("failed to read embedded %s.yaml: %w", name, err)
	}

	var defaults addonDefaults

	err = yaml.Unmarshal(data, &defaults)
	if err != nil {
		return addonDefaults{}, fmt.Errorf("failed to parse embedded %s.yaml: %w", name, err)
	}

	return defaults, nil
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

// errAddonMissingChart and errAddonMissingNamespace are static sentinels
// — err113 flags a dynamically constructed errors.New/fmt.Errorf call
// without a wrapped static error, same as controller.go's own
// errKubeconfigNotStored.
var (
	errAddonMissingChart = errors.New(
		"addon has no chart repo/name — set spec.addons[].chart (no built-in default for this name)")
	errAddonMissingNamespace = errors.New(
		"addon has no namespace — set spec.addons[].namespace (no built-in default for this name)")
)

// builtinAddonNames lists TalosCluster's own default addons — each is
// also the embedded values/<name>.yaml filename this addon's own defaults
// come from (see loadAddonDefaults). Installed even with no entry in
// cluster.Spec.Addons at all — see resolveAddons. A function, not a
// package-level slice, so nothing can mutate the shared backing array.
func builtinAddonNames() []string {
	return []string{ciliumAddonName, certManagerAddonName}
}

// resolveAddons merges cluster.Spec.Addons against builtinAddonNames' own
// embedded defaults and returns one AddonInstallRequest per *enabled*
// addon: a built-in name absent from Spec.Addons installs with its own
// embedded defaults untouched; a Spec.Addons entry naming a built-in
// overrides that built-in's fields one at a time (an unset field keeps
// the built-in's own value); any other Spec.Addons entry is a fully
// user-defined addon with no built-in default — its own Chart is
// required. Order is irrelevant — callers install these in parallel.
func resolveAddons(cluster *v1alpha2.TalosCluster, celCtx map[string]any) ([]AddonInstallRequest, error) {
	userAddons := make(map[string]v1alpha2.AddonSpec, len(cluster.Spec.Addons))
	for _, spec := range cluster.Spec.Addons {
		userAddons[spec.Name] = spec
	}

	names := builtinAddonNames()
	seen := make(map[string]bool, len(names))
	requests := make([]AddonInstallRequest, 0, len(cluster.Spec.Addons)+len(names))

	for _, name := range names {
		seen[name] = true

		req, enabled, err := resolveBuiltinAddon(name, userAddons, celCtx)
		if err != nil {
			return nil, err
		}

		if enabled {
			requests = append(requests, req)
		}
	}

	for _, spec := range cluster.Spec.Addons {
		if seen[spec.Name] || !addonEnabled(spec) {
			continue
		}

		req, err := resolveAddon(spec, addonDefaults{}, celCtx)
		if err != nil {
			return nil, err
		}

		requests = append(requests, req)
	}

	return requests, nil
}

// resolveBuiltinAddon resolves name's own request, applying userAddons'
// same-named entry (if any) as an override — see resolveAddons' own doc.
// enabled is false when the addon is disabled; callers should skip it,
// not append req, in that case.
func resolveBuiltinAddon(
	name string, userAddons map[string]v1alpha2.AddonSpec, celCtx map[string]any,
) (AddonInstallRequest, bool, error) {
	spec, overridden := userAddons[name]
	if !overridden {
		spec = v1alpha2.AddonSpec{Name: name}
	}

	if !addonEnabled(spec) {
		return AddonInstallRequest{}, false, nil
	}

	def, err := loadAddonDefaults(name)
	if err != nil {
		return AddonInstallRequest{}, false, err
	}

	req, err := resolveAddon(spec, def, celCtx)
	if err != nil {
		return AddonInstallRequest{}, false, err
	}

	return req, true, nil
}

// resolveAddonChart resolves repo/chartName/version against def.Chart
// (empty for a non-built-in addon), each overridden by the matching
// spec.Chart field when set.
func resolveAddonChart(spec v1alpha2.AddonSpec, def addonDefaults) (string, string, string, error) {
	repo, chartName, version := def.Chart.Repo, spec.Name, def.Chart.Version

	if spec.Chart != nil {
		if spec.Chart.Repo != "" {
			repo = spec.Chart.Repo
		}

		if spec.Chart.Name != "" {
			chartName = spec.Chart.Name
		}

		if spec.Chart.Version != "" {
			version = spec.Chart.Version
		}
	}

	if repo == "" || chartName == "" {
		return "", "", "", fmt.Errorf("%q: %w", spec.Name, errAddonMissingChart)
	}

	return repo, chartName, version, nil
}

// resolveAddon resolves one addon's chart/namespace/values against def —
// the zero value for a non-built-in addon, meaning "no fallback": its own
// spec.Chart/spec.Namespace must supply everything, or this returns a
// descriptive error rather than installing something half-configured.
func resolveAddon(spec v1alpha2.AddonSpec, def addonDefaults, celCtx map[string]any) (AddonInstallRequest, error) {
	repo, chartName, version, err := resolveAddonChart(spec, def)
	if err != nil {
		return AddonInstallRequest{}, err
	}

	namespace := def.Namespace
	if spec.Namespace != "" {
		namespace = spec.Namespace
	}

	if namespace == "" {
		return AddonInstallRequest{}, fmt.Errorf("%q: %w", spec.Name, errAddonMissingNamespace)
	}

	userValues, err := addonUserValues(spec)
	if err != nil {
		return AddonInstallRequest{}, err
	}

	resolvedDefaults, err := evaluateComputedValues(def.Values, celCtx)
	if err != nil {
		return AddonInstallRequest{}, fmt.Errorf("failed to resolve computed values for addon %q: %w", spec.Name, err)
	}

	return AddonInstallRequest{
		ReleaseName: spec.Name,
		RepoURL:     repo,
		ChartName:   chartName,
		Version:     version,
		Namespace:   namespace,
		Method:      addonMethod(spec),
		Values:      mergeValues(resolvedDefaults, userValues),
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
	// Values are already fully merged (computed defaults + user overrides)
	// — see resolveAddon, which builds every AddonInstallRequest.
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
