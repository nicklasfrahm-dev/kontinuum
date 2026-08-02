// Package addon implements Addon's own reconcile loop — resolving one
// addon's chart/namespace/values (built-in fallback for "cilium"/
// "cert-manager", or a fully user-defined chart for any other
// ReleaseName), installing it via the Helm SDK, and health-probing its
// pods. TalosCluster's own reconciler (pkg/domain/taloscluster) only
// seeds the two built-ins (see EnsureBuiltinSeeds) and aggregates Ready
// across whatever Addons reference it — this package owns everything
// about one addon's own lifecycle.
package addon

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	"helm.sh/helm/v3/pkg/registry"
	"sigs.k8s.io/yaml"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// Addon names — each is also the embedded values/<name>.yaml filename (see
// loadAddonDefaults). Also the Helm release name and, unless the embedded
// file's own chart.name overrides it (see resolveAddonChart), the chart
// name too.
const (
	ciliumAddonName         = "cilium"
	certManagerAddonName    = "cert-manager"
	gatewayAPICRDsAddonName = "gateway-api-crds"
	defaultAddonPriority    = int32(100)
)

//go:embed values/*.yaml
var defaultValuesFS embed.FS

// addonDefaults is one embedded values/<name>.yaml file's shape — chart
// identity/version, install namespace, lifecycle, and Helm values all
// live together so there's a single place to update when e.g. bumping a
// chart version. Namespace/Lifecycle deliberately mirror AddonSpec's own
// field shapes (AddonNamespaceSpec, AddonLifecycleSpec) rather than a
// flattened ad-hoc one, so a built-in's own defaults read like a real
// spec fragment. The chart's own name defaults to the embedded file's
// own name (see loadAddonDefaults) unless Chart.Name overrides it —
// needed for an OCI chart reference (e.g. gateway-api-crds' own
// "oci://..." chart name, which differs from its release name). An
// Addon's own spec can override each of these fields individually — see
// AddonSpec's own doc.
type addonDefaults struct {
	Chart struct {
		Repo    string `json:"repo"`
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"chart"`
	Namespace v1alpha2.AddonNamespaceSpec `json:"namespace"`
	Lifecycle struct {
		// Provisioning.Method is this built-in's own default
		// provisioning method — see AddonProvisioningSpec.Method's own
		// doc. Empty means "no built-in default", i.e. the global
		// default (HelmUpgradeInstall) applies. gateway-api-crds' own
		// embedded default sets this to KubectlApply: its own CRDs are
		// large enough that a Helm release record (a single Secret,
		// capped at 1MiB) can't hold them.
		Provisioning v1alpha2.AddonProvisioningSpec `json:"provisioning"`
		// Priority is this built-in's own default install-ordering
		// priority — see AddonLifecycleSpec.Priority's own doc. Zero
		// means "no built-in default", i.e. the global default (100)
		// applies.
		Priority int32 `json:"priority"`
	} `json:"lifecycle"`
	Values map[string]any `json:"values"`
}

// loadAddonDefaults reads and parses name's embedded values/<name>.yaml —
// see that directory's own files for what each addon's required/functional
// defaults are and why. Kept as real YAML files rather than Go literals
// purely for readability/diffability. Returns an error, not a panic: only
// the built-in names (see builtinAddonNames) are ever expected to
// resolve to a real embedded file, but a generic addon system shouldn't
// have a function's safety depend on every call site getting that
// discipline right by convention.
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
// Used to apply an Addon's own spec.values on top of this package's
// required/functional defaults — user values win on conflict.
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

// Enabled reports whether spec allows this addon to install — nil (the
// zero value, matching a hand-built Go value in a test that never set it)
// or an explicit true both mean enabled; only an explicit false disables
// it. See AddonSpec.Enabled's own doc for why this is a pointer.
func Enabled(spec v1alpha2.AddonSpec) bool {
	return spec.Enabled == nil || *spec.Enabled
}

// ReleaseName returns addon's own effective release name — spec.ReleaseName
// when set, defaulting to the Addon's own metadata.name otherwise, so a
// user creating e.g. an Addon named "cilium" doesn't need to repeat the
// name in spec.releaseName too.
func ReleaseName(addon *v1alpha2.Addon) string {
	if addon.Spec.ReleaseName != "" {
		return addon.Spec.ReleaseName
	}

	return addon.Name
}

// addonMethod returns spec's own provisioning method, falling back to
// def's own built-in default (see addonDefaults.Lifecycle.Provisioning's
// own doc), or HelmUpgradeInstall when neither sets one.
func addonMethod(spec v1alpha2.AddonSpec, def addonDefaults) v1alpha2.AddonProvisioningMethod {
	if spec.Lifecycle.Provisioning.Method != "" {
		return spec.Lifecycle.Provisioning.Method
	}

	if def.Lifecycle.Provisioning.Method != "" {
		return def.Lifecycle.Provisioning.Method
	}

	return v1alpha2.AddonProvisioningMethodHelmUpgradeInstall
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
// without a wrapped static error.
var (
	errAddonMissingChart = errors.New(
		"addon has no chart repo/name — set spec.chart (no built-in default for this ReleaseName)")
	errAddonMissingNamespace = errors.New(
		"addon has no namespace — set spec.namespace (no built-in default for this ReleaseName)")
)

// builtinAddonNames lists the addons installed by default even with no
// entry anywhere at all — see EnsureBuiltinSeeds. A function, not a
// package-level slice, so nothing can mutate the shared backing array.
func builtinAddonNames() []string {
	return []string{gatewayAPICRDsAddonName, ciliumAddonName, certManagerAddonName}
}

// resolveAddonChart resolves repo/chartName/version against def.Chart
// (empty for a non-built-in addon), each overridden by the matching
// spec.Chart field when set. repo is required only when chartName isn't
// itself an OCI reference (e.g. "oci://docker.io/..." — see
// gateway-api-crds' own embedded default) — an OCI chart is addressed
// entirely by its own name, with no separate repo URL.
func resolveAddonChart(spec v1alpha2.AddonSpec, def addonDefaults) (string, string, string, error) {
	repo, chartName, version := def.Chart.Repo, spec.ReleaseName, def.Chart.Version
	if def.Chart.Name != "" {
		chartName = def.Chart.Name
	}

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

	if chartName == "" || (repo == "" && !registry.IsOCI(chartName)) {
		return "", "", "", fmt.Errorf("%q: %w", spec.ReleaseName, errAddonMissingChart)
	}

	return repo, chartName, version, nil
}

// EffectivePriority resolves addon's own install-ordering priority (see
// AddonLifecycleSpec.Priority's own doc): its spec value when set, this
// addon's built-in default otherwise (see values/*.yaml's own priority
// field), or the global default (100) for anything else — including a
// non-built-in ReleaseName, which has no default of its own to fall back
// on.
func EffectivePriority(addon *v1alpha2.Addon) (int32, error) {
	if addon.Spec.Lifecycle.Priority != nil {
		return *addon.Spec.Lifecycle.Priority, nil
	}

	releaseName := ReleaseName(addon)
	if !slices.Contains(builtinAddonNames(), releaseName) {
		return defaultAddonPriority, nil
	}

	def, err := loadAddonDefaults(releaseName)
	if err != nil {
		return 0, err
	}

	if def.Lifecycle.Priority != 0 {
		return def.Lifecycle.Priority, nil
	}

	return defaultAddonPriority, nil
}

// resolveAddon resolves one addon's chart/namespace/values into an
// install request. spec.ReleaseName matching a built-in name (see
// builtinAddonNames) falls back to that addon's own embedded defaults for
// whatever Chart/Namespace/Values spec itself leaves unset; any other
// ReleaseName has no fallback — Chart/Namespace become required, or this
// returns a descriptive error rather than installing something
// half-configured.
func resolveAddon(spec v1alpha2.AddonSpec, celCtx map[string]any) (InstallRequest, error) {
	var def addonDefaults

	if slices.Contains(builtinAddonNames(), spec.ReleaseName) {
		var err error

		def, err = loadAddonDefaults(spec.ReleaseName)
		if err != nil {
			return InstallRequest{}, err
		}
	}

	repo, chartName, version, err := resolveAddonChart(spec, def)
	if err != nil {
		return InstallRequest{}, err
	}

	namespace := def.Namespace.Name
	if spec.Namespace.Name != "" {
		namespace = spec.Namespace.Name
	}

	if namespace == "" {
		return InstallRequest{}, fmt.Errorf("%q: %w", spec.ReleaseName, errAddonMissingNamespace)
	}

	userValues, err := addonUserValues(spec)
	if err != nil {
		return InstallRequest{}, err
	}

	resolvedDefaults, err := EvaluateComputedValues(def.Values, celCtx)
	if err != nil {
		return InstallRequest{}, fmt.Errorf("failed to resolve computed values for addon %q: %w", spec.ReleaseName, err)
	}

	return InstallRequest{
		ReleaseName: spec.ReleaseName,
		RepoURL:     repo,
		ChartName:   chartName,
		Version:     version,
		Namespace:   namespace,
		Method:      addonMethod(spec, def),
		Values:      mergeValues(resolvedDefaults, userValues),
	}, nil
}

// InstallRequest describes one addon install/upgrade — the seam
// Installer.Install acts on.
type InstallRequest struct {
	ReleaseName string
	RepoURL     string
	ChartName   string
	Version     string
	Namespace   string
	Method      v1alpha2.AddonProvisioningMethod
	// Values are already fully merged (computed defaults + user overrides)
	// — see resolveAddon, which builds every InstallRequest.
	Values map[string]any
}
