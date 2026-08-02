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

	"sigs.k8s.io/yaml"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// Addon names — each is also the embedded values/<name>.yaml filename, and
// the Helm release/chart name for a built-in (see loadAddonDefaults and
// resolveAddon, which infer both from this same string).
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
// loadAddonDefaults) — only Repo/Version live here. An Addon's own spec
// can override each of these fields individually — see AddonSpec's own
// doc.
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
// the two built-in names (see builtinAddonNames) are ever expected to
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
	return []string{ciliumAddonName, certManagerAddonName}
}

// resolveAddonChart resolves repo/chartName/version against def.Chart
// (empty for a non-built-in addon), each overridden by the matching
// spec.Chart field when set.
func resolveAddonChart(spec v1alpha2.AddonSpec, def addonDefaults) (string, string, string, error) {
	repo, chartName, version := def.Chart.Repo, spec.ReleaseName, def.Chart.Version

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
		return "", "", "", fmt.Errorf("%q: %w", spec.ReleaseName, errAddonMissingChart)
	}

	return repo, chartName, version, nil
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

	namespace := def.Namespace
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

	resolvedDefaults, err := evaluateComputedValues(def.Values, celCtx)
	if err != nil {
		return InstallRequest{}, fmt.Errorf("failed to resolve computed values for addon %q: %w", spec.ReleaseName, err)
	}

	return InstallRequest{
		ReleaseName: spec.ReleaseName,
		RepoURL:     repo,
		ChartName:   chartName,
		Version:     version,
		Namespace:   namespace,
		Method:      addonMethod(spec),
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
