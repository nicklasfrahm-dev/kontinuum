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
	"errors"
	"fmt"
	"os"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/kube"
	"helm.sh/helm/v3/pkg/storage/driver"
)

// Addon chart identity/version pins — see the implementation plan's "Chart
// sourcing" decision: fetched at reconcile time from each project's own
// Helm repo, not vendored. Versions are pinned explicitly rather than left
// to float, same as e2eTalosImage's own pinned-version rationale.
const (
	ciliumReleaseName  = "cilium"
	ciliumRepoURL      = "https://helm.cilium.io"
	ciliumChartName    = "cilium"
	ciliumChartVersion = "1.16.5"
	ciliumNamespace    = "kube-system"

	certManagerReleaseName  = "cert-manager"
	certManagerRepoURL      = "https://charts.jetstack.io"
	certManagerChartName    = "cert-manager"
	certManagerChartVersion = "v1.17.2"
	certManagerNamespace    = "cert-manager"
)

// AddonInstaller installs (or upgrades, idempotently) a Helm chart into a
// real cluster reachable via kubeconfig — the seam this package's
// controller installs Cilium and cert-manager through. helmInstaller is
// the production implementation; tests inject a fake to avoid a real Helm
// install and the network dependency that comes with it.
type AddonInstaller interface {
	// Install fetches chartName@version from repoURL and does the
	// `helm upgrade --install` dance for releaseName in namespace: install
	// if the release doesn't exist yet, upgrade in place otherwise —
	// idempotent either way.
	Install(
		ctx context.Context, kubeconfig []byte,
		releaseName, repoURL, chartName, version, namespace string, values map[string]any,
	) error
}

// helmInstaller is AddonInstaller's production implementation.
type helmInstaller struct{}

// NewHelmInstaller returns the production AddonInstaller, which performs
// real Helm installs against a real cluster. AddonInstaller is this
// package's own seam for injecting a fake in tests — mirrors
// NewTalosBootstrapper's identical rationale.
//
//nolint:ireturn // see doc above
func NewHelmInstaller() AddonInstaller {
	return helmInstaller{}
}

// Install implements AddonInstaller.
func (helmInstaller) Install(
	ctx context.Context, kubeconfig []byte,
	releaseName, repoURL, chartName, version, namespace string, values map[string]any,
) error {
	kubeconfigPath, cleanup, err := writeTempKubeconfig(kubeconfig)
	if err != nil {
		return err
	}
	defer cleanup()

	actionConfig := new(action.Configuration)

	getter := kube.GetConfig(kubeconfigPath, "", namespace)

	err = actionConfig.Init(getter, namespace, "secret", func(string, ...any) {})
	if err != nil {
		return fmt.Errorf("failed to init helm action configuration for %q: %w", releaseName, err)
	}

	chrt, err := loadChart(chartName, repoURL, version)
	if err != nil {
		return err
	}

	_, err = action.NewHistory(actionConfig).Run(releaseName)

	switch {
	case errors.Is(err, driver.ErrReleaseNotFound):
		return installRelease(ctx, actionConfig, releaseName, namespace, chrt, values)
	case err != nil:
		return fmt.Errorf("failed to check existing release %q: %w", releaseName, err)
	default:
		return upgradeRelease(ctx, actionConfig, releaseName, namespace, chrt, values)
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
// doesn't already exist.
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
// release.
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

// ciliumValues builds the Helm values for the Cilium install — envoy and
// node LB IPAM enabled, kube-proxy replaced, per the issue's own scope.
// apiServerHost/apiServerPort must point at the real Kubernetes API
// endpoint: with kube-proxy disabled, Cilium can no longer discover it any
// other way, and the agent fails to start without these set explicitly.
func ciliumValues(apiServerHost string, apiServerPort int) map[string]any {
	return map[string]any{
		"kubeProxyReplacement": true,
		"k8sServiceHost":       apiServerHost,
		"k8sServicePort":       apiServerPort,
		"envoy":                map[string]any{"enabled": true},
		"l2announcements":      map[string]any{"enabled": true},
		"nodeIPAM":             map[string]any{"enabled": true},
	}
}

// certManagerValues builds the Helm values for the cert-manager install —
// the chart doesn't install its CRDs by default.
func certManagerValues() map[string]any {
	return map[string]any{"crds": map[string]any{"enabled": true}}
}
