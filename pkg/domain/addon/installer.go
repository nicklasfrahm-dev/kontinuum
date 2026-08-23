package addon

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
	"helm.sh/helm/v3/pkg/registry"
	"helm.sh/helm/v3/pkg/storage/driver"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// Installer installs (or upgrades, idempotently) a chart into a real
// cluster reachable via kubeconfig, via whichever ProvisioningMethod
// req.Method selects — the seam Reconciler installs addons through.
// helmInstaller is the production implementation; tests inject a fake to
// avoid a real Helm install and the network dependency that comes with
// it.
type Installer interface {
	Install(ctx context.Context, kubeconfig []byte, req InstallRequest) error
}

// helmInstaller is Installer's production implementation.
type helmInstaller struct{}

// NewHelmInstaller returns the production Installer, which performs
// real installs against a real cluster. Installer is this package's
// own seam for injecting a fake in tests.
//
//nolint:ireturn // see doc above
func NewHelmInstaller() Installer {
	return helmInstaller{}
}

// Install implements Installer, dispatching on req.Method.
func (h helmInstaller) Install(ctx context.Context, kubeconfig []byte, req InstallRequest) error {
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
func (helmInstaller) installViaHelm(ctx context.Context, kubeconfig []byte, req InstallRequest) error {
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

	actionConfig.RegistryClient, err = registry.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create helm registry client: %w", err)
	}

	chrt, err := loadChart(actionConfig, req.ChartName, req.RepoURL, req.Version)
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

// loadChart locates and loads chartName@version from repoURL — or, when
// chartName is itself an OCI reference (e.g. "oci://..."), pulls it
// directly, ignoring repoURL. actionConfig must already have
// RegistryClient set (see installViaHelm/renderChart, its only two
// callers) — LocateChart hard-errors on an OCI chartName otherwise; its
// own ChartPathOptions is the only way to reach the unexported field a
// registry client lives on.
func loadChart(actionConfig *action.Configuration, chartName, repoURL, version string) (*chart.Chart, error) {
	chartPathOptions := action.NewInstall(actionConfig).ChartPathOptions
	chartPathOptions.RepoURL = repoURL
	chartPathOptions.Version = version

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

// releaseMaxHistory bounds how many past revisions Helm keeps per release —
// deliberately below the Helm CLI's own --history-max default of 10, for
// extra headroom. Storage.History (used both by action.NewHistory, called
// on every installViaHelm to check whether a release already exists, and
// internally by Helm itself before every write) always lists and fully
// decodes every revision Secret it finds with no cap of its own — see
// helm.sh/helm/v3/pkg/storage.Storage. Leaving MaxHistory unset (as
// Install/Upgrade both default to) never prunes anything, so a release
// reconciled repeatedly over a long enough time accumulates one more
// multi-hundred-KB revision Secret per attempt forever; History.Run then
// has to decode all of them on every single reconcile, without bound.
// That's what actually OOM-killed this process — not any storage/gRPC-layer
// change — once one release's history grew large enough that a single
// existence check alone took hundreds of MB to decode.
const releaseMaxHistory = 5

// installRelease runs a fresh `helm install`, creating namespace if it
// doesn't already exist. Deliberately non-blocking (no Wait/WaitForJobs):
// this returns as soon as the manifests are applied, without waiting for
// any pod to actually start. Pod health is gated separately, by
// PodProber, on a later reconcile.
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
	upgradeAction.MaxHistory = releaseMaxHistory

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
	file, err := os.CreateTemp("", "addon-kubeconfig-*.yaml")
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
