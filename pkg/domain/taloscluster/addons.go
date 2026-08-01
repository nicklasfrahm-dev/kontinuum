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

// ciliumValues builds the Helm values for the Cilium install — envoy and
// node LB IPAM enabled, kube-proxy replaced, per the issue's own scope.
// These follow Talos's own documented Cilium install
// (docs.siderolabs.com/kubernetes-guides/cni/deploying-cilium), not just the
// chart's defaults — every non-obvious value below diverges from that
// default for a specific, Talos-driven reason.
//
// k8sServiceHost/k8sServicePort point at KubePrism, Talos's own built-in
// local apiserver proxy that every node runs on localhost:7445 — not a
// specific control-plane member's address. With kube-proxy disabled,
// Cilium can't discover the apiserver any other way, and the agent won't
// start without these set explicitly; KubePrism sidesteps needing a real
// control-plane load balancer, since every node's own copy is always
// reachable at the same fixed address regardless of which (or how many)
// control-plane members exist.
//
// securityContext.capabilities pins the agent and clean-cilium-state init
// container to Talos's reduced capability sets, instead of the chart's own
// broader defaults. clean-cilium-state in particular is otherwise prone to
// "OCI runtime create failed: ... unable to apply caps: can't apply
// capabilities: operation not permitted" — the chart's default set isn't
// fully grantable under Talos's own container runtime constraints.
//
// cgroup.autoMount.enabled is disabled, with hostRoot pointed at Talos's
// own cgroup2 mount: Talos already mounts cgroup2 itself, so Cilium's own
// redundant auto-mount inside its init container is both unnecessary and,
// under the same capability constraints as above, another likely source of
// that same "operation not permitted" failure.
//
// ipam.mode is set to "kubernetes" per Talos's own guide — Cilium reads pod
// CIDRs from each Node's own spec rather than running its own IPAM
// allocator, which needs no extra moving parts on a Talos-managed cluster.
//
// operator.replicas is pinned to 1: the chart's own default is 2 (for HA)
// with operator.hostNetwork: true, which makes the operator's prometheus
// containerPort an implicit hostPort. On a single-node cluster (every
// TalosCluster this reconciler bootstraps today — see
// AllowSchedulingOnControlPlanes in config.go) a second replica can never
// be scheduled: it permanently fails with "0/1 nodes are available: ...
// didn't have free ports for the requested pod ports", which is exactly
// what a two-replica default looks like from PodProber's perspective —
// bare Pending that never resolves, indistinguishable from a genuine hang
// without inspecting the pod's own PodScheduled condition message. The
// chart's own values.yaml even documents this, verbatim: "In HA mode,
// cilium-operator pods must not be scheduled on the same node as they will
// clash with each other".
func ciliumValues() map[string]any {
	return map[string]any{
		"kubeProxyReplacement": true,
		"k8sServiceHost":       "localhost",
		"k8sServicePort":       kubePrismPort,
		"envoy":                map[string]any{"enabled": true},
		"l2announcements":      map[string]any{"enabled": true},
		"nodeIPAM":             map[string]any{"enabled": true},
		"operator":             map[string]any{"replicas": 1},
		"ipam":                 map[string]any{"mode": "kubernetes"},
		"cgroup": map[string]any{
			"autoMount": map[string]any{"enabled": false},
			"hostRoot":  "/sys/fs/cgroup",
		},
		"securityContext": map[string]any{
			"capabilities": map[string]any{
				"ciliumAgent": []string{
					"CHOWN", "KILL", "NET_ADMIN", "NET_RAW", "IPC_LOCK", "SYS_ADMIN",
					"SYS_RESOURCE", "DAC_OVERRIDE", "FOWNER", "SETGID", "SETUID",
				},
				"cleanCiliumState": []string{"NET_ADMIN", "SYS_ADMIN", "SYS_RESOURCE"},
			},
		},
	}
}

// certManagerValues builds the Helm values for the cert-manager install —
// the chart doesn't install its CRDs by default.
func certManagerValues() map[string]any {
	return map[string]any{"crds": map[string]any{"enabled": true}}
}
