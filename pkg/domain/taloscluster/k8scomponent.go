package taloscluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/cosi-project/runtime/pkg/controller/generic"
	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/safe"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	v1alpha1cfg "github.com/siderolabs/talos/pkg/machinery/config/types/v1alpha1"
	"github.com/siderolabs/talos/pkg/machinery/constants"
	talosconfig "github.com/siderolabs/talos/pkg/machinery/resources/config"
	"github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	"github.com/siderolabs/talos/pkg/machinery/resources/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// Kubernetes component names. Each one doubles as the ID of the COSI
// resource Talos renders that component's own configuration into and as
// the prefix of the static pod its kubelet reports status under, which is
// why there is a single constant per component rather than one set per
// use.
const (
	componentAPIServer         = k8s.APIServerID
	componentControllerManager = k8s.ControllerManagerID
	componentScheduler         = k8s.SchedulerID
	componentProxy             = "kube-proxy"
	componentKubelet           = "kubelet"
)

// errUnknownComponent guards the component switches below against a name
// kubernetesComponents does not list — unreachable from this package's own
// callers, which only ever pass a kubernetesComponent's own name.
var errUnknownComponent = errors.New("unknown kubernetes component")

// staticPodNamespace is the namespace Talos runs the control plane's
// static pods in, and so the first segment of every StaticPodStatus
// resource ID — Talos's own KubeletStaticPodController builds those as
// "<namespace>/<pod name>", and the pod name is "<component>-<nodename>",
// which is why finding a component's status is a prefix match rather than
// a Get by a known ID.
const staticPodNamespace = "kube-system"

// kubernetesComponent is one step of a Kubernetes version roll: a single
// image reference, pinned in the machine config, that Talos's own
// controllers turn into a running static pod or service.
type kubernetesComponent struct {
	// name identifies the component — see the constants above.
	name string
	// imageRepository is the registry path spec.kubernetes.version is a
	// tag on. Talos enforces these repositories rather than accepting an
	// arbitrary image, so they are constants, not configuration.
	imageRepository string
	// onWorkers marks a component every member runs, as opposed to one
	// only the control plane does. Only the kubelet does.
	onWorkers bool
	// awaitsRollout marks a component whose rollout this package can
	// observe, and therefore waits for on the node it just patched before
	// touching the next one. kube-proxy is the exception: it is a
	// DaemonSet rendered from the bootstrap manifests rather than
	// something the patched node itself runs, so there is nothing
	// node-local to wait for — talosctl upgrade-k8s does not wait for it
	// either.
	awaitsRollout bool
}

// kubernetesComponents returns every component a Kubernetes version bump
// rolls, in the order talosctl upgrade-k8s rolls them: the three control
// plane static pods first, innermost outwards, then kube-proxy, and the
// kubelet last.
//
// The order is the point. An apiserver may run ahead of the components
// that talk to it, but a controller-manager, scheduler or kubelet running
// ahead of the apiserver is outside the version skew Kubernetes supports.
// Returned fresh per call rather than kept as a package-level table so no
// caller can reorder the shared copy.
func kubernetesComponents() []kubernetesComponent {
	return []kubernetesComponent{
		{
			name:            componentAPIServer,
			imageRepository: constants.KubernetesAPIServerImage,
			awaitsRollout:   true,
		},
		{
			name:            componentControllerManager,
			imageRepository: constants.KubernetesControllerManagerImage,
			awaitsRollout:   true,
		},
		{
			name:            componentScheduler,
			imageRepository: constants.KubernetesSchedulerImage,
			awaitsRollout:   true,
		},
		{
			name:            componentProxy,
			imageRepository: constants.KubeProxyImage,
		},
		{
			name:            componentKubelet,
			imageRepository: constants.KubeletImage,
			onWorkers:       true,
			awaitsRollout:   true,
		},
	}
}

// image is the reference component should run at version, which is
// expected to already carry its "v" prefix (see normalizeVersion).
//
// suffix carries a kubelet image's own flavour — Talos publishes
// ghcr.io/siderolabs/kubelet:<version>, :<version>-slim and :<version>-fat,
// and an upgrade must stay on the flavour the node already runs rather
// than silently moving it onto the default one. Every other component's
// repository has no such variants, so suffix is empty for them.
func (c kubernetesComponent) image(version, suffix string) string {
	return c.imageRepository + ":" + version + suffix
}

// imageFlavourSuffix reports the flavour suffix of a kubelet image
// reference, i.e. the part of its tag after the version. Talos's own
// upgrade path preserves this across a version bump; see image above.
func imageFlavourSuffix(ref string) string {
	for _, suffix := range []string{"-fat", "-slim"} {
		if strings.HasSuffix(ref, suffix) {
			return suffix
		}
	}

	return ""
}

// kubeletVersionFromImage is the Kubernetes version a kubelet image
// reference runs, which is its tag minus the flavour suffix — a node on
// ghcr.io/siderolabs/kubelet:v1.33.0-slim runs Kubernetes v1.33.0, not
// "v1.33.0-slim". Without this a -slim or -fat node could never compare
// equal to a pinned spec.kubernetes.version, so it would read as
// permanently stale and its cluster as permanently mid-upgrade.
func kubeletVersionFromImage(ref string) string {
	tag := imageTag(ref)

	return strings.TrimSuffix(tag, imageFlavourSuffix(tag))
}

// KubernetesComponentImage implements ClusterBootstrapper.
func (t talosBootstrapper) KubernetesComponentImage(
	ctx context.Context, endpoint, node string, talosCfg *clientconfig.Config, component string,
) (string, error) {
	talosClient, err := t.dial(ctx, endpoint, talosCfg)
	if err != nil {
		return "", err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived component-image connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	rpcCtx = talosclient.WithNode(rpcCtx, node)

	image, err := componentImageFromCOSI(rpcCtx, talosClient, component)
	if err != nil {
		return "", fmt.Errorf("failed to read %s image for %s via %s: %w", component, node, endpoint, err)
	}

	return image, nil
}

// componentImageFromCOSI reads the image Talos has rendered for component
// on the node ctx is already routed to. These are the same resources
// `talosctl get k8scontrolplaneapiserverconfig` and friends print: what
// the node's own machine config resolves to, which is what its controllers
// then run — not what some other node was told to run.
func componentImageFromCOSI(ctx context.Context, talosClient *talosclient.Client, component string) (string, error) {
	switch component {
	case componentAPIServer:
		return cosiImage(ctx, talosClient, k8s.APIServerConfigID,
			func(res *k8s.APIServerConfig) string { return res.TypedSpec().Image })
	case componentControllerManager:
		return cosiImage(ctx, talosClient, k8s.ControllerManagerConfigID,
			func(res *k8s.ControllerManagerConfig) string { return res.TypedSpec().Image })
	case componentScheduler:
		return cosiImage(ctx, talosClient, k8s.SchedulerConfigID,
			func(res *k8s.SchedulerConfig) string { return res.TypedSpec().Image })
	case componentProxy:
		return cosiImage(ctx, talosClient, k8s.BootstrapManifestsConfigID,
			func(res *k8s.BootstrapManifestsConfig) string { return res.TypedSpec().ProxyImage })
	case componentKubelet:
		return cosiImage(ctx, talosClient, k8s.KubeletID,
			func(res *k8s.KubeletSpec) string { return res.TypedSpec().Image })
	default:
		return "", fmt.Errorf("%w: %s", errUnknownComponent, component)
	}
}

// cosiImage reads one COSI resource by ID and picks an image reference out
// of it — the shape every case of componentImageFromCOSI above shares, and
// the only reason any of them needs more than one line.
func cosiImage[T generic.ResourceWithRD](
	ctx context.Context, talosClient *talosclient.Client, id resource.ID, image func(T) string,
) (string, error) {
	res, err := safe.StateGetByID[T](ctx, talosClient.COSI, id)
	if err != nil {
		return "", err
	}

	return image(res), nil
}

// PatchKubernetesComponent implements ClusterBootstrapper.
func (t talosBootstrapper) PatchKubernetesComponent(
	ctx context.Context, endpoint, node string, talosCfg *clientconfig.Config, component, image string,
) error {
	talosClient, err := t.dial(ctx, endpoint, talosCfg)
	if err != nil {
		return err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived component-patch connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	rpcCtx = talosclient.WithNode(rpcCtx, node)

	// The node's own active config is the base, not a freshly generated
	// one: patching in place is what makes a per-component roll possible
	// at all, since regenerating the whole config would move every
	// component's image at once. It also preserves whatever else has been
	// applied to this node since it was bootstrapped.
	active, err := safe.StateGetByID[*talosconfig.MachineConfig](rpcCtx, talosClient.COSI, talosconfig.ActiveID)
	if err != nil {
		return fmt.Errorf("failed to read active machine config for %s via %s: %w", node, endpoint, err)
	}

	patched, err := active.Provider().PatchV1Alpha1(func(cfg *v1alpha1cfg.Config) error {
		return patchComponentImage(cfg, component, image)
	})
	if err != nil {
		return fmt.Errorf("failed to patch %s image for %s: %w", component, node, err)
	}

	data, err := patched.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	if err != nil {
		return fmt.Errorf("failed to encode patched config for %s: %w", node, err)
	}

	// NO_REBOOT, not AUTO: an image tag is one of the fields Talos can
	// apply without restarting the node, and asking for AUTO would let a
	// config drift that crept in elsewhere reboot a control-plane member
	// as a side effect of a Kubernetes upgrade. If Talos decides the patch
	// cannot be applied this way it refuses, which surfaces as a failed
	// upgrade rather than a surprise reboot.
	_, err = talosClient.ApplyConfiguration(rpcCtx, &machineapi.ApplyConfigurationRequest{
		Data: data,
		Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT,
	})
	if err != nil {
		return fmt.Errorf("failed to apply %s image to %s via %s: %w", component, node, endpoint, err)
	}

	return nil
}

// patchComponentImage points component's image field in cfg at image.
func patchComponentImage(cfg *v1alpha1cfg.Config, component, image string) error {
	if component == componentKubelet {
		kubeletConfigOf(cfg).KubeletImage = image

		return nil
	}

	return patchControlPlaneImage(cfg, component, image)
}

// patchControlPlaneImage points one control-plane component's image field
// in cfg at image.
func patchControlPlaneImage(cfg *v1alpha1cfg.Config, component, image string) error {
	if cfg.ClusterConfig == nil {
		cfg.ClusterConfig = &v1alpha1cfg.ClusterConfig{}
	}

	cluster := cfg.ClusterConfig

	switch component {
	case componentAPIServer:
		apiServerConfigOf(cluster).ContainerImage = image
	case componentControllerManager:
		controllerManagerConfigOf(cluster).ContainerImage = image
	case componentScheduler:
		schedulerConfigOf(cluster).ContainerImage = image
	case componentProxy:
		proxyConfigOf(cluster).ContainerImage = image
	default:
		return fmt.Errorf("%w: %s", errUnknownComponent, component)
	}

	return nil
}

// The four accessors below each return their component's section of the
// cluster config, materialising it when a generated config left it out —
// the same job kubeletConfigOf does on the machine side, kept one per
// section so the switch above stays a plain dispatch.

func apiServerConfigOf(cluster *v1alpha1cfg.ClusterConfig) *v1alpha1cfg.APIServerConfig {
	if cluster.APIServerConfig == nil {
		cluster.APIServerConfig = &v1alpha1cfg.APIServerConfig{}
	}

	return cluster.APIServerConfig
}

func controllerManagerConfigOf(cluster *v1alpha1cfg.ClusterConfig) *v1alpha1cfg.ControllerManagerConfig {
	if cluster.ControllerManagerConfig == nil {
		cluster.ControllerManagerConfig = &v1alpha1cfg.ControllerManagerConfig{}
	}

	return cluster.ControllerManagerConfig
}

func schedulerConfigOf(cluster *v1alpha1cfg.ClusterConfig) *v1alpha1cfg.SchedulerConfig {
	if cluster.SchedulerConfig == nil {
		cluster.SchedulerConfig = &v1alpha1cfg.SchedulerConfig{}
	}

	return cluster.SchedulerConfig
}

func proxyConfigOf(cluster *v1alpha1cfg.ClusterConfig) *v1alpha1cfg.ProxyConfig {
	if cluster.ProxyConfig == nil {
		cluster.ProxyConfig = &v1alpha1cfg.ProxyConfig{}
	}

	return cluster.ProxyConfig
}

// kubeletConfigOf returns cfg's kubelet section, materialising whichever
// parts of the path to it are missing — a config generated without an
// explicit kubelet block still upgrades.
func kubeletConfigOf(cfg *v1alpha1cfg.Config) *v1alpha1cfg.KubeletConfig {
	if cfg.MachineConfig == nil {
		cfg.MachineConfig = &v1alpha1cfg.MachineConfig{}
	}

	if cfg.MachineConfig.MachineKubelet == nil {
		cfg.MachineConfig.MachineKubelet = &v1alpha1cfg.KubeletConfig{}
	}

	return cfg.MachineConfig.MachineKubelet
}

// KubernetesComponentRolledOut implements ClusterBootstrapper.
func (t talosBootstrapper) KubernetesComponentRolledOut(
	ctx context.Context, endpoint, node string, talosCfg *clientconfig.Config, component, image string,
) (bool, error) {
	talosClient, err := t.dial(ctx, endpoint, talosCfg)
	if err != nil {
		return false, err
	}
	defer talosClient.Close() //nolint:errcheck // best-effort close of a short-lived rollout-check connection

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	rpcCtx = talosclient.WithNode(rpcCtx, node)

	if component == componentKubelet {
		return kubeletRolledOut(rpcCtx, talosClient, image)
	}

	return staticPodRolledOut(rpcCtx, talosClient, component, image)
}

// kubeletRolledOut reports whether node's kubelet is running image and is
// back to healthy. Both halves matter: the service stays "running" across
// the restart that picks up a new image, so a health check alone would
// wave the old kubelet through, and an image check alone would wave
// through one that has come up but not yet passed its own checks.
func kubeletRolledOut(ctx context.Context, talosClient *talosclient.Client, image string) (bool, error) {
	spec, err := safe.StateGetByID[*k8s.KubeletSpec](ctx, talosClient.COSI, k8s.KubeletID)
	if err != nil {
		return false, fmt.Errorf("failed to read kubelet spec: %w", err)
	}

	if spec.TypedSpec().Image != image {
		return false, nil
	}

	service, err := safe.StateGetByID[*v1alpha1.Service](ctx, talosClient.COSI, componentKubelet)
	if err != nil {
		return false, fmt.Errorf("failed to read kubelet service: %w", err)
	}

	return service.TypedSpec().Running && service.TypedSpec().Healthy, nil
}

// staticPodRolledOut reports whether the static pod component runs on this
// node is both running image and Ready.
//
// This is the equivalent of talosctl upgrade-k8s's own post-patch wait,
// reading the node's own StaticPodStatus rather than watching Pods through
// the Kubernetes API: the same information, from the node being upgraded,
// over the connection this package already has. Checking the running image
// rather than talosctl's config-version annotation is if anything the
// stricter of the two — it cannot pass on a pod that kept its old image.
func staticPodRolledOut(
	ctx context.Context, talosClient *talosclient.Client, component, image string,
) (bool, error) {
	statuses, err := safe.StateList[*k8s.StaticPodStatus](ctx, talosClient.COSI,
		resource.NewMetadata(k8s.NamespaceName, k8s.StaticPodStatusType, "", resource.VersionUndefined))
	if err != nil {
		return false, fmt.Errorf("failed to list static pod statuses: %w", err)
	}

	prefix := staticPodNamespace + "/" + component + "-"

	for status := range statuses.All() {
		if !strings.HasPrefix(status.Metadata().ID(), prefix) {
			continue
		}

		podStatus, err := decodePodStatus(status)
		if err != nil {
			return false, err
		}

		return podRunsImage(podStatus, image) && podIsReady(podStatus), nil
	}

	// Talos has not reported this pod yet, which is the normal state for
	// the first moments after a patch: not an error, just not rolled out.
	return false, nil
}

// decodePodStatus turns a StaticPodStatus's own untyped spec back into the
// Kubernetes PodStatus Talos serialized into it.
func decodePodStatus(status *k8s.StaticPodStatus) (corev1.PodStatus, error) {
	raw, err := json.Marshal(status.TypedSpec().PodStatus)
	if err != nil {
		return corev1.PodStatus{}, fmt.Errorf("failed to encode static pod status %q: %w", status.Metadata().ID(), err)
	}

	var podStatus corev1.PodStatus

	err = json.Unmarshal(raw, &podStatus)
	if err != nil {
		return corev1.PodStatus{}, fmt.Errorf("failed to decode static pod status %q: %w", status.Metadata().ID(), err)
	}

	return podStatus, nil
}

// podRunsImage reports whether any of the pod's containers is running
// image. The component's own container is the only one in these pods that
// carries a Kubernetes version, so no name match is needed.
func podRunsImage(status corev1.PodStatus, image string) bool {
	for _, container := range status.ContainerStatuses {
		if container.Image == image {
			return true
		}
	}

	return false
}

// podIsReady reports whether the pod carries a true Ready condition.
func podIsReady(status corev1.PodStatus) bool {
	for _, condition := range status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}
