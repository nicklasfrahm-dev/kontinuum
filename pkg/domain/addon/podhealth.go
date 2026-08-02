package addon

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// PodProber checks whether every pod belonging to one addon's own
// release is healthy — the seam Reconciler gates an addon's own Ready
// condition through once its manifests have been applied (see
// installRelease's own doc for why that apply is non-blocking, leaving
// pod health as a separate, probed-later step). podProber is the
// production implementation, building a real client-go clientset from a
// kubeconfig; tests inject a fake to avoid a real cluster dependency.
type PodProber interface {
	// NamespaceHealthy reports whether every pod in namespace belonging
	// to this release — matched via releaseSelectors, see that
	// function's own doc for why two different label conventions are
	// checked — is healthy: Running with its PodReady condition true, or
	// Succeeded (a completed one-shot Job pod, e.g. cert-manager's own
	// startupapicheck, is expected, not a failure) — and a human-readable
	// reason when it isn't. Deliberately scoped to this release's own
	// pods, not every pod in namespace: two different addons can share a
	// namespace (see gateway-api-crds' and cert-manager's own values
	// files), and an unscoped scan would make one addon's Ready condition
	// flap based on a completely unrelated addon's own pod churn. Zero
	// matching pods is healthy only if this release also has no workload
	// controller (Job, CronJob, Deployment, DaemonSet, StatefulSet) of
	// its own that could ever create one — some addons (e.g. a CRD-only
	// chart) legitimately never install any pod, but a Deployment/
	// DaemonSet/etc. that simply hasn't created its pods yet (its own
	// controller hasn't caught up right after install) must not be
	// mistaken for that same "no pods" state.
	NamespaceHealthy(
		ctx context.Context, kubeconfig []byte, namespace, releaseName, chartLabel string,
	) (bool, string, error)
}

// podProber is PodProber's production implementation.
type podProber struct{}

// NewPodProber returns the production PodProber, which checks real pods in
// a real cluster. PodProber is this package's own seam for injecting a
// fake in tests.
//
//nolint:ireturn // see doc above
func NewPodProber() PodProber {
	return podProber{}
}

// helmChartLabel computes the helm.sh/chart label value a Helm chart's
// own standard _helpers.tpl scaffold sets ("<chart name>-<version>") —
// used as one of releaseSelectors' two selectors (see that function's
// own doc). Returns "" — meaning "skip this selector" — when chartName
// isn't a bare chart name at all (e.g. an OCI reference like
// gateway-api-crds' own "oci://..."), since the computed value wouldn't
// be a valid label value, and Chart.yaml's own internal name (what the
// chart's templates actually use) isn't recoverable from an OCI
// reference string alone.
func helmChartLabel(chartName, version string) string {
	label := chartName + "-" + version
	if len(validation.IsValidLabelValue(label)) > 0 {
		return ""
	}

	return label
}

// releaseSelectors returns every label selector NamespaceHealthy/
// HasWorkloadControllers check to find one release's own resources.
// Real charts are inconsistent about which of Helm's own two
// conventional release labels they set on every resource — e.g.
// cert-manager's own chart sets app.kubernetes.io/instance on its pods,
// while cilium's own chart never does, only helm.sh/chart — so both are
// checked and their results unioned, rather than trusting either alone.
// chartLabel empty (see helmChartLabel's own doc) just means fewer
// selectors to check, not an error.
func releaseSelectors(releaseName, chartLabel string) []string {
	selectors := []string{"app.kubernetes.io/instance=" + releaseName}
	if chartLabel != "" {
		selectors = append(selectors, "helm.sh/chart="+chartLabel)
	}

	return selectors
}

// AllPodsHealthy reports whether every pod in namespace is healthy, with
// no release/chart filtering at all — a broader sanity check than
// NamespaceHealthy's own one-release scope, for callers (e.g. the real
// e2e test, confirming a just-joined worker node's entire kube-system
// converges, not just one specific addon's own pods) that want to
// confirm literally nothing in a namespace is broken.
func AllPodsHealthy(ctx context.Context, kubeconfig []byte, namespace string) (bool, string, error) {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return false, "", fmt.Errorf("failed to build rest config from kubeconfig: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return false, "", fmt.Errorf("failed to build kubernetes clientset: %w", err)
	}

	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, "", fmt.Errorf("failed to list pods in %q: %w", namespace, err)
	}

	for _, pod := range pods.Items {
		ok, reason := podHealthy(&pod)
		if !ok {
			return false, reason, nil
		}
	}

	return true, "", nil
}

// NamespaceHealthy implements PodProber.
func (podProber) NamespaceHealthy(
	ctx context.Context, kubeconfig []byte, namespace, releaseName, chartLabel string,
) (bool, string, error) {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return false, "", fmt.Errorf("failed to build rest config from kubeconfig: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return false, "", fmt.Errorf("failed to build kubernetes clientset: %w", err)
	}

	selectors := releaseSelectors(releaseName, chartLabel)

	pods, err := listPodsUnion(ctx, clientset, namespace, selectors)
	if err != nil {
		return false, "", err
	}

	if len(pods) == 0 {
		return noPodsHealthy(ctx, clientset, namespace, selectors)
	}

	for _, pod := range pods {
		ok, reason := podHealthy(&pod)
		if !ok {
			return false, reason, nil
		}
	}

	return true, "", nil
}

// listPodsUnion lists namespace's pods matching any one of selectors,
// deduped by UID (a pod could in principle match more than one
// selector) — see releaseSelectors' own doc for why more than one
// selector is checked at all.
func listPodsUnion(
	ctx context.Context, clientset kubernetes.Interface, namespace string, selectors []string,
) ([]corev1.Pod, error) {
	seen := make(map[types.UID]corev1.Pod)

	for _, selector := range selectors {
		list, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return nil, fmt.Errorf("failed to list pods in %q matching %q: %w", namespace, selector, err)
		}

		for _, pod := range list.Items {
			seen[pod.UID] = pod
		}
	}

	pods := make([]corev1.Pod, 0, len(seen))
	for _, pod := range seen {
		pods = append(pods, pod)
	}

	return pods, nil
}

// noPodsHealthy handles NamespaceHealthy's own "zero matching pods"
// case — see that method's own doc for why this isn't simply vacuous
// truth.
func noPodsHealthy(
	ctx context.Context, clientset kubernetes.Interface, namespace string, selectors []string,
) (bool, string, error) {
	hasControllers, err := HasWorkloadControllers(ctx, clientset, namespace, selectors)
	if err != nil {
		return false, "", err
	}

	if hasControllers {
		return false, "no pods found yet", nil
	}

	return true, "", nil
}

// existenceListLimit bounds every HasWorkloadControllers list
// call to a single item — only existence is checked, never the actual
// content.
const existenceListLimit = 1

// workloadControllerKinds lists every kind HasWorkloadControllers checks
// for, purely for its own error message's sake — kept in sync with the
// list() closures below by construction (each closure corresponds 1:1
// with an entry here in the same order). A function, not a package-level
// slice, so nothing can mutate the shared backing array.
func workloadControllerKinds() []string {
	return []string{"Job", "CronJob", "Deployment", "DaemonSet", "StatefulSet"}
}

// HasWorkloadControllers reports whether namespace contains any object
// of a kind that creates pods asynchronously, sometime after being
// applied (Job, CronJob, Deployment, DaemonSet, StatefulSet), matching
// any one of selectors — exported purely so it's independently
// unit-testable against a fake clientset; see NamespaceHealthy's own
// doc for why noPodsHealthy needs it, and releaseSelectors' own doc for
// why more than one selector is checked at all.
func HasWorkloadControllers(
	ctx context.Context, clientset kubernetes.Interface, namespace string, selectors []string,
) (bool, error) {
	listers := []func(metav1.ListOptions) (int, error){
		func(opts metav1.ListOptions) (int, error) {
			list, err := clientset.BatchV1().Jobs(namespace).List(ctx, opts)

			return len(list.Items), err
		},
		func(opts metav1.ListOptions) (int, error) {
			list, err := clientset.BatchV1().CronJobs(namespace).List(ctx, opts)

			return len(list.Items), err
		},
		func(opts metav1.ListOptions) (int, error) {
			list, err := clientset.AppsV1().Deployments(namespace).List(ctx, opts)

			return len(list.Items), err
		},
		func(opts metav1.ListOptions) (int, error) {
			list, err := clientset.AppsV1().DaemonSets(namespace).List(ctx, opts)

			return len(list.Items), err
		},
		func(opts metav1.ListOptions) (int, error) {
			list, err := clientset.AppsV1().StatefulSets(namespace).List(ctx, opts)

			return len(list.Items), err
		},
	}

	kinds := workloadControllerKinds()

	for i, list := range listers {
		count, err := listAcrossSelectors(list, selectors)
		if err != nil {
			return false, fmt.Errorf("failed to list %s in %q: %w", kinds[i], namespace, err)
		}

		if count > 0 {
			return true, nil
		}
	}

	return false, nil
}

// listAcrossSelectors calls list once per selector (existenceListLimit-
// bounded), returning as soon as any of them find at least one match —
// see releaseSelectors' own doc for why a single kind can need more
// than one selector checked.
func listAcrossSelectors(list func(metav1.ListOptions) (int, error), selectors []string) (int, error) {
	for _, selector := range selectors {
		count, err := list(metav1.ListOptions{Limit: existenceListLimit, LabelSelector: selector})
		if err != nil {
			return 0, err
		}

		if count > 0 {
			return count, nil
		}
	}

	return 0, nil
}

// podHealthy reports whether pod is either Running with its PodReady
// condition true, or Succeeded — see NamespaceHealthy's own doc for why a
// completed Job pod counts as healthy rather than a failure. Any other
// phase's reason includes the pod's own condition messages (e.g. why
// PodScheduled is False) — a bare "is Pending" gives no way to tell a
// stalled image pull from an unschedulable pod from anything else.
//
//nolint:exhaustive // Pending/Failed/Unknown are deliberately grouped under default: none of them are healthy
func podHealthy(pod *corev1.Pod) (bool, string) {
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		return true, ""
	case corev1.PodRunning:
		return podCondition(pod, corev1.PodReady, "Running but not Ready", "Running with no PodReady condition yet")
	default:
		reason := fmt.Sprintf("%s/%s is %s", pod.Namespace, pod.Name, pod.Status.Phase)
		if detail := podConditionDetail(pod); detail != "" {
			reason += ": " + detail
		}

		if detail := initContainerDetail(pod); detail != "" {
			reason += "; " + detail
		}

		return false, reason
	}
}

// podCondition checks pod's condType condition, returning notTrueMsg/
// missingMsg-derived reasons (prefixed the same way podHealthy's other
// branches are) when it isn't true or isn't present at all.
func podCondition(pod *corev1.Pod, condType corev1.PodConditionType, notTrueMsg, missingMsg string) (bool, string) {
	for _, cond := range pod.Status.Conditions {
		if cond.Type != condType {
			continue
		}

		if cond.Status == corev1.ConditionTrue {
			return true, ""
		}

		return false, fmt.Sprintf("%s/%s is %s", pod.Namespace, pod.Name, notTrueMsg)
	}

	return false, fmt.Sprintf("%s/%s is %s", pod.Namespace, pod.Name, missingMsg)
}

// podConditionDetail returns the most relevant non-true condition's own
// message (e.g. PodScheduled=False's "0/1 nodes are available: ..."), or
// the empty string if every condition is either true or carries no
// message. Used to explain non-Running/Succeeded phases like Pending,
// where the phase alone ("is Pending") doesn't say why.
func podConditionDetail(pod *corev1.Pod) string {
	for _, cond := range pod.Status.Conditions {
		if cond.Status != corev1.ConditionTrue && cond.Message != "" {
			return string(cond.Type) + "=" + string(cond.Status) + ": " + cond.Message
		}
	}

	return ""
}

// initContainerDetail returns the state of the first init container that
// hasn't terminated successfully — e.g. "clean-cilium-state is waiting:
// CrashLoopBackOff" or "clean-cilium-state is running (started at ...)".
// Init containers run sequentially with no built-in timeout: unlike a
// failing readiness probe (bounded by failureThreshold), a single init
// container that hangs (stuck on a syscall, crash-looping, blocked on an
// image pull) leaves the pod in bare Pending indefinitely, and
// podConditionDetail's Initialized=False message only lists container
// *names*, not which one is actually stuck or why. Returns "" if every
// init container status is unavailable or already Terminated
// successfully.
func initContainerDetail(pod *corev1.Pod) string {
	for _, status := range pod.Status.InitContainerStatuses {
		if status.State.Terminated != nil && status.State.Terminated.ExitCode == 0 {
			continue
		}

		switch {
		case status.State.Waiting != nil:
			return waitingContainerDetail(status)
		case status.State.Running != nil:
			return fmt.Sprintf("%s is running (started at %s)", status.Name, status.State.Running.StartedAt)
		case status.State.Terminated != nil:
			return terminatedContainerDetail(status)
		}
	}

	return ""
}

// waitingContainerDetail formats status's Waiting reason/message, plus its
// restart count and (when available) the previous attempt's own exit
// detail — see initContainerDetail's own doc for why CrashLoopBackOff's
// Waiting message alone doesn't say why the container actually failed.
func waitingContainerDetail(status corev1.ContainerStatus) string {
	detail := status.Name + " is waiting: " + status.State.Waiting.Reason
	if status.State.Waiting.Message != "" {
		detail += ": " + status.State.Waiting.Message
	}

	if status.RestartCount > 0 {
		detail += fmt.Sprintf(" (restarted %d times)", status.RestartCount)
	}

	// CrashLoopBackOff's own Waiting.Message is just the backoff timer, not
	// why the container actually failed — that's on LastTerminationState,
	// the previous attempt's own exit detail.
	if last := status.LastTerminationState.Terminated; last != nil {
		detail += fmt.Sprintf("; last exit %d: %s", last.ExitCode, last.Reason)
		if last.Message != "" {
			detail += ": " + last.Message
		}
	}

	return detail
}

// terminatedContainerDetail formats status's own Terminated detail for a
// container that exited non-zero — initContainerDetail already skips
// zero-exit ones before reaching here.
func terminatedContainerDetail(status corev1.ContainerStatus) string {
	detail := fmt.Sprintf("%s exited %d: %s", status.Name, status.State.Terminated.ExitCode,
		status.State.Terminated.Reason)
	if status.State.Terminated.Message != "" {
		detail += ": " + status.State.Terminated.Message
	}

	return detail
}
