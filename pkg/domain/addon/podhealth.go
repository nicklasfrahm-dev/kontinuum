package addon

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// PodProber checks whether every pod in a namespace is healthy — the seam
// Reconciler gates an addon's own Ready condition through once its
// manifests have been applied (see installRelease's own doc for why that
// apply is non-blocking, leaving pod health as a separate, probed-later
// step). podProber is the production implementation, building a real
// client-go clientset from a kubeconfig; tests inject a fake to avoid a
// real cluster dependency.
type PodProber interface {
	// NamespaceHealthy reports whether every pod in namespace is healthy —
	// Running with its PodReady condition true, or Succeeded (a completed
	// one-shot Job pod, e.g. cert-manager's own startupapicheck, is
	// expected, not a failure) — and a human-readable reason when it
	// isn't, or when namespace has no pods yet.
	NamespaceHealthy(ctx context.Context, kubeconfig []byte, namespace string) (bool, string, error)
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

// NamespaceHealthy implements PodProber.
func (podProber) NamespaceHealthy(ctx context.Context, kubeconfig []byte, namespace string) (bool, string, error) {
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

	if len(pods.Items) == 0 {
		return false, "no pods found yet", nil
	}

	for _, pod := range pods.Items {
		ok, reason := podHealthy(&pod)
		if !ok {
			return false, reason, nil
		}
	}

	return true, "", nil
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
