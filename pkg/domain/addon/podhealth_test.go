package addon_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/nicklasfrahm/kontinuum/pkg/domain/addon"
)

const podHealthTestNamespace = "kontinuum-system"

func instanceSelector() []string {
	return []string{"app.kubernetes.io/instance=my-release"}
}

func TestHasWorkloadControllersEmptyNamespace(t *testing.T) {
	t.Parallel()

	clientset := k8sfake.NewClientset()

	has, err := addon.HasWorkloadControllers(context.Background(), clientset, podHealthTestNamespace, instanceSelector())
	require.NoError(t, err)
	assert.False(t, has, "a namespace with no workload objects at all must report none present")
}

// TestHasWorkloadControllersDetectsEachKind covers every kind
// addon.HasWorkloadControllers checks — a CRD-only chart's namespace has
// none of these; a real workload chart (Deployment, DaemonSet, ...) has
// at least one, even before its own controller has created any pod —
// see that function's own doc for why NamespaceHealthy relies on it
// instead of treating a namespace with zero pods as vacuously healthy.
func TestHasWorkloadControllersDetectsEachKind(t *testing.T) {
	t.Parallel()

	labels := map[string]string{"app.kubernetes.io/instance": "my-release"}

	tests := []struct {
		name   string
		object runtime.Object
	}{
		{"Job", &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: podHealthTestNamespace, Labels: labels}}},
		{
			"CronJob",
			&batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "cronjob", Namespace: podHealthTestNamespace, Labels: labels}},
		},
		{
			"Deployment",
			&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "deploy", Namespace: podHealthTestNamespace, Labels: labels}},
		},
		{
			"DaemonSet",
			&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "ds", Namespace: podHealthTestNamespace, Labels: labels}},
		},
		{
			"StatefulSet",
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "sts", Namespace: podHealthTestNamespace, Labels: labels}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			clientset := k8sfake.NewClientset(test.object)

			has, err := addon.HasWorkloadControllers(context.Background(), clientset, podHealthTestNamespace, instanceSelector())
			require.NoError(t, err)
			assert.True(t, has, "%s must be detected", test.name)
		})
	}
}

// TestHasWorkloadControllersIgnoresOtherReleases covers the real bug
// this whole selector scheme fixes: two different addons' releases can
// share a namespace (e.g. gateway-api-crds and cert-manager both
// install into kontinuum-system) — a Deployment belonging to a
// *different* release must never count as "my-release has a workload
// controller", or that other release's own pod startup churn would
// make my-release's own Ready condition flap.
func TestHasWorkloadControllersIgnoresOtherReleases(t *testing.T) {
	t.Parallel()

	otherRelease := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "other-deploy", Namespace: podHealthTestNamespace,
			Labels: map[string]string{"app.kubernetes.io/instance": "some-other-release"},
		},
	}

	clientset := k8sfake.NewClientset(otherRelease)

	has, err := addon.HasWorkloadControllers(context.Background(), clientset, podHealthTestNamespace, instanceSelector())
	require.NoError(t, err)
	assert.False(t, has, "a Deployment belonging to a different release must not count toward my-release")
}
