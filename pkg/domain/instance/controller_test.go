package instance_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
)

const testRetryInterval = 30 * time.Second

// errNoResultConfigured, errConnectionRefused, and errIOTimeout are fixed
// test-fixture errors — static, so err113 doesn't flag them as dynamically
// constructed.
var (
	errNoResultConfigured = errors.New("fakeDiscoverer: no result configured for address")
	errConnectionRefused  = errors.New("connection refused")
	errIOTimeout          = errors.New("i/o timeout")
)

// fakeResult is one candidate address's canned Discover outcome.
type fakeResult struct {
	talosVersion string
	interfaces   []v1alpha2.InstanceInterfaceStatus
	err          error
}

// fakeDiscoverer is instance.Discoverer's test double — it never dials real
// gRPC, and records every address it was asked to probe so tests can assert
// on call order/count (e.g. that an already-Discovered Instance is never
// probed again).
type fakeDiscoverer struct {
	results map[string]fakeResult
	calls   []string
}

func (f *fakeDiscoverer) Discover(
	_ context.Context, addr string,
) (string, []v1alpha2.InstanceInterfaceStatus, error) {
	f.calls = append(f.calls, addr)

	res, ok := f.results[addr]
	if !ok {
		return "", nil, fmt.Errorf("%w: %s", errNoResultConfigured, addr)
	}

	return res.talosVersion, res.interfaces, res.err
}

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	err := v1alpha2.AddToScheme(scheme)
	require.NoError(t, err)

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.Instance{}).
		WithObjects(objects...).
		Build()
}

func newReconciler(fakeClient client.Client, discoverer instance.Discoverer) *instance.Reconciler {
	return &instance.Reconciler{
		Client:        fakeClient,
		Discoverer:    discoverer,
		DialTimeout:   time.Second,
		RetryInterval: testRetryInterval,
		Logger:        slog.Default(),
	}
}

func TestReconcileDiscoversOnFirstCandidate(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{"10.0.0.1"}},
	}

	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{
		"10.0.0.1": {talosVersion: "v1.9.0", interfaces: []v1alpha2.InstanceInterfaceStatus{{Name: "eth0"}}},
	}}

	reconciler := newReconciler(fakeClient, discoverer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "node-a"},
	})
	require.NoError(t, err)
	assert.Zero(t, result, "a successful discovery needs no requeue")
	assert.Equal(t, []string{"10.0.0.1"}, discoverer.calls)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-a"}, &got))
	assert.Equal(t, "v1.9.0", got.Status.TalosVersion)
	assert.Equal(t, []v1alpha2.InstanceInterfaceStatus{{Name: "eth0"}}, got.Status.Interfaces)
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, instance.DiscoveredConditionType))
}

func TestReconcileFallsBackToNextCandidate(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "node-b"},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{"10.0.0.1", "10.0.0.2"}},
	}

	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{
		"10.0.0.1": {err: errConnectionRefused},
		"10.0.0.2": {talosVersion: "v1.9.0", interfaces: []v1alpha2.InstanceInterfaceStatus{{Name: "eth0"}}},
	}}

	reconciler := newReconciler(fakeClient, discoverer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "node-b"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)
	assert.Equal(t, []string{"10.0.0.1", "10.0.0.2"}, discoverer.calls)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-b"}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, instance.DiscoveredConditionType))
}

func TestReconcileAllCandidatesFail(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "node-c"},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{"10.0.0.1", "10.0.0.2"}},
	}

	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{
		"10.0.0.1": {err: errConnectionRefused},
		"10.0.0.2": {err: errIOTimeout},
	}}

	reconciler := newReconciler(fakeClient, discoverer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "node-c"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: "node-c"}, &got))

	cond := meta.FindStatusCondition(got.Status.Conditions, instance.DiscoveredConditionType)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
}

func TestReconcileNoInterfacesConfigured(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{ObjectMeta: metav1.ObjectMeta{Name: "node-d"}}

	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{}}

	reconciler := newReconciler(fakeClient, discoverer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "node-d"},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)
	assert.Empty(t, discoverer.calls, "no candidates configured means no dial attempt at all")
}

func TestReconcileSkipsAlreadyDiscovered(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "node-e"},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{"10.0.0.1"}},
		Status: v1alpha2.InstanceStatus{
			TalosVersion: "v1.9.0",
			Conditions: []metav1.Condition{
				{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: "Discovered"},
			},
		},
	}

	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{}}

	reconciler := newReconciler(fakeClient, discoverer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "node-e"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)
	assert.Empty(t, discoverer.calls, "an already-Discovered instance must not be re-probed")
}

func TestReconcileIgnoresMissingInstance(t *testing.T) {
	t.Parallel()

	fakeClient := newFakeClient(t)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{}}

	reconciler := newReconciler(fakeClient, discoverer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing"},
	})
	require.NoError(t, err)
	assert.Zero(t, result)

	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "missing"}, &v1alpha2.Instance{})
	assert.True(t, apierrors.IsNotFound(err))
}
