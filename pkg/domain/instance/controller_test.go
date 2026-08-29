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
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
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
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

const (
	testRetryInterval   = 30 * time.Second
	testRecheckInterval = 5 * time.Minute

	reasonDiscoveredFixture = "Discovered"
	nodeAName               = "node-a"
	nodeBName               = "node-b"
	nodeCName               = "node-c"
	nodeFName               = "node-f"
	nodeGName               = "node-g"
	nodeHName               = "node-h"
	nodeIName               = "node-i"

	candidateAddress1  = "10.0.0.1"
	candidateAddress2  = "10.0.0.2"
	candidateInterface = "eth0"

	talosVersionFixture = "v1.9.0"
	talosArchFixture    = "amd64"

	diskDevPathFixture   = "/dev/sda"
	diskTransportFixture = "nvme"
)

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
	disks        []v1alpha2.InstanceDiskStatus
	cpus         []v1alpha2.InstanceCPUStatus
	memory       []v1alpha2.InstanceMemoryStatus
	serialNumber string
	err          error
}

// fakeDiscoverer is instance.Discoverer's test double — it never dials real
// gRPC, and records every address it was asked to probe so tests can assert
// on call order/count (e.g. that an already-Discovered Instance is never
// probed again). hardwareCalls separately records just the addresses probed
// with includeHardware set, for TestReconcileOnlyReprobesHardwareWhenNotAlreadyDiscovered.
type fakeDiscoverer struct {
	results       map[string]fakeResult
	calls         []string
	hardwareCalls []string
}

func (f *fakeDiscoverer) Discover(
	_ context.Context, addr string, includeHardware bool,
) (instance.DiscoveryResult, error) {
	f.calls = append(f.calls, addr)

	if includeHardware {
		f.hardwareCalls = append(f.hardwareCalls, addr)
	}

	res, ok := f.results[addr]
	if !ok {
		return instance.DiscoveryResult{}, fmt.Errorf("%w: %s", errNoResultConfigured, addr)
	}

	result := instance.DiscoveryResult{TalosVersion: res.talosVersion, Interfaces: res.interfaces}

	// Mirrors talosDiscoverer's own real behavior: hardware fields are only
	// populated when includeHardware is set — see Discover's own doc.
	if includeHardware {
		result.Disks = res.disks
		result.CPUs = res.cpus
		result.Memory = res.memory
		result.SerialNumber = res.serialNumber
	}

	return result, res.err
}

// discoveredResult is the fakeResult a successfully-probed candidate
// returns — talosVersionFixture and one candidateInterface interface,
// reused across every test below that just needs "discovery succeeded"
// without a distinct version/interface of its own.
func discoveredResult() fakeResult {
	return fakeResult{
		talosVersion: talosVersionFixture,
		interfaces:   []v1alpha2.InstanceInterfaceStatus{{Name: candidateInterface}},
	}
}

// statusUpdateCountingClient wraps a client.Client, counting every
// Status().Update call made through it — see
// TestReconcileSkipsRedundantStatusUpdate's own doc for what this is used
// to verify.
type statusUpdateCountingClient struct {
	client.Client

	statusUpdates *int
}

//nolint:ireturn // client.Client's own Status() signature dictates this; wrapping it is the point.
func (c statusUpdateCountingClient) Status() client.SubResourceWriter {
	return countingStatusWriter{c.Client.Status(), c.statusUpdates}
}

type countingStatusWriter struct {
	client.SubResourceWriter

	count *int
}

func (w countingStatusWriter) Update(
	ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption,
) error {
	*w.count++

	err := w.SubResourceWriter.Update(ctx, obj, opts...)
	if err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	return nil
}

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	err := v1alpha2.AddToScheme(scheme)
	require.NoError(t, err)
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, coordinationv1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha2.Instance{}).
		WithObjects(objects...).
		Build()
}

func newReconciler(fakeClient client.Client, discoverer instance.Discoverer) *instance.Reconciler {
	return &instance.Reconciler{
		Client:          fakeClient,
		Discoverer:      discoverer,
		DialTimeout:     time.Second,
		RetryInterval:   testRetryInterval,
		RecheckInterval: testRecheckInterval,
		Locker:          zonelease.NewLocker(fakeClient, fakeClient, "test-hub", "", 0),
		Logger:          slog.Default(),
	}
}

func TestReconcileDiscoversOnFirstCandidate(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: nodeAName},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{candidateAddress1}},
	}

	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{
		candidateAddress1: discoveredResult(),
	}}

	reconciler := newReconciler(fakeClient, discoverer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: nodeAName},
	})
	require.NoError(t, err)
	assert.Equal(t, testRecheckInterval, result.RequeueAfter,
		"a successful discovery keeps periodically rechecking while still unclaimed")
	assert.Equal(t, []string{candidateAddress1}, discoverer.calls)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: nodeAName}, &got))
	assert.Equal(t, talosVersionFixture, got.Status.Talos.Version)
	assert.Equal(t, []v1alpha2.InstanceInterfaceStatus{{Name: candidateInterface}}, got.Status.Interfaces)
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, instance.DiscoveredConditionType))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, instance.LiveConditionType),
		"Live mirrors Discovered pre-claim — see LiveConditionType's own doc")
	assert.False(t, got.Status.LastProbeTime.IsZero(), "LastProbeTime is stamped on every probe, successful or not")
}

// TestReconcileDiscoversHardwareInventory covers issue #76: a successful
// probe persists Disks/CPUs/Memory/SerialNumber alongside Interfaces,
// straight through from Discoverer's own DiscoveryResult.
func TestReconcileDiscoversHardwareInventory(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: nodeHName},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{"10.0.0.1"}},
	}

	disks := []v1alpha2.InstanceDiskStatus{
		{DevPath: diskDevPathFixture, PrettySize: "512 GB", Transport: diskTransportFixture},
	}
	cpus := []v1alpha2.InstanceCPUStatus{{ProductName: "AMD EPYC 7302P", CoreCount: 16, ThreadCount: 32}}
	memory := []v1alpha2.InstanceMemoryStatus{{SizeMiB: 32768, Manufacturer: "Micron"}}

	const serialNumber = "SN-1234567890"

	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{
		"10.0.0.1": {talosVersion: "v1.9.0", disks: disks, cpus: cpus, memory: memory, serialNumber: serialNumber},
	}}

	reconciler := newReconciler(fakeClient, discoverer)

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: nodeHName},
	})
	require.NoError(t, err)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: nodeHName}, &got))
	assert.Equal(t, disks, got.Status.Disks)
	assert.Equal(t, cpus, got.Status.CPUs)
	assert.Equal(t, memory, got.Status.Memory)
	assert.Equal(t, serialNumber, got.Status.SerialNumber)
}

// TestReconcileOnlyReprobesHardwareWhenNotAlreadyDiscovered covers the
// other half of issue #76's hardware-inventory scope note: hardware is
// enumerated on first discovery, then skipped on a steady-state recheck of
// an already-Live candidate — see probeCandidates' own doc. The
// re-enumerate-after-an-outage half is covered separately by
// TestReconcileReenumeratesHardwareAfterOutage.
func TestReconcileOnlyReprobesHardwareWhenNotAlreadyDiscovered(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: nodeIName},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{candidateAddress1}},
	}

	disks := []v1alpha2.InstanceDiskStatus{
		{DevPath: diskDevPathFixture, PrettySize: "512 GB", Transport: diskTransportFixture},
	}
	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{
		candidateAddress1: {talosVersion: talosVersionFixture, disks: disks},
	}}

	reconciler := newReconciler(fakeClient, discoverer)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: nodeIName}}

	// First reconcile: not yet Discovered, so hardware is enumerated.
	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, []string{candidateAddress1}, discoverer.hardwareCalls,
		"first discovery must enumerate hardware")

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: nodeIName}, &got))
	assert.Equal(t, disks, got.Status.Disks)

	// Second reconcile: already Live=true, so hardware is skipped — Disks
	// must survive untouched even though the fake returns none this round.
	_, err = reconciler.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, []string{candidateAddress1}, discoverer.hardwareCalls,
		"a steady-state recheck of an already-Live candidate must not re-enumerate hardware")

	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: nodeIName}, &got))
	assert.Equal(t, disks, got.Status.Disks, "Disks must survive a hardware-skipped recheck untouched")
}

// TestReconcileReenumeratesHardwareAfterOutage covers the rest of issue
// #76's hardware-inventory scope note: Discovered is a one-time latch (see
// its own doc) that survives an outage, but Live doesn't, and it's Live —
// not Discovered — that gates hardware re-enumeration (see probeCandidates'
// own doc), so a reboot correctly re-enumerates and can pick up a disk
// swap made while the node was down.
func TestReconcileReenumeratesHardwareAfterOutage(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: nodeIName},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{candidateAddress1}},
		Status: v1alpha2.InstanceStatus{
			Conditions: []metav1.Condition{
				{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: reasonDiscoveredFixture},
				{Type: instance.LiveConditionType, Status: metav1.ConditionTrue, Reason: reasonDiscoveredFixture},
			},
		},
	}

	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{
		candidateAddress1: {err: errConnectionRefused},
	}}

	reconciler := newReconciler(fakeClient, discoverer)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: nodeIName}}

	// The candidate is unreachable: Live flips false, Discovered stays true.
	_, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: nodeIName}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, instance.DiscoveredConditionType),
		"Discovered is a one-time latch and must not revert")
	assert.False(t, meta.IsStatusConditionTrue(got.Status.Conditions, instance.LiveConditionType))

	// The node comes back with a swapped disk: since Live was false on
	// entry, this reconcile must re-enumerate hardware and pick it up.
	newDisks := []v1alpha2.InstanceDiskStatus{
		{DevPath: diskDevPathFixture, PrettySize: "1 TB", Transport: diskTransportFixture},
	}
	discoverer.results = map[string]fakeResult{
		candidateAddress1: {talosVersion: talosVersionFixture, disks: newDisks},
	}

	_, err = reconciler.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, []string{candidateAddress1}, discoverer.hardwareCalls,
		"rediscovery after an outage must re-enumerate hardware")

	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: nodeIName}, &got))
	assert.Equal(t, newDisks, got.Status.Disks, "the swapped disk discovered while down must be picked up")
}

func TestReconcileFallsBackToNextCandidate(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: nodeBName},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{candidateAddress1, candidateAddress2}},
	}

	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{
		candidateAddress1: {err: errConnectionRefused},
		candidateAddress2: discoveredResult(),
	}}

	reconciler := newReconciler(fakeClient, discoverer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: nodeBName},
	})
	require.NoError(t, err)
	assert.Equal(t, testRecheckInterval, result.RequeueAfter)
	assert.Equal(t, []string{candidateAddress1, candidateAddress2}, discoverer.calls)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: nodeBName}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, instance.DiscoveredConditionType))
}

func TestReconcileAllCandidatesFail(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: nodeCName},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{candidateAddress1, candidateAddress2}},
	}

	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{
		candidateAddress1: {err: errConnectionRefused},
		candidateAddress2: {err: errIOTimeout},
	}}

	reconciler := newReconciler(fakeClient, discoverer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: nodeCName},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: nodeCName}, &got))

	assert.Nil(t, meta.FindStatusCondition(got.Status.Conditions, instance.DiscoveredConditionType),
		"a candidate that's never once been successfully probed must not get a Discovered condition at all — "+
			"it's a one-time latch, only ever set on success")
	assert.False(t, meta.IsStatusConditionTrue(got.Status.Conditions, instance.LiveConditionType))
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

// TestReconcileSkipsClaimedInstance covers Reconcile's own claimed
// short-circuit: once v1alpha2.LabelClaimedBy is set, taloscluster's own
// member reconciler owns this Instance's progress from there — see issue
// #62's own follow-up. It must never be touched again, discovered or not,
// or a maintenance-mode probe that's now *expected* to fail (the node has
// left maintenance mode) would incorrectly flip Discovered back to false.
func TestReconcileSkipsClaimedInstance(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-e",
			Labels: map[string]string{v1alpha2.LabelClaimedBy: "cp-pool"},
		},
		Spec: v1alpha2.InstanceSpec{Interfaces: []string{candidateAddress1}},
		Status: v1alpha2.InstanceStatus{
			Talos: v1alpha2.InstanceTalosStatus{Version: talosVersionFixture},
			Conditions: []metav1.Condition{
				{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: reasonDiscoveredFixture},
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
	assert.Empty(t, discoverer.calls, "a claimed instance must not be re-probed")
}

// TestReconcileRechecksUnclaimedDiscoveredInstance covers the opposite case:
// an already-Discovered Instance that's still unclaimed keeps being
// periodically re-probed (RecheckInterval) — a bare node sitting in the
// discovery pool can go offline before anything claims it, and without
// this, a stale Discovered=True would stand forever.
func TestReconcileRechecksUnclaimedDiscoveredInstance(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: nodeFName},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{candidateAddress1}},
		Status: v1alpha2.InstanceStatus{
			Talos: v1alpha2.InstanceTalosStatus{Version: talosVersionFixture},
			Conditions: []metav1.Condition{
				{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: reasonDiscoveredFixture},
			},
		},
	}

	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{
		candidateAddress1: discoveredResult(),
	}}

	reconciler := newReconciler(fakeClient, discoverer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: nodeFName},
	})
	require.NoError(t, err)
	assert.Equal(t, testRecheckInterval, result.RequeueAfter)
	assert.Equal(t, []string{candidateAddress1}, discoverer.calls,
		"an unclaimed instance must be re-probed even if already Discovered")

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: nodeFName}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, instance.DiscoveredConditionType))
}

// TestReconcileSkipsRedundantStatusUpdate guards against a reconcile
// storm: this controller's own Instance watch (see SetupWithManager)
// carries no predicate, so any Status().Update — even one that changes
// nothing — re-triggers Reconcile. obj already carries the exact
// Talos.Version/Interfaces/condition a successful reprobe of
// candidateAddress1 via discoveredResult() would recompute, so a second
// reconcile is a true no-op and must not write status again.
func TestReconcileSkipsRedundantStatusUpdate(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: nodeGName},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{candidateAddress1}},
		Status: v1alpha2.InstanceStatus{
			Talos:      v1alpha2.InstanceTalosStatus{Version: talosVersionFixture},
			Interfaces: []v1alpha2.InstanceInterfaceStatus{{Name: candidateInterface}},
			Conditions: []metav1.Condition{
				{
					Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue,
					Reason: reasonDiscoveredFixture, Message: "discovered via " + candidateAddress1,
				},
				{
					Type: instance.LiveConditionType, Status: metav1.ConditionTrue,
					Reason: reasonDiscoveredFixture, Message: "discovered via " + candidateAddress1,
				},
			},
			LastProbeTime: metav1.Now(),
		},
	}

	statusUpdates := 0
	fakeClient := statusUpdateCountingClient{newFakeClient(t, obj), &statusUpdates}
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{candidateAddress1: discoveredResult()}}
	reconciler := newReconciler(fakeClient, discoverer)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: nodeGName}}

	_, err := reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 0, statusUpdates, "reprobing to an identical result should not write status")

	_, err = reconciler.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 0, statusUpdates, "second reconcile computes the same result and should not write again")
}

// TestReconcileFlipsDiscoveredFalseWhenUnclaimedNodeGoesOffline covers the
// recheck actually catching something: a previously Discovered, still
// unclaimed Instance whose candidate no longer answers must flip back to
// Discovered=False, and retry sooner (RetryInterval) rather than waiting a
// full RecheckInterval to try again.
// TestReconcileFlipsLiveFalseButKeepsDiscoveredWhenUnclaimedNodeGoesOffline
// covers DiscoveredConditionType's own one-time-latch doc: Discovered
// stays true once achieved, even through a later outage, while Live —
// unlike Discovered — tracks current reachability and flips false right
// away. See instancepool's own claim gate for why Live still has to be
// checked separately even though Discovered alone doesn't revert.
func TestReconcileFlipsLiveFalseButKeepsDiscoveredWhenUnclaimedNodeGoesOffline(t *testing.T) {
	t.Parallel()

	obj := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: nodeGName},
		Spec:       v1alpha2.InstanceSpec{Interfaces: []string{candidateAddress1}},
		Status: v1alpha2.InstanceStatus{
			Talos: v1alpha2.InstanceTalosStatus{Version: talosVersionFixture},
			Conditions: []metav1.Condition{
				{Type: instance.DiscoveredConditionType, Status: metav1.ConditionTrue, Reason: reasonDiscoveredFixture},
				{Type: instance.LiveConditionType, Status: metav1.ConditionTrue, Reason: reasonDiscoveredFixture},
			},
		},
	}

	fakeClient := newFakeClient(t, obj)
	discoverer := &fakeDiscoverer{results: map[string]fakeResult{
		candidateAddress1: {err: errConnectionRefused},
	}}

	reconciler := newReconciler(fakeClient, discoverer)

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: nodeGName},
	})
	require.NoError(t, err)
	assert.Equal(t, testRetryInterval, result.RequeueAfter)

	var got v1alpha2.Instance

	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{Name: nodeGName}, &got))
	assert.True(t, meta.IsStatusConditionTrue(got.Status.Conditions, instance.DiscoveredConditionType),
		"Discovered is a one-time latch — it must not revert just because the node stopped answering")
	assert.False(t, meta.IsStatusConditionTrue(got.Status.Conditions, instance.LiveConditionType),
		"Live tracks current reachability, unlike Discovered, so it flips false right away")
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
