package instance

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const (
	// DiscoveredConditionType is Instance.status.conditions' condition set
	// once one of spec.interfaces has been successfully probed in
	// maintenance mode.
	DiscoveredConditionType = "Discovered"

	reasonDiscovered  = "Discovered"
	reasonNoInterface = "NoInterfaces"
	reasonProbeFailed = "ProbeFailed"

	defaultDialTimeout   = 10 * time.Second
	defaultRetryInterval = 30 * time.Second
	// defaultRecheckInterval is how often an already-Discovered, still
	// unclaimed Instance gets re-probed — see Reconcile's own doc for why
	// this stops the moment it's claimed.
	defaultRecheckInterval = 5 * time.Minute
)

// Config configures a Controller.
type Config struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// Discoverer probes a candidate address in Talos maintenance mode.
	// Defaults to NewTalosDiscoverer() when nil.
	Discoverer Discoverer
	// DialTimeout bounds each candidate address probe. Defaults to ten
	// seconds when zero.
	DialTimeout time.Duration
	// RetryInterval is how long Reconcile waits before retrying after every
	// candidate in spec.interfaces has failed. Defaults to thirty seconds
	// when zero.
	RetryInterval time.Duration
	// RecheckInterval is how long Reconcile waits before re-probing an
	// already-Discovered Instance that's still unclaimed — see Reconcile's
	// own doc. Defaults to five minutes when zero.
	RecheckInterval time.Duration
}

// Controller wires the Instance discovery reconciler onto a
// controller-runtime Manager. See NewController.
type Controller struct {
	Config Config
}

// NewController builds a Controller from cfg, defaulting Discoverer,
// DialTimeout, RetryInterval, and RecheckInterval when left zero.
func NewController(cfg Config) *Controller {
	if cfg.Discoverer == nil {
		cfg.Discoverer = NewTalosDiscoverer()
	}

	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = defaultDialTimeout
	}

	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = defaultRetryInterval
	}

	if cfg.RecheckInterval == 0 {
		cfg.RecheckInterval = defaultRecheckInterval
	}

	return &Controller{Config: cfg}
}

// SetupWithManager registers the Instance discovery reconciler on mgr. The
// four zone-join CRDs themselves are ensured separately, via EnsureCRDs
// registered as a libkapi.WithPostStartHook (see pkg/cli/serve.go) — not
// here, for the same reason registry.Controller.SetupWithManager's own doc
// gives: SetupWithManager runs before the listener is bound.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	reconciler := &Reconciler{
		Client:          mgr.GetClient(),
		Discoverer:      c.Config.Discoverer,
		DialTimeout:     c.Config.DialTimeout,
		RetryInterval:   c.Config.RetryInterval,
		RecheckInterval: c.Config.RecheckInterval,
		Logger:          c.Config.Logger,
	}

	err := ctrl.NewControllerManagedBy(mgr).For(&v1alpha2.Instance{}).Complete(reconciler)
	if err != nil {
		return fmt.Errorf("failed to register instance controller: %w", err)
	}

	return nil
}

// Reconciler probes an Instance's spec.interfaces in Talos maintenance mode
// and writes the result to status — see issue #27: discovery/probing only,
// no claiming logic yet.
type Reconciler struct {
	Client          client.Client
	Discoverer      Discoverer
	DialTimeout     time.Duration
	RetryInterval   time.Duration
	RecheckInterval time.Duration
	Logger          *slog.Logger
}

// Reconcile implements reconcile.Reconciler. Once an Instance is claimed
// (v1alpha2.LabelClaimedBy set — only ever true for one this reconciler has
// already probed successfully, see instancepool.Reconciler's own claim
// logic), it's never touched again: taloscluster's own member reconciler
// takes over driving its real progress from there (Configured/Joined/Ready
// — see issue #62's own follow-up), and maintenance-mode probing is by then
// *expected* to fail permanently once the node's real config is applied and
// it leaves maintenance mode — re-probing here would misread that expected
// failure as the node going offline and incorrectly flip Discovered back to
// false. Below that point — still unclaimed, whether or not Discovered yet
// — probeCandidates/setDiscovered keep periodically re-verifying it: a bare
// node sitting in the discovery pool can be powered off or unplugged before
// anything ever claims it, and without this, a stale Discovered=True would
// stand forever and let instancepool.Reconciler claim a node that's no
// longer actually reachable.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var inst v1alpha2.Instance

	err := r.Client.Get(ctx, req.NamespacedName, &inst)
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}

	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get instance %q: %w", req.Name, err)
	}

	if _, claimed := inst.Labels[v1alpha2.LabelClaimedBy]; claimed {
		return ctrl.Result{}, nil
	}

	if len(inst.Spec.Interfaces) == 0 {
		return r.setDiscovered(ctx, &inst, metav1.ConditionFalse, reasonNoInterface, "spec.interfaces is empty")
	}

	return r.probeCandidates(ctx, &inst)
}

// probeCandidates tries every inst.Spec.Interfaces entry in order, stopping
// at the first that succeeds.
func (r *Reconciler) probeCandidates(ctx context.Context, inst *v1alpha2.Instance) (ctrl.Result, error) {
	var lastErr error

	for _, candidate := range inst.Spec.Interfaces {
		talosVersion, interfaces, err := r.probe(ctx, candidate)
		if err == nil {
			inst.Status.Talos.Version = talosVersion
			inst.Status.Interfaces = interfaces

			return r.setDiscovered(ctx, inst, metav1.ConditionTrue, reasonDiscovered, "discovered via "+candidate)
		}

		lastErr = err

		r.Logger.Warn("failed to probe instance candidate address",
			"instance", inst.Name, "address", candidate, "error", err)
	}

	return r.setDiscovered(ctx, inst, metav1.ConditionFalse, reasonProbeFailed,
		fmt.Sprintf("all %d candidate(s) failed, last error: %v", len(inst.Spec.Interfaces), lastErr))
}

// probe bounds a single candidate's discovery call by DialTimeout — each
// candidate gets its own budget, rather than sharing one across the whole
// list, so one slow/unreachable candidate can't starve the rest.
func (r *Reconciler) probe(ctx context.Context, addr string) (string, []v1alpha2.InstanceInterfaceStatus, error) {
	dialCtx, cancel := context.WithTimeout(ctx, r.DialTimeout)
	defer cancel()

	talosVersion, interfaces, err := r.Discoverer.Discover(dialCtx, addr)
	if err != nil {
		return "", nil, fmt.Errorf("failed to discover %s: %w", addr, err)
	}

	return talosVersion, interfaces, nil
}

// setDiscovered records DiscoveredConditionType on inst and persists status,
// then requeues so an unclaimed Instance keeps being re-verified — per
// Reconcile's own doc, this is never reached at all once claimed. A false
// status requeues after RetryInterval so a since-fixed (or since-failed)
// candidate gets probed again soon; a true status requeues after the much
// longer RecheckInterval instead, to confirm it's still there without
// hammering it.
func (r *Reconciler) setDiscovered(
	ctx context.Context, inst *v1alpha2.Instance, status metav1.ConditionStatus, reason, message string,
) (ctrl.Result, error) {
	meta.SetStatusCondition(&inst.Status.Conditions, metav1.Condition{
		Type:    DiscoveredConditionType,
		Status:  status,
		Reason:  reason,
		Message: message,
	})

	err := r.Client.Status().Update(ctx, inst)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update instance %q status: %w", inst.Name, err)
	}

	if status == metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: r.RecheckInterval}, nil
	}

	return ctrl.Result{RequeueAfter: r.RetryInterval}, nil
}
