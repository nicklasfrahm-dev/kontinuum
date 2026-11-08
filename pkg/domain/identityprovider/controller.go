// Package identityprovider discovers users from OIDC issuers configured at
// runtime through two CRDs, inspired by Kubernetes' own
// AuthenticationConfiguration: ClusterIdentityProvider (cluster-wide) and
// IdentityProvider (scoped to one tenant namespace). Each one maps a
// verified JWT's claims onto a Kubernetes user with CEL, exactly as a
// kube-apiserver jwt[] authenticator does.
//
// Every kontinuum process runs this controller and keeps its own in-memory
// Authenticator, which libkapi.WithAuthenticator plugs into the apiserver's
// authentication chain. A Registry recompiles changed providers and swaps
// the active set atomically, so providers are hot-swappable: creating,
// updating, or deleting one takes effect on the next request, with no
// restart. Signing keys are held in a shared KeyCache with
// stale-while-revalidate semantics; reconciling a provider populates it,
// so the first token never pays for discovery.
//
// Identities from a namespaced IdentityProvider are always prefixed with
// "tenant:<namespace>:" — see IdentityPrefix — so a tenant administrator
// can bring their own issuer without being able to mint cluster-wide,
// admin-group, or other tenants' identities.
package identityprovider

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/go-jose/go-jose/v4"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

const (
	// ReadyConditionType reports whether a provider authenticates
	// requests with signing keys in hand.
	ReadyConditionType = "Ready"
	// ReasonKeyFetchFailed is the Ready condition reason of an active
	// provider whose signing keys couldn't be fetched.
	ReasonKeyFetchFailed = "KeyFetchFailed"
	// defaultErrorRequeueInterval is how soon a provider that isn't Ready
	// is reconciled again.
	defaultErrorRequeueInterval = 30 * time.Second
)

// Config configures a Controller.
type Config struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// Registry compiles providers and swaps the active set.
	Registry *Registry
	// Cache holds every provider's signing keys.
	Cache *KeyCache
	// ZoneLease is this process's own zonelease.Locker identity. Every
	// process keeps its own Authenticator up to date, but only the holder
	// of zonelease.GlobalKey writes status, since providers are hub-owned,
	// fleet-wide state.
	ZoneLease zonelease.Identity
	// ErrorRequeueInterval is how soon a provider that isn't Ready is
	// reconciled again. Defaults to thirty seconds when zero.
	ErrorRequeueInterval time.Duration
}

// Controller wires the identity provider reconcilers onto a
// controller-runtime Manager. See NewController.
type Controller struct {
	Config Config
}

// NewController builds a Controller from cfg, filling in defaults.
func NewController(cfg Config) *Controller {
	if cfg.ErrorRequeueInterval == 0 {
		cfg.ErrorRequeueInterval = defaultErrorRequeueInterval
	}

	return &Controller{Config: cfg}
}

// SetupWithManager registers one reconciler per kind on mgr, sharing one
// Reconciler. It performs no API calls itself.
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	reconciler := &Reconciler{
		Client:               mgr.GetClient(),
		Registry:             c.Config.Registry,
		Cache:                c.Config.Cache,
		ErrorRequeueInterval: c.Config.ErrorRequeueInterval,
		Locker: zonelease.NewLocker(
			mgr.GetClient(), mgr.GetAPIReader(), c.Config.ZoneLease.HolderIdentity, c.Config.ZoneLease.SelfZoneKey, 0),
		Logger: c.Config.Logger,
	}

	err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha2.ClusterIdentityProvider{}).
		Complete(reconcile.Func(reconciler.Reconcile))
	if err != nil {
		return fmt.Errorf("failed to register cluster identity provider controller: %w", err)
	}

	err = ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha2.IdentityProvider{}).
		Complete(reconcile.Func(reconciler.Reconcile))
	if err != nil {
		return fmt.Errorf("failed to register identity provider controller: %w", err)
	}

	return nil
}

// Reconciler reconciles both identity provider kinds. A request with an
// empty namespace names a ClusterIdentityProvider; any other names an
// IdentityProvider.
type Reconciler struct {
	Client               client.Client
	Registry             *Registry
	Cache                *KeyCache
	ErrorRequeueInterval time.Duration
	// Locker gates status writes — see Config.ZoneLease.
	Locker *zonelease.Locker
	Logger *slog.Logger
}

// Reconcile resyncs the Registry with every provider, then populates the
// requested provider's signing keys and reports its status.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	providers, err := r.listProviders(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	results := r.Registry.Sync(ctx, providers)

	target, err := r.get(ctx, req)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, fmt.Errorf("failed to get identity provider %s: %w", req, err)
	}

	ref := ProviderRef{Namespace: req.Namespace, Name: req.Name}
	provider := Provider{Ref: ref, Spec: target.spec}

	desired, ready := r.desiredStatus(ctx, provider, results[ref], *target.status, target.object.GetGeneration())

	requeue := ctrl.Result{RequeueAfter: zonelease.Jitter(r.Cache.FreshFor())}
	if !ready {
		requeue = ctrl.Result{RequeueAfter: zonelease.Jitter(r.ErrorRequeueInterval)}
	}

	if equality.Semantic.DeepEqual(*target.status, desired) {
		return requeue, nil
	}

	acquired, err := r.Locker.TryAcquire(ctx, zonelease.GlobalKey)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to acquire global lease: %w", err)
	}

	if !acquired {
		return requeue, nil
	}

	*target.status = desired

	err = r.Client.Status().Update(ctx, target.object)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update identity provider %s status: %w", req, err)
	}

	return requeue, nil
}

// desiredStatus computes provider's status, fetching its signing keys —
// blocking only when none are cached yet — when it is active. ready
// reports whether the Ready condition is True.
func (r *Reconciler) desiredStatus(
	ctx context.Context, provider Provider, result SyncResult, current v1alpha2.IdentityProviderStatus, generation int64,
) (v1alpha2.IdentityProviderStatus, bool) {
	desired := *current.DeepCopy()
	desired.ObservedGeneration = generation
	desired.UsernamePrefix = IdentityPrefix(provider.Ref.Namespace)
	desired.GroupsPrefix = IdentityPrefix(provider.Ref.Namespace)

	condition := metav1.Condition{
		Type:               ReadyConditionType,
		Status:             metav1.ConditionFalse,
		Reason:             result.Reason,
		Message:            result.Message,
		ObservedGeneration: generation,
	}

	if result.Reason == "" {
		condition.Reason = ReasonInvalidConfiguration
		condition.Message = "Identity provider has not been compiled yet"
	}

	if result.Active {
		_, err := r.Cache.Keys(ctx, provider.Source())
		if err != nil {
			condition.Reason = ReasonKeyFetchFailed
			condition.Message = err.Error()
		} else {
			condition.Status = metav1.ConditionTrue
		}
	}

	desired.SigningKeys = 0
	desired.LastKeyRefreshTime = nil

	snapshot, ok := r.Cache.Snapshot(provider.Source())
	if ok && result.Active {
		desired.SigningKeys = countKeys(snapshot.Keys)
		fetchedAt := metav1.NewTime(snapshot.FetchedAt)
		desired.LastKeyRefreshTime = &fetchedAt
	}

	meta.SetStatusCondition(&desired.Conditions, condition)

	return desired, condition.Status == metav1.ConditionTrue
}

// countKeys counts keys as an int32, saturating instead of overflowing.
func countKeys(keys []jose.JSONWebKey) int32 {
	var count int32

	for range keys {
		if count == math.MaxInt32 {
			break
		}

		count++
	}

	return count
}

// listProviders lists every provider of both kinds.
func (r *Reconciler) listProviders(ctx context.Context) ([]Provider, error) {
	var clusterProviders v1alpha2.ClusterIdentityProviderList

	err := r.Client.List(ctx, &clusterProviders)
	if err != nil {
		return nil, fmt.Errorf("failed to list cluster identity providers: %w", err)
	}

	var namespacedProviders v1alpha2.IdentityProviderList

	err = r.Client.List(ctx, &namespacedProviders)
	if err != nil {
		return nil, fmt.Errorf("failed to list identity providers: %w", err)
	}

	providers := make([]Provider, 0, len(clusterProviders.Items)+len(namespacedProviders.Items))

	for i := range clusterProviders.Items {
		if clusterProviders.Items[i].DeletionTimestamp == nil {
			providers = append(providers, FromClusterIdentityProvider(&clusterProviders.Items[i]))
		}
	}

	for i := range namespacedProviders.Items {
		if namespacedProviders.Items[i].DeletionTimestamp == nil {
			providers = append(providers, FromIdentityProvider(&namespacedProviders.Items[i]))
		}
	}

	return providers, nil
}

// reconcileTarget is a fetched provider of either kind: the object itself
// for status updates, its spec, and a pointer into its status.
type reconcileTarget struct {
	object client.Object
	spec   v1alpha2.IdentityProviderSpec
	status *v1alpha2.IdentityProviderStatus
}

// get fetches req's object of the right kind.
func (r *Reconciler) get(ctx context.Context, req ctrl.Request) (reconcileTarget, error) {
	if req.Namespace == "" {
		var obj v1alpha2.ClusterIdentityProvider

		err := r.Client.Get(ctx, req.NamespacedName, &obj)
		if err != nil {
			return reconcileTarget{}, fmt.Errorf("failed to get cluster identity provider: %w", err)
		}

		return reconcileTarget{object: &obj, spec: obj.Spec, status: &obj.Status}, nil
	}

	var obj v1alpha2.IdentityProvider

	err := r.Client.Get(ctx, req.NamespacedName, &obj)
	if err != nil {
		return reconcileTarget{}, fmt.Errorf("failed to get identity provider: %w", err)
	}

	return reconcileTarget{object: &obj, spec: obj.Spec, status: &obj.Status}, nil
}
