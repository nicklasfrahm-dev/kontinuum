// Package adminrbac reconciles a ClusterRoleBinding for every OIDC admin
// group configured on cfg.OIDC.AdminGroups (see pkg/config), so the grant
// libkapi.WithRBACAuthorizer enforces is backed by real, inspectable RBAC
// objects — `kubectl get clusterrolebindings` — that its RBAC authorizer
// half actually evaluates on every request, not just a row an authorizer
// computes on the fly from config. See issue #41.
//
// AdminGroups comes from static process config, not a watched Kubernetes
// object, so there's nothing for a reconcile.Reconciler to trigger on when
// it changes — a config change only takes effect on the next process
// restart anyway. What still needs to self-heal without a restart is a
// binding being deleted out from under this process (manually, by kubectl,
// by anything else with access), so Runnable re-derives and re-applies the
// desired state on a fixed interval instead, the same ticker-driven
// self-healing shape as pkg/domain/registry.Heartbeat.
package adminrbac

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zonelease"
)

const (
	// ManagedByValue is api/v1alpha2.LabelManagedBy's value on every
	// ClusterRole/ClusterRoleBinding this controller owns.
	ManagedByValue = "admin-group-controller"

	// AdminGroupAnnotation records the exact OIDC group name a
	// ClusterRoleBinding grants access to. Kept as an annotation rather
	// than encoded into the object's name or a label value, since OIDC
	// group names (LDAP DNs, emails, arbitrary IdP-defined strings) can
	// contain characters that aren't valid in either — see bindingName.
	AdminGroupAnnotation = "kontinuum.sh/admin-group"

	// RoleName is the ClusterRole every admin group is bound to — a
	// cluster-admin equivalent (see adminClusterRole's rules). Kontinuum's
	// own apiserver doesn't bootstrap upstream's default ClusterRoles (no
	// "rbac/bootstrap-roles" PostStartHook is wired — see registry.go's
	// package doc), so this controller ensures its own copy exists rather
	// than assuming "cluster-admin" is already there.
	RoleName = "kontinuum-admin"

	bindingNamePrefix = "kontinuum-admin-"

	defaultInterval = 30 * time.Second
)

// Config configures a Controller.
type Config struct {
	// Logger receives the controller's log output.
	Logger *slog.Logger
	// AdminGroups is cfg.OIDC.AdminGroups's raw, comma-delimited value.
	AdminGroups string
	// Interval is how often the desired-state reconcile loop runs, both to
	// pick up self-healing after an out-of-band delete and, in principle, a
	// future config source that changes without a process restart. Defaults
	// to thirty seconds when zero.
	Interval time.Duration
	// ZoneLease is this process's own zonelease.Locker identity — see
	// zonelease.Identity's own doc. Every tick is gated by
	// zonelease.GlobalKey — these ClusterRoleBindings are shared, hub-owned
	// state, not any one zone's own resources, so a zone's own process
	// never writes them.
	ZoneLease zonelease.Identity
}

// Controller wires the admin-group RBAC reconciler onto a
// controller-runtime Manager. See NewController.
type Controller struct {
	Config Config
}

// NewController builds a Controller from cfg, defaulting Interval when left
// zero.
func NewController(cfg Config) *Controller {
	if cfg.Interval == 0 {
		cfg.Interval = defaultInterval
	}

	return &Controller{Config: cfg}
}

// SetupWithManager registers the admin-group RBAC runnable on mgr. It must
// not perform any API calls itself — mgr.Add only registers runnable to be
// started once the manager actually starts (see
// TestSetupWithManagerDoesNotRequireLiveAPIServer in pkg/cli).
func (c *Controller) SetupWithManager(mgr ctrl.Manager) error {
	runnable := &Runnable{
		Client:      mgr.GetClient(),
		AdminGroups: c.Config.AdminGroups,
		Interval:    c.Config.Interval,
		Locker: zonelease.NewLocker(
			mgr.GetClient(), mgr.GetAPIReader(), c.Config.ZoneLease.HolderIdentity, c.Config.ZoneLease.SelfZoneKey, 0),
		Logger: c.Config.Logger,
	}

	err := mgr.Add(runnable)
	if err != nil {
		return fmt.Errorf("failed to register admin rbac runnable: %w", err)
	}

	return nil
}

// Runnable implements manager.Runnable, ticking reconcile on Interval —
// see the package doc for why this is a plain ticker rather than a
// reconcile.Reconciler watching a Kubernetes object.
type Runnable struct {
	Client      client.Client
	AdminGroups string
	Interval    time.Duration
	// Locker gates every write below against zonelease — see
	// Config.ZoneLease's own doc.
	Locker *zonelease.Locker
	Logger *slog.Logger
}

// Start implements manager.Runnable. It blocks until ctx is canceled,
// running one reconcile immediately and then on every tick thereafter.
func (r *Runnable) Start(ctx context.Context) error {
	r.reconcile(ctx)

	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.reconcile(ctx)
		case <-ctx.Done():
			return nil
		}
	}
}

// reconcile ensures RoleName's ClusterRole exists, then reconciles the set
// of managed ClusterRoleBindings against AdminGroups. Failures are logged,
// not fatal — like Heartbeat.beat, the next tick tries again. Skipped
// entirely — quietly, no log line — while r.Locker doesn't hold
// zonelease.GlobalKey (see Config.ZoneLease's own doc); the next tick tries
// again.
func (r *Runnable) reconcile(ctx context.Context) {
	acquired, err := r.Locker.TryAcquire(ctx, zonelease.GlobalKey)
	if err != nil {
		r.Logger.Error("Failed to acquire zone lease for admin rbac", "error", err)

		return
	}

	if !acquired {
		return
	}

	err = r.ensureRole(ctx)
	if err != nil {
		r.Logger.Error("Failed to ensure admin cluster role", "error", err)

		return
	}

	err = r.reconcileBindings(ctx)
	if err != nil {
		r.Logger.Error("Failed to reconcile admin group bindings", "error", err)
	}
}

// ensureRole creates RoleName's ClusterRole if it doesn't already exist. Its
// rules never change once created, so an AlreadyExists response is treated
// as success rather than reconciled for drift.
func (r *Runnable) ensureRole(ctx context.Context) error {
	role := adminClusterRole()

	err := r.Client.Create(ctx, role)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create %q cluster role: %w", RoleName, err)
	}

	return nil
}

// adminClusterRole builds the cluster-admin-equivalent ClusterRole every
// admin group is bound to — the same resource*/verb* and nonResourceURLs*/
// verb* rule shape upstream Kubernetes bootstraps its own "cluster-admin"
// ClusterRole with.
func adminClusterRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name:   RoleName,
			Labels: map[string]string{v1alpha2.LabelManagedBy: ManagedByValue},
		},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{rbacv1.APIGroupAll}, Resources: []string{rbacv1.ResourceAll}, Verbs: []string{rbacv1.VerbAll}},
			{NonResourceURLs: []string{rbacv1.NonResourceAll}, Verbs: []string{rbacv1.VerbAll}},
		},
	}
}

// reconcileBindings lists every ClusterRoleBinding this controller manages
// (via v1alpha2.LabelManagedBy), diffs the groups they carry (via
// AdminGroupAnnotation) against parseAdminGroups(r.AdminGroups), creates a
// binding for each group missing one, and deletes bindings for groups no
// longer present — leaving any unlabeled ClusterRoleBinding untouched.
func (r *Runnable) reconcileBindings(ctx context.Context) error {
	desired := config.ParseAdminGroups(r.AdminGroups)

	var existing rbacv1.ClusterRoleBindingList

	err := r.Client.List(ctx, &existing, client.MatchingLabels{v1alpha2.LabelManagedBy: ManagedByValue})
	if err != nil {
		return fmt.Errorf("failed to list managed cluster role bindings: %w", err)
	}

	existingByGroup := make(map[string]rbacv1.ClusterRoleBinding, len(existing.Items))
	for _, item := range existing.Items {
		existingByGroup[item.Annotations[AdminGroupAnnotation]] = item
	}

	desiredSet := make(map[string]struct{}, len(desired))

	for _, group := range desired {
		desiredSet[group] = struct{}{}

		if _, ok := existingByGroup[group]; ok {
			continue
		}

		createErr := r.createBinding(ctx, group)
		if createErr != nil {
			r.Logger.Error("Failed to create admin group binding", "group", group, "error", createErr)
		}
	}

	for group, item := range existingByGroup {
		if _, ok := desiredSet[group]; ok {
			continue
		}

		deleteErr := r.Client.Delete(ctx, &item)
		if deleteErr != nil && !apierrors.IsNotFound(deleteErr) {
			r.Logger.Error("Failed to delete stale admin group binding",
				"group", group, "name", item.Name, "error", deleteErr)
		}
	}

	return nil
}

// createBinding creates a ClusterRoleBinding granting group RoleName,
// labeled and annotated so a future reconcile recognizes it as managed and
// can read group back from it.
func (r *Runnable) createBinding(ctx context.Context, group string) error {
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:        bindingName(group),
			Labels:      map[string]string{v1alpha2.LabelManagedBy: ManagedByValue},
			Annotations: map[string]string{AdminGroupAnnotation: group},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     RoleName,
		},
		Subjects: []rbacv1.Subject{
			{Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: group},
		},
	}

	err := r.Client.Create(ctx, binding)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create %q cluster role binding: %w", binding.Name, err)
	}

	return nil
}

// bindingName derives a deterministic, RFC 1123 subdomain-safe
// ClusterRoleBinding name from group — a content hash rather than group
// itself, since OIDC group names aren't guaranteed to be valid Kubernetes
// object names (they may contain uppercase, spaces, "@", ":", and other
// characters a Kubernetes object name can't).
func bindingName(group string) string {
	sum := sha256.Sum256([]byte(group))

	return bindingNamePrefix + hex.EncodeToString(sum[:])[:12]
}
