package rbac

import (
	"context"
	"errors"
	"fmt"
	"sync"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	unionauth "k8s.io/apiserver/pkg/authorization/union"
	upstreamrbac "k8s.io/kubernetes/plugin/pkg/auth/authorizer/rbac"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/pkg/config"
)

// ErrAdminGroupRequired is returned by NewAuthorizer when adminGroupsRaw
// parses to zero groups — mirrors
// github.com/kommodity-io/kommodity/pkg/libkapi/auth.ErrAdminGroupRequired:
// an OIDC deployment with no admin groups configured would lock everyone
// out, including whoever needs to fix it.
var ErrAdminGroupRequired = errors.New("rbac: at least one admin group is required")

// errClientNotReady is returned by ClientSource's Getter/Lister methods
// before SetClient has been called — see ClientSource's doc for why this
// window exists and why it's safe.
var errClientNotReady = errors.New("rbac: client not ready")

// ClientSource adapts a client.Client, installed later via SetClient, to
// the RoleGetter/RoleBindingLister/ClusterRoleGetter/ClusterRoleBindingLister
// interfaces k8s.io/kubernetes/plugin/pkg/auth/authorizer/rbac.New expects
// (k8s.io/kubernetes/pkg/registry/rbac/validation's own interfaces) — a
// thin, uncached, direct-client alternative to upstream's own convenience
// adapters, which require client-go listers/informers kontinuum doesn't
// run (see NewAuthorizer's doc for why the client can't be built until
// later).
//
// Every method locks around the same client field, so SetClient is safe to
// call concurrently with Get/List calls arriving from in-flight Authorize
// checks.
type ClientSource struct {
	mu     sync.RWMutex
	client client.Client
}

// SetClient installs c, unblocking every Getter/Lister method.
func (s *ClientSource) SetClient(c client.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.client = c
}

// GetRole implements
// k8s.io/kubernetes/pkg/registry/rbac/validation.RoleGetter.
func (s *ClientSource) GetRole(ctx context.Context, namespace, name string) (*rbacv1.Role, error) {
	rbacClient, err := s.get()
	if err != nil {
		return nil, err
	}

	var role rbacv1.Role

	err = rbacClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &role)
	if err != nil {
		return nil, fmt.Errorf("failed to get role %q: %w", name, err)
	}

	return &role, nil
}

// ListRoleBindings implements
// k8s.io/kubernetes/pkg/registry/rbac/validation.RoleBindingLister.
func (s *ClientSource) ListRoleBindings(ctx context.Context, namespace string) ([]*rbacv1.RoleBinding, error) {
	rbacClient, err := s.get()
	if err != nil {
		return nil, err
	}

	var list rbacv1.RoleBindingList

	err = rbacClient.List(ctx, &list, client.InNamespace(namespace))
	if err != nil {
		return nil, fmt.Errorf("failed to list role bindings in %q: %w", namespace, err)
	}

	bindings := make([]*rbacv1.RoleBinding, len(list.Items))
	for i := range list.Items {
		bindings[i] = &list.Items[i]
	}

	return bindings, nil
}

// GetClusterRole implements
// k8s.io/kubernetes/pkg/registry/rbac/validation.ClusterRoleGetter.
func (s *ClientSource) GetClusterRole(ctx context.Context, name string) (*rbacv1.ClusterRole, error) {
	rbacClient, err := s.get()
	if err != nil {
		return nil, err
	}

	var role rbacv1.ClusterRole

	err = rbacClient.Get(ctx, client.ObjectKey{Name: name}, &role)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster role %q: %w", name, err)
	}

	return &role, nil
}

// ListClusterRoleBindings implements
// k8s.io/kubernetes/pkg/registry/rbac/validation.ClusterRoleBindingLister.
func (s *ClientSource) ListClusterRoleBindings(ctx context.Context) ([]*rbacv1.ClusterRoleBinding, error) {
	rbacClient, err := s.get()
	if err != nil {
		return nil, err
	}

	var list rbacv1.ClusterRoleBindingList

	err = rbacClient.List(ctx, &list)
	if err != nil {
		return nil, fmt.Errorf("failed to list cluster role bindings: %w", err)
	}

	bindings := make([]*rbacv1.ClusterRoleBinding, len(list.Items))
	for i := range list.Items {
		bindings[i] = &list.Items[i]
	}

	return bindings, nil
}

// get returns the installed client, or errClientNotReady if SetClient
// hasn't been called yet.
func (s *ClientSource) get() (client.Client, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.client == nil {
		return nil, errClientNotReady
	}

	return s.client, nil
}

// NewAuthorizer builds kontinuum's full authorization chain and the
// ClientSource it reads Role/RoleBinding/ClusterRole/ClusterRoleBinding
// objects through. adminGroupsRaw is cfg.OIDC.AdminGroups's raw,
// comma-delimited value; NewAuthorizer fails with ErrAdminGroupRequired if
// it parses to zero groups.
//
// The chain tries upstream's real RBAC authorizer first, falling back to
// AdminAuthorizer (system:masters, adminGroups, service accounts, health
// paths) whenever RBAC finds no matching rule. This ordering matters:
// upstream's RBACAuthorizer.Authorize only ever returns Allow or
// NoOpinion, never Deny — RBAC is purely additive — so union.New tried in
// this order preserves every bit of AdminAuthorizer's existing
// deny-by-default behavior exactly, while letting a real
// ClusterRoleBinding/RoleBinding (see pkg/domain/adminrbac, which
// reconciles one per admin group) also grant access through actual rule
// evaluation, satisfying `kubectl auth can-i`.
//
// The returned ClientSource has no client installed yet. libkapi.WithAuthorizer
// requires a working authorizer.Authorizer value synchronously, before
// ListenAndServe is ever called — but the privileged client this
// authorizer's RBAC half needs to read from can only be built from a
// loopback *rest.Config available once the server starts listening (see a
// libkapi.WithPostStartHook registration). Until the caller calls
// SetClient there, every Get/List call the RBAC authorizer makes fails
// with errClientNotReady, so RBACAuthorizer.Authorize resolves zero rules
// and returns NoOpinion — safe, since AdminAuthorizer is always right
// behind it as the deny-by-default fallback, and the loopback traffic
// (system:masters) that flows during this startup window is exactly what
// AdminAuthorizer allows unconditionally anyway.
func NewAuthorizer(adminGroupsRaw string) (authorizer.Authorizer, *ClientSource, error) {
	groups := config.ParseAdminGroups(adminGroupsRaw)
	if len(groups) == 0 {
		return nil, nil, ErrAdminGroupRequired
	}

	source := &ClientSource{}

	rbacAuthorizer := upstreamrbac.New(source, source, source, source)
	adminAuthorizer := AdminAuthorizer{Groups: groups}

	return unionauth.New(rbacAuthorizer, adminAuthorizer), source, nil
}
