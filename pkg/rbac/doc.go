// Package rbac builds kontinuum's authorization chain: upstream
// Kubernetes's own RBAC authorizer — which evaluates real
// Role/RoleBinding/ClusterRole/ClusterRoleBinding objects, including the
// ones pkg/domain/adminrbac reconciles for cfg.OIDC.AdminGroups — unioned
// in front of a deny-by-default admin-group fallback. See NewAuthorizer's
// doc for the full design, and issue #41 for why: without this, the
// ClusterRoleBinding objects pkg/domain/adminrbac creates would be
// inspectable (`kubectl get clusterrolebindings`) but inert — no
// authorizer would ever actually evaluate their rules.
package rbac
