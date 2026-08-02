package rbac_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/nicklasfrahm/kontinuum/pkg/rbac"
)

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, rbacv1.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func TestClientSourceReturnsErrorBeforeSetClient(t *testing.T) {
	t.Parallel()

	source := &rbac.ClientSource{}

	_, err := source.GetRole(context.Background(), "default", "reader")
	require.Error(t, err)

	_, err = source.ListRoleBindings(context.Background(), "default")
	require.Error(t, err)

	_, err = source.GetClusterRole(context.Background(), "reader")
	require.Error(t, err)

	_, err = source.ListClusterRoleBindings(context.Background())
	require.Error(t, err)
}

func TestClientSourceServesAfterSetClient(t *testing.T) {
	t.Parallel()

	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "default"}}
	roleBinding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "readers", Namespace: "default"}}
	clusterRole := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "cluster-reader"}}
	clusterRoleBinding := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "cluster-readers"}}

	fakeClient := newFakeClient(t, role, roleBinding, clusterRole, clusterRoleBinding)

	source := &rbac.ClientSource{}
	source.SetClient(fakeClient)

	gotRole, err := source.GetRole(context.Background(), "default", "reader")
	require.NoError(t, err)
	assert.Equal(t, "reader", gotRole.Name)

	roleBindings, err := source.ListRoleBindings(context.Background(), "default")
	require.NoError(t, err)
	require.Len(t, roleBindings, 1)
	assert.Equal(t, "readers", roleBindings[0].Name)

	gotClusterRole, err := source.GetClusterRole(context.Background(), "cluster-reader")
	require.NoError(t, err)
	assert.Equal(t, "cluster-reader", gotClusterRole.Name)

	clusterRoleBindings, err := source.ListClusterRoleBindings(context.Background())
	require.NoError(t, err)
	require.Len(t, clusterRoleBindings, 1)
	assert.Equal(t, "cluster-readers", clusterRoleBindings[0].Name)
}

func TestNewAuthorizerRequiresAdminGroup(t *testing.T) {
	t.Parallel()

	_, _, err := rbac.NewAuthorizer("  , ,")
	assert.ErrorIs(t, err, rbac.ErrAdminGroupRequired)
}

func TestNewAuthorizerAllowsSystemMastersBeforeClientIsReady(t *testing.T) {
	t.Parallel()

	authz, _, err := rbac.NewAuthorizer("platform-admins")
	require.NoError(t, err)

	decision, _, err := authz.Authorize(context.Background(), authorizer.AttributesRecord{
		User:            &user.DefaultInfo{Name: "loopback", Groups: []string{"system:masters"}},
		ResourceRequest: true, Verb: "list", Resource: "kontinuums",
	})
	require.NoError(t, err)
	assert.Equal(t, authorizer.DecisionAllow, decision, "the admin-group fallback must still work while the RBAC "+
		"authorizer's client isn't ready yet")
}

// TestNewAuthorizerGrantsAccessViaRealClusterRoleBinding proves the RBAC
// half is actually consulted, not just present: a group with no entry in
// AdminGroups gets access purely because a ClusterRoleBinding/ClusterRole
// pair (the same shape pkg/domain/adminrbac reconciles) grants it — see
// issue #41's ask to make the RBAC objects the real enforcement path, not
// just an inspectable record.
func TestNewAuthorizerGrantsAccessViaRealClusterRoleBinding(t *testing.T) {
	t.Parallel()

	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "widget-reader"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"widgets.example.com"}, Resources: []string{"widgets"}, Verbs: []string{"get", "list"}},
		},
	}
	clusterRoleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "widget-readers"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "widget-reader"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: "widget-readers"}},
	}

	fakeClient := newFakeClient(t, clusterRole, clusterRoleBinding)

	authz, source, err := rbac.NewAuthorizer("platform-admins")
	require.NoError(t, err)

	source.SetClient(fakeClient)

	reader := &user.DefaultInfo{Name: "bob", Groups: []string{"widget-readers"}}

	decision, _, err := authz.Authorize(context.Background(), authorizer.AttributesRecord{
		User: reader, ResourceRequest: true, Verb: "get", APIGroup: "widgets.example.com", Resource: "widgets",
	})
	require.NoError(t, err)
	assert.Equal(t, authorizer.DecisionAllow, decision, "the ClusterRoleBinding's rule should grant this")

	decision, _, err = authz.Authorize(context.Background(), authorizer.AttributesRecord{
		User: reader, ResourceRequest: true, Verb: "delete", APIGroup: "widgets.example.com", Resource: "widgets",
	})
	require.NoError(t, err)
	assert.Equal(t, authorizer.DecisionDeny, decision,
		"delete isn't in the bound ClusterRole's rules and the user isn't in any admin group, so the "+
			"admin-group fallback must deny it too")
}

