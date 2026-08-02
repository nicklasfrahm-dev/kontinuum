package rbac_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"

	"github.com/nicklasfrahm/kontinuum/pkg/rbac"
)

func TestAdminAuthorizerAllowsHealthPathsEvenWithoutUser(t *testing.T) {
	t.Parallel()

	admin := rbac.AdminAuthorizer{Groups: []string{"platform-admins"}}

	for _, path := range []string{"/livez", "/readyz", "/healthz", "/readyz/ping"} {
		decision, _, err := admin.Authorize(context.Background(), authorizer.AttributesRecord{Path: path})
		require.NoError(t, err)
		assert.Equalf(t, authorizer.DecisionAllow, decision, "path %q should be allowed", path)
	}
}

func TestAdminAuthorizerDeniesNilUser(t *testing.T) {
	t.Parallel()

	admin := rbac.AdminAuthorizer{Groups: []string{"platform-admins"}}

	decision, _, err := admin.Authorize(context.Background(), authorizer.AttributesRecord{
		ResourceRequest: true, Verb: "get", Resource: "kontinuums", Path: "/apis/kontinuum.sh/v1alpha2/kontinuums",
	})
	require.NoError(t, err)
	assert.Equal(t, authorizer.DecisionDeny, decision)
}

func TestAdminAuthorizerAllowsSystemMasters(t *testing.T) {
	t.Parallel()

	admin := rbac.AdminAuthorizer{Groups: []string{"platform-admins"}}

	decision, _, err := admin.Authorize(context.Background(), authorizer.AttributesRecord{
		User:            &user.DefaultInfo{Name: "loopback", Groups: []string{"system:masters"}},
		ResourceRequest: true, Verb: "delete", Resource: "kontinuums",
	})
	require.NoError(t, err)
	assert.Equal(t, authorizer.DecisionAllow, decision)
}

func TestAdminAuthorizerAllowsConfiguredGroup(t *testing.T) {
	t.Parallel()

	admin := rbac.AdminAuthorizer{Groups: []string{"platform-admins", "sre"}}

	decision, _, err := admin.Authorize(context.Background(), authorizer.AttributesRecord{
		User:            &user.DefaultInfo{Name: "alice", Groups: []string{"sre"}},
		ResourceRequest: true, Verb: "get", Resource: "kontinuums",
	})
	require.NoError(t, err)
	assert.Equal(t, authorizer.DecisionAllow, decision)
}

func TestAdminAuthorizerAllowsServiceAccounts(t *testing.T) {
	t.Parallel()

	admin := rbac.AdminAuthorizer{Groups: []string{"platform-admins"}}

	saUser := &user.DefaultInfo{
		Name: "system:serviceaccount:default:autoscaler", Groups: []string{"system:serviceaccounts"},
	}

	decision, _, err := admin.Authorize(context.Background(), authorizer.AttributesRecord{
		User: saUser, ResourceRequest: true, Verb: "get", Resource: "kontinuums",
	})
	require.NoError(t, err)
	assert.Equal(t, authorizer.DecisionAllow, decision)
}

func TestAdminAuthorizerDeniesUnrecognizedGroup(t *testing.T) {
	t.Parallel()

	admin := rbac.AdminAuthorizer{Groups: []string{"platform-admins"}}

	decision, _, err := admin.Authorize(context.Background(), authorizer.AttributesRecord{
		User:            &user.DefaultInfo{Name: "mallory", Groups: []string{"random-group"}},
		ResourceRequest: true, Verb: "get", Resource: "kontinuums",
	})
	require.NoError(t, err)
	assert.Equal(t, authorizer.DecisionDeny, decision)
}
