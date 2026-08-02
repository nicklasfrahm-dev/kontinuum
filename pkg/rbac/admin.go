package rbac

import (
	"context"
	"slices"
	"strings"

	"k8s.io/apiserver/pkg/authorization/authorizer"
)

const (
	systemMastersGroup         = "system:masters"
	systemServiceAccountsGroup = "system:serviceaccounts"
)

// healthPaths returns the non-resource paths that must stay reachable
// without authentication, for liveness/readiness/health probes — mirrors
// github.com/kommodity-io/kommodity/pkg/libkapi/auth.HealthPaths.
func healthPaths() []string {
	return []string{"/livez", "/readyz", "/healthz"}
}

// AdminAuthorizer allows health check endpoints, system:masters (the
// loopback identity every in-process client — domain controllers, the
// RBAC authorizer's own ClientSource — authenticates as, see
// NewAuthorizer's doc), any of Groups, and system:serviceaccounts; denies
// everything else.
//
// This is kontinuum's own copy of
// github.com/kommodity-io/kommodity/pkg/libkapi/auth.AdminAuthorizer's
// behavior, not a re-export of it — that subpackage isn't part of
// libkapi's public API (see libkapi's authoptions.go doc: "so callers can
// use libkapi.WithOIDC(...) etc. without importing the auth subpackage
// directly"). It has to live under kontinuum's own control here, rather
// than behind libkapi.WithAdminAuthorizer, because NewAuthorizer unions it
// with the RBAC authorizer into a single authorizer.Authorizer value —
// libkapi.WithAdminAuthorizer and libkapi.WithAuthorizer both set the same
// internal field with last-call-wins semantics, so passing both Options
// would silently drop one instead of combining them.
type AdminAuthorizer struct {
	Groups []string
}

// Authorize implements authorizer.Authorizer.
func (a AdminAuthorizer) Authorize(
	_ context.Context, attrs authorizer.Attributes,
) (authorizer.Decision, string, error) {
	path := attrs.GetPath()

	for _, healthPath := range healthPaths() {
		if !attrs.IsResourceRequest() && (path == healthPath || strings.HasPrefix(path, healthPath+"/")) {
			return authorizer.DecisionAllow, "allowed: health check endpoint", nil
		}
	}

	user := attrs.GetUser()
	if user == nil {
		return authorizer.DecisionDeny, "no user in attributes", nil
	}

	if slices.Contains(user.GetGroups(), systemMastersGroup) {
		return authorizer.DecisionAllow, "allowed: user is in system:masters group", nil
	}

	if containsAny(user.GetGroups(), a.Groups) {
		return authorizer.DecisionAllow, "allowed: user is in admin group", nil
	}

	if slices.Contains(user.GetGroups(), systemServiceAccountsGroup) {
		return authorizer.DecisionAllow, "allowed: user is an authenticated service account", nil
	}

	return authorizer.DecisionDeny,
		"forbidden: user is not in admin group, system:masters group, or a service account", nil
}

// containsAny reports whether groups contains any element of candidates.
func containsAny(groups []string, candidates []string) bool {
	for _, candidate := range candidates {
		if slices.Contains(groups, candidate) {
			return true
		}
	}

	return false
}
