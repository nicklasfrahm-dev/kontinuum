package ui

import (
	"fmt"
	"net"
	"net/http"
)

// kubeconfigTemplate is a kubectl kubeconfig that authenticates via
// kubectl oidc-login (https://github.com/int128/kubelogin) as an exec
// credential plugin, matching kontinuum's /app login flow: a public OAuth
// 2.0 client (no client secret) requesting the email and groups scopes
// needed to resolve group membership for kontinuum's admin-group
// authorization (see libkapi.WithAdminAuthorizer). %s placeholders are, in
// order: the cluster name, the API server origin, the cluster's
// insecure-skip-tls-verify line (empty unless host looks local — see
// probablySelfSigned), the context name, the cluster name again (the
// context's cluster reference), the user name (the context's user
// reference), the context name again (current-context), the user name
// again (the users entry), and the OIDC issuer URL and client ID.
const kubeconfigTemplate = `apiVersion: v1
kind: Config
clusters:
  - name: %s
    cluster:
      server: %s
%scontexts:
  - name: %s
    context:
      cluster: %s
      user: %s
current-context: %s
users:
  - name: %s
    user:
      exec:
        apiVersion: client.authentication.k8s.io/v1beta1
        command: kubectl
        args:
          - oidc-login
          - get-token
          - --oidc-issuer-url=%s
          - --oidc-client-id=%s
          - --oidc-extra-scope=email
          - --oidc-extra-scope=groups
        interactiveMode: Never
`

// kubeconfig renders a kubectl-oidc-login-based kubeconfig pointed at
// origin (see requestOrigin), authenticating against issuerURL/clientID.
// The cluster and user are both named "kontinuum-<host>", with any port
// stripped from host (e.g. "kontinuum-kontinuum.example.com") — so
// importing kubeconfigs from multiple kontinuum instances never collides
// on a shared "oidc" user entry. The same cluster name is used for the
// context's cluster reference, and the same user name for its user
// reference, so all three always match. The context itself is named
// "oidc@kontinuum-<host>". When host looks local (see probablySelfSigned),
// insecure-skip-tls-verify is set on the cluster entry: kubectl otherwise
// refuses to send oidc-login's bearer token over a connection whose
// certificate it can't verify against a trusted CA — which a local
// deployment's self-signed certificate (e.g. compose.yaml's "proxy"
// service) never is.
func kubeconfig(origin, host, issuerURL, clientID string) string {
	clusterName := "kontinuum-" + stripPort(host)
	userName := clusterName
	contextName := "oidc@" + clusterName

	insecureLine := ""
	if probablySelfSigned(host) {
		insecureLine = "      insecure-skip-tls-verify: true\n"
	}

	return fmt.Sprintf(kubeconfigTemplate,
		clusterName, origin, insecureLine, contextName, clusterName, userName, contextName, userName, issuerURL, clientID)
}

// probablySelfSigned reports whether host is a loopback address or
// "localhost" — the only case kontinuum can infer a self-signed
// certificate from the hostname alone, since a real deployment always uses
// a real hostname with a CA-issued certificate, while a local one (e.g.
// compose.yaml's "proxy" service, which fronts kontinuum with exactly such
// a certificate) is reached through localhost or a loopback IP.
func probablySelfSigned(host string) bool {
	hostOnly := stripPort(host)

	if hostOnly == "localhost" {
		return true
	}

	ip := net.ParseIP(hostOnly)

	return ip != nil && ip.IsLoopback()
}

// stripPort removes a ":<port>" suffix from host, if present, e.g.
// "kontinuum.example.com:8080" -> "kontinuum.example.com". host without a
// port (or an unparseable one, e.g. a bare IPv6 address) is returned as-is.
func stripPort(host string) string {
	hostOnly, _, err := net.SplitHostPort(host)
	if err != nil {
		return host
	}

	return hostOnly
}

// requestOrigin derives the scheme+host a browser used to reach request,
// e.g. "https://kontinuum.example.com" or "http://localhost:8080". Prefers
// the X-Forwarded-Proto header a TLS-terminating reverse proxy sets over
// request.TLS, since kontinuum itself never terminates TLS (see README) and
// is expected to sit behind exactly such a proxy in any deployment where
// this matters.
func requestOrigin(request *http.Request) string {
	scheme := request.Header.Get("X-Forwarded-Proto")

	if scheme == "" {
		scheme = "http"

		if request.TLS != nil {
			scheme = "https"
		}
	}

	return scheme + "://" + request.Host
}
