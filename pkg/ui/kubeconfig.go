package ui

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

// kubeconfigTemplate is a kubectl kubeconfig that authenticates via
// kubectl oidc-login (https://github.com/int128/kubelogin) as an exec
// credential plugin, matching kontinuum's /app login flow: a public OAuth
// 2.0 client (no client secret) using PKCE. %s placeholders are, in order:
// the cluster name (the host), the API server origin, the cluster's
// insecure-skip-tls-verify line (empty unless the origin is plain HTTP),
// the context name, the cluster name again (the context's cluster
// reference), the context name again (current-context), and the OIDC
// issuer URL and client ID.
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
      user: oidc
current-context: %s
users:
  - name: oidc
    user:
      exec:
        apiVersion: client.authentication.k8s.io/v1beta1
        command: kubectl
        args:
          - oidc-login
          - get-token
          - --oidc-issuer-url=%s
          - --oidc-client-id=%s
          - --oidc-use-pkce
        interactiveMode: IfAvailable
`

// kubeconfig renders a kubectl-oidc-login-based kubeconfig pointed at
// origin (see requestOrigin), authenticating against issuerURL/clientID.
// The cluster is named after host with any port stripped (e.g.
// "kontinuum.example.com"), and the same name is used for the context's
// cluster reference, so the two always match. The context itself is named
// "oidc@<host>", matching the "oidc" user entry. A plain-HTTP origin
// (kontinuum itself never terminates TLS — see README) sets
// insecure-skip-tls-verify on the cluster entry.
func kubeconfig(origin, host, issuerURL, clientID string) string {
	clusterName := stripPort(host)
	contextName := "oidc@" + clusterName

	insecureLine := ""
	if strings.HasPrefix(origin, "http://") {
		insecureLine = "      insecure-skip-tls-verify: true\n"
	}

	return fmt.Sprintf(kubeconfigTemplate,
		clusterName, origin, insecureLine, contextName, clusterName, contextName, issuerURL, clientID)
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
