package etcdproxy

import "net/url"

// RelayScheme is the KONTINUUM_SERVER_STORAGE URL scheme that tells
// kontinuum's own server bootstrap (see pkg/cli/serve.go) to start a Relay
// instead of handing the value straight to libkapi.WithStorage — see
// doc.go's own description of why a zone needs this indirection at all.
const RelayScheme = "grpc"

// BuildRelayDSN builds the KONTINUUM_SERVER_STORAGE value a newly joined
// zone's own kontinuum-env ConfigMap carries (see
// pkg/domain/zone/workload.go's ensureConfigMap): "grpc://<zone>@
// <hubEndpoint>", parsed back apart by ParseRelayDSN. Carries no
// credential of its own — unlike the bearer-token scheme this replaced, a
// zone's own identity now lives in its own mounted kubernetes.io/tls
// Secret (see BuildDownstreamIdentitySecret), not in this DSN string; zone
// stays here purely so the DSN remains self-describing.
func BuildRelayDSN(zone, hubEndpoint string) string {
	dsn := url.URL{
		Scheme: RelayScheme,
		User:   url.User(zone),
		Host:   hubEndpoint,
	}

	return dsn.String()
}

// ParseRelayDSN reverses BuildRelayDSN, returning the zone name and the hub
// endpoint it encodes. The final bool is false for anything that isn't a
// well-formed "grpc://<zone>@<host>[:<port>]" value — callers (see
// pkg/cli/serve.go) use that to tell "this is a zone talking through
// Relay" apart from every other KONTINUUM_SERVER_STORAGE shape
// (postgres://, sqlite://, etcd://, ...), which are handed to
// libkapi.WithStorage unchanged.
func ParseRelayDSN(dsn string) (string, string, bool) {
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Scheme != RelayScheme || parsed.User == nil || parsed.Host == "" {
		return "", "", false
	}

	if parsed.User.Username() == "" {
		return "", "", false
	}

	return parsed.User.Username(), parsed.Host, true
}
