package etcdproxy

import "net/url"

// RelayScheme is the KONTINUUM_SERVER_STORAGE URL scheme that tells
// kontinuum's own server bootstrap (see pkg/cli/serve.go) to start a Relay
// instead of handing the value straight to libkapi.WithStorage — see
// doc.go's own description of why a zone needs this indirection at all.
const RelayScheme = "grpc"

// BuildRelayDSN builds the KONTINUUM_SERVER_STORAGE value a newly joined
// zone's own kontinuum-env ConfigMap carries (see
// pkg/domain/zone/workload.go's ensureConfigMap): "grpc://<zone>:<key>@
// <hubEndpoint>", parsed back apart by ParseRelayDSN.
func BuildRelayDSN(zone, key, hubEndpoint string) string {
	dsn := url.URL{
		Scheme: RelayScheme,
		User:   url.UserPassword(zone, key),
		Host:   hubEndpoint,
	}

	return dsn.String()
}

// ParseRelayDSN reverses BuildRelayDSN, returning the zone name, its
// current auth key, and the hub endpoint it encodes. The final bool is
// false for anything that isn't a well-formed
// "grpc://<zone>:<key>@<host>[:<port>]" value — callers (see
// pkg/cli/serve.go) use that to tell "this is a zone talking through
// Relay" apart from every other KONTINUUM_SERVER_STORAGE shape
// (postgres://, sqlite://, etcd://, ...), which are handed to
// libkapi.WithStorage unchanged.
func ParseRelayDSN(dsn string) (string, string, string, bool) {
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Scheme != RelayScheme || parsed.User == nil || parsed.Host == "" {
		return "", "", "", false
	}

	key, hasKey := parsed.User.Password()
	if !hasKey || parsed.User.Username() == "" {
		return "", "", "", false
	}

	return parsed.User.Username(), key, parsed.Host, true
}
