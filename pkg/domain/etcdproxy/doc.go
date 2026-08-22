// Package etcdproxy lets a zone's own kontinuum-server reach the hub's
// storage without ever dialing the database directly — only the control
// plane needs (or gets) that. The hub exposes its own local Kine gRPC
// endpoint (see pkg/libkapi/storage) as an authenticated KV/Watch/Lease
// passthrough, multiplexed onto the same port as everything else
// (libkapi.Ctx.GRPCServer). A zone connects to that endpoint through a
// small local relay of its own — see Relay — that signs a fresh, short-lived
// JWT with its own ed25519 identity key on every call (see SignToken), so
// the zone's own apiserver storage layer only ever has to dial a plain,
// unauthenticated local unix socket (libkapi's already-supported "unix://"
// storage scheme), no different in shape from talking to a local Kine
// instance.
//
// Each zone's identity is a long-lived ed25519 keypair, periodically
// rotated (see IdentityRotationInterval): the hub keeps only the public
// half of whichever keypair is current (plus the just-superseded one, for
// a brief overlap window — see IdentityPair), wrapped in a self-signed
// certificate (see BuildPublicSecret), and the zone's own downstream
// cluster keeps the full current keypair as a kubernetes.io/tls Secret
// (see BuildDownstreamIdentitySecret). What actually bounds a single JWT's
// own freshness is its own short exp, not the certificate's — see jwt.go's
// own doc — but the certificate itself is what's rotated, and needs
// verifying, since it's what SignToken's key traces back to. A zone's own
// Relay reads its own private key through WatchIdentity, not once at
// startup, so a rotation delivered to its downstream Secret takes effect
// immediately, with no restart of the process running Relay.
//
// The proxy relays KV/Watch/Lease calls verbatim to whatever Kine already
// exposes on the hub — it never interprets or transforms them — so it
// works unchanged regardless of which real backend (postgres/sqlite/mysql/
// nats) sits behind Kine there.
package etcdproxy
