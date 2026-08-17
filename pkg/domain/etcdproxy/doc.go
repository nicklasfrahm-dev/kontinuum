// Package etcdproxy lets a zone's own kontinuum-server reach the hub's
// storage without ever dialing the database directly — only the control
// plane needs (or gets) that. The hub exposes its own local Kine gRPC
// endpoint (see pkg/libkapi/storage) as an authenticated KV/Watch/Lease
// passthrough, multiplexed onto the same port as everything else
// (libkapi.Ctx.GRPCServer). A zone connects to that endpoint through a
// small local relay of its own — see Relay — that attaches its bearer
// credential (see EncodeToken) on every call, so the zone's own apiserver
// storage layer only ever has to dial a plain, unauthenticated local
// unix socket (libkapi's already-supported "unix://" storage scheme), no
// different in shape from talking to a local Kine instance.
//
// The proxy relays KV/Watch/Lease calls verbatim to whatever Kine already
// exposes on the hub — it never interprets or transforms them — so it
// works unchanged regardless of which real backend (postgres/sqlite/mysql/
// nats) sits behind Kine there.
package etcdproxy
