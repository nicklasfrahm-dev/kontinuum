// Package registry implements kontinuum's server registry: every running
// kontinuum process registers itself as a kontinuum.sh/v1alpha2 Kontinuum
// object (see api/v1alpha2), heartbeats it on an interval, deregisters it on
// graceful shutdown, and runs a TTL reconciler that deletes any Kontinuum
// whose heartbeat has gone stale.
package registry
