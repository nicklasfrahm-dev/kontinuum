# Architecture

## Server

Kontinuum embeds [kommodity](https://github.com/kommodity-io/kommodity)'s `libkapi` package, which provides a generic Kubernetes-style apiserver, an apiextensions (CRD) server, and an aggregation layer, backed by pluggable storage (SQLite, PostgreSQL, etcd, ...). `pkg/cli/serve.go` assembles this into a single process:

- `api/v1alpha1` and `api/v1alpha2` register kontinuum's own custom resource types (`Kontinuum`, `Zone`, `Fabric`, `Instance`, `InstancePool`, `TalosCluster`, `Addon`) against `libkapi`'s scheme.
- A `libkapi.ServerFactory` mounts the `/app` web UI (`pkg/ui`) and, when OIDC is configured, the login/logout routes (`pkg/auth`) alongside the generated Kubernetes API — any request that doesn't match a UI route falls through to the API server's own handler.
- Each domain package below is wired in as a `libkapi.Controller`, running on the same controller-runtime manager the apiserver starts.

## CLI

`pkg/cli` holds the cobra command tree: `kontinuum serve` (start the server), `kontinuum version`, `kontinuum config` (import/inspect configuration), `kontinuum zone add` (fan out a new zone's `Zone`/`Instance`/`InstancePool`/`TalosCluster` objects — see [Add zone](workflows/zone-add.md)), and `kontinuum fabricmanager run` (a node agent, installed as a DaemonSet on every zone's own downstream cluster, that watches `Fabric` directly and self-discovers which one(s), if any, elect it as a gateway node — see below). `pkg/config` loads configuration from `KONTINUUM_`-prefixed environment variables, with CLI flags overriding it when explicitly set.

## Domain controllers (`pkg/domain/`)

Each controller lives in its own package and owns one CRD's reconcile loop, per [AGENTS.md](https://github.com/nicklasfrahm-dev/kontinuum/blob/main/AGENTS.md)'s convention:

| Package | Responsibility |
| --- | --- |
| `registry` | Every running kontinuum process registers itself as a `Kontinuum` object, heartbeats it on an interval, deregisters on graceful shutdown, and runs a TTL reconciler that deletes stale registrations. Also owns the `v1alpha1`↔`v1alpha2` conversion webhook. |
| `instance` | Discovers candidate machines: dials each `Instance`'s `spec.interfaces` in Talos maintenance mode and records the node's Talos version and interfaces once reachable. |
| `instancepool` | Claims discovered `Instance`s into an `InstancePool` via `spec.selector`, up to `spec.replicas`, using a conditional (CAS) update so two pools can't claim the same instance. |
| `taloscluster` | Bootstraps a Talos Kubernetes cluster from a control-plane `InstancePool` (and optional worker pools), then installs Cilium and cert-manager as addons. State machine driven by `status.conditions`. |
| `addon` | Backs `taloscluster`'s Helm-based addon install and pod-health probing. |
| `zone` | Once a zone's `TalosCluster` reports Ready, installs kontinuum's own downstream footprint into it: `kontinuum-system` namespace, `kontinuum-env` Secret/ConfigMap, `kontinuum` Deployment/Service, a cert-manager-backed `ClusterIssuer`/`Gateway`/`Certificate`/`HTTPRoute` exposing that zone's own kontinuum-server, and the `kontinuum fabricmanager` DaemonSet (with its own RBAC — read/watch on `Fabric`, update on `fabrics/status`, and a `ResourceNames`-scoped read on the Talos credential Secret `fabric` delivers). Also exports the shared `Zone`/`Instance`/`InstancePool`/`TalosCluster` fan-out logic both `kontinuum zone add` and the registry page's "Add zone" modal call into. |
| `fabric` | Carves a fixed-size subnet and gateway IP per zone out of a `Fabric`'s region-wide `spec.cidr` (deterministic, sticky IPAM — see `Allocate`), elects a per-zone gateway node via `spec.gatewaySelector`, and publishes that node's own free interfaces (`status.zones[].gatewayInterfaces` — every discovered interface not already carrying an address, its existing uplink left alone) as the desired target for the zone's own gateway address. It delivers a scoped Talos admin credential (a fresh `os:admin` client cert plus the cluster's own OS CA, not the raw secrets bundle) to the downstream cluster as a Secret, but never dials Talos or manages any workload itself — `pkg/cli/fabricmanager`'s own node agent, watching `Fabric` directly, reads that credential and does the actual work: assigning the gateway address (bridging more than one free interface first, STP enabled as a loop-prevention default, and tagging onto an 802.1q VLAN sub-interface when `spec.vlanID` is set), and installing the nftables masquerade rule (over netlink — no `nft(8)` binary needed) that routes outbound traffic through the node's own uplink. It reports the outcome back onto that same `Fabric`'s `status.zones[].conditions` (`NetworkConfigured`/`NATInstalled`/`Ready`) — the only conditions the hub controller itself never claims true, since only the process that actually applies the state can know whether it succeeded. Each zone's own network is independent: one zone's missing gateway candidate (or one with only a single, already-addressed interface — a gateway needs at least one free interface beyond its own uplink) only blocks that zone's own readiness, never the whole `Fabric`'s. No finalizer: a re-elected or deleted `Fabric` needs no hub-side teardown, since `pkg/cli/fabricmanager` notices on its own next reconcile pass that it's no longer any zone's own gateway and prunes its own nftables state accordingly. |
| `kms` | A dummy in-memory KMS server implementing Talos's disk-encryption KMS gRPC service, for local dev and integration tests — not for production use. |

See [Add zone](workflows/zone-add.md) for the full instance → pool → cluster → addon bootstrap flow (including a flow chart) and how a zone's own kontinuum-server gets installed and registers back into the hub, and [Remove zone](workflows/zone-remove.md) for tearing that back down.

## Web UI (`pkg/ui`)

Server-rendered HTML (Go `html/template`) styled with Tailwind CSS and progressively enhanced with [htmx](https://htmx.org/) — no separate frontend build or JavaScript framework. `pkg/ui/assets.go` vendors Tailwind, htmx, PrismJS, and the JetBrains Mono webfont at `go generate` time (`cmd/vendor-web-assets`) so the shipped container image has no CDN runtime dependency. Pages: topology, instance detail, registry, IAM (OIDC admin group role bindings), connect (kubectl access, with API keys/federated identity credentials planned).

## Authentication (`pkg/auth`)

A PKCE-based OpenID Connect login flow for the `/app` UI, plus bearer-token validation for the API itself. See [Authentication](authentication.md) for details.

## Storage

Backed by `libkapi`'s pluggable storage layer — SQLite for local development, PostgreSQL or etcd for anything shared. Selected via the `KONTINUUM_SERVER_STORAGE` connection string (see [Configuration](reference.md#configuration)).

A joined zone's own `kontinuum-server` is deliberately never handed a direct connection string to the hub's real database — only the control plane needs (or gets) that kind of network access. Instead, `pkg/domain/etcdproxy` exposes the hub's local Kine gRPC endpoint (etcd3-compatible KV/Watch/Lease) as an authenticated proxy, multiplexed onto the same port as everything else the hub serves (`libkapi.Ctx.GRPCServer`, the same h2c switch the web UI and API share). Each zone dials that endpoint through a small local relay of its own, which signs a fresh, short-lived JWT with the zone's own ed25519 identity key and attaches it as `Authorization: Bearer <jwt>` on every call; from the zone's own apiserver's point of view, this is indistinguishable from talking to a local Kine instance.

The `zone` controller issues that identity once, the first time a zone joins: a long-lived ed25519 keypair, wrapped in a self-signed certificate. The private half is delivered to the zone's own downstream cluster as a `kubernetes.io/tls` Secret; the hub keeps only the public half (also in a Secret, owned by the `Zone` so it's garbage-collected on teardown) and shows its SHA-256 thumbprint on the zone's own detail page. There is no rotation schedule — the certificate is effectively non-expiring, and it's the JWT's own short `exp` (not the certificate) that bounds how long a credential stays usable if intercepted. The hub authenticates incoming proxy calls against these Secrets via a small LRU-cached verifier, avoiding a Kubernetes API round trip on every proxied etcd RPC. `KONTINUUM_SERVER_GRPC_ENDPOINT` (see [Configuration](reference.md#zones)) tells the `zone` controller where that proxy is publicly reachable — it's what gets baked into every newly joined zone's own `KONTINUUM_SERVER_STORAGE`.
