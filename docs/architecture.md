# Architecture

## Server

Kontinuum embeds [kommodity](https://github.com/kommodity-io/kommodity)'s `libkapi` package, which provides a generic Kubernetes-style apiserver, an apiextensions (CRD) server, and an aggregation layer, backed by pluggable storage (SQLite, PostgreSQL, etcd, ...). `pkg/cli/serve.go` assembles this into a single process:

- `api/v1alpha1` and `api/v1alpha2` register kontinuum's own custom resource types (`Kontinuum`, `Zone`, `Instance`, `InstancePool`, `TalosCluster`, `Addon`) against `libkapi`'s scheme.
- A `libkapi.ServerFactory` mounts the `/app` web UI (`pkg/ui`) and, when OIDC is configured, the login/logout routes (`pkg/auth`) alongside the generated Kubernetes API — any request that doesn't match a UI route falls through to the API server's own handler.
- Each domain package below is wired in as a `libkapi.Controller`, running on the same controller-runtime manager the apiserver starts.

## CLI

`pkg/cli` holds the cobra command tree: `kontinuum serve` (start the server), `kontinuum version`, `kontinuum config` (import/inspect configuration), and `kontinuum zone add` (fan out a new zone's `Zone`/`Instance`/`InstancePool`/`TalosCluster` objects — see [Add zone](workflows/zone-add.md)). `pkg/config` loads configuration from `KONTINUUM_`-prefixed environment variables, with CLI flags overriding it when explicitly set.

## Domain controllers (`pkg/domain/`)

Each controller lives in its own package and owns one CRD's reconcile loop, per [AGENTS.md](https://github.com/nicklasfrahm-dev/kontinuum/blob/main/AGENTS.md)'s convention:

| Package | Responsibility |
| --- | --- |
| `registry` | Every running kontinuum process registers itself as a `Kontinuum` object, heartbeats it on an interval, deregisters on graceful shutdown, and runs a TTL reconciler that deletes stale registrations. Also owns the `v1alpha1`↔`v1alpha2` conversion webhook. |
| `instance` | Discovers candidate machines: dials each `Instance`'s `spec.interfaces` in Talos maintenance mode and records the node's Talos version and interfaces once reachable. |
| `instancepool` | Claims discovered `Instance`s into an `InstancePool` via `spec.selector`, up to `spec.replicas`, using a conditional (CAS) update so two pools can't claim the same instance. |
| `taloscluster` | Bootstraps a Talos Kubernetes cluster from a control-plane `InstancePool` (and optional worker pools), then installs Cilium and cert-manager as addons. State machine driven by `status.conditions`. |
| `addon` | Backs `taloscluster`'s Helm-based addon install and pod-health probing. |
| `zone` | Once a zone's `TalosCluster` reports Ready, installs kontinuum's own downstream footprint into it: `kontinuum-system` namespace, `kontinuum-env` Secret/ConfigMap, `kontinuum` Deployment/Service, and a cert-manager-backed `ClusterIssuer`/`Gateway`/`Certificate`/`HTTPRoute` exposing that zone's own kontinuum-server. Also exports the shared `Zone`/`Instance`/`InstancePool`/`TalosCluster` fan-out logic both `kontinuum zone add` and the registry page's "Add zone" modal call into. |
| `frontend` | With a frontend host configured, keeps that one hostname resolving to every `Ready` zone: the holder of the fleet-wide lease maintains a single `DNSEndpoint`, in one zone's cluster, listing every `Ready` zone's `Gateway` address. See [Frontend host](reference.md#frontend-host). |
| `kms` | A dummy in-memory KMS server implementing Talos's disk-encryption KMS gRPC service, for local dev and integration tests — not for production use. |

See [Add zone](workflows/zone-add.md) for the full instance → pool → cluster → addon bootstrap flow (including a flow chart) and how a zone's own kontinuum-server gets installed and registers back into the hub, and [Remove zone](workflows/zone-remove.md) for tearing that back down.

## Zone leases and hub-less fleets (`pkg/domain/zonelease`)

Every kontinuum-server process in a fleet runs the same controllers against the same shared storage, so each zone-scoped write is gated on a per-zone `Lease` (`zonelock-<region>-<zone>`): whichever process holds it reconciles that zone, and the rest back off. With a hub, a zone's own process never takes its own zone's lease, and the hub reconciles it. Hub-less, a zone contends for its own lease like any other peer; see [Single-zone fleets](#single-zone-fleets).

A lease is held by one process, named after its pod, and stays with it for as long as it keeps renewing it:

- **During a reconcile**, the holder renews the lease in the background, and the reconcile's writes stop the moment the lease is lost or goes unrenewed for too long. A long reconcile, such as a Talos health check or a Helm install, therefore never keeps writing after another process took over.
- **On graceful shutdown**, once its controllers have stopped, a process releases every lease it holds. The next pod of the zone `Deployment`'s rolling update, or another peer, takes over at once instead of after the 45-second lease duration.

Fleet-wide state that belongs to no single zone, such as the registry's stale-`Kontinuum` sweep and the admin-group `ClusterRoleBinding`s, uses one more lease, `zonelock-global`. Who may contend for it depends on the [storage mode](reference.md#storage-modes):

- **`Proxy`** needs a hub, because every zone's storage DSN is a relay back to the hub's own endpoint. Only the hub contends for the global lease.
- **`Direct` and `NATS`** are hub-less: every process holds an identical storage DSN, so any of them can onboard a joining zone. Every peer, zone or hub, contends for the global lease, and the lease serializes them exactly as it does for zone keys. A hub may still run in these modes; it is simply one more peer.

Hub-less follows from the storage mode and nothing else. It is never inferred from whether a hub happens to be registered, so a hub restart can't switch the fleet's behaviour.

### Single-zone fleets

A hub-less fleet may consist of a single zone, for example a test setup, or a fleet whose other zones are down during an upgrade. Its only zone reconciles itself: it keeps its own `TalosCluster`, addons and `kontinuum` `Deployment` converged, including rolling that `Deployment` onto a new version.

The exception is removing the zone. Tearing a zone down deletes the very `Deployment`, and resets the very nodes, that its own process runs on. So a zone's process stops taking its own zone's lease as soon as its `Zone` or `TalosCluster` is being deleted, or is gone. Another process finishes the removal:

- in a fleet with other zones, any other zone;
- for a fleet's last zone, an outside process against the same storage — see [Removing a fleet's last zone](workflows/zone-remove.md#removing-a-fleets-last-zone).

What a zone can't do for itself is recover from a version that fails to start: nothing else in a single-zone fleet can roll it back. Run [`kontinuum up`](workflows/up.md) or a hub against the same storage to repair it.

## Web UI (`pkg/ui`)

Server-rendered HTML (Go `html/template`) styled with Tailwind CSS and progressively enhanced with [htmx](https://htmx.org/) — no separate frontend build or JavaScript framework. `pkg/ui/assets.go` vendors Tailwind, htmx, PrismJS, and the JetBrains Mono webfont at `go generate` time (`cmd/vendor-web-assets`) so the shipped container image has no CDN runtime dependency. Pages: topology, instance detail, registry, IAM (OIDC admin group role bindings), connect (kubectl access, with API keys/federated identity credentials planned).

## Authentication (`pkg/auth`)

A PKCE-based OpenID Connect login flow for the `/app` UI, plus bearer-token validation for the API itself. See [Authentication](authentication.md) for details.

## Storage

Backed by `libkapi`'s pluggable storage layer — SQLite for local development, PostgreSQL or etcd for anything shared. Selected via the `KONTINUUM_SERVER_STORAGE` connection string (see [Configuration](reference.md#configuration)).

How a joined zone reaches that storage depends on the [storage mode](reference.md#storage-modes). In `Direct` mode, used when the database is reachable from every zone, each zone is simply handed the database's own connection string. The rest of this section describes `Proxy` mode, the default whenever the database is only reachable from the hub.

In `Proxy` mode, a joined zone's own `kontinuum-server` is deliberately never handed a direct connection string to the hub's real database — only the control plane needs (or gets) that kind of network access. Instead, `pkg/domain/etcdproxy` exposes the hub's local Kine gRPC endpoint (etcd3-compatible KV/Watch/Lease) as an authenticated proxy, multiplexed onto the same port as everything else the hub serves (`libkapi.Ctx.GRPCServer`, the same h2c switch the web UI and API share). Each zone dials that endpoint through a small local relay of its own, which signs a fresh, short-lived JWT with the zone's own ed25519 identity key and attaches it as `Authorization: Bearer <jwt>` on every call; from the zone's own apiserver's point of view, this is indistinguishable from talking to a local Kine instance.

The `zone` controller issues that identity once, the first time a zone joins: a long-lived ed25519 keypair, wrapped in a self-signed certificate. The private half is delivered to the zone's own downstream cluster as a `kubernetes.io/tls` Secret; the hub keeps only the public half (also in a Secret, owned by the `Zone` so it's garbage-collected on teardown) and shows its SHA-256 thumbprint on the zone's own detail page. There is no rotation schedule — the certificate is effectively non-expiring, and it's the JWT's own short `exp` (not the certificate) that bounds how long a credential stays usable if intercepted. The hub authenticates incoming proxy calls against these Secrets via a small LRU-cached verifier, avoiding a Kubernetes API round trip on every proxied etcd RPC. `KONTINUUM_SERVER_GRPC_ENDPOINT` (see [Configuration](reference.md#zones)) tells the `zone` controller where that proxy is publicly reachable — it's what gets baked into every newly joined zone's own `KONTINUUM_SERVER_STORAGE`.
