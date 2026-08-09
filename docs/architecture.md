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
| `kms` | A dummy in-memory KMS server implementing Talos's disk-encryption KMS gRPC service, for local dev and integration tests — not for production use. |

See [Add zone](workflows/zone-add.md) for the full instance → pool → cluster → addon bootstrap flow (including a flow chart) and how a zone's own kontinuum-server gets installed and registers back into the hub, and [Remove zone](workflows/zone-remove.md) for tearing that back down.

## Web UI (`pkg/ui`)

Server-rendered HTML (Go `html/template`) styled with Tailwind CSS and progressively enhanced with [htmx](https://htmx.org/) — no separate frontend build or JavaScript framework. `pkg/ui/assets.go` vendors Tailwind, htmx, PrismJS, and the JetBrains Mono webfont at `go generate` time (`cmd/vendor-web-assets`) so the shipped container image has no CDN runtime dependency. Pages: topology, instance detail, registry (includes kubectl access), IAM (OIDC admin group role bindings).

## Authentication (`pkg/auth`)

A PKCE-based OpenID Connect login flow for the `/app` UI, plus bearer-token validation for the API itself. See [Authentication](authentication.md) for details.

## Storage

Backed by `libkapi`'s pluggable storage layer — SQLite for local development, PostgreSQL or etcd for anything shared. Selected via the `KONTINUUM_SERVER_STORAGE` connection string (see [Configuration](reference.md#configuration)).
