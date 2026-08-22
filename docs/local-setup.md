# Local setup

## Prerequisites

- [Go 1.26+](https://go.dev/dl/) — we recommend using [gvm](https://github.com/moovweb/gvm) to manage Go versions:
  ```sh
  gvm install go1.26.4 -B
  gvm use go1.26.4 --default
  ```
- [Docker](https://docs.docker.com/get-docker/) (for the hot-reload dev environment)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- `/dev/kvm` (hardware virtualization) on the Docker host — needed by the `talos` service's QEMU-based node, see below

## Build & run

```sh
make build
make run
```

This builds the `kontinuum` binary and starts it with dev-friendly logging (`info`, console format), listening on `:8080` with a local SQLite database by default.

## Connect with kubectl

```sh
export KUBECONFIG=kontinuum.yaml
kubectl get namespaces
kubectl create namespace demo
```

See [Configuration](reference.md#configuration) for the full list of `KONTINUUM_`-prefixed environment variables, and [Authentication](authentication.md) for how OIDC and anonymous access interact — the server refuses to start until one of those is configured.

## Hot-reload development environment

`make dev` starts PostgreSQL, [air](https://github.com/air-verse/air) hot-reload, a Caddy proxy that terminates TLS with a self-signed certificate on `https://localhost:8443` (in front of the plain-HTTP `http://localhost:8080` air serves), and a single-node Talos "cluster" for exercising the full zone-join flow (see below), all via Docker Compose:

```sh
# start dev environment (air + postgres + proxy + talos)
make dev
# stop dev environment
make dev-down
# stop and remove volumes
make dev-clean
```

For local development, copy `.env.example` to `.env` and adjust as needed — `make dev` loads it automatically via `compose.yaml`'s `env_file`.

## Trying the full zone flow locally

`make dev`'s `talos` service is a real single-node Talos "cluster" — actual Talos OS running as a QEMU/KVM VM (see `hack/talos-qemu`), not Talos's own Docker container-mode image — so you can exercise [Add zone](workflows/zone-add.md)'s entire flow end to end, including [Remove zone](workflows/zone-remove.md)'s reset step, without any real hardware. Container-mode Talos can't do that last part: its `runtime.ModeContainer` Reset sequence never actually wipes disks or reboots, no matter how it's called — a structural limitation of that image, not a kontinuum bug. A QEMU-booted node reports `runtime.ModeMetal` instead, so Reset behaves exactly like it would on real hardware.

Once it's up, add the zone the same way you would against real hardware — just point `--talos-address` at the `talos` service's own Compose DNS name instead of a real machine's IP:

```sh
export KUBECONFIG=kontinuum.yaml
kontinuum zone add --region local --zone a --talos-address talos
```

`zone add` resolves `talos` to its container's own IP before ever storing it (see `instance.ResolveAddress`), and the taloscluster controller always dials the node's real discovered IP from then on, never the hostname — so this works even though, once the node moves past maintenance mode, Talos issues its own server certificate scoped to its detected hostname and addresses, not to whatever name it happened to originally be dialed by.

Watch progress with `kubectl get zone,taloscluster,addon -n kontinuum-system`: `TalosCluster` should reach `Ready`/`AddonsInstalled` (a real Cilium + cert-manager install, on a real control-plane node) within a few minutes, at which point kontinuum's own downstream footprint — namespace, `kontinuum-env`, Deployment/Service — gets installed, including the hub issuing this zone's own etcd-proxy identity (`kubectl get secret -n kontinuum-system local-a-etcd-auth`) and building its `KONTINUUM_SERVER_STORAGE` DSN, dialing back through the hub's etcd gRPC proxy for shared storage (see [Architecture](architecture.md#storage)) — the same path a real remote zone uses.

What happens next depends on `KONTINUUM_SERVER_DNS_DOMAIN` (see `.env.example`):

- **Left unset (the default):** the Zone skips its network layer entirely — no `ClusterIssuer`/`Gateway`/`Certificate`/`HTTPRoute` (see `pkg/domain/zone`'s own `installNetwork` doc) — and `--wait` reaches `Installed` on its own, once the workload itself deploys. This is the setup for exercising the bootstrap/join flow itself, not for actually reaching the deployed `kontinuum` pod over HTTPS.
- **Set to a real domain you control:** the full network layer installs, but skip `--wait` — cert-manager's `ClusterIssuer` uses a real Let's Encrypt ACME HTTP-01 challenge (see [Add zone](workflows/zone-add.md#tls-acme-over-the-gateway-api-not-ingress)), which needs a publicly reachable endpoint to answer it — nothing inside a private Docker network is, so the zone plateaus at `Installed = False, WaitingForCertificate` unless you've also exposed the proxy to the internet under that domain. Treat everything up through `TalosCluster` reporting `Ready`/`AddonsInstalled` as the part this setup actually proves out.

One more thing to expect either way, not a sign anything's broken: **the deployed `kontinuum` pod runs whatever your local hub's own registered version publishes, not your working tree directly** — `zone.Reconciler.resolveImage` always inherits the image tag off an already-registered Kontinuum's own `status.version`, which for a plain `make dev` hub (no `-ldflags -X` version override) is literally `"dev"`. If you're testing local changes to `pkg/cli/serve.go`/`pkg/domain/etcdproxy`/etc., publish them first:

```sh
VERSION=dev make image-push
```

This builds and pushes the working tree under `ghcr.io/nicklasfrahm-dev/kontinuum:dev` (see the Makefile's own doc) — the same tag CI keeps in sync with `main` on every push, so without running this, a freshly joined zone deploys whatever `main` last published, not your local changes. (`kubectl logs -n kontinuum-system deploy/kontinuum` on the downstream cluster's own kubeconfig, fetched from `kubectl get secret -n kontinuum-system taloscluster-local-a -o jsonpath='{.data.kubeconfig}' | base64 -d`, shows which image actually landed.)

!!! tip
    Ready to contribute a change back? See [Contribution guidelines](contributing.md) for repo conventions and how to validate a change before opening a PR.
