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

`make dev` starts PostgreSQL, [air](https://github.com/air-verse/air) hot-reload, a Caddy proxy that terminates TLS with a self-signed certificate on `https://localhost:8443` (in front of the plain-HTTP `http://localhost:8080` air serves), a single-node Talos "cluster" for exercising the full zone-join flow, and a local DNS server and ACME CA for that zone's hostname and certificate (see below), all via Docker Compose:

```sh
# start dev environment (air + postgres + proxy + talos + pdns + pebble)
make dev
# stop dev environment
make dev-down
# stop and remove volumes
make dev-clean
```

For local development, copy `.env.example` to `.env` and adjust as needed — `make dev` loads it automatically via `compose.yaml`'s `env_file`.

## Trying the full zone flow locally

`make dev`'s `talos` service is a real single-node Talos "cluster" — actual Talos OS running as a QEMU/KVM VM (see [`hack/talos-qemu`](https://github.com/nicklasfrahm-dev/kontinuum/tree/main/hack/talos-qemu)), not Talos's own Docker container-mode image — so you can exercise [Add zone](workflows/zone-add.md)'s entire flow end to end, including [Remove zone](workflows/zone-remove.md)'s reset step, without any real hardware. Container-mode Talos can't do that last part: its `runtime.ModeContainer` Reset sequence never actually wipes disks or reboots, no matter how it's called — a structural limitation of that image, not a kontinuum bug. A QEMU-booted node reports `runtime.ModeMetal` instead, so Reset behaves exactly like it would on real hardware.

Once it's up, add the zone the same way you would against real hardware — just point `--talos-address` at the `talos` service's own Compose DNS name instead of a real machine's IP:

```sh
export KUBECONFIG=kontinuum.yaml
kontinuum zone add --region local --zone a --talos-address talos
```

`zone add` resolves `talos` to its container's own IP before ever storing it (see `instance.ResolveAddress`), and the taloscluster controller always dials the node's real discovered IP from then on, never the hostname — so this works even though, once the node moves past maintenance mode, Talos issues its own server certificate scoped to its detected hostname and addresses, not to whatever name it happened to originally be dialed by.

Watch progress with `kubectl get zone,taloscluster,addon -n kontinuum-system`: `TalosCluster` should reach `Ready`/`AddonsInstalled` (a real Cilium + cert-manager install, on a real control-plane node) within a few minutes, at which point kontinuum's own downstream footprint — namespace, `kontinuum-env`, Deployment/Service — gets installed, including the hub issuing this zone's own etcd-proxy identity (`kubectl get secret -n kontinuum-system local-a-etcd-auth`) and building its `KONTINUUM_SERVER_STORAGE` DSN, dialing back through the hub's etcd gRPC proxy for shared storage (see [Architecture](architecture.md#storage)) — the same path a real remote zone uses.

`make dev` also runs a local DNS server and a local certificate authority, so the zone's network layer installs and finishes too:

- **`pdns`** ([PowerDNS](https://www.powerdns.com/)) hosts `kontinuum.test`, the default `KONTINUUM_SERVER_DNS_DOMAIN` (a reserved test TLD, so it can never collide with a real public record). The hub is configured with `KONTINUUM_SERVER_DNS_PROVIDER=pdns`, so each zone's auto-wired external-dns publishes `<zone>.<region>.kontinuum.test` there (see [Add zone](workflows/zone-add.md#dns-auto-wiring-external-dns-from-a-flat-yaml-credential)). The `talos` container forwards that domain to `pdns`, so the zone's own cluster can resolve it too.
- **`pebble`** ([Pebble](https://github.com/letsencrypt/pebble), Let's Encrypt's ACME test server) issues each zone's certificate in place of Let's Encrypt, which could never validate a zone on a private Docker network. It's configured to accept every challenge without checking it. Its API certificate comes from its own test CA, so the hub also sets `KONTINUUM_ACME_INSECURE_SKIP_VERIFY=true` — never set that against a real ACME server.

With both in place, `kontinuum zone add --wait` reaches `Installed` once the certificate is issued. To look up a zone's record from your machine:

```sh
dig @127.0.0.1 -p 1053 a.local.kontinuum.test
```

Pebble generates a new root CA every time it starts, so your browser won't trust a zone's certificate unless you import the current root:

```sh
curl -k https://localhost:15000/roots/0 > pebble-root.pem
```

One more thing to expect, not a sign anything's broken: **the deployed `kontinuum` pod runs whatever your local hub's own registered version publishes, not your working tree directly** — `zone.Reconciler.resolveImage` always inherits the image tag off an already-registered Kontinuum's own `status.version`, which for a plain `make dev` hub (no `-ldflags -X` version override) is literally `"dev"`. If you're testing local changes to `pkg/cli/serve.go`/`pkg/domain/etcdproxy`/etc., publish them first:

```sh
VERSION=dev make image-push
```

This builds and pushes the working tree under `ghcr.io/nicklasfrahm-dev/kontinuum:dev` (see the Makefile's own doc) — the same tag CI keeps in sync with `main` on every push, so without running this, a freshly joined zone deploys whatever `main` last published, not your local changes. (`kubectl logs -n kontinuum-system deploy/kontinuum` on the downstream cluster's own kubeconfig, fetched from `kubectl get secret -n kontinuum-system taloscluster-local-a -o jsonpath='{.data.kubeconfig}' | base64 -d`, shows which image actually landed.)

A zone that already joined before you ran this picks up the new push on its own — `resolveImage` pins `:dev` to the digest it currently resolves to, so pushing a new build under the same tag changes that pinned reference and triggers a normal Deployment rollout, typically within the reconciler's own retry interval. No manual `kubectl rollout restart` needed.

!!! tip
    Ready to contribute a change back? See [Contribution guidelines](contributing.md) for repo conventions and how to validate a change before opening a PR.
