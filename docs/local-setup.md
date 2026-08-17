# Local setup

## Prerequisites

- [Go 1.26+](https://go.dev/dl/) — we recommend using [gvm](https://github.com/moovweb/gvm) to manage Go versions:
  ```sh
  gvm install go1.26.4 -B
  gvm use go1.26.4 --default
  ```
- [Docker](https://docs.docker.com/get-docker/) (for the hot-reload dev environment)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

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

`make dev`'s `talos` service is a real single-node Talos "cluster" — actual Talos OS running in container mode (not a VM), the same shape [`pkg/domain/taloscluster`'s own e2e tests](https://github.com/nicklasfrahm-dev/kontinuum/blob/main/pkg/domain/taloscluster/e2e_test.go) boot — so you can exercise [Add zone](workflows/zone-add.md)'s entire flow end to end without any real hardware.

Once it's up, add the zone the same way you would against real hardware — just point `--talos-address` at the `talos` service's own static IP (`172.28.0.20`, fixed in `compose.yaml`) instead of a real machine's IP:

```sh
export KUBECONFIG=kontinuum.yaml
kontinuum zone add --region local --zone a --talos-address 172.28.0.20
```

Use the IP, not the Compose service name (`talos`): once the node moves past maintenance mode, Talos issues its own server certificate scoped to its detected hostname and addresses, not to whatever name it happened to be dialed by, so addressing it as `talos` fails TLS hostname verification the moment bootstrap starts applying its real machine config.

Skip `--wait` here — see below for why this setup never reaches `Installed`. Watch progress with `kubectl get zone,taloscluster,addon -n kontinuum-system` instead: `TalosCluster` should reach `Ready`/`AddonsInstalled` (a real Cilium + cert-manager install, on a real control-plane node) within a few minutes, at which point kontinuum's own downstream footprint — namespace, `kontinuum-env`, Deployment/Service, `ClusterIssuer`/`Gateway`/`Certificate` — gets installed, including the hub issuing and rotating this zone's own etcd-proxy credential (`kubectl get secret -n kontinuum-system local-a-etcd-auth`) and building its `KONTINUUM_SERVER_STORAGE` DSN, dialing back through the hub's etcd gRPC proxy for shared storage (see [Architecture](architecture.md#storage)) — the same path a real remote zone uses.

Two things to expect from there, neither a sign anything's broken:

- **The zone plateaus at `Installed = False, WaitingForCertificate`.** cert-manager's `ClusterIssuer` uses a real Let's Encrypt ACME HTTP-01 challenge (see [Add zone](workflows/zone-add.md#tls-acme-over-the-gateway-api-not-ingress)), which needs a real, publicly resolvable domain and a publicly reachable endpoint to answer it — neither exists inside a private Docker network. There's no local workaround for this short of standing up real DNS and exposing the proxy to the internet; treat everything up through `TalosCluster` reporting `Ready`/`AddonsInstalled` as the part this setup actually proves out.
- **The deployed `kontinuum` pod itself can crash-loop with an out-of-date image** if you're testing code that hasn't been merged and published yet: `zone.Reconciler.resolveImage` deploys whatever tag is actually published (`:latest`, or a registered Kontinuum's own `status.version`) — never your own working tree. Local changes to `pkg/cli/serve.go`/`pkg/domain/etcdproxy`/etc. only reach a joined zone once they've shipped in that published image (`kubectl logs -n kontinuum-system deploy/kontinuum` on the downstream cluster's own kubeconfig, fetched from `kubectl get secret -n kontinuum-system taloscluster-local-a -o jsonpath='{.data.kubeconfig}' | base64 -d`, shows why).

!!! tip
    Ready to contribute a change back? See [Contribution guidelines](contributing.md) for repo conventions and how to validate a change before opening a PR.
