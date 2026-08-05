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

`make dev` starts PostgreSQL, [air](https://github.com/air-verse/air) hot-reload, and a Caddy proxy that terminates TLS with a self-signed certificate on `https://localhost:8443` (in front of the plain-HTTP `http://localhost:8080` air serves), all via Docker Compose:

```sh
# start dev environment (air + postgres + proxy)
make dev
# stop dev environment
make dev-down
# stop and remove volumes
make dev-clean
```

For local development, copy `.env.example` to `.env` and adjust as needed — `make dev` loads it automatically via `compose.yaml`'s `env_file`.

!!! tip
    Ready to contribute a change back? See [Contribution guidelines](contributing.md) for repo conventions and how to validate a change before opening a PR.
