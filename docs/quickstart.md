# Quickstart

## Prerequisites

- [Go 1.26+](https://go.dev/dl/) — we recommend using [gvm](https://github.com/moovweb/gvm) to manage Go versions:
  ```sh
  gvm install go1.26.4 -B
  gvm use go1.26.4 --default
  ```
- [Docker](https://docs.docker.com/get-docker/) (for the dev environment)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

## Build & run

```sh
make build
make run
```

This builds the `kontinuum` binary and starts it with dev-friendly logging (`info`, console format), listening on `:8080` with a local SQLite database by default.

## Development environment

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

## Connect with kubectl

```sh
export KUBECONFIG=kontinuum.yaml
kubectl get namespaces
kubectl create namespace demo
```

## Configuration

Configuration is loaded from `KONTINUUM_`-prefixed environment variables. Env-var names are auto-derived from the config struct path (e.g. `Server.Addr` → `KONTINUUM_SERVER_ADDR`).

| Env var                       | Description                                                                        | Default                     |
| ------------------------------ | ----------------------------------------------------------------------------------- | ---------------------------- |
| `KONTINUUM_SERVER_ADDR`       | Listener address                                                                     | `:8080`                     |
| `KONTINUUM_SERVER_STORAGE`    | Storage connection string (`sqlite://`, `postgres://`, `mysql://`, `etcd://`, ...)   | `sqlite://kontinuum.db`     |
| `KONTINUUM_SERVER_REGION`     | Region this server manages. Leave unset, along with Zone, to run as the control-plane entrypoint. | *(empty)*    |
| `KONTINUUM_SERVER_ZONE`       | Availability zone this server manages. Leave unset, along with Region, to run as the control-plane entrypoint. | *(empty)* |
| `KONTINUUM_LOG_LEVEL`         | Log level (`debug`, `info`, `warn`, `error`)                                         | `warn`                      |
| `KONTINUUM_LOG_FORMAT`        | Log format (`console`, `text`, `json`)                                              | `json`                      |
| `KONTINUUM_OIDC_ISSUER_URL`   | OIDC issuer URL (e.g. Dex). Empty disables OIDC entirely.                            | *(empty)*                   |
| `KONTINUUM_OIDC_CLIENT_ID`    | OAuth 2.0 public client ID registered with the issuer                                | `kontinuum`                 |
| `KONTINUUM_OIDC_REDIRECT_URL` | Callback URL registered with the issuer for the /app login flow                      | `http://localhost:8080/app` |
| `KONTINUUM_OIDC_ADMIN_GROUPS` | Comma-delimited OIDC groups granted full (system:masters-equivalent) access          | *(empty)*                   |

Flags override environment variables when explicitly set:

```sh
kontinuum serve --addr :9090 --storage postgres://user:pass@host/db
```

See [Authentication](authentication.md) for the OIDC variables.

## Container

```sh
# builds kontinuum:<version> via distroless/static
make image
docker run -p 8080:8080 -e KONTINUUM_SERVER_STORAGE=postgres://... kontinuum:latest
```

The container image is built on `distroless/static` with `CGO_ENABLED=0`, so SQLite storage is not available — use PostgreSQL or etcd.
