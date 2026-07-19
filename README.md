# kontinuum

A Kubernetes-style API server built on [kommodity](https://github.com/kommodity-io/kommodity)'s `libkapi` package. It embeds a generic apiserver + apiextensions (CRD) server + aggregation layer, backed by pluggable storage (SQLite, PostgreSQL, etcd, ...).

> **Warning:** The server ships with no TLS and no authentication by default. Put a TLS-terminating, authenticating proxy in front before exposing it outside a trusted network.

## Quick start

### Prerequisites

- [Go 1.26+](https://go.dev/dl/) — we recommend using [gvm](https://github.com/moovweb/gvm) to manage Go versions:
  ```sh
  gvm install go1.26.4 -B
  gvm use go1.26.4 --default
  ```
- [Docker](https://docs.docker.com/get-docker/) (for the dev environment)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

### Build & run

```sh
make build
make run
```

### Development environment

Starts PostgreSQL and [air](https://github.com/air-verse/air) hot-reload via Docker Compose:

```sh
# start dev environment (air + postgres)
make dev
# stop dev environment
make dev-down
# stop and remove volumes
make dev-clean
```

### Connect with kubectl

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

### Authentication (OIDC)

Setting `KONTINUUM_OIDC_ISSUER_URL` turns on three things at once:

- **API bearer-token validation** — requests to the Kubernetes-style API must carry a valid `Authorization: Bearer <id_token>` issued by the configured issuer for `KONTINUUM_OIDC_CLIENT_ID`.
- **Deny-by-default authorization** — only `system:masters`, authenticated service accounts, and the groups listed in `KONTINUUM_OIDC_ADMIN_GROUPS` get access; every other group gets nothing (`libkapi.WithAdminAuthorizer`). **`KONTINUUM_OIDC_ADMIN_GROUPS` is required once OIDC is enabled** — the server refuses to start with OIDC on and no admin groups configured, since that would lock everyone out.
- **The /app UI's PKCE login flow** — since the client is public (no client secret), the browser exchanges an authorization code for an ID token using PKCE (RFC 7636) and stores it in an HttpOnly session cookie. `KONTINUUM_OIDC_REDIRECT_URL` must exactly match one of the redirect URIs registered with the issuer for the client. `/app` itself never auto-redirects into the login flow — it shows a "Login via SSO" button (linking to `/app/login`) unless a valid session is already present, in which case it forwards straight to `/app/home`. Everything under `/app/*` other than `/app`, `/app/login`, and `/app/logout` requires a valid session.

Leaving `KONTINUUM_OIDC_ISSUER_URL` unset keeps kontinuum's default: anonymous authentication, always-allow authorization.

For local development, copy `.env.example` to `.env` and adjust as needed — `make dev` loads it automatically via `compose.yaml`'s `env_file`.

## Make targets

```
Usage:
  make <target>

General
  help           Display this help

Development
  build          Build the binary
  run            Run the server locally with dev-friendly logging (info, console)
  dev            Start development environment with hot reload (air + postgres)
  dev-down       Stop development environment
  dev-clean      Stop development environment and remove volumes
  image          Build the container image

Quality
  test           Run tests
  vet            Run go vet
  lint           Run golangci-lint
  lint-fix       Run golangci-lint and fix issues
  tidy           Download and tidy dependencies

Cleanup
  clean          Remove build artifacts
```

## Container

```sh
# builds kontinuum:<version> via distroless/static
make image
docker run -p 8080:8080 -e KONTINUUM_SERVER_STORAGE=postgres://... kontinuum:latest
```

The container image is built on `distroless/static` with `CGO_ENABLED=0`, so SQLite storage is not available — use PostgreSQL or etcd.

## License

Apache License 2.0.
