# API reference

Kontinuum's API is a Kubernetes-style API: every resource below is a regular custom resource, served by the embedded apiserver, readable and writable with `kubectl` (see [Running via Docker](running-via-docker.md#connect-with-kubectl) or [Local setup](local-setup.md#connect-with-kubectl)) or any Kubernetes client library — there is no separate, bespoke REST API to learn.

## Configuration

Configuration is loaded from `KONTINUUM_`-prefixed environment variables. Env-var names are auto-derived from the config struct path (e.g. `Server.Addr` → `KONTINUUM_SERVER_ADDR`).

### Server & logging

| Env var                    | Description                                                                                        | Default                 |
| --------------------------- | --------------------------------------------------------------------------------------------------- | ------------------------ |
| `KONTINUUM_SERVER_ADDR`    | Listener address                                                                                     | `:8080`                 |
| `KONTINUUM_SERVER_STORAGE` | Storage connection string (`sqlite://`, `postgres://`, `mysql://`, `etcd://`, ...)                   | `sqlite://kontinuum.db` |
| `KONTINUUM_SERVER_REGION`  | Region this server manages. Leave unset, along with Zone, to run as the control-plane entrypoint.    | *(empty)*               |
| `KONTINUUM_SERVER_ZONE`    | Availability zone this server manages. Leave unset, along with Region, to run as the control-plane entrypoint. | *(empty)*     |
| `KONTINUUM_LOG_LEVEL`      | Log level (`debug`, `info`, `warn`, `error`)                                                         | `warn`                  |
| `KONTINUUM_LOG_FORMAT`     | Log format (`console`, `text`, `json`)                                                               | `json`                  |

### Authentication (OIDC)

See [Authentication](authentication.md) for how these interact and the server's startup behavior when they're missing or conflicting.

| Env var                              | Description                                                                                             | Default                     |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------- | ---------------------------- |
| `KONTINUUM_OIDC_ISSUER_URL`          | OIDC issuer URL. Empty disables OIDC entirely.                                                             | *(empty)*                   |
| `KONTINUUM_OIDC_CLIENT_ID`           | OAuth 2.0 public client ID registered with the issuer                                                       | `kontinuum`                  |
| `KONTINUUM_OIDC_REDIRECT_URL`        | Callback URL registered with the issuer for the `/app` login flow                                          | `http://localhost:8080/app` |
| `KONTINUUM_OIDC_ADMIN_GROUPS`        | Comma-delimited OIDC groups granted full (`system:masters`-equivalent) access                              | *(empty)*                   |
| `KONTINUUM_INSECURE_ALLOW_ANONYMOUS` | Explicitly acknowledges anonymous access. Must be `true` to start with no OIDC issuer configured; mutually exclusive with `KONTINUUM_OIDC_ISSUER_URL`. | `false` |

### Zones

See [Add zone](workflows/zone-add.md) for how these are used.

| Env var                        | Description                                                                                            | Default                                                         |
| -------------------------------- | --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `KONTINUUM_SERVER_DNS_DOMAIN`  | Base domain a zone's own kontinuum-server is published under (`<zone>.<region>.<domain>`)                | *(empty)*                                                       |
| `KONTINUUM_SERVER_GRPC_ENDPOINT` | This hub's own publicly reachable `host:port` for its etcd gRPC proxy (see [Architecture](architecture.md#storage)) — normally the same `host:port` a browser reaches `/app` on. Required for `zone add` to succeed: without it, a newly joined zone has no way to reach shared storage. | *(empty)* |
| `KONTINUUM_ACME_EMAIL`         | ACME account email used when the `zone` controller creates a joined zone's cert-manager `ClusterIssuer` | *(empty)*                                                       |
| `KONTINUUM_ACME_SERVER`        | ACME directory URL used for the same `ClusterIssuer`                                                     | `https://acme-v02.api.letsencrypt.org/directory` (Let's Encrypt production) |

Set `KONTINUUM_SERVER_DNS_DOMAIN` and `KONTINUUM_SERVER_GRPC_ENDPOINT` once, on the hub — `kontinuum zone add` never needs its own copy of either: it infers the domain from any already-registered `Kontinuum`'s own published config, and the `zone` controller reads the gRPC endpoint directly off the hub's own config to build each zone's storage credential. See [Add zone](workflows/zone-add.md) for the full mechanism.

Flags override environment variables when explicitly set:

```sh
kontinuum serve --addr :9090 --storage postgres://user:pass@host/db
```

## Generated CRD reference

Full generated field-by-field documentation for every CRD, kept in sync with `api/` automatically, is published at:

**[doc.crds.dev/github.com/nicklasfrahm-dev/kontinuum](https://doc.crds.dev/github.com/nicklasfrahm-dev/kontinuum)**

## Resources

All kinds below belong to the `kontinuum.sh/v1alpha2` API group/version (`kontinuum.sh/v1alpha1` also exists, for backward compatibility — the `registry` controller runs a conversion webhook between the two).

| Kind | Owning controller | Purpose |
| --- | --- | --- |
| `Kontinuum` | `pkg/domain/registry` | Self-registration record for a running kontinuum process — role, region/zone, version, heartbeat. |
| `Zone` | `pkg/domain/zone` | A single availability zone. Once its `TalosCluster` is Ready, installs kontinuum's downstream footprint (namespace, Deployment/Service, Gateway/Certificate) onto that zone's own cluster — see [Add zone](workflows/zone-add.md). |
| `Fabric` | `pkg/domain/fabric` | A region-scoped network: carves a subnet and gateway IP per zone out of `spec.cidr`, and (unless `spec.nat.disabled`) elects and configures a per-zone NAT gateway node via `spec.gatewaySelector`. |
| `Instance` | `pkg/domain/instance` | A candidate machine, discovered over the Talos maintenance-mode API. |
| `InstancePool` | `pkg/domain/instancepool` | Claims a set of `Instance`s matching a selector, up to `spec.replicas`. |
| `TalosCluster` | `pkg/domain/taloscluster` | Bootstraps a Talos Kubernetes cluster from a control-plane `InstancePool` and optional worker pools. |
| `Addon` | `pkg/domain/addon` | A Helm-based addon (Cilium, cert-manager) installed and health-probed as part of `TalosCluster` bootstrap. |

See [Architecture](architecture.md) for how these controllers cooperate, and [Add zone](workflows/zone-add.md) for the full `Instance` → `InstancePool` → `TalosCluster` → `Addon` flow.

## CRD manifests in this repo

The raw CRD YAML manifests (source of truth for the generated reference above) live under [`config/crd`](https://github.com/nicklasfrahm-dev/kontinuum/tree/main/config/crd), regenerated from `api/v1alpha1` and `api/v1alpha2` via `make generate` (`controller-gen`).
