# Production deployment

Kontinuum ships with no TLS by default — the server itself only speaks plain HTTP. Before exposing it outside a trusted network, put a TLS-terminating proxy in front of it, the same way the [local dev environment](local-setup.md) does with Caddy; any reverse proxy, load balancer, or ingress that terminates TLS works just as well in production. Pair that with deliberate authentication: kontinuum refuses to start until you either set `KONTINUUM_OIDC_ISSUER_URL` to require OIDC-backed logins and API access, or explicitly opt into anonymous, always-allow access via `KONTINUUM_INSECURE_ALLOW_ANONYMOUS=true`. For anything beyond a trusted network or a local demo, that means OIDC — see [Authentication](authentication.md) for how to configure it.

## Storage access for joined zones

Every zone's own `kontinuum-server` reconciles against the same shared storage as the hub. How it reaches that storage is the [storage mode](reference.md#storage-modes), inferred from `KONTINUUM_SERVER_STORAGE` unless `KONTINUUM_SERVER_STORAGE_MODE` overrides it:

| Mode | Chosen when | What a zone is handed | Credential a zone holds | Hub |
| --- | --- | --- | --- | --- |
| `Proxy` | The database is only reachable from the hub (private address, `sqlite://`) | A relay DSN back to the hub's etcd gRPC proxy | Its own ed25519 identity, scoped to that zone | Required |
| `Direct` | The database's host resolves only to public addresses | The database's own connection string | The full database credential | Optional ([hub-less](architecture.md#hub-less-fleets)) |

The mode is fixed once a zone is installed. A fleet whose resolved mode changes, for example because its database's DNS record moved from a private to a public address, refuses to touch already-installed zones (`Installed=False`, reason `StorageModeChanged`) rather than rewriting their environment and restarting them against storage they may not reach. Pin `KONTINUUM_SERVER_STORAGE_MODE` to the zones' mode to keep them running.

!!! warning "Upgrading a hub on a public database"
    Zones installed before storage modes existed all run in `Proxy` mode. If your hub's `KONTINUUM_SERVER_STORAGE` points at a database with a public address, it now infers `Direct`, and those zones report `StorageModeChanged`. Set `KONTINUUM_SERVER_STORAGE_MODE=Proxy` on the hub before upgrading to keep them as they are.

### Proxy mode

Set `KONTINUUM_SERVER_GRPC_ENDPOINT` to the hub's own publicly reachable `host:port` — normally the same `host:port` your TLS-terminating proxy fronts `/app` and the rest of the API on, since the etcd gRPC proxy every joined zone dials back into is multiplexed onto that same port (see [Architecture](architecture.md#storage)). Without it, `zone add` fails outright: a newly joined zone has no way to reach shared storage. Because that proxy is exposed through the same TLS-terminating listener as everything else, it needs the proxy in front to forward gRPC (HTTP/2) traffic through unmodified, not just plain HTTP/1.1.

If the hub is down, zones can't reach storage at all until it's back.

### Direct mode

Choose direct mode when you already run a managed PostgreSQL (or MySQL) that every zone can reach over the internet, and you want the fleet to keep working without any single hub. No gRPC endpoint is needed. [`kontinuum up`](workflows/up.md) creates and converges a direct-mode fleet from a declarative `kontinuum.yml`.

- **Network prerequisites:** the database must accept connections from every zone's nodes, and its hostname must resolve to the same public addresses from the hub and from every zone. Behind split-horizon DNS, set `KONTINUUM_SERVER_STORAGE_MODE=Direct` explicitly, since inference would see the private answer. Use TLS (`sslmode=require` or stricter for PostgreSQL), since the connection crosses the internet.
- **Failure behaviour:** losing the hub, or any one zone, doesn't stop the others: every process talks to the database itself, and the fleet is [hub-less](architecture.md#hub-less-fleets), so fleet-wide work moves to whichever peer takes the lease next. Losing the database stops every zone's apiserver until it's back.
- **Credential posture:** every zone holds the full database credential in its `kontinuum-env` Secret, never its ConfigMap, and so does whoever runs [`kontinuum up`](workflows/up.md) against the fleet. Zones are treated as trusted infrastructure: anyone with access to a zone's `kontinuum-system` namespace can read and write the whole fleet's state. Use a dedicated database user for Kontinuum, and rotate it by updating `KONTINUUM_SERVER_STORAGE` everywhere it is set.

## Running with a read-only root filesystem

`kontinuum serve` runs cleanly under a hardened `readOnlyRootFilesystem: true` container — the container image itself is never written to, and the `serve` command points Helm's own cache/config/data directories (used by the addon controller's OCI chart pulls, e.g. `oci://` charts) at a subdirectory of `os.TempDir()` instead of `$HOME`, so no `$HOME`-relative path is ever written either.

The one thing this doesn't solve for you: **a writable `/tmp` still has to exist**. `os.TempDir()` resolves to `/tmp` unless `$TMPDIR` is set, and a fully read-only root filesystem has no writable path at all unless the platform is explicitly given one — every kontinuum-managed zone Deployment already mounts an `emptyDir` there for exactly this reason (see `pkg/domain/zone/workload.go`). Google Cloud Run needs the equivalent: an in-memory volume mounted at `/tmp`, since Cloud Run's read-only mode ships with no writable path by default, not even `/tmp`:

```bash
gcloud run services update <SERVICE> \
  --add-volume=name=scratch,type=in-memory \
  --add-volume-mount=volume=scratch,mount-path=/tmp
```

(or the Terraform/YAML equivalent for however you deploy the service). Without it, the very first OCI chart pull fails trying to create Helm's cache directory.
