# Production deployment

Kontinuum ships with no TLS by default — the server itself only speaks plain HTTP. Before exposing it outside a trusted network, put a TLS-terminating proxy in front of it, the same way the [local dev environment](local-setup.md) does with Caddy; any reverse proxy, load balancer, or ingress that terminates TLS works just as well in production. Pair that with deliberate authentication: kontinuum refuses to start until you either set `KONTINUUM_OIDC_ISSUER_URL` to require OIDC-backed logins and API access, or explicitly opt into anonymous, always-allow access via `KONTINUUM_INSECURE_ALLOW_ANONYMOUS=true`. For anything beyond a trusted network or a local demo, that means OIDC — see [Authentication](authentication.md) for how to configure it.

## Storage access for joined zones

If you plan to run `kontinuum zone add` against this hub, also set `KONTINUUM_SERVER_GRPC_ENDPOINT` to the hub's own publicly reachable `host:port` — normally the same `host:port` your TLS-terminating proxy fronts `/app` and the rest of the API on, since the etcd gRPC proxy every joined zone dials back into is multiplexed onto that same port (see [Architecture](architecture.md#storage)). Without it, `zone add` fails outright: a newly joined zone has no way to reach shared storage. Because that proxy is exposed through the same TLS-terminating listener as everything else, it needs the proxy in front to forward gRPC (HTTP/2) traffic through unmodified, not just plain HTTP/1.1.

## Running with a read-only root filesystem

`kontinuum serve` runs cleanly under a hardened `readOnlyRootFilesystem: true` container — the container image itself is never written to, and the `serve` command points Helm's own cache/config/data directories (used by the addon controller's OCI chart pulls, e.g. `oci://` charts) at a subdirectory of `os.TempDir()` instead of `$HOME`, so no `$HOME`-relative path is ever written either.

The one thing this doesn't solve for you: **a writable `/tmp` still has to exist**. `os.TempDir()` resolves to `/tmp` unless `$TMPDIR` is set, and a fully read-only root filesystem has no writable path at all unless the platform is explicitly given one — every kontinuum-managed zone Deployment already mounts an `emptyDir` there for exactly this reason (see `pkg/domain/zone/workload.go`). Google Cloud Run needs the equivalent: an in-memory volume mounted at `/tmp`, since Cloud Run's read-only mode ships with no writable path by default, not even `/tmp`:

```bash
gcloud run services update <SERVICE> \
  --add-volume=name=scratch,type=in-memory \
  --add-volume-mount=volume=scratch,mount-path=/tmp
```

(or the Terraform/YAML equivalent for however you deploy the service). Without it, the very first OCI chart pull fails trying to create Helm's cache directory.
