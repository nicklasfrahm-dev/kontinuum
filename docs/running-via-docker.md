# Running via Docker

The fastest way to try kontinuum without setting up a Go toolchain.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

## Run the container

Pull the hosted image from GitHub Container Registry:

```sh
docker pull ghcr.io/nicklasfrahm-dev/kontinuum:latest
```

The image is built on `distroless/static` with `CGO_ENABLED=0`, so SQLite storage is not available in the container — use PostgreSQL or etcd.

For a quick local try-out, opt into anonymous access with `KONTINUUM_INSECURE_ALLOW_ANONYMOUS=true` instead of setting up OIDC — see [Authentication](authentication.md) for what that trades away:

```sh
docker run -p 8080:8080 \
  -e KONTINUUM_SERVER_STORAGE=postgres://... \
  -e KONTINUUM_INSECURE_ALLOW_ANONYMOUS=true \
  ghcr.io/nicklasfrahm-dev/kontinuum:latest
```

See [Configuration](reference.md#configuration) for the full list of `KONTINUUM_`-prefixed environment variables, and [Authentication](authentication.md) for how OIDC and anonymous access interact — the server refuses to start until one of those is configured.

!!! warning
    `KONTINUUM_INSECURE_ALLOW_ANONYMOUS=true` disables login and access control entirely. Don't expose it outside a trusted network without a TLS-terminating, authenticating proxy in front — see [Authentication](authentication.md) for a production OIDC setup instead.

## Connect with kubectl

```sh
export KUBECONFIG=kontinuum.yaml
kubectl get namespaces
kubectl create namespace demo
```

!!! tip
    Hacking on kontinuum itself instead of just running it? See [Local setup](local-setup.md) for building from source with hot-reload.
