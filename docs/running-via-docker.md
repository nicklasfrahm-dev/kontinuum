# Running via Docker

The fastest way to try kontinuum without setting up a Go toolchain.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

## Build the image

```sh
# builds kontinuum:<version> via distroless/static
make image
```

The container image is built on `distroless/static` with `CGO_ENABLED=0`, so SQLite storage is not available in the container — use PostgreSQL or etcd.

## Run the container

```sh
docker run -p 8080:8080 -e KONTINUUM_SERVER_STORAGE=postgres://... kontinuum:latest
```

See [Configuration](reference.md#configuration) for the full list of `KONTINUUM_`-prefixed environment variables, and [Authentication](authentication.md) for how OIDC and anonymous access interact — the server refuses to start until one of those is configured.

## Connect with kubectl

```sh
export KUBECONFIG=kontinuum.yaml
kubectl get namespaces
kubectl create namespace demo
```

!!! tip
    Hacking on kontinuum itself instead of just running it? See [Local setup](local-setup.md) for building from source with hot-reload.
