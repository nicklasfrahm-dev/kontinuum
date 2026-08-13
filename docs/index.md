# Kontinuum

Kontinuum is a framework for building your own private (or public) cloud on hardware you control. One Kubernetes-style API discovers bare-metal machines, claims them into pools, and bootstraps Kubernetes clusters on top of them — and is growing to cover networking, virtualization, and managed databases as well. See [Vision & roadmap](vision.md) for the full picture of where kontinuum is headed and what's available today.

Under the hood it's a Kubernetes-style API server built on [kommodity](https://github.com/kommodity-io/kommodity)'s `libkapi` package: a generic apiserver + apiextensions (CRD) server + aggregation layer, backed by pluggable storage (SQLite, PostgreSQL, etcd, ...).

<p align="center" markdown>
  ![Login screen](screenshots/login.png){ width="32%" }
  ![Topology view](screenshots/topology.png){ width="32%" }
  ![Settings view](screenshots/settings.png){ width="32%" }
</p>

!!! warning
    The server ships with no TLS by default. Put a TLS-terminating proxy in front before exposing it outside a trusted network. Authentication must be configured deliberately — see [Authentication](authentication.md).

## Where to go next

- [**Vision & roadmap**](vision.md) — why kontinuum exists, and what's available today vs. planned across bare-metal, networking, compute, and databases.
- [**Architecture**](architecture.md) — how the server, controllers, and UI fit together.
- [**Running via Docker**](running-via-docker.md) — the fastest way to try kontinuum, no Go toolchain required.
- [**Authentication**](authentication.md) — the OIDC/PKCE login flow, anonymous access, and how to turn each on.
- [**Cluster provisioning**](workflows/cluster-provisioning.md) — how kontinuum turns bare-metal machines into a running Talos Kubernetes cluster.
- [**Contribution guidelines**](contributing.md) — conventions for contributing to kontinuum.
- [**Local setup**](local-setup.md) — build kontinuum from source and run the hot-reload dev environment.
- [**API reference**](reference.md) — the CRDs and configuration variables that make up kontinuum's API, and the generated field reference.

## What kontinuum does today

Kontinuum manages `Instance`, `InstancePool`, and `TalosCluster` custom resources through a Kubernetes-style API of its own — the same `kubectl`-compatible interface you'd use against a real cluster — to discover machines, claim them into pools, and bootstrap Talos Kubernetes clusters on top of them, with Cilium and cert-manager installed as addons. See [Architecture](architecture.md) for how the pieces fit together, or [Vision & roadmap](vision.md) for what's next.
