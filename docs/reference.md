# Reference

Kontinuum's API is a Kubernetes-style API: every resource below is a regular custom resource, served by the embedded apiserver, readable and writable with `kubectl` (see [Quickstart](quickstart.md#connect-with-kubectl)) or any Kubernetes client library — there is no separate, bespoke REST API to learn.

## Generated CRD reference

Full generated field-by-field documentation for every CRD, kept in sync with `api/` automatically, is published at:

**[doc.crds.dev/github.com/nicklasfrahm-dev/kontinuum](https://doc.crds.dev/github.com/nicklasfrahm-dev/kontinuum)**

## Resources

All kinds below belong to the `kontinuum.sh/v1alpha2` API group/version (`kontinuum.sh/v1alpha1` also exists, for backward compatibility — the `registry` controller runs a conversion webhook between the two).

| Kind | Owning controller | Purpose |
| --- | --- | --- |
| `Kontinuum` | `pkg/domain/registry` | Self-registration record for a running kontinuum process — role, region/zone, version, heartbeat. |
| `Zone` | — | Groups instances by availability zone. |
| `Instance` | `pkg/domain/instance` | A candidate machine, discovered over the Talos maintenance-mode API. |
| `InstancePool` | `pkg/domain/instancepool` | Claims a set of `Instance`s matching a selector, up to `spec.replicas`. |
| `TalosCluster` | `pkg/domain/taloscluster` | Bootstraps a Talos Kubernetes cluster from a control-plane `InstancePool` and optional worker pools. |
| `Addon` | `pkg/domain/addon` | A Helm-based addon (Cilium, cert-manager) installed and health-probed as part of `TalosCluster` bootstrap. |

See [Architecture](architecture.md) for how these controllers cooperate, and [Cluster provisioning](workflows/cluster-provisioning.md) for the full `Instance` → `InstancePool` → `TalosCluster` → `Addon` flow.

## CRD manifests in this repo

The raw CRD YAML manifests (source of truth for the generated reference above) live under [`config/crd`](https://github.com/nicklasfrahm-dev/kontinuum/tree/main/config/crd), regenerated from `api/v1alpha1` and `api/v1alpha2` via `make generate` (`controller-gen`).
