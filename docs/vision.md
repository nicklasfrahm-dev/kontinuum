# Vision & roadmap

## Why kontinuum

Standing up your own cloud today means stitching together a pile of separate tools: something like [MAAS](https://maas.io/) or [Metal³](https://metal3.io/) to discover and commission bare metal, [Cluster API](https://cluster-api.sigs.k8s.io/) or hand-rolled scripts to turn that metal into Kubernetes, a CNI and an ingress controller for networking, [Proxmox](https://www.proxmox.com/) or [OpenStack](https://www.openstack.org/) if you also need VMs, and yet another set of operators for databases — each with its own API, its own auth model, and its own failure modes.

Kontinuum's goal is to collapse that stack into one Kubernetes-style API. Discover a machine, claim it into a pool, hand it a Kubernetes cluster, network it, and (eventually) run VMs, containers, and databases on top of it — all as `kubectl`-compatible custom resources managed by one set of controllers, the same way you'd manage any other Kubernetes object. Not a reimplementation of OpenStack, but the same "compute, network, and storage as one API" idea, built API-native and CRD-based from the start instead of bolted onto VMs.

This makes kontinuum a fit for platform teams building an internal private cloud, self-hosters and homelabbers running their own bare-metal cluster, and service providers offering compute to others — anyone who wants cloud-shaped infrastructure on hardware they control rather than rent.

## How to read the tables below

Each theme below lists the capabilities it covers and where they stand:

- **Available** — implemented and documented; you can use it today.
- **In progress** — under active development, may be partially usable.
- **Planned** — designed or scoped, not yet implemented.

## Bare-metal, commissioning & networking

Getting from a rack of unconfigured machines to something workloads can run on: discovery, provisioning, patching, and the underlay/overlay networking and DNS that ties it together.

| Capability | Status | Notes |
| --- | --- | --- |
| Machine discovery | Available | The `instance` controller dials each `Instance`'s configured interfaces in Talos maintenance mode and records interfaces, MAC/IP, and Talos version once reachable. |
| Pool claiming | Available | `InstancePool` claims discovered `Instance`s up to `spec.replicas` via a conditional (CAS) update, so two pools can never claim the same machine. See [Cluster provisioning](workflows/cluster-provisioning.md). |
| On-demand provisioning | Planned | `InstancePool.spec.template` already exists on the type for creating instances from a provider template on demand, but the create path is explicitly out of scope for the current phase — claiming is claim-only today. |
| OS commissioning (imaging, disk layout) | Planned | No automated disk provisioning beyond booting into Talos maintenance mode yet. |
| Patch management | Planned | No automated OS/firmware patching yet. |
| Hardware inventory (CPU/RAM/disk facts) | Planned | Discovery today records network identity, not full hardware facts. |
| Overlay networking (CNI) | Available | Cilium is installed as a built-in `Addon` during cluster bootstrap, giving every `TalosCluster` pod networking and a Gateway API implementation out of the box. |
| Underlay networking (VLANs, L2/L3 provisioning) | Planned | Kontinuum doesn't configure switches or physical network topology yet. |
| Ingress & TLS | Available | The `zone` controller provisions a cert-manager `ClusterIssuer`, `Certificate`, `Gateway`, and `HTTPRoute` per joined zone to expose that zone's own kontinuum-server over HTTPS. |
| General traffic management (load balancing beyond ingress) | Planned | No general-purpose `LoadBalancer` CRD yet. |
| DNS | Planned | The `<zone>.<region>.<domain>` naming scheme exists as configuration today; there's no automated DNS record management yet. |

## Compute services

Turning claimed machines into places to run workloads — Kubernetes today, virtualization and standalone containers on the roadmap.

| Capability | Status | Notes |
| --- | --- | --- |
| Kubernetes cluster provisioning | Available | `TalosCluster` bootstraps a Talos Kubernetes cluster from control-plane and worker `InstancePool`s — generates machine configs, bootstraps etcd, waits for health. See [Cluster provisioning](workflows/cluster-provisioning.md). |
| Cluster addons | Available | `Addon` installs and health-checks Helm-based addons (Cilium, cert-manager, or custom charts) onto a `TalosCluster`. |
| Regional/multi-zone clusters | Available | `Zone` ties a region/zone pair to its own `TalosCluster` and installs kontinuum's downstream footprint into it. See [Add zone](workflows/zone-add.md). |
| Virtualization (VMs) | Planned | No VM support yet. |
| Standalone containers (outside Kubernetes) | Planned | Not started. |
| Function apps / serverless | Planned | Not started. |

## Databases

Managed data services for workloads running on kontinuum-provisioned clusters.

| Capability | Status | Notes |
| --- | --- | --- |
| Managed PostgreSQL | Planned | Not started. (Kontinuum itself can already use PostgreSQL as *its own* storage backend via `KONTINUUM_SERVER_STORAGE` — that's a deployment option for kontinuum, not a managed database offering for tenant workloads.) |
| Managed MongoDB (via [FerretDB](https://www.ferretdb.com/)) | Planned | Not started. |

## Contributing to the roadmap

Every new CRD, controller, or capability lands with its own docs page per [AGENTS.md](https://github.com/nicklasfrahm-dev/kontinuum/blob/main/AGENTS.md)'s documentation rule — so this page should stay accurate as things move from Planned to Available. See [Contribution guidelines](contributing.md) to get started.
