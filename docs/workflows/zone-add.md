# Add zone

`kontinuum zone add` fans a new zone out into four hub-side objects and
bootstraps a real Talos Kubernetes cluster for it, and once that
`TalosCluster` reports `Ready`, the `zone` controller installs kontinuum's
own footprint onto that downstream cluster — closing the loop by
registering that zone's own `kontinuum-server` back into the hub as a
worker [`Kontinuum`](../architecture.md#domain-controllers-pkgdomain).

Kontinuum maps one region to potentially many zones, and one zone to
exactly one Kubernetes cluster — never more than one cluster per zone, and
never one cluster shared across zones — so a zone's identity (`Zone`) and
its underlying cluster (`TalosCluster`) stay in a fixed 1:1 relationship
throughout this whole flow, found by name alone (see stage 1 below).

## Try it

The hub needs `KONTINUUM_SERVER_DNS_DOMAIN` and `KONTINUUM_SERVER_GRPC_ENDPOINT`
set once (e.g. in its own `compose.yaml`/environment) — `zone add` never
passes either itself: the domain is inferred from any already-registered
`Kontinuum`'s published config, and the `zone` controller reads the gRPC
endpoint straight off the hub's own config (see
[Configuration](../reference.md#zones)):

```sh
export KUBECONFIG=kontinuum.yaml
kontinuum zone add --region eu --zone eu-1a --talos-address 10.0.0.5 --wait
```

Fans out a new zone's hub-side objects and, once its `TalosCluster` is bootstrapped, installs and exposes that zone's own kontinuum-server at `eu-1a.eu.example.com` (assuming the hub's `KONTINUUM_SERVER_DNS_DOMAIN` is `example.com`). See [Configuration](../reference.md#zones) for `KONTINUUM_ACME_EMAIL`/`KONTINUUM_ACME_SERVER`, which the `zone` controller needs to issue that certificate.

## Stages

1. **Fan-out** (`kontinuum zone add`, `pkg/domain/zone`'s shared
   `BuildAddObjects`/`Add`) — creates four hub-side objects, all sharing
   one name, `<region>-<zone>`, except the seed `Instance` (suffixed with a
   short hash of `--talos-address`, the same way a Kubernetes ReplicaSet
   hashes its pod template — re-running `zone add` with the same address is
   idempotent, and a different address gets a new `Instance` identity
   instead of colliding): a `Zone`, the seed `Instance` (`spec.interfaces` from
   `--talos-address`, labeled `kontinuum.sh/region`/`kontinuum.sh/zone`), a
   `replicas: 1` `InstancePool` selecting that `Instance` by those same
   labels, and a `TalosCluster` whose control plane references that pool.
   This is the same shared naming convention `Addon` objects already assume
   (`<cluster>-<releaseName>` — see `pkg/domain/addon/resources.go`), and
   it's what lets the `zone` controller find "its" `TalosCluster` by name
   alone, with no extra reference field. Ownership is a strict chain, not
   four siblings all owned by `Zone`: `Zone` owns `TalosCluster`,
   `TalosCluster` owns `InstancePool`, and `Instance` is owned by nobody —
   see [Remove zone](zone-remove.md) for why, and for what
   `--unregister-instances-on-delete` (unset by default, also settable from
   the UI's own "Add zone" form) controls on the created `TalosCluster`'s
   own `spec.teardown.unregisterInstances`. `TalosCluster` also owns every
   `Addon` seeded or created for it (namespaced alongside it, like
   `InstancePool`) — so deleting a `TalosCluster` cleans up its addons for
   free via native garbage collection, with no separate teardown step
   needed.
2. **Cluster bootstrap** (`pkg/domain/instance`, `pkg/domain/instancepool`,
   `pkg/domain/taloscluster`) — four cooperating controllers turn the seed
   `Instance` into a running, Cilium- and cert-manager-equipped Talos
   Kubernetes cluster: `Instance` discovery → `InstancePool` claiming →
   `TalosCluster` bootstrap → addon install. See
   [Cluster bootstrap details](#cluster-bootstrap-details) below for why
   Cilium/cert-manager install in the order they do, and the
   [cluster bootstrap flow chart](#cluster-bootstrap) for the full state
   machine. All three built-in addons (Cilium, the standard Gateway API
   CRDs, and cert-manager) are guaranteed present by the time
   `TalosCluster.status.Ready` is true.
3. **Downstream install** (`pkg/domain/zone`'s `Reconciler`) — once
   `ClusterReady`, builds a client against the zone's own cluster (from the
   kubeconfig `TalosCluster.status.secretRef` points at) and installs, in
   order: the `kontinuum-system` namespace; a `kontinuum-env`
   Secret/ConfigMap; the `kontinuum` Deployment/Service; a cert-manager
   `ClusterIssuer`; a `Gateway`; a `Certificate`; and an `HTTPRoute` —
   exposing that zone's own kontinuum-server at
   `<zone>.<region>.<domain>`. `Installed` only flips `True` once
   cert-manager's own `Certificate` reports `Ready` — a real signal that
   TLS issuance actually succeeded, not just that the object was created.
4. **Registry join** (`pkg/domain/zone`'s `Reconciler`, once `Installed`) —
   the reconciler checks the hub's own registry for a `Kontinuum` matching
   this zone's `region`/`zone` with a non-zero heartbeat (see
   `FindJoinedKontinuum`), and only then flips the aggregate `Ready`
   condition true. `Installed` on its own only means the downstream objects
   exist and TLS was issued — it says nothing about whether the deployed
   `kontinuum` container actually managed to start and register itself, which
   is a real, separate failure mode: the `kontinuum-env` ConfigMap/Secret
   carry the *same* authentication choice (`KONTINUUM_INSECURE_ALLOW_ANONYMOUS`
   or `KONTINUUM_OIDC_*`) the hub itself is running with, mirrored by the
   `zone` controller's own `AuthConfig` — without it, the deployed process
   refuses to even start (see [Authentication](../authentication.md)) and
   never gets as far as heartbeating.

Every step past machine-config generation is best-effort and idempotent: a
maintenance-mode call against a node that's already moved past maintenance
mode is *expected* to fail, and is logged rather than treated as fatal —
Talos's own `ClusterHealthCheck` is the real convergence gate, and the
reconciler simply retries on the next tick.

## Details

### How the new zone reaches shared storage, without a direct database connection

A zone's `kontinuum-server` only "closes the loop" — registering itself as
a worker `Kontinuum` the hub can see — if it's pointed at the *same*
storage the hub itself uses. But a zone's only network path back to the
hub is the control-plane connection itself; it's never expected to reach
the hub's real database (Postgres/etc.) directly, and it isn't given
credentials to try.

Instead, the `zone` controller issues each zone its own scoped credential
for the hub's etcd gRPC proxy (`pkg/domain/etcdproxy` — see
[Architecture](../architecture.md#storage) for the full mechanism): a
128-character random key, stored in a Secret owned by the `Zone` (so it's
garbage-collected on teardown), rotated hourly with a 5-minute overlap so
an already-running zone is never cut off mid-rotation. That credential,
together with the hub's own `KONTINUUM_SERVER_GRPC_ENDPOINT`, is encoded
into a `grpc://zone:key@hub-endpoint` DSN and written into the new zone's
own `kontinuum-env` Secret as `KONTINUUM_SERVER_STORAGE`. On startup, the
zone's own `kontinuum-server` recognizes that scheme, starts a small local
relay that attaches the credential to every call, and hands `libkapi` a
plain local `unix://` socket instead — indistinguishable, from
`libkapi`'s point of view, from talking to a local Kine instance
directly.

### TLS: ACME over the Gateway API, not Ingress

The `ClusterIssuer` the `zone` controller creates uses ACME (Let's
Encrypt, configured via the hub's own `KONTINUUM_ACME_EMAIL`/
`KONTINUUM_ACME_SERVER`) with an HTTP-01 challenge solved through the
Gateway API (`solvers[].http01.gatewayHTTPRoute`), not the older
Ingress-based solver — this repo has no Ingress controller anywhere, and
Cilium (already installed as a built-in addon with `gatewayAPI.enabled:
true`) implements Gateway API directly. The `Gateway` the controller
creates has two listeners for exactly this reason: a plain HTTP one (port
80, no hostname restriction) that cert-manager attaches its own ephemeral
challenge `HTTPRoute` to, and an HTTPS one (port 443) terminating TLS from
the `Certificate`'s own secret.

The `Gateway`'s `gatewayClassName` is `"cilium"` — assumed already present
on the downstream cluster from Cilium's own chart, not created by this
controller. This is an unverified assumption worth checking with `kubectl
get gatewayclass cilium` against a real added zone; if it's ever missing,
the `Gateway` simply never reports `Accepted`, which is a visible, easy to
diagnose failure rather than a silent one.

### Cluster bootstrap details

#### Why Cilium installs before the cluster is "healthy"

Talos's `ClusterHealthCheck` waits for CoreDNS to report ready, which
itself needs a working pod network. If Cilium only installed *after* a
passing health check, nothing would ever provide that network in the first
place — a deadlock. Kontinuum breaks the cycle two ways:

- Talos's own default CNI (flannel) is disabled (`CNIName: none`) at
  machine-config-generation time, so the health check *skips* the
  network-dependent checks entirely instead of waiting on a CNI that was
  never configured.
- Cilium is installed as soon as the apiserver is reachable — gated on a
  successful kubeconfig fetch, not on cluster health.

cert-manager has no such dependency, so it installs normally, after
`ControlPlaneReady`.

#### `cilium-operator` runs a single replica

The Cilium chart defaults `operator.replicas` to `2` (for HA) alongside
`operator.hostNetwork: true`, which makes the operator's prometheus
`containerPort` an implicit hostPort. Every `TalosCluster` this reconciler
bootstraps today is single-node (`AllowSchedulingOnControlPlanes: true` in
`config.go`), so a second replica can never be scheduled — it fails
permanently with `0/1 nodes are available: ... didn't have free ports for
the requested pod ports`, which looks identical to a genuine hang from
`PodProber`'s point of view: bare `Pending` that never resolves. `addons.go`
pins `operator.replicas` to `1` to avoid this — the chart's own
`values.yaml` documents the clash directly: "In HA mode, cilium-operator
pods must not be scheduled on the same node as they will clash with each
other".

#### Cilium values follow Talos's own guide, not just the chart's defaults

`addons.go`'s `ciliumValues` deviates from the chart's own defaults in
several places, matching Talos's documented Cilium install
(docs.siderolabs.com/kubernetes-guides/cni/deploying-cilium) rather than
values chosen ad hoc:

- `k8sServiceHost`/`k8sServicePort` point at `localhost:7445` — Talos's own
  KubePrism apiserver proxy, which every node runs locally — instead of a
  specific control-plane member's address. With kube-proxy disabled, Cilium
  can't discover the apiserver any other way; KubePrism means this doesn't
  depend on a real control-plane load balancer existing.
- `securityContext.capabilities` pins the agent and `clean-cilium-state`
  init container to Talos's reduced capability sets rather than the
  chart's broader defaults. The chart's own defaults for
  `clean-cilium-state` aren't fully grantable under Talos's container
  runtime constraints, surfacing as `OCI runtime create failed: ... unable
  to apply caps: can't apply capabilities: operation not permitted` and a
  permanent `CrashLoopBackOff`.
- `cgroup.autoMount.enabled` is disabled, with `cgroup.hostRoot` pointed at
  Talos's own cgroup2 mount, since Talos already provides one — Cilium's
  own redundant auto-mount attempt inside the init container is both
  unnecessary and another likely source of the same capability failure.
- `ipam.mode` is set to `kubernetes`, so Cilium reads pod CIDRs from each
  Node's own spec instead of running its own allocator.

#### Pod health is probed separately from the Helm apply

The Helm install/upgrade calls (`installRelease`/`upgradeRelease`) are
deliberately non-blocking — no `Wait`/`WaitForJobs` — so they return as
soon as manifests are applied, not once pods actually start. Whether an
addon is actually usable is checked afterward, by `PodProber`, on a later
reconcile: `ControlPlaneReady` doesn't go true until Cilium's own pods
report healthy, and `Ready` doesn't go true until cert-manager's do. This
matches the rest of the reconciler's own self-healing, non-blocking style
(`ApplyConfiguration`/`Bootstrap` don't block for completion either) rather
than tying up a reconcile — and, on a manager with few concurrent workers,
potentially other objects' reconciles too — for however long a cold
rollout takes.

"Healthy" means every pod in the addon's namespace is either `Running`
with its `PodReady` condition true, or `Succeeded` — a completed one-shot
Job pod (e.g. cert-manager's own `startupapicheck`) is expected, not a
failure.

#### Timeouts

Every blocking call in this pipeline — every Talos gRPC RPC and both Helm
installs — has an explicit client-side timeout. Talos's own
`ClusterHealthCheck` request carries a `waitTimeout` field, but that only
bounds the *server's* internal retry loop, not the client call; without an
explicit `context.WithTimeout` on the client side too, a stalled server or
a slow chart fetch could block a reconcile indefinitely. A bounded failure
is just another retry on the next tick — safe and expected — where an
unbounded hang is not.

### Removing a zone

Deleting a `Zone` tears down its downstream footprint and resets its seed
node back to maintenance mode via a finalizer — see
[Remove zone](zone-remove.md) for the full sequence, what happens when the
downstream cluster is unreachable, and the operator escape hatch for a
stuck finalizer.

## Flow charts

### Cluster bootstrap

The state machine stage 2 above drives, from a freshly created seed
`Instance` through to a `Ready` `TalosCluster` with every built-in addon
installed and healthy:

```mermaid
flowchart TD
    Start([Instance created]) --> Discover[Discovery controller probes\nspec.interfaces in maintenance mode]
    Discover --> Discovered[Discovered = True\nstatus.interfaces populated]

    Discovered --> Claim{InstancePool selector\nmatches, capacity available?}
    Claim -- yes --> Claimed[Instance labeled\nkontinuum.sh/claimed-by]
    Claim -- no --> Insufficient[InsufficientCapacity = True]

    Claimed --> CPCheck{ControlPlaneReady?}

    CPCheck -- No --> Resolve[Resolve control-plane pool's\nclaimed + Discovered members]
    Resolve --> HasMembers{Any members?}
    HasMembers -- No --> WaitInstances[ControlPlaneReady = False\nWaitingForInstances]
    WaitInstances --> Requeue1[Requeue]

    HasMembers -- Yes --> GenConfig[Generate machine config\nCNI disabled, CP scheduling enabled]
    GenConfig --> ApplyConfig[Apply config to control-plane\nmembers in maintenance mode]
    ApplyConfig --> Bootstrap[Trigger etcd bootstrap]
    Bootstrap --> FetchKubeconfig[Fetch kubeconfig]
    FetchKubeconfig --> APIUp{Apiserver reachable?}
    APIUp -- No --> Bootstrapping[ControlPlaneReady = False\nBootstrapping]
    Bootstrapping --> Requeue1

    APIUp -- Yes --> StoreKubeconfig[Store kubeconfig in Secret]
    StoreKubeconfig --> InstallCilium[Apply Cilium manifests via Helm\nnon-blocking]
    InstallCilium --> CiliumOK{Apply succeeded?}
    CiliumOK -- No --> CiliumFailed[ControlPlaneReady = False\nCiliumInstallFailed]
    CiliumFailed --> Requeue1

    CiliumOK -- Yes --> ProbeCilium[Probe cilium pods\nvia PodProber]
    ProbeCilium --> CiliumHealthy{All pods healthy?}
    CiliumHealthy -- No --> CiliumNotHealthy[ControlPlaneReady = False\nAddonNotHealthy]
    CiliumNotHealthy --> Requeue1

    CiliumHealthy -- Yes --> HealthCheck[Run Talos ClusterHealthCheck\nCNI checks skipped]
    HealthCheck --> Healthy{Healthy?}
    Healthy -- No --> Bootstrapping
    Healthy -- Yes --> SetReady[Bootstrapped = True\nControlPlaneReady = True]
    SetReady --> Requeue1

    CPCheck -- Yes --> Workers[Apply worker pool configs\nto claimed + Discovered members]
    Workers --> ReadyCheck{Ready?}
    ReadyCheck -- No --> InstallCertManager[Apply cert-manager manifests\nvia Helm, non-blocking]
    InstallCertManager --> CertOK{Apply succeeded?}
    CertOK -- No --> AddonFailed[Ready = False\nAddonInstallFailed]
    AddonFailed --> Requeue1

    CertOK -- Yes --> ProbeCert[Probe cert-manager pods\nvia PodProber]
    ProbeCert --> CertHealthy{All pods healthy?}
    CertHealthy -- No --> CertNotHealthy[Ready = False\nAddonNotHealthy]
    CertNotHealthy --> Requeue1
    CertHealthy -- Yes --> Done([Ready = True])
    ReadyCheck -- Yes --> NoOp([Nothing to do])
```

### Downstream install

Stage 3 above, once `ClusterReady`:

```mermaid
flowchart TD
    Start([kontinuum zone add]) --> FanOut[Create Zone, seed Instance,\nInstancePool, TalosCluster]
    FanOut --> Bootstrap[Cluster bootstrap\nsee flow chart above]

    Bootstrap --> ClusterCheck{TalosCluster found\nand Ready?}
    ClusterCheck -- not found --> NotFound[ClusterReady = False\nTalosClusterNotFound]
    NotFound --> Requeue1[Requeue]
    ClusterCheck -- not ready --> Waiting[ClusterReady = False\nWaitingForTalosCluster]
    Waiting --> Requeue1

    ClusterCheck -- yes --> ClusterReady[ClusterReady = True]
    ClusterReady --> LoadKubeconfig[Load downstream kubeconfig\nfrom TalosCluster.status.secretRef]
    LoadKubeconfig --> KubeconfigOK{Available?}
    KubeconfigOK -- No --> NotReady[Installed = False\nDownstreamNotReady]
    NotReady --> Requeue1

    KubeconfigOK -- Yes --> BuildDSN[Build zone's own etcd proxy DSN\nfrom hub's GRPCEndpoint + zone's auth key]
    BuildDSN --> DSNOk{GRPCEndpoint configured\nand auth key ready?}
    DSNOk -- No --> NoStorage[Installed = False\nNoStorageSecretFound]
    NoStorage --> Requeue1

    DSNOk -- Yes --> InstallWorkload[Ensure namespace, kontinuum-env\nSecret/ConfigMap incl. auth config, Deployment, Service]
    InstallWorkload --> InstallNetwork[Ensure ClusterIssuer, Gateway,\nCertificate, HTTPRoute]
    InstallNetwork --> CertCheck{Certificate reports Ready?}
    CertCheck -- No --> WaitCert[Installed = False\nWaitingForCertificate]
    WaitCert --> Requeue1
    CertCheck -- Yes --> Installed[Installed = True]
    Installed --> RegistryCheck{Kontinuum matching this\nzone's region/zone heartbeating?}
    RegistryCheck -- No --> WaitRegistry[Ready = False\nWaitingForRegistry]
    WaitRegistry --> Requeue1
    RegistryCheck -- Yes --> Done([Ready = True\nRegistryJoined])
```
