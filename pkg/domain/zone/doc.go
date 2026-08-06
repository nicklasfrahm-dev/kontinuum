// Package zone implements Zone's downstream-install reconciler — see issue
// #24's architecture decision 1/5 and #29. Once a zone's TalosCluster
// (found by the shared Zone/Instance/InstancePool/TalosCluster naming
// convention — see BuildJoinObjects) reports status.Ready, this package
// installs kontinuum's own downstream footprint into that cluster: a
// kontinuum-system namespace, a kontinuum-env Secret/ConfigMap, a
// kontinuum Deployment/Service, and a cert-manager ClusterIssuer + Gateway
// + Certificate + HTTPRoute exposing that zone's own kontinuum-server at
// <zone>.<region>.<domain>. zones.kontinuum.sh's CRD is already ensured by
// pkg/domain/instance.EnsureCRDs — no separate ensure step lives here.
//
// This package also exports the shared hub-side fan-out logic
// (BuildJoinObjects/Apply) used by both pkg/cli/zone and pkg/ui to create a
// zone's four hub objects identically — see join.go.
package zone
