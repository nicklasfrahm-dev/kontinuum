// Package fabric implements Fabric's IPAM allocator and controller — a
// region-scoped network, modeled like an Azure VNet (one address space
// spanning every AZ/Zone in a region) but, unlike a VNet, deliberately
// carved into independent per-zone subnets and NAT gateways: each Zone is
// already its own separate Talos-bootstrapped cluster with no L2 adjacency
// to any other zone (issue #24's "one cluster per zone" decision), so
// there's no shared broadcast domain to build a VRRP/anycast VIP over in
// the first place. See api/v1alpha2/fabric_types.go's own doc for the full
// design.
package fabric
