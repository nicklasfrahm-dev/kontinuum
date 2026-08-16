package instance

import (
	"hash/fnv"
	"strconv"

	"github.com/davecgh/go-spew/spew"
	"k8s.io/apimachinery/pkg/util/rand"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// Hash derives a short, deterministic suffix for an Instance's name from its
// full spec, using the same algorithm a Kubernetes Deployment uses to derive
// a ReplicaSet's own pod-template-hash suffix from its pod template
// (controller.ComputeHash, in k8s.io/kubernetes/pkg/controller). That
// function itself isn't importable here without pulling in the whole
// k8s.io/kubernetes module — a much heavier, independently-versioned
// dependency this repo has no other reason to take — so this reimplements
// its own two building blocks directly: DeepHashObject's spew.Fprintf-based
// deep dump (go-spew is already an indirect dependency here via
// client-go/apimachinery's own test helpers, and DeepHashObject's exact
// spew.ConfigState — Indent " ", SortKeys/DisableMethods/SpewKeys true — is
// public knowledge, reproduced below) hashed with FNV-1a-32 (hash/fnv, not
// a cryptographic hash — collision-resistance against an adversary was
// never the goal, just a short, stable, low-collision identity), then
// encoded with k8s.io/apimachinery/pkg/util/rand.SafeEncodeString — the
// exact same function ComputeHash itself calls, genuinely imported rather
// than reimplemented, since it already lives in our existing apimachinery
// dependency.
//
// Shared by zone.BuildAddObjects (its own seed Instance's name suffix) and
// Add (a standalone-registered Instance's own name) — both derive an
// Instance's identity from its spec this same way, so the same spec always
// hashes to the same short suffix regardless of which caller built it.
func Hash(spec v1alpha2.InstanceSpec) string {
	spewConfig := spew.ConfigState{Indent: " ", SortKeys: true, DisableMethods: true, SpewKeys: true}

	hasher := fnv.New32a()
	// hash.Hash's Write (which Fprintf calls into) never returns an error,
	// by that interface's own documented contract.
	_, _ = spewConfig.Fprintf(hasher, "%#v", spec)

	return rand.SafeEncodeString(strconv.FormatUint(uint64(hasher.Sum32()), 10))
}
