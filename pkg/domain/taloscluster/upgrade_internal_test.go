package taloscluster

import (
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// testKubeletImageVersion is the version tag every fixture image/status in
// this file carries.
const testKubeletImageVersion = "v1.32.0"

// testUpgradeTargetVersion is the Talos version the fixtures below treat as
// the pinned target.
const testUpgradeTargetVersion = "v1.13.0"

func TestImageTag(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		ref  string
		want string
	}{
		"tagged": {
			ref:  "ghcr.io/siderolabs/kubelet:" + testKubeletImageVersion,
			want: testKubeletImageVersion,
		},
		"untagged":                {ref: "ghcr.io/siderolabs/kubelet", want: ""},
		"registry port, untagged": {ref: "registry.local:5000/siderolabs/kubelet", want: ""},
		"registry port and tag": {
			ref:  "registry.local:5000/siderolabs/kubelet:" + testKubeletImageVersion,
			want: testKubeletImageVersion,
		},
		"empty":                    {ref: "", want: ""},
		"digest pinned, no tag":    {ref: "ghcr.io/siderolabs/kubelet@sha256", want: ""},
		"no registry, tagged only": {ref: "kubelet:" + testKubeletImageVersion, want: testKubeletImageVersion},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, imageTag(test.ref))
		})
	}
}

func TestNormalizeVersion(t *testing.T) {
	t.Parallel()

	assert.Equal(t, testKubeletImageVersion, normalizeVersion("1.32.0"),
		"a bare version gains the v Talos itself reports with")
	assert.Equal(t, testKubeletImageVersion, normalizeVersion(testKubeletImageVersion),
		"an already-prefixed version is left alone")
	assert.Empty(t, normalizeVersion(""), "an empty version means unmanaged, never a bare v")
}

func TestInstallerImage(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "ghcr.io/siderolabs/installer:"+testUpgradeTargetVersion,
		installerImage(testUpgradeTargetVersion))
	assert.Equal(t, "ghcr.io/siderolabs/installer:"+testUpgradeTargetVersion, installerImage("1.13.0"),
		"an unprefixed spec version still has to resolve to a real installer tag")
}

// upgradeMemberAt builds a single-member fixture reporting the given
// versions — agreedVersion and staleMembers only ever read those two
// fields, so nothing else about the Instance matters here.
func upgradeMemberAt(talosVersion, kubernetesVersion string) upgradeMember {
	return upgradeMember{
		instance: &v1alpha2.Instance{
			Status: v1alpha2.InstanceStatus{
				Talos:      v1alpha2.InstanceTalosStatus{Version: talosVersion},
				Kubernetes: v1alpha2.InstanceKubernetesStatus{Version: kubernetesVersion},
			},
		},
		machineType: machine.TypeControlPlane,
	}
}

func TestAgreedVersion(t *testing.T) {
	t.Parallel()

	agreeing := []upgradeMember{
		upgradeMemberAt(testUpgradeTargetVersion, testKubeletImageVersion),
		upgradeMemberAt(testUpgradeTargetVersion, testKubeletImageVersion),
	}
	assert.Equal(t, testUpgradeTargetVersion, agreedVersion(agreeing, talosVersionOf))
	assert.Equal(t, testKubeletImageVersion, agreedVersion(agreeing, kubernetesVersionOf))

	split := []upgradeMember{
		upgradeMemberAt(testUpgradeTargetVersion, testKubeletImageVersion),
		upgradeMemberAt("v1.9.0", testKubeletImageVersion),
	}
	assert.Empty(t, agreedVersion(split, talosVersionOf),
		"a cluster mid-roll reports no talos version rather than the first member's")
	assert.Equal(t, testKubeletImageVersion, agreedVersion(split, kubernetesVersionOf),
		"the component that isn't rolling still reports its own agreed version")

	unreported := []upgradeMember{upgradeMemberAt("", "")}
	assert.Empty(t, agreedVersion(unreported, talosVersionOf))
}

func TestStaleMembers(t *testing.T) {
	t.Parallel()

	members := []upgradeMember{
		upgradeMemberAt(testUpgradeTargetVersion, testKubeletImageVersion),
		upgradeMemberAt("v1.9.0", testKubeletImageVersion),
		upgradeMemberAt("", testKubeletImageVersion),
	}

	stale := staleMembers(members, testUpgradeTargetVersion, talosVersionOf)
	assert.Len(t, stale, 2, "both the older member and the one reporting nothing yet count as stale")

	assert.Empty(t, staleMembers(members, "", talosVersionOf),
		"an unpinned version must never make any member stale")
	assert.Empty(t, staleMembers(members, testKubeletImageVersion, kubernetesVersionOf),
		"a version every member already runs leaves nothing to roll")
}

// TestImageFlavourSuffix covers the three kubelet image flavours Talos
// publishes — an upgrade has to keep a node on the one it already runs.
func TestImageFlavourSuffix(t *testing.T) {
	t.Parallel()

	assert.Empty(t, imageFlavourSuffix("ghcr.io/siderolabs/kubelet:v1.32.0"))
	assert.Equal(t, "-slim", imageFlavourSuffix("ghcr.io/siderolabs/kubelet:v1.32.0-slim"))
	assert.Equal(t, "-fat", imageFlavourSuffix("ghcr.io/siderolabs/kubelet:v1.32.0-fat"))
}

// TestKubeletVersionFromImage is the other half of the flavour handling:
// the flavour is part of the image, never part of the version. A node
// whose reported version kept its suffix could never compare equal to a
// pinned spec.kubernetes.version, leaving its cluster permanently
// mid-upgrade.
func TestKubeletVersionFromImage(t *testing.T) {
	t.Parallel()

	for _, ref := range []string{
		"ghcr.io/siderolabs/kubelet:" + testKubeletImageVersion,
		"ghcr.io/siderolabs/kubelet:" + testKubeletImageVersion + "-slim",
		"ghcr.io/siderolabs/kubelet:" + testKubeletImageVersion + "-fat",
	} {
		assert.Equal(t, testKubeletImageVersion, kubeletVersionFromImage(ref),
			"every flavour of the same version reports that version")
	}

	assert.Empty(t, kubeletVersionFromImage("ghcr.io/siderolabs/kubelet"),
		"an untagged reference reports no version at all")
}

// TestComponentImage covers the reference each component's step targets,
// including the kubelet's flavour being carried through.
func TestComponentImage(t *testing.T) {
	t.Parallel()

	components := kubernetesComponents()
	require.Len(t, components, 5, "the plan moves five components")

	assert.Equal(t, componentAPIServer, components[0].name,
		"the apiserver moves first — everything else talks to it")
	assert.Equal(t, componentKubelet, components[len(components)-1].name,
		"the kubelet moves last, which is what lets its version gate the whole plan")

	assert.Equal(t, "registry.k8s.io/kube-apiserver:"+testUpgradeTargetVersion,
		components[0].image(testUpgradeTargetVersion, ""))
	assert.Equal(t, "ghcr.io/siderolabs/kubelet:"+testUpgradeTargetVersion+"-slim",
		components[len(components)-1].image(testUpgradeTargetVersion, "-slim"))
}
