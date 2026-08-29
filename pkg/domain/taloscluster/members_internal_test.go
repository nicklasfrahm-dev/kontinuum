package taloscluster

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

const (
	testDialAddressHostname = "node1.example.com"
	testDialAddressEthName  = "eth0"
	testDialAddressFallback = "10.0.0.9"
)

func TestDialAddress(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		inst v1alpha2.Instance
		want string
	}{
		"prefers a real discovered address": {
			inst: v1alpha2.Instance{
				Spec: v1alpha2.InstanceSpec{Interfaces: []string{testDialAddressHostname}},
				Status: v1alpha2.InstanceStatus{
					Interfaces: []v1alpha2.InstanceInterfaceStatus{
						{Name: testDialAddressEthName, Addresses: []string{"10.0.0.5/24"}},
					},
				},
			},
			want: "10.0.0.5",
		},
		"skips loopback addresses": {
			inst: v1alpha2.Instance{
				Spec: v1alpha2.InstanceSpec{Interfaces: []string{testDialAddressFallback}},
				Status: v1alpha2.InstanceStatus{
					Interfaces: []v1alpha2.InstanceInterfaceStatus{
						{Name: "lo", Addresses: []string{"127.0.0.1/8"}},
						{Name: testDialAddressEthName, Addresses: []string{"10.0.0.5/24"}},
					},
				},
			},
			want: "10.0.0.5",
		},
		"skips unparseable addresses": {
			inst: v1alpha2.Instance{
				Spec: v1alpha2.InstanceSpec{Interfaces: []string{testDialAddressFallback}},
				Status: v1alpha2.InstanceStatus{
					Interfaces: []v1alpha2.InstanceInterfaceStatus{
						{Name: testDialAddressEthName, Addresses: []string{"not-a-cidr"}},
					},
				},
			},
			want: testDialAddressFallback,
		},
		"falls back to spec.interfaces when status has no usable address": {
			inst: v1alpha2.Instance{
				Spec: v1alpha2.InstanceSpec{Interfaces: []string{testDialAddressHostname}},
			},
			want: testDialAddressHostname,
		},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, testCase.want, dialAddress(testCase.inst))
		})
	}
}
