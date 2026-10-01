package zone

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// externalDNSGroupVersion is the GroupVersion external-dns' own `crd` source
// watches DNSEndpoint objects under — see
// https://github.com/kubernetes-sigs/external-dns/blob/master/endpoint/endpoint.go,
// the upstream source of truth for this CRD's shape. A function, not a
// package-level var, purely to keep this package's own gochecknoglobals
// linting clean — schema.GroupVersion is an immutable value type, so
// there's no aliasing concern either way.
func externalDNSGroupVersion() schema.GroupVersion {
	return schema.GroupVersion{Group: "externaldns.k8s.io", Version: "v1alpha1"}
}

// DNSEndpoint mirrors external-dns' own DNSEndpoint CRD (see
// externalDNSGroupVersion's own doc) — hand-rolled here rather than
// imported from sigs.k8s.io/external-dns: that module is a single,
// unsplit Go module for the whole external-dns binary, so importing even
// just its endpoint package pulls in every cloud provider SDK it vendors
// (AWS, Azure, GCP, ...) as go.mod/go.sum requirements — verified at
// roughly 1,100 added go.sum lines and several shared dependency version
// bumps for six struct fields' worth of types. This shape is fixed by
// external-dns' own CRD schema, not something kontinuum controls, so
// duplicating the handful of fields this package actually needs (see
// ensureDNSEndpoint) carries no real drift risk. Only Endpoint's DNSName/
// Targets/RecordType are included — every other upstream Endpoint field
// (SetIdentifier, RecordTTL, Labels, ProviderSpecific) exists to support
// features (multi-value routing policies, TTL overrides, ownership
// tracking) this package doesn't use.
type DNSEndpoint struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec DNSEndpointSpec `json:"spec"`
	// +optional
	Status DNSEndpointStatus `json:"status"`
}

// DeepCopyObject implements runtime.Object.
func (in *DNSEndpoint) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}

	out := *in
	out.ObjectMeta = *in.DeepCopy()
	out.Spec.Endpoints = make([]Endpoint, len(in.Spec.Endpoints))

	for i, ep := range in.Spec.Endpoints {
		out.Spec.Endpoints[i] = Endpoint{
			DNSName:    ep.DNSName,
			Targets:    append([]string(nil), ep.Targets...),
			RecordType: ep.RecordType,
		}
	}

	return &out
}

// DNSEndpointSpec mirrors external-dns' own DNSEndpointSpec.
type DNSEndpointSpec struct {
	Endpoints []Endpoint `json:"endpoints,omitempty"`
}

// DNSEndpointStatus mirrors external-dns' own DNSEndpointStatus.
type DNSEndpointStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// Endpoint mirrors the subset of external-dns' own Endpoint fields this
// package sets — see DNSEndpoint's own doc for why the rest are omitted.
type Endpoint struct {
	DNSName    string   `json:"dnsName,omitempty"`
	Targets    []string `json:"targets,omitempty"`
	RecordType string   `json:"recordType,omitempty"`
}

// DNSEndpointList mirrors external-dns' own DNSEndpointList — needed to
// satisfy client.Client's List, even though this package only ever Gets or
// Creates/Updates a single named DNSEndpoint.
type DNSEndpointList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []DNSEndpoint `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (in *DNSEndpointList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}

	out := *in
	out.Items = make([]DNSEndpoint, len(in.Items))

	for i := range in.Items {
		itemCopy, ok := in.Items[i].DeepCopyObject().(*DNSEndpoint)
		if ok {
			out.Items[i] = *itemCopy
		}
	}

	return &out
}

// AddExternalDNSToScheme registers DNSEndpoint/DNSEndpointList under
// externalDNSGroupVersion — called from downstreamScheme (see
// downstreamclient.go). Exported so this package's own tests can build an
// equivalent fake downstream client's scheme without duplicating
// externalDNSGroupVersion's own group/version literals.
func AddExternalDNSToScheme(scheme *runtime.Scheme) {
	scheme.AddKnownTypes(externalDNSGroupVersion(), &DNSEndpoint{}, &DNSEndpointList{})
	metav1.AddToGroupVersion(scheme, externalDNSGroupVersion())
}
