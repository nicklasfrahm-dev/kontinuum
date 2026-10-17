package zone

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// The helpers below give other domain packages — pkg/domain/frontend's
// fleet-wide DNS writer, today — the same access to a zone's own cluster
// this package's own Reconciler uses, without duplicating how a zone's
// kubeconfig, Gateway and DNSEndpoints are found.

// DownstreamClient builds a client against zoneObj's own cluster from the
// kubeconfig its TalosCluster (same name and namespace as zoneObj) stores.
func DownstreamClient(
	ctx context.Context, hubClient client.Client, builder DownstreamClientBuilder, zoneObj *v1alpha2.Zone,
) (client.Client, error) {
	var cluster v1alpha2.TalosCluster

	err := hubClient.Get(ctx, client.ObjectKey{Name: zoneObj.Name, Namespace: zoneObj.Namespace}, &cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get talos cluster of zone %q: %w", zoneObj.Name, err)
	}

	kubeconfig, err := loadClusterKubeconfig(ctx, hubClient, &cluster)
	if err != nil {
		return nil, err
	}

	downstream, err := builder.Build(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build downstream client for zone %q: %w", zoneObj.Name, err)
	}

	return downstream, nil
}

// GatewayAddress reports the address of a zone's own kontinuum Gateway and
// the DNS record type pointing at it needs — see gatewayAddress.
func GatewayAddress(ctx context.Context, downstream client.Client) (string, string, bool, error) {
	return gatewayAddress(ctx, downstream, downstreamNamespace, gatewayName)
}

// Record types GatewayAddress reports.
const (
	RecordTypeA     = dnsRecordTypeA
	RecordTypeAAAA  = dnsRecordTypeAAAA
	RecordTypeCNAME = dnsRecordTypeCNAME
)

// EnsureDNSEndpoint upserts the DNSEndpoint named name in a zone's own
// kontinuum-system namespace with exactly endpoints, for that zone's
// external-dns to publish.
func EnsureDNSEndpoint(ctx context.Context, downstream client.Client, name string, endpoints []Endpoint) error {
	endpoint := &DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: downstreamNamespace},
		Spec:       DNSEndpointSpec{Endpoints: endpoints},
	}

	err := downstream.Create(ctx, endpoint)
	if !apierrors.IsAlreadyExists(err) {
		if err != nil {
			return fmt.Errorf("failed to create %q dnsendpoint: %w", name, err)
		}

		return nil
	}

	var existing DNSEndpoint

	err = downstream.Get(ctx, client.ObjectKeyFromObject(endpoint), &existing)
	if err != nil {
		return fmt.Errorf("failed to fetch existing %q dnsendpoint: %w", name, err)
	}

	existing.Spec = endpoint.Spec

	err = downstream.Update(ctx, &existing)
	if err != nil {
		return fmt.Errorf("failed to update %q dnsendpoint: %w", name, err)
	}

	return nil
}

// DeleteDNSEndpoint deletes the DNSEndpoint named name from a zone's own
// kontinuum-system namespace, tolerating NotFound.
func DeleteDNSEndpoint(ctx context.Context, downstream client.Client, name string) error {
	return deleteDNSEndpoint(ctx, downstream, downstreamNamespace, name)
}
