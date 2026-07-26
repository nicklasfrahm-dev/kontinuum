package v1alpha1

import (
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/conversion"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
)

// ErrUnsupportedConversionType is returned by ConvertTo/ConvertFrom when
// given a Hub that isn't *v1alpha2.Kontinuum — unreachable in practice,
// since the conversion webhook machinery only ever calls these with the
// real hub type.
var ErrUnsupportedConversionType = errors.New("unsupported conversion type")

// ConvertTo converts this v1alpha1 Kontinuum to the v1alpha2 hub type,
// moving Role from spec into status — see api/v1alpha2's KontinuumStatus.
func (in *Kontinuum) ConvertTo(dstRaw conversion.Hub) error {
	dst, ok := dstRaw.(*v1alpha2.Kontinuum)
	if !ok {
		return fmt.Errorf("%w: %T", ErrUnsupportedConversionType, dstRaw)
	}

	dst.ObjectMeta = in.ObjectMeta
	dst.Spec.Region = in.Spec.Region
	dst.Spec.Zone = in.Spec.Zone
	dst.Status.Role = in.Spec.Role
	dst.Status.LastHeartbeatTime = in.Status.LastHeartbeatTime

	return nil
}

// ConvertFrom converts the v1alpha2 hub type into this v1alpha1 Kontinuum,
// moving Role back from status into spec.
func (in *Kontinuum) ConvertFrom(srcRaw conversion.Hub) error {
	src, ok := srcRaw.(*v1alpha2.Kontinuum)
	if !ok {
		return fmt.Errorf("%w: %T", ErrUnsupportedConversionType, srcRaw)
	}

	in.ObjectMeta = src.ObjectMeta
	in.Spec.Region = src.Spec.Region
	in.Spec.Zone = src.Spec.Zone
	in.Spec.Role = src.Status.Role
	in.Status.LastHeartbeatTime = src.Status.LastHeartbeatTime

	return nil
}
