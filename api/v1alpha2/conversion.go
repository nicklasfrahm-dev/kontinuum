package v1alpha2

// Hub marks Kontinuum as the conversion hub — the version every other
// version converts to and from. See api/v1alpha1's ConvertTo/ConvertFrom,
// and registry.CustomResourceDefinition's conversion webhook wiring.
func (*Kontinuum) Hub() {}
