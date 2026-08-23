package zone

import (
	"fmt"

	"helm.sh/helm/v3/pkg/registry"
)

// DigestResolver resolves image (a plain "repo:tag" reference) to the
// digest its tag currently points at on the registry — the seam
// resolveImage uses to pin a floating tag ("dev" or "latest" — see
// imagePullPolicy's own doc) to an exact, immutable "repo:tag@sha256:..."
// reference. Without this, CI and `make image-push` (see the Makefile's
// own doc) can move a floating tag to a different image after a zone's
// kontinuum Deployment has already pulled it: the Deployment's own
// "repo:tag" string never changes, so it never produces a pod-template
// diff, so Kubernetes never cuts a new pod to notice the tag moved —
// imagePullPolicy's PullAlways only helps a pod being created right now,
// not one that's already running. Baking the digest into the reference
// itself turns a moved tag into a real spec diff, which triggers a normal
// automatic rollout. helmDigestResolver is the production implementation,
// reusing the exact OCI registry client addon.Installer already depends on
// (helm.sh/helm/v3/pkg/registry) rather than adding a new dependency just
// for this; tests inject a fake to avoid a real network call.
type DigestResolver interface {
	ResolveDigest(image string) (string, error)
}

// helmDigestResolver is DigestResolver's production implementation.
type helmDigestResolver struct{}

// NewHelmDigestResolver returns the production DigestResolver.
//
//nolint:ireturn // see DigestResolver's own doc
func NewHelmDigestResolver() DigestResolver {
	return helmDigestResolver{}
}

// ResolveDigest implements DigestResolver, via the same anonymous,
// unauthenticated OCI registry client addon.Installer's own Helm-based
// chart pulls already use — ghcr.io/nicklasfrahm-dev/kontinuum is a public
// package, so no credentials are configured or required here.
func (helmDigestResolver) ResolveDigest(image string) (string, error) {
	client, err := registry.NewClient()
	if err != nil {
		return "", fmt.Errorf("failed to create registry client: %w", err)
	}

	descriptor, err := client.Resolve(image)
	if err != nil {
		return "", fmt.Errorf("failed to resolve %q: %w", image, err)
	}

	return descriptor.Digest.String(), nil
}
