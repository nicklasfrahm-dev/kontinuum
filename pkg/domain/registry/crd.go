package registry

import (
	"context"
	"fmt"
	"log/slog"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	restclient "k8s.io/client-go/rest"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	crdconfig "github.com/nicklasfrahm/kontinuum/config/crd"
	"github.com/nicklasfrahm/kontinuum/pkg/crd"
)

const (
	crdPlural = "kontinuums"
	crdKind   = "Kontinuum"
	// crdManifestFile is kontinuums' generated manifest's name within
	// crdconfig.Files — see definition.
	crdManifestFile = "kontinuum.sh_kontinuums.yaml"
)

// ConversionWebhookPort is the port the conversion webhook server listens
// on — wired into libkapi.WithWebhookServer by pkg/cli/serve.go, and used
// here to build CustomResourceDefinition's conversion webhook clientConfig
// URL. Both must agree, since the same process serves and applies both.
const ConversionWebhookPort = 9443

// conversionWebhookDNSName is the only hostname the apiserver ever dials
// the conversion webhook on — see EnsureCRD's doc for why "localhost" (not
// a Service) is correct here.
const conversionWebhookDNSName = "localhost"

// conversionWebhookPath is where Controller.SetupWithManager registers the
// conversion webhook handler on the manager's webhook server, and where
// definition's conversion webhook clientConfig URL points.
//
// This one handler — conversion.NewWebhookHandler(mgr.GetScheme()) — already
// generalizes to any number of resource kinds and versions: it dispatches
// purely by the apiVersion/kind embedded in each ConversionReview request,
// not by path, so a future CRD only needs its own types to implement
// conversion.Convertible/Hub and be registered in the same shared scheme
// (see pkg/cli/serve.go's buildServer) to be served here too — no new path
// or handler required. The one real constraint is that
// webhook.Server.Register panics if the same path is registered twice, so
// if kontinuum ever grows a second Controller, that Controller must NOT
// also call Register on this path — this is the one, shared registration
// for every convertible CRD in the process.
const conversionWebhookPath = "/convert"

// crdName is the kontinuums.kontinuum.sh CustomResourceDefinition's name.
func crdName() string {
	return crdPlural + "." + v1alpha2.GroupName
}

// definition builds this CRD's pkg/crd.Definition — the kontinuums CRD is
// the only one in this codebase that carries a Conversion (v1alpha1<->v1alpha2),
// since it's the only kind with a prior served version — see
// api/v1alpha1/doc.go.
func definition(caBundle []byte) crd.Definition {
	return crd.Definition{
		Name:         crdName(),
		ManifestFile: crdManifestFile,
		GVKs: []schema.GroupVersionKind{
			v1alpha2.GroupVersion().WithKind(crdKind),
			v1alpha1.GroupVersion().WithKind(crdKind),
		},
		Conversion: &crd.ConversionWebhook{
			Path:     conversionWebhookPath,
			DNSName:  conversionWebhookDNSName,
			Port:     ConversionWebhookPort,
			CABundle: caBundle,
		},
	}
}

// CustomResourceDefinition builds the kontinuums.kontinuum.sh CRD by
// reading crdManifestFile out of crdconfig.Files — the config/crd manifest
// controller-gen generates from api/v1alpha1 and api/v1alpha2's kubebuilder
// markers — and patching in the one piece that manifest can't contain: the
// conversion webhook's clientConfig. controller-gen has no marker for a
// webhook's URL or CABundle, and CABundle in particular is only known at
// runtime — libkapi's own Server.WebhookCABundle, synced across every
// replica sharing this same central storage (see libkapi.WithSystemNamespace)
// — so it can't be baked into a generated file at all. Region/zone's CEL
// rule, the role enum, printer columns, and which version is storage are
// all controller-gen's responsibility now (see api/v1alpha2/doc.go); this
// function no longer hand-builds any of that, so schema and markers can't
// drift apart the way they already had once.
func CustomResourceDefinition(caBundle []byte) *apiextensionsv1.CustomResourceDefinition {
	return crd.Build(crdconfig.Files, definition(caBundle))
}

// EnsureCRD is a libkapi.PostStartHookFunc — see its registration in
// pkg/cli/serve.go via libkapi.WithPostStartHook. It builds an
// apiextensions client from loopbackConfig (the server's own privileged
// identity, only reachable once libkapi's post-start hooks run — after
// ListenAndServe's listener is bound and Serve is already running, but
// before the controller manager starts), creates the CRD and waits for it
// to become Established, then waits for Kontinuum's GVK to actually resolve
// through a RESTMapper built off the same loopbackConfig — the same kind of
// RESTMapper the registry's own Manager (and any other controller-runtime
// client) uses. Established alone doesn't guarantee that; since
// WithPostStartHook registrations run before the controller manager starts
// (see libkapi's own doc on WithPostStartHook and WithController), this
// closes the gap before anything downstream can lose the race. caBundle
// comes from libkapi's own Server.WebhookCABundle, read by the caller's own
// WithPostStartHook closure (registered before this one runs) — see that
// method's own doc for why it's already synced and safe to read by the time
// any WithPostStartHook fires. logger receives a warning for every retry
// along the way.
func EnsureCRD(ctx context.Context, loopbackConfig *restclient.Config, caBundle []byte, logger *slog.Logger) error {
	def := definition(caBundle)

	migrated, err := crd.MigrateScope(ctx, loopbackConfig, crdconfig.Files, def, logger)
	if err != nil {
		return fmt.Errorf("failed to migrate %s crd scope: %w", crdName(), err)
	}

	err = crd.Ensure(ctx, loopbackConfig, crdconfig.Files, def, logger)
	if err != nil {
		return fmt.Errorf("failed to ensure %s crd: %w", crdName(), err)
	}

	err = crd.RestoreMigrated(ctx, loopbackConfig, migrated, v1alpha2.KontinuumSystemNamespace, logger)
	if err != nil {
		return fmt.Errorf("failed to restore migrated %s objects: %w", crdName(), err)
	}

	return nil
}

// ApplyCRD creates the kontinuums.kontinuum.sh CRD, or — if it already
// exists — updates its spec to match CustomResourceDefinition(caBundle)'s
// current definition whenever the two differ. Without this, an
// already-registered CRD from a previous run would keep serving its stale
// definition forever, since Create alone is a no-op once the object exists.
func ApplyCRD(
	ctx context.Context, crds apiextensionsv1client.CustomResourceDefinitionInterface, caBundle []byte,
) error {
	err := crd.Apply(ctx, crds, crdconfig.Files, definition(caBundle))
	if err != nil {
		return fmt.Errorf("failed to apply %s crd: %w", crdName(), err)
	}

	return nil
}
