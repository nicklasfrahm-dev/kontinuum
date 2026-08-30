// Package ui exposes an HTMX + Tailwind based web UI for kontinuum, mounted
// at /app. It renders kontinuum's Kubernetes-style resources as friendlier
// domain concepts — for now, namespaces are shown as tenants.
package ui

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"maps"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/auth"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/adminrbac"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/etcdproxy"
	instancedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	zonedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

// NamespaceLister is the subset of the Kubernetes API the UI needs to list
// namespaces. It is satisfied by a clientset's CoreV1().Namespaces().
type NamespaceLister interface {
	List(ctx context.Context, opts metav1.ListOptions) (*corev1.NamespaceList, error)
}

// NamespaceListerFactory builds a NamespaceLister scoped to ctx. The caller
// supplies one so the UI runs its own API calls as whatever identity ctx
// carries — see pkg/auth.WithToken/TokenFromContext — instead of through a
// separate, privileged internal client.
type NamespaceListerFactory func(ctx context.Context) (NamespaceLister, error)

// KontinuumClient is the subset of the Kubernetes API the UI needs to get,
// list, create, and delete registered kontinuum instances (see
// pkg/domain/registry, which owns the kontinuums.kontinuum.sh CRD and the
// objects it acts on) as well as RBAC objects (see the IAM handlers below).
// It is satisfied by a controller-runtime client.Client.
type KontinuumClient interface {
	Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error
	List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error
	Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error
	Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error
}

// KontinuumClientFactory builds a KontinuumClient scoped to ctx — see
// NamespaceListerFactory, which follows the same per-request-identity
// pattern.
type KontinuumClientFactory func(ctx context.Context) (KontinuumClient, error)

// ZoneClientFactory builds a client scoped to ctx for the "Add zone" form
// — see NamespaceListerFactory for the same per-request-identity pattern.
// Unlike KontinuumClient/NamespaceLister, this isn't narrowed to a small
// interface: it's handed straight to pkg/domain/zone.Add, the same
// shared fan-out function kontinuum zone add calls, which itself expects
// a full controller-runtime client.Client — see that function's own doc.
type ZoneClientFactory func(ctx context.Context) (client.Client, error)

// SessionInvalidator ends the caller's session and sends them back to the
// login page with message shown as a human-readable error. Called when a
// Kubernetes API request comes back Forbidden — a valid session cookie only
// proves who the signed-in user is, not what they're allowed to do, so this
// package (which has no notion of sessions itself) delegates ending one to
// whoever wraps this router. See pkg/auth.Handler.InvalidateSession, the
// only implementation kontinuum wires up today. nil disables this behavior
// — the Forbidden error is just shown as a plain response instead.
type SessionInvalidator func(writer http.ResponseWriter, request *http.Request, message string)

//go:embed templates/*.html templates/components/*.html
var templatesFS embed.FS

const (
	pageRegistry  = "registry"
	pageKontinuum = "kontinuum"
	// pageInstances and pageInstanceDetail back
	// /app/kontinuum.sh/v1alpha2/namespaces/{ns}/instances and
	// .../instances/{name} — the Instance CRD (api/v1alpha2.Instance, a
	// bare-metal or provider-backed machine InstancePool/TalosCluster claims
	// from). Not to be confused with pageKontinuum above: a registered
	// Kontinuum server process, a completely different type despite the
	// similar-sounding name — see issue #52, which is why the two stay
	// separate pages under separate routes (/instances vs /kontinuums)
	// rather than sharing one. The URL's own kontinuum.sh/v1alpha2/namespaces/{ns}
	// shape (rather than a flat /app/instances) is what issue #63's
	// architecture needs: Instance became a namespaced CRD there, one
	// tenant's own namespace at a time — see the nav's tenant switcher
	// (renderTenantSwitcher) for how {ns} is chosen.
	pageInstances      = "instances"
	pageInstanceDetail = "instance-detail"
	pageTalosClusters  = "talosclusters"
	pageTalosCluster   = "taloscluster"
	// pageZoneDetail backs /app/kontinuum.sh/v1alpha2/namespaces/{ns}/zones/{name} —
	// see handleZoneDetail.
	pageZoneDetail   = "zone-detail"
	pageIAMNamespace = "iam-namespace"
	pageIAMCluster   = "iam-cluster"
	pageConnect      = "connect"
)

// Template data-map keys every page's own render() call shares — layout.html
// and its nav read these off every page's data map identically, so each
// handler below builds its map with these same keys rather than repeating
// the literal at every call site.
const (
	dataKeyTitle        = "Title"
	dataKeyActiveMenu   = "ActiveMenu"
	dataKeyVersion      = "Version"
	dataKeyNamespace    = "Namespace"
	dataKeyInstances    = "Instances"
	dataKeyAuthEnabled  = "AuthEnabled"
	dataKeyTalosVersion = "TalosVersion"
	dataKeyName         = "Name"
	dataKeyAge          = "Age"
	dataKeyRegion       = "Region"
	dataKeyDeleting     = "Deleting"
	dataKeyConditions   = "Conditions"
)

// defaultTenantNamespace is where GET /app and /app/home land a caller who
// hasn't picked a tenant yet — v1alpha2.KontinuumSystemNamespace
// ("kontinuum-system"), the one namespace guaranteed to exist and be worth
// looking at on every install (see pkg/domain/zone.Add's own doc).
const defaultTenantNamespace = v1alpha2.KontinuumSystemNamespace

// defaultInstancesPath is defaultTenantNamespace's own instances list URL —
// shared by handleAppRoot/handleHome's redirects and nav.html's own
// "Instances" link default (see renderTenantSwitcher).
const defaultInstancesPath = "/app/kontinuum.sh/v1alpha2/namespaces/" + defaultTenantNamespace + "/instances"

// dictPairSize is the number of arguments (one key, one value) templateDict
// consumes per resulting map entry.
const dictPairSize = 2

// errDictOddArgs and errDictKeyType are templateDict's own sentinel errors —
// wrapped with call-specific detail below rather than constructed inline,
// since err113 requires dynamic error text to wrap a static base error.
var (
	errDictOddArgs = errors.New("dict requires an even number of arguments")
	errDictKeyType = errors.New("dict key must be a string")
)

// templateDict builds a map[string]any from alternating key/value
// arguments — it lets a template call site build an ad hoc map literal —
// e.g. {{template "reveal-panel" dict "ID" "foo" "Label" "bar"}} — since
// html/template has no map-literal syntax of its own, and the
// "reveal-panel"/"reveal-panel-script" components (see
// templates/components/reveal_panel.html) both need several named fields
// passed in at once, not just the page's own top-level data. Registered
// with every page's template tree by mustParsePage.
func templateDict(pairs ...any) (map[string]any, error) {
	if len(pairs)%dictPairSize != 0 {
		return nil, errDictOddArgs
	}

	dict := make(map[string]any, len(pairs)/dictPairSize)

	for idx := 0; idx < len(pairs); idx += dictPairSize {
		key, ok := pairs[idx].(string)
		if !ok {
			return nil, fmt.Errorf("%w: pair %d, got %T", errDictKeyType, idx/dictPairSize, pairs[idx])
		}

		dict[key] = pairs[idx+1]
	}

	return dict, nil
}

// mustParsePage parses the layout and partials shared by every page, plus
// the given page-specific content files, into their own template tree —
// isolated from other pages' template trees, because every content file
// defines a template literally named "content". Sharing one tree across
// pages would let the last-parsed page's "content" definition silently win
// for all of them.
func mustParsePage(content ...string) *template.Template {
	shared := []string{
		"templates/layout.html",
		"templates/components/nav.html",
		"templates/components/tenant_switcher.html",
		"templates/components/icon_tenants.html",
		"templates/components/icon_chevrons_updown.html",
		"templates/components/icon_registry.html",
		"templates/components/icon_server.html",
		"templates/components/icon_kubernetes.html",
		"templates/components/icon_shield.html",
		"templates/components/icon_unplug.html",
		"templates/components/icon_key.html",
		"templates/components/icon_globe_lock.html",
		"templates/components/icon_door_closed_lock.html",
		"templates/components/icon_logout.html",
		"templates/components/icon_book_open_text.html",
		"templates/components/icon_external_link.html",
		// icon_check.html is used by layout.html's own toast (see
		// showToast) — every page needs it, not just the pages that also
		// happen to use it for their own copy-confirmation/checkmark UI.
		"templates/components/icon_check.html",
		// copy_chip.html is used by layout.html's own toast (see
		// buildCopyChip) — every page needs it, same reasoning as
		// icon_check.html above.
		"templates/components/copy_chip.html",
	}

	files := make([]string, 0, len(shared)+len(content))
	files = append(files, shared...)
	files = append(files, content...)

	// dict/hasVerb are the only template funcs made available to every
	// page's template tree — see templateDict/hasVerb's own docs for why.
	funcs := template.FuncMap{"dict": templateDict, "hasVerb": hasVerb}

	return template.Must(template.New("").Funcs(funcs).ParseFS(templatesFS, files...))
}

// Router handles HTTP routing for the /app UI.
type Router struct {
	namespacesFor     NamespaceListerFactory
	kontinuumsFor     KontinuumClientFactory
	zonesFor          ZoneClientFactory
	pages             map[string]*template.Template
	version           string
	cfg               config.Config
	authEnabled       bool
	invalidateSession SessionInvalidator
}

// NewRouter creates a new UI router backed by namespacesFor, kontinuumsFor,
// and zonesFor. cfg drives the registry page's own kubectl access card
// (its OIDC settings decide whether the served kubeconfig needs a login
// step) and is expected to already be redacted (see config.Config.Redact) —
// Router does not redact it itself. The "Add zone" form leaves
// zonedomain.AddOptions.Domain empty for
// pkg/domain/zone.Add to infer from an already-registered Kontinuum — see
// that field's own doc — rather than this Router needing a domain of its
// own. authEnabled shows or hides the nav's logout link; pass true only
// when a /app/logout route is actually registered (see pkg/auth), since
// otherwise the link would 404. invalidateSession may be nil (see
// SessionInvalidator).
func NewRouter(
	namespacesFor NamespaceListerFactory, kontinuumsFor KontinuumClientFactory, zonesFor ZoneClientFactory,
	version string, cfg config.Config, authEnabled bool, invalidateSession SessionInvalidator,
) *Router {
	return &Router{
		namespacesFor:     namespacesFor,
		kontinuumsFor:     kontinuumsFor,
		zonesFor:          zonesFor,
		pages:             buildPages(),
		version:           version,
		cfg:               cfg,
		authEnabled:       authEnabled,
		invalidateSession: invalidateSession,
	}
}

// buildPages parses every page's own template tree — split out of NewRouter
// purely to keep that function's own line count under funlen's limit, not
// for any functional reason.
func buildPages() map[string]*template.Template {
	return map[string]*template.Template{
		pageRegistry: mustParsePage("templates/registry_content.html",
			"templates/components/icon_trash.html", "templates/components/icon_globe.html",
			"templates/components/icon_loader_circle.html", "templates/components/icon_x.html",
			"templates/components/modal_close_button.html",
			"templates/components/zone_add_modal.html", "templates/components/zone_leave_modal.html"),
		pageKontinuum: mustParsePage("templates/kontinuum_content.html",
			"templates/components/icon_chevron_left.html", "templates/components/icon_eye.html",
			"templates/components/icon_eye_off.html", "templates/components/icon_copy.html",
			"templates/components/icon_key.html", "templates/components/icon_globe.html",
			"templates/components/icon_info.html", "templates/components/icon_download.html",
			"templates/components/reveal_panel.html", "templates/components/reveal_panel_script.html"),
		pageInstances: mustParsePage("templates/instances_content.html",
			"templates/components/icon_server_plus.html", "templates/components/icon_loader_circle.html",
			"templates/components/icon_x.html", "templates/components/modal_close_button.html",
			"templates/components/instance_add_modal.html", "templates/components/icon_trash.html"),
		pageInstanceDetail: mustParsePage("templates/instance_detail_content.html",
			"templates/components/icon_chevron_left.html", "templates/components/icon_info.html",
			"templates/components/icon_ethernet_port.html", "templates/components/icon_list_checks.html",
			"templates/components/icon_trash.html", "templates/components/icon_loader_circle.html",
			"templates/components/icon_cpu.html", "templates/components/icon_memory_stick.html",
			"templates/components/icon_hard_drive.html", "templates/components/conditions_table.html"),
		pageTalosClusters: mustParsePage("templates/talosclusters_content.html",
			"templates/components/icon_trash.html"),
		pageTalosCluster: mustParsePage("templates/taloscluster_content.html",
			"templates/components/icon_chevron_left.html", "templates/components/icon_info.html",
			"templates/components/icon_list_checks.html",
			"templates/components/icon_key.html", "templates/components/icon_download.html",
			"templates/components/icon_eye.html", "templates/components/icon_eye_off.html",
			"templates/components/icon_copy.html", "templates/components/icon_trash.html",
			"templates/components/icon_loader_circle.html",
			"templates/components/reveal_panel.html", "templates/components/reveal_panel_script.html",
			"templates/components/copy_snippet.html", "templates/components/conditions_table.html"),
		pageZoneDetail: mustParsePage("templates/zone_detail_content.html",
			"templates/components/icon_chevron_left.html", "templates/components/icon_info.html",
			"templates/components/icon_kubernetes.html", "templates/components/icon_server.html",
			"templates/components/icon_list_checks.html", "templates/components/icon_trash.html",
			"templates/components/icon_key.html", "templates/components/icon_loader_circle.html",
			"templates/components/conditions_table.html"),
		pageIAMNamespace: mustParsePage("templates/iam_namespace_content.html",
			"templates/components/icon_info.html", "templates/components/icon_shield_user.html",
			"templates/components/icon_user_shield.html",
			"templates/components/icon_x.html", "templates/components/modal_close_button.html",
			"templates/components/icon_loader_circle.html", "templates/components/icon_trash.html",
			"templates/components/role_rows.html", "templates/components/iam_delete_modal.html",
			"templates/components/role_add_modal.html", "templates/components/rolebinding_add_modal.html"),
		pageIAMCluster: mustParsePage("templates/iam_cluster_content.html",
			"templates/components/icon_info.html", "templates/components/icon_shield_user.html",
			"templates/components/icon_user_shield.html",
			"templates/components/icon_x.html", "templates/components/modal_close_button.html",
			"templates/components/icon_loader_circle.html", "templates/components/icon_trash.html",
			"templates/components/role_rows.html", "templates/components/iam_delete_modal.html",
			"templates/components/role_add_modal.html", "templates/components/rolebinding_add_modal.html"),
		pageConnect: mustParsePage("templates/connect_content.html",
			"templates/components/icon_terminal.html",
			"templates/components/copy_snippet.html", "templates/components/icon_copy.html",
			"templates/components/icon_download.html",
			"templates/components/icon_eye.html", "templates/components/icon_eye_off.html",
			"templates/components/reveal_panel.html", "templates/components/reveal_panel_script.html"),
	}
}

// RegisterRoutes registers UI routes on the given mux.
//
// The root path ("/") is shared with the Kubernetes-style API server's own
// discovery handler (it normally answers GET / with {"paths": [...]}), so
// the redirect to /app only fires for requests that look like a browser
// navigation. kubectl, controller-runtime, and client-go never send an
// Accept header preferring text/html, so they fall through to a plain 404 —
// same as any other unregistered path — instead of being redirected
// somewhere their REST clients don't expect.
//
// appRoot and protect let a caller layer authentication onto the UI without
// this package needing to know anything about it. appRoot overrides the
// GET /app handler; nil keeps the default unconditional redirect to
// defaultInstancesPath. protect wraps the /app/home and /app/registry/kubeconfig
// handlers; nil leaves them unprotected. See pkg/auth for kontinuum's OIDC
// login flow, which supplies both when OIDC is configured, and redirects to
// /app/home itself on a successful login — kept as a real route (rather than
// folded into GET /app) purely so that redirect target keeps working.
func (r *Router) RegisterRoutes(
	mux *http.ServeMux, appRoot http.HandlerFunc, protect func(http.HandlerFunc) http.HandlerFunc,
) {
	if appRoot == nil {
		appRoot = handleAppRoot
	}

	if protect == nil {
		protect = func(next http.HandlerFunc) http.HandlerFunc { return next }
	}

	// wrap composes protect with notFoundFallback (see that function's own
	// doc) — every /app page route below goes through both, so a stale link
	// to a since-deleted object walks up to its nearest still-valid parent
	// instead of dead-ending on a bare 404.
	wrap := func(next http.HandlerFunc) http.HandlerFunc { return protect(notFoundFallback(next)) }

	mux.Handle("GET "+vendorURLPrefix, vendorHandler())
	mux.HandleFunc("GET /{$}", handleRoot)
	mux.HandleFunc("GET /app", appRoot)
	mux.HandleFunc("GET /app/home", wrap(handleAppHome))
	mux.HandleFunc("GET /app/kontinuum.sh/v1alpha2/namespaces/{ns}/kontinuums", wrap(r.renderRegistry))
	mux.HandleFunc("GET /app/kontinuum.sh/v1alpha2/namespaces/{ns}/kontinuums/{name}", wrap(r.handleKontinuumDetail))
	mux.HandleFunc("GET /app/kontinuum.sh/v1alpha2/namespaces/{ns}/kontinuums/{name}/secret",
		wrap(r.handleKontinuumSecretDownload))
	mux.HandleFunc("DELETE /app/kontinuum.sh/v1alpha2/namespaces/{ns}/kontinuums/{name}", wrap(r.handleDeleteInstance))
	mux.HandleFunc("POST /app/zones/add", wrap(r.handleZoneAdd))
	mux.HandleFunc("GET /app/kontinuum.sh/v1alpha2/namespaces/{ns}/zones/{name}", wrap(r.handleZoneDetail))
	mux.HandleFunc("DELETE /app/kontinuum.sh/v1alpha2/namespaces/{ns}/zones/{name}", wrap(r.handleDeleteZone))
	mux.HandleFunc("GET /app/kontinuum.sh/v1alpha2/namespaces/{ns}/instances", wrap(r.handleInstances))
	mux.HandleFunc("POST /app/kontinuum.sh/v1alpha2/namespaces/{ns}/instances/add", wrap(r.handleInstanceAdd))
	mux.HandleFunc("GET /app/kontinuum.sh/v1alpha2/namespaces/{ns}/instances/{name}", wrap(r.handleInstanceDetail))
	mux.HandleFunc("DELETE /app/kontinuum.sh/v1alpha2/namespaces/{ns}/instances/{name}", wrap(r.handleDeleteInstanceObject))
	mux.HandleFunc("GET /app/kontinuum.sh/v1alpha2/namespaces/{ns}/talosclusters", wrap(r.handleTalosClusters))
	mux.HandleFunc("GET /app/kontinuum.sh/v1alpha2/namespaces/{ns}/talosclusters/{name}", wrap(r.handleTalosClusterDetail))
	mux.HandleFunc("DELETE /app/kontinuum.sh/v1alpha2/namespaces/{ns}/talosclusters/{name}", wrap(r.handleDeleteTalosCluster))
	mux.HandleFunc("GET /app/kontinuum.sh/v1alpha2/namespaces/{ns}/talosclusters/{name}/kubeconfig",
		wrap(r.handleTalosClusterKubeconfigDownload))
	r.registerIAMRoutes(mux, wrap)
	mux.HandleFunc("GET /app/connect", wrap(r.handleConnect))
	mux.HandleFunc("GET /app/registry/kubeconfig", wrap(r.handleRegistryKubeconfigDownload))

	// Catch-all for any /app/... path that doesn't match one of the more
	// specific patterns above (ServeMux always prefers the most specific
	// match, so this never shadows them) — a mistyped or otherwise
	// unroutable URL gets the same walk-up-to-parent treatment as a
	// genuinely deleted object, via notFoundFallback below, rather than a
	// bare 404 with nowhere obvious to go from here.
	mux.HandleFunc("/app/", wrap(func(writer http.ResponseWriter, request *http.Request) {
		http.NotFound(writer, request)
	}))
}

// registerIAMRoutes registers every namespaced Role/RoleBinding and
// cluster-scoped ClusterRole/ClusterRoleBinding route — split out of
// RegisterRoutes purely to keep that function under funlen's statement
// limit, the same reasoning buildPages was split out of NewRouter for.
func (r *Router) registerIAMRoutes(mux *http.ServeMux, wrap func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("GET /app/rbac.authorization.k8s.io/v1/namespaces/{ns}/roles", wrap(r.handleIAMNamespaceRoles))
	mux.HandleFunc("GET /app/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings",
		wrap(r.handleIAMNamespaceRoleBindings))
	mux.HandleFunc("POST /app/rbac.authorization.k8s.io/v1/namespaces/{ns}/roles", wrap(r.handleRoleAdd))
	mux.HandleFunc("POST /app/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings", wrap(r.handleRoleBindingAdd))
	mux.HandleFunc("DELETE /app/rbac.authorization.k8s.io/v1/namespaces/{ns}/roles/{name}", wrap(r.handleDeleteRole))
	mux.HandleFunc("DELETE /app/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings/{name}",
		wrap(r.handleDeleteRoleBinding))
	mux.HandleFunc("GET /app/rbac.authorization.k8s.io/v1/clusterroles", wrap(r.handleIAMClusterRoles))
	mux.HandleFunc("GET /app/rbac.authorization.k8s.io/v1/clusterrolebindings", wrap(r.handleIAMClusterRoleBindings))
	mux.HandleFunc("POST /app/rbac.authorization.k8s.io/v1/clusterroles", wrap(r.handleClusterRoleAdd))
	mux.HandleFunc("POST /app/rbac.authorization.k8s.io/v1/clusterrolebindings", wrap(r.handleClusterRoleBindingAdd))
	mux.HandleFunc("DELETE /app/rbac.authorization.k8s.io/v1/clusterroles/{name}", wrap(r.handleDeleteClusterRole))
	mux.HandleFunc("DELETE /app/rbac.authorization.k8s.io/v1/clusterrolebindings/{name}",
		wrap(r.handleDeleteClusterRoleBinding))
}

// notFoundInterceptor is a http.ResponseWriter that withholds a 404 from
// reaching the real client — see notFoundFallback's own doc for why.
// Anything else (a real 200, an error page, a redirect) passes straight
// through untouched.
type notFoundInterceptor struct {
	http.ResponseWriter

	notFound bool
}

func (interceptor *notFoundInterceptor) WriteHeader(statusCode int) {
	if statusCode == http.StatusNotFound {
		interceptor.notFound = true

		return
	}

	interceptor.ResponseWriter.WriteHeader(statusCode)
}

func (interceptor *notFoundInterceptor) Write(body []byte) (int, error) {
	if interceptor.notFound {
		// http.NotFound's own plain-text body — discarded; notFoundFallback
		// sends a redirect instead once next has finished running.
		return len(body), nil
	}

	written, err := interceptor.ResponseWriter.Write(body)
	if err != nil {
		return written, fmt.Errorf("failed to write response: %w", err)
	}

	return written, nil
}

// notFoundFallback wraps next so that whenever it responds 404 — however
// that happens: an object lookup's own apierrors.IsNotFound branch, or the
// catch-all route's unconditional http.NotFound in RegisterRoutes above —
// the caller is redirected to request's own parent path (the URL with its
// final segment removed) instead of the bare "404 page not found" a
// deleted object's own stale link (bookmarked, or linked to from
// elsewhere) would otherwise dead-end on. If that parent itself doesn't
// resolve to anything either — e.g. a TalosCluster's kubeconfig download
// 404s because the cluster itself is gone, so its own detail page 404s
// too — that parent's own handler runs through this exact same wrapping
// and redirects up again, so a stale link naturally walks all the way up
// to the nearest ancestor that still exists — a list page, which never
// 404s on its own, at the very least, or /app itself (see appRoot's own
// unconditional redirect) — purely through the browser following each
// redirect in turn, no server-side loop needed. Only "/" has no parent
// left to walk up to, so that's the one case this still answers with a
// real 404.
func notFoundFallback(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		interceptor := &notFoundInterceptor{ResponseWriter: writer}
		next(interceptor, request)

		if !interceptor.notFound {
			return
		}

		parent := path.Dir(request.URL.Path)
		if !isWithinApp(parent) || parent == request.URL.Path {
			http.NotFound(writer, request)

			return
		}

		target := url.URL{Path: parent}

		// A detail page's own "every 15s" hx-get (e.g.
		// instance_detail_content.html's own poll) hitting this 404 branch
		// is an htmx AJAX request, not a top-level navigation — a plain
		// http.Redirect can't steer that: the browser's own XHR/fetch layer
		// follows a 3xx transparently without ever touching the address
		// bar or re-running RegisterRoutes' page handlers, so htmx just
		// tries to hx-select an element (e.g. "#instance-detail-content")
		// that doesn't exist in whatever page the redirect silently landed
		// on and does nothing — the caller is left staring at a stale
		// detail page for an object that's already gone, exactly the bug
		// this branch fixes. HX-Request marks exactly these requests (see
		// htmx's own docs); answering them with Hx-Redirect instead makes
		// htmx itself perform the real top-level navigation — the same
		// mechanism deleteAndRedirect already uses for an explicit delete,
		// just reached here for an object that vanished out from under a
		// still-open page instead.
		if request.Header.Get("Hx-Request") == "true" {
			writer.Header().Set("Hx-Redirect", target.String())
			writer.WriteHeader(http.StatusOK)

			return
		}

		http.Redirect(writer, request, target.String(), http.StatusFound)
	}
}

// isWithinApp reports whether target is safe for notFoundFallback to send a
// caller to: root-relative (so it can never carry a scheme or host of its
// own) and still under /app. request.URL.Path — the only input path.Dir
// ever derives target from — is already a parsed path with no scheme or
// host of its own to begin with, so this is never expected to actually
// reject anything in practice; it exists to make that invariant checked
// rather than merely assumed, since target does end up as the target of an
// http.Redirect.
func isWithinApp(target string) bool {
	return strings.HasPrefix(target, "/app") && !strings.HasPrefix(target, "//")
}

func handleRoot(writer http.ResponseWriter, request *http.Request) {
	if !acceptsHTML(request) {
		http.NotFound(writer, request)

		return
	}

	http.Redirect(writer, request, "/app", http.StatusFound)
}

func handleAppRoot(writer http.ResponseWriter, request *http.Request) {
	http.Redirect(writer, request, defaultInstancesPath, http.StatusFound)
}

// handleAppHome is GET /app/home's handler — kept as a real route purely as
// pkg/auth's own post-login redirect target (see RegisterRoutes' own doc);
// there is no "home" page of its own anymore (see issue #63's UI comment:
// the old tenants list this used to render is gone, replaced by nav.html's
// own tenant switcher), so it just forwards straight to
// defaultInstancesPath.
func handleAppHome(writer http.ResponseWriter, request *http.Request) {
	http.Redirect(writer, request, defaultInstancesPath, http.StatusFound)
}

// acceptsHTML reports whether request's Accept header prefers HTML, the
// signal real browsers send for top-level navigations. Kubernetes API
// clients ask for application/json or the apidiscovery media types instead.
func acceptsHTML(request *http.Request) bool {
	return strings.Contains(request.Header.Get("Accept"), "text/html")
}

// instance is a Kontinuum object rendered as a registry row in the UI.
type instance struct {
	Name          string
	Namespace     string
	Role          string
	Region        string
	Zone          string
	LastHeartbeat string
	Age           string
	APIVersion    string
	Version       string
}

// zoneRow is a Zone object rendered as a row in the registry page's own
// zones table. Condition/Message reflect whichever of status.conditions
// most recently transitioned (see latestCondition) — a Zone only ever
// carries ClusterReady and Installed (see ZoneStatus's own doc), so
// showing "whichever changed last" is a reasonable single-glance summary
// without needing a column per condition type.
type zoneRow struct {
	Name   string
	Region string
	Age    string
	// Condition is that condition's own Type only (e.g. "ClusterReady"),
	// not "Type=Status" — the badge's own color (see ConditionOK) already
	// carries the status, matching the pill-per-condition shape every
	// other conditions display in this package uses (see
	// templates/components/conditions_table.html and
	// instances_content.html's own Discovered/Deleting pills).
	Condition string
	// ConditionOK is whether Condition's own status is True — the zones
	// table's badge template colors on this.
	ConditionOK bool
	Message     string
	// Deleting is whether this Zone's own DeletionTimestamp is set — the
	// template shows this instead of Condition/Message, since a condition
	// like "ClusterReady" left over from before deletion started, still
	// colored green, reads as "everything's fine" when it's actually
	// mid-teardown (see ZoneFinalizer's own doc for that window).
	Deleting bool
}

// handleDeleteInstance deletes the Kontinuum object named by the {name}
// path value, then re-renders the registry page — the same response a GET
// would produce — so the htmx button that triggers this (hx-select'ing
// #registry-content out of the response, same as the page's own polling)
// shows the updated list immediately instead of waiting for the next poll.
// deleteDebounce is how long handleDeleteInstance waits after a successful
// Delete before re-rendering the registry page. A live instance re-registers
// itself the moment its own deletion reaches the registry's self-healing
// controller (see pkg/domain/registry.Heartbeat.Reconcile) — typically well
// under this window. Rendering immediately would instead show the row gone
// for this one response, then have it reappear on the next poll, which
// reads as "the delete didn't work" rather than what actually happened.
const deleteDebounce = 500 * time.Millisecond

func (r *Router) handleDeleteInstance(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	target := &v1alpha2.Kontinuum{
		ObjectMeta: metav1.ObjectMeta{Name: request.PathValue("name"), Namespace: request.PathValue("ns")},
	}

	err = kontinuums.Delete(request.Context(), target)
	if err != nil && !apierrors.IsNotFound(err) {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		http.Error(writer, "failed to delete kontinuum instance: "+err.Error(), http.StatusBadGateway)

		return
	}

	select {
	case <-time.After(deleteDebounce):
	case <-request.Context().Done():
	}

	r.renderRegistry(writer, request)
}

// handleDeleteZone deletes the Zone object named by the {name} path value,
// then re-renders the registry page — the same shape as handleDeleteInstance
// above. {ns} is not Zone's own scope (Zone is cluster-scoped) but
// renderRegistry's own Kontinuum-instances table needs it, so the "leave
// zone" button carries the page's current namespace through the URL purely
// to thread it back into the re-render, same as every other route on this
// page.
//
// Deleting here only sets Zone's deletionTimestamp — pkg/domain/zone's own
// finalizer (see ZoneFinalizer) drives the actual downstream teardown and
// seed-node reset asynchronously, then removes the finalizer once done, so
// the row keeps showing (with its Teardown condition surfacing progress)
// until that completes rather than disappearing immediately.
func (r *Router) handleDeleteZone(writer http.ResponseWriter, request *http.Request) {
	zones, err := r.zonesFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	// Unlike Kontinuum, Zone always lives in the fixed
	// v1alpha2.KontinuumSystemNamespace (see pkg/domain/zone.Add) regardless of
	// which tenant namespace the registry page currently shows — {ns} in
	// this route is only along for renderRegistry's own re-render below, not
	// Zone's actual namespace.
	target := &v1alpha2.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: request.PathValue("name"), Namespace: v1alpha2.KontinuumSystemNamespace},
	}

	err = zones.Delete(request.Context(), target)
	if err != nil && !apierrors.IsNotFound(err) {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		http.Error(writer, "failed to delete zone: "+err.Error(), http.StatusBadGateway)

		return
	}

	r.renderRegistry(writer, request)
}

// renderRegistry is GET /app/kontinuums's handler — it lists Kontinuum
// instances and renders the registry page. Also called by
// handleDeleteInstance after a delete, so both the page's own polling and a
// delete action produce byte-identical #registry-content.
func (r *Router) renderRegistry(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	namespace := request.PathValue("ns")

	var list v1alpha2.KontinuumList

	err = kontinuums.List(request.Context(), &list, client.InNamespace(namespace))
	if err != nil {
		// Forbidden means the signed-in identity is authenticated but not
		// authorized — the session itself isn't the problem, but there's
		// no reason to keep it either, so send the caller back to sign in
		// rather than leave them stuck on a page they can't use.
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		http.Error(writer, "failed to list kontinuum instances: "+err.Error(), http.StatusBadGateway)

		return
	}

	instances := make([]instance, 0, len(list.Items))
	for _, item := range list.Items {
		instances = append(instances, instance{
			Name:          item.Name,
			Namespace:     item.Namespace,
			Role:          item.Status.Role,
			Region:        item.Spec.Region,
			Zone:          item.Spec.Zone,
			LastHeartbeat: formatAge(item.Status.LastHeartbeatTime.Time),
			Age:           formatAge(item.CreationTimestamp.Time),
			APIVersion:    v1alpha2.GroupVersion().String(),
			Version:       item.Status.Version,
		})
	}

	sort.Slice(instances, func(i, j int) bool { return instances[i].Name < instances[j].Name })

	zones, err := r.listZoneRows(writer, request)
	if err != nil {
		return
	}

	data := map[string]any{
		dataKeyTitle:       "Registry",
		dataKeyActiveMenu:  pageRegistry,
		dataKeyVersion:     r.version,
		dataKeyNamespace:   namespace,
		dataKeyInstances:   instances,
		"Zones":            zones,
		dataKeyAuthEnabled: r.authEnabled,
	}

	// The "Add zone" modal's own fragment data — always empty on a plain
	// page load, since only a submission (see handleZoneAdd) ever carries
	// a preserved/error/success state. Merged in here so the same
	// "zone-add-modal-body" template works whether it's embedded on
	// initial page load or swapped in on submit.
	maps.Copy(data, r.zoneAddFormData(zoneAddFields{}, "", "", r.listInstanceSuggestions(request.Context())))

	r.render(writer, request, pageRegistry, data)
}

// instanceSuggestion is one candidate the "Add zone" modal's own
// instance-picker offers — an already-registered, unclaimed Instance in
// v1alpha2.KontinuumSystemNamespace (see zone.AddOptions.ExistingInstanceName's
// own doc) the user can reuse instead of typing a fresh address.
type instanceSuggestion struct {
	Name    string
	Address string
}

// listInstanceSuggestions lists every unclaimed Instance with a usable
// address in v1alpha2.KontinuumSystemNamespace — the same namespace
// zone.Add always operates in (see BuildAddObjects' own doc) — for the "Add
// zone" modal's own instance-picker. Best-effort: a list failure (e.g. the
// signed-in identity can't list Instance) just means no suggestions are
// offered, not a page-render failure, since typing a fresh address always
// still works.
func (r *Router) listInstanceSuggestions(ctx context.Context) []instanceSuggestion {
	zones, err := r.zonesFor(ctx)
	if err != nil {
		return nil
	}

	var list v1alpha2.InstanceList

	err = zones.List(ctx, &list, client.InNamespace(v1alpha2.KontinuumSystemNamespace))
	if err != nil {
		return nil
	}

	suggestions := make([]instanceSuggestion, 0, len(list.Items))

	for _, item := range list.Items {
		if _, claimed := item.Labels[v1alpha2.LabelClaimedBy]; claimed {
			continue
		}

		if len(item.Spec.Interfaces) == 0 {
			continue
		}

		suggestions = append(suggestions, instanceSuggestion{Name: item.Name, Address: item.Spec.Interfaces[0]})
	}

	sort.Slice(suggestions, func(i, j int) bool { return suggestions[i].Name < suggestions[j].Name })

	return suggestions
}

// listZoneRows lists Zone objects and maps them to zoneRow, sorted by name
// — the registry page's own zones table. On error it writes the
// appropriate HTTP response itself (same as renderRegistry's own
// kontinuums.List handling) and returns a non-nil error so the caller
// knows not to render the page.
func (r *Router) listZoneRows(writer http.ResponseWriter, request *http.Request) ([]zoneRow, error) {
	zones, err := r.zonesFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return nil, fmt.Errorf("failed to build kubernetes client: %w", err)
	}

	var list v1alpha2.ZoneList

	err = zones.List(request.Context(), &list, client.InNamespace(request.PathValue("ns")))
	if err != nil {
		// Forbidden means the signed-in identity is authenticated but not
		// authorized — the session itself isn't the problem, but there's
		// no reason to keep it either, so send the caller back to sign in
		// rather than leave them stuck on a page they can't use.
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return nil, fmt.Errorf("forbidden: %w", err)
		}

		http.Error(writer, "failed to list zones: "+err.Error(), http.StatusBadGateway)

		return nil, fmt.Errorf("failed to list zones: %w", err)
	}

	rows := make([]zoneRow, 0, len(list.Items))

	for _, item := range list.Items {
		row := zoneRow{
			Name: item.Name, Region: item.Spec.Region, Age: formatAge(item.CreationTimestamp.Time),
			Deleting: !item.DeletionTimestamp.IsZero(),
		}

		if cond := latestCondition(item.Status.Conditions); cond != nil {
			row.Condition = cond.Type
			row.ConditionOK = cond.Status == metav1.ConditionTrue
			row.Message = capitalizeFirst(cond.Message)
		}

		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

	return rows, nil
}

// sortConditionsNewestFirst returns a copy of conditions ordered by
// LastTransitionTime, newest first — shared by every detail page's own
// conditions table (see conditions-table's own doc) so the most recently
// changed condition is always the top row, rather than whatever order
// status.conditions happens to store them in (Kubernetes gives no ordering
// guarantee there). A copy, not an in-place sort: conditions here is always
// item.Status.Conditions straight off a live object, and this package has
// no business reordering that object's own status.
func sortConditionsNewestFirst(conditions []metav1.Condition) []metav1.Condition {
	sorted := append([]metav1.Condition(nil), conditions...)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].LastTransitionTime.After(sorted[j].LastTransitionTime.Time)
	})

	return sorted
}

// latestCondition returns whichever of conditions most recently
// transitioned, or nil if conditions is empty — see zoneRow's own doc for
// why "most recent" is the summary this package shows.
func latestCondition(conditions []metav1.Condition) *metav1.Condition {
	var latest *metav1.Condition

	for index := range conditions {
		cond := &conditions[index]
		if latest == nil || cond.LastTransitionTime.After(latest.LastTransitionTime.Time) {
			latest = cond
		}
	}

	return latest
}

// fetchZone fetches the Zone named name — Zone is always cluster-scoped
// into v1alpha2.KontinuumSystemNamespace regardless of which tenant the
// caller is currently browsing (see handleDeleteZone's own doc for the
// same convention). ok is false only when fetchZone has already written the
// response itself: NotFound, a forbidden-redirect, or a bad-gateway error —
// mirrors fetchTalosCluster's identical contract.
func (r *Router) fetchZone(
	writer http.ResponseWriter, request *http.Request, zones client.Client, name string,
) (v1alpha2.Zone, bool) {
	var zoneObj v1alpha2.Zone

	key := client.ObjectKey{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace}

	err := zones.Get(request.Context(), key, &zoneObj)
	if err != nil {
		if apierrors.IsNotFound(err) {
			http.NotFound(writer, request)

			return v1alpha2.Zone{}, false
		}

		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return v1alpha2.Zone{}, false
		}

		http.Error(writer, "failed to get zone: "+err.Error(), http.StatusBadGateway)

		return v1alpha2.Zone{}, false
	}

	return zoneObj, true
}

// fetchZoneCluster looks up the TalosCluster sharing zoneObj's own name —
// see zone-add's shared <region>-<zone> naming convention (BuildAddObjects)
// — for the zone detail page's own "Cluster" section. found is false, ok is
// true when there's simply no TalosCluster yet (a Zone can exist for a
// moment before zone-add's own fan-out finishes creating the rest): that's
// "not provisioned yet," not a page-load failure, mirroring fetchPoolRow's
// identical convention. ok is false only when fetchZoneCluster has already
// written the response itself.
func (r *Router) fetchZoneCluster(
	writer http.ResponseWriter, request *http.Request, zones client.Client, zoneObj v1alpha2.Zone,
) (v1alpha2.TalosCluster, bool, bool) {
	var cluster v1alpha2.TalosCluster

	key := client.ObjectKey{Name: zoneObj.Name, Namespace: zoneObj.Namespace}

	err := zones.Get(request.Context(), key, &cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return v1alpha2.TalosCluster{}, false, true
		}

		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return v1alpha2.TalosCluster{}, false, false
		}

		http.Error(writer, "failed to get taloscluster: "+err.Error(), http.StatusBadGateway)

		return v1alpha2.TalosCluster{}, false, false
	}

	return cluster, true, true
}

// handleZoneDetail is GET /app/kontinuum.sh/v1alpha2/namespaces/{ns}/zones/{name}'s
// handler — it shows one Zone's own identity (region/zone/domain), its
// full status.conditions list (ClusterReady, Installed, RegistryJoined,
// Ready — see pkg/domain/zone's own Reconciler), a link to its owning
// TalosCluster if one exists yet, and whether/which Kontinuum has actually
// joined the hub's registry for it (see zonedomain.FindJoinedKontinuum) —
// the one signal issue #95 asked for a page to surface directly, rather
// than an operator having to cross-reference the registry's own Kontinuum
// Nodes table by region/zone themselves.
func (r *Router) handleZoneDetail(writer http.ResponseWriter, request *http.Request) {
	zones, err := r.zonesFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	zoneObj, found := r.fetchZone(writer, request, zones, request.PathValue("name"))
	if !found {
		return
	}

	cluster, hasCluster, ok := r.fetchZoneCluster(writer, request, zones, zoneObj)
	if !ok {
		return
	}

	kontinuum, joined, err := zonedomain.FindJoinedKontinuum(
		request.Context(), zones, zoneObj.Spec.Region, zoneObj.Spec.Zone)
	if err != nil {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		http.Error(writer, "failed to check zone registry membership: "+err.Error(), http.StatusBadGateway)

		return
	}

	thumbprint, issuedAt, hasIdentity := fetchZoneIdentity(request.Context(), zones, zoneObj.Name)

	data := zoneDetailData(zoneObj, cluster, hasCluster, kontinuum, joined,
		request.PathValue("ns"), r.version, r.authEnabled, thumbprint, issuedAt, hasIdentity)

	r.render(writer, request, pageZoneDetail, data)
}

// fetchZoneIdentity reads zoneName's own hub-side etcd gRPC proxy identity
// Secret (see etcdproxy.BuildPublicSecret) and returns its currently
// active (Current) certificate's own thumbprint and issuance time — ok is
// false for anything that keeps this from resolving (Secret not found
// yet, no permission to read Secrets, malformed content, ...), in which
// case the caller simply omits its own "identity"/"thumbprint" display
// rather than erroring the whole page: unlike RegistryJoined/HasCluster,
// this is a purely informational display, not something the rest of a
// page depends on. Shared by both handleZoneDetail (the zone's own
// detail page) and handleKontinuumDetail (a Worker Kontinuum's gRPC
// card, keyed by that Kontinuum's inferred zone name — see
// zoneNameForKontinuum), so it only requires Get, which KontinuumClient
// and client.Client both satisfy.
func fetchZoneIdentity(ctx context.Context, zones KontinuumClient, zoneName string) (string, time.Time, bool) {
	var secret corev1.Secret

	key := client.ObjectKey{Name: etcdproxy.AuthSecretName(zoneName), Namespace: v1alpha2.KontinuumSystemNamespace}

	err := zones.Get(ctx, key, &secret)
	if err != nil {
		return "", time.Time{}, false
	}

	pair, ok := etcdproxy.ParsePublicSecret(&secret)
	if !ok {
		return "", time.Time{}, false
	}

	thumbprint, err := etcdproxy.Thumbprint(pair.Current.CertPEM)
	if err != nil {
		return "", time.Time{}, false
	}

	return thumbprint, pair.Current.IssuedAt, true
}

// zoneDetailData builds handleZoneDetail's template data from zoneObj and
// its already-fetched cluster/registry-membership (see
// fetchZoneCluster/zonedomain.FindJoinedKontinuum) — factored out purely to
// keep handleZoneDetail short, mirroring kontinuumDetailData/
// talosClusterDetailData's identical role for their own detail pages.
// namespace is the currently browsed tenant (the route's own {ns} path
// value) — used only for the back-link/delete-button URLs and the nav's
// tenant switcher, never to locate zoneObj itself (see fetchZone's own
// doc).
func zoneDetailData(
	zoneObj v1alpha2.Zone, cluster v1alpha2.TalosCluster, hasCluster bool,
	kontinuum *v1alpha2.Kontinuum, joined bool, namespace, version string, authEnabled bool,
	identityThumbprint string, identityIssuedAt time.Time, hasIdentity bool,
) map[string]any {
	sortedConditions := sortConditionsNewestFirst(zoneObj.Status.Conditions)
	conditions := make([]conditionRow, 0, len(sortedConditions))

	for _, cond := range sortedConditions {
		conditions = append(conditions, conditionRow{
			Type: cond.Type, Status: string(cond.Status), OK: cond.Status == metav1.ConditionTrue,
			Reason: cond.Reason, Message: capitalizeFirst(cond.Message), Age: formatAge(cond.LastTransitionTime.Time),
		})
	}

	data := map[string]any{
		dataKeyTitle:       zoneObj.Name,
		dataKeyActiveMenu:  pageRegistry,
		dataKeyVersion:     version,
		dataKeyAuthEnabled: authEnabled,
		dataKeyName:        zoneObj.Name,
		dataKeyNamespace:   namespace,
		dataKeyRegion:      zoneObj.Spec.Region,
		"ZoneName":         zoneObj.Spec.Zone,
		"Domain":           zoneObj.Spec.Domain,
		dataKeyAge:         formatAge(zoneObj.CreationTimestamp.Time),
		dataKeyDeleting:    !zoneObj.DeletionTimestamp.IsZero(),
		dataKeyConditions:  conditions,
		"HasCluster":       hasCluster,
		"ClusterName":      cluster.Name,
		"ClusterNamespace": cluster.Namespace,
		"RegistryJoined":   joined,
	}

	if cond := conditionOfType(zoneObj.Status.Conditions, "Ready"); cond != nil {
		data["Ready"] = string(cond.Status)
		data["ReadyOK"] = cond.Status == metav1.ConditionTrue
	}

	if hasCluster {
		if cond := conditionOfType(cluster.Status.Conditions, "Ready"); cond != nil {
			data["ClusterReady"] = string(cond.Status)
			data["ClusterReadyOK"] = cond.Status == metav1.ConditionTrue
		}
	}

	data["HasIdentity"] = hasIdentity
	if hasIdentity {
		data["IdentityThumbprint"] = identityThumbprint
		data["IdentityIssuedAge"] = formatAge(identityIssuedAt)
	}

	if joined && kontinuum != nil {
		data["KontinuumName"] = kontinuum.Name
		data["KontinuumNamespace"] = kontinuum.Namespace
		data["KontinuumRole"] = kontinuum.Status.Role
		data["KontinuumLastHeartbeat"] = formatAge(kontinuum.Status.LastHeartbeatTime.Time)
	}

	return data
}

// maxZoneAddFormBytes bounds the "Add zone" form's request body — every
// field is a short identifier/address, so this is generous, not tight.
const maxZoneAddFormBytes = 1 << 16

// zoneAddFormData is the "zone-add-modal-body" fragment's template data —
// fields is the (possibly just-submitted) form state, createdZone/formErr
// surface a just-completed submission's outcome, and suggestions is the
// instance-picker's own candidate list (see listInstanceSuggestions).
// Rendered three times: once embedded in the registry page's own initial
// render (always empty — see renderRegistry), and again by handleZoneAdd on
// every submission.
func (r *Router) zoneAddFormData(
	fields zoneAddFields, createdZone, formErr string, suggestions []instanceSuggestion,
) map[string]any {
	return map[string]any{
		dataKeyRegion:         fields.region,
		"Zone":                fields.zone,
		"TalosAddress":        fields.talosAddress,
		dataKeyTalosVersion:   fields.talosVersion,
		"KubernetesVersion":   fields.kubernetesVersion,
		"UnregisterInstances": fields.unregisterInstances,
		"ExistingInstance":    fields.existingInstance,
		"InstanceSuggestions": suggestions,
		"CreatedZone":         createdZone,
		"Error":               formErr,
	}
}

// zoneAddFields is "Add zone"'s parsed form fields.
type zoneAddFields struct {
	region              string
	zone                string
	talosAddress        string
	talosVersion        string
	kubernetesVersion   string
	unregisterInstances bool
	// existingInstance is the instance-picker's own selection (see
	// zone.AddOptions.ExistingInstanceName's own doc) — set by the "Add
	// zone" modal's own combobox script when a suggestion is chosen, empty
	// when talosAddress was typed freehand instead.
	existingInstance string
}

// renderZoneAddModalBody renders just the "zone-add-modal-body" fragment
// (not a full page via layout.html) — the registry page's own dialog swaps
// #zone-add-modal-body's innerHTML with this on every form submission, so
// the modal never navigates away from the registry page.
func (r *Router) renderZoneAddModalBody(writer http.ResponseWriter, data map[string]any) {
	var buf bytes.Buffer

	err := r.pages[pageRegistry].ExecuteTemplate(&buf, "zone-add-modal-body", data)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)

		return
	}

	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(writer)
}

// handleZoneAdd is POST /app/zones/add's handler — it creates the
// submitted zone's four hub-side objects via the same shared
// pkg/domain/zone.Add fan-out `kontinuum zone add` calls, so the CLI and
// UI never construct these objects differently. On success it swaps the
// modal body to a success message; on failure it re-renders the form with
// the submitted values preserved and an error message, so the user doesn't
// have to retype everything to fix one field. Either way the response stays
// a fragment — the registry page underneath is never navigated away from.
func (r *Router) handleZoneAdd(writer http.ResponseWriter, request *http.Request) {
	// Every field here is a short identifier/address, never a bulk upload —
	// bound the body before ParseForm reads it into memory.
	request.Body = http.MaxBytesReader(writer, request.Body, maxZoneAddFormBytes)

	err := request.ParseForm()
	if err != nil {
		http.Error(writer, "failed to parse form: "+err.Error(), http.StatusBadRequest)

		return
	}

	fields := zoneAddFields{
		region:            request.PostFormValue("region"),
		zone:              request.PostFormValue("zone"),
		talosAddress:      request.PostFormValue("talos-address"),
		talosVersion:      request.PostFormValue("talos-version"),
		kubernetesVersion: request.PostFormValue("kubernetes-version"),
		// An unchecked checkbox omits its own key from the submitted form
		// entirely (rather than submitting "false") — PostFormValue returns
		// "" either way, same as any other never-submitted field.
		unregisterInstances: request.PostFormValue("unregister-instances") == "on",
		existingInstance:    request.PostFormValue("existing-instance"),
	}

	zones, err := r.zonesFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	createdZone, err := zonedomain.Add(request.Context(), zones, zonedomain.AddOptions{
		Region:                      fields.region,
		Zone:                        fields.zone,
		TalosAddress:                fields.talosAddress,
		TalosVersion:                fields.talosVersion,
		KubernetesVersion:           fields.kubernetesVersion,
		UnregisterInstancesOnDelete: fields.unregisterInstances,
		ExistingInstanceName:        fields.existingInstance,
	})
	if err != nil {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		r.renderZoneAddModalBody(writer,
			r.zoneAddFormData(fields, "", err.Error(), r.listInstanceSuggestions(request.Context())))

		return
	}

	r.renderZoneAddModalBody(writer, r.zoneAddFormData(zoneAddFields{}, createdZone.Name, "", nil))
}

// handleKontinuumDetail is GET /app/kontinuums/{name}'s handler — it shows one
// Kontinuum instance's own settings (status.config, status.secretRef),
// sourced from the shared Kontinuum object store rather than this
// process's own local config, so it renders the same regardless of which
// instance's UI you happen to be browsing from.
func (r *Router) handleKontinuumDetail(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	var item v1alpha2.Kontinuum

	key := client.ObjectKey{Name: request.PathValue("name"), Namespace: request.PathValue("ns")}

	err = kontinuums.Get(request.Context(), key, &item)
	if err != nil {
		if apierrors.IsNotFound(err) {
			http.NotFound(writer, request)

			return
		}

		// Forbidden means the signed-in identity is authenticated but not
		// authorized — the session itself isn't the problem, but there's
		// no reason to keep it either, so send the caller back to sign in
		// rather than leave them stuck on a page they can't use.
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		http.Error(writer, "failed to get kontinuum instance: "+err.Error(), http.StatusBadGateway)

		return
	}

	secretDataYAML, ok := r.fetchSecretDataYAML(writer, request, kontinuums, item.Status.SecretRef)
	if !ok {
		return
	}

	thumbprint, issuedAt, hasIdentity := "", time.Time{}, false
	if zoneName, ok := zoneNameForKontinuum(item); ok {
		thumbprint, issuedAt, hasIdentity = fetchZoneIdentity(request.Context(), kontinuums, zoneName)
	}

	r.render(writer, request, pageKontinuum,
		kontinuumDetailData(item, secretDataYAML != "", r.version, r.authEnabled, thumbprint, issuedAt, hasIdentity))
}

// zoneNameForKontinuum reports the hub-side Zone object name a Worker
// Kontinuum corresponds to, so handleKontinuumDetail can look up that
// zone's own etcd gRPC proxy identity the same way handleZoneDetail does
// (see fetchZoneIdentity) — deterministic from item's own Region/Zone
// (see addObjectName's identical "<region>-<zone>" convention), no list/
// scan needed the way the reverse lookup (zonedomain.FindJoinedKontinuum)
// requires. ok is false for the ControlPlane (hub) Kontinuum, whose
// Region/Zone are both empty and which has no corresponding Zone at all.
func zoneNameForKontinuum(item v1alpha2.Kontinuum) (string, bool) {
	if item.Spec.Region == "" || item.Spec.Zone == "" {
		return "", false
	}

	return item.Spec.Region + "-" + item.Spec.Zone, true
}

// handleKontinuumSecretDownload is GET /app/kontinuums/{name}/secret's
// handler — it serves the Kontinuum's own config Secret contents (see
// fetchSecretDataYAML) as plain text, fetched on demand by
// kontinuum_content.html's "Reveal" button rather than embedded in
// handleKontinuumDetail's own page response, which only ever learns whether
// a reveal panel should render at all (see kontinuumDetailData's
// SecretDataReady). Mirrors handleTalosClusterKubeconfigDownload's same
// on-demand-reveal pattern for a different secret.
func (r *Router) handleKontinuumSecretDownload(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	var item v1alpha2.Kontinuum

	key := client.ObjectKey{Name: request.PathValue("name"), Namespace: request.PathValue("ns")}

	err = kontinuums.Get(request.Context(), key, &item)
	if err != nil {
		if apierrors.IsNotFound(err) {
			http.NotFound(writer, request)

			return
		}

		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		http.Error(writer, "failed to get kontinuum instance: "+err.Error(), http.StatusBadGateway)

		return
	}

	secretDataYAML, ok := r.fetchSecretDataYAML(writer, request, kontinuums, item.Status.SecretRef)
	if !ok {
		return
	}

	if secretDataYAML == "" {
		http.NotFound(writer, request)

		return
	}

	writer.Header().Set("Content-Type", "application/yaml")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte(secretDataYAML))
}

// kontinuumDetailData builds handleKontinuumDetail's template data from
// item — factored out of handleKontinuumDetail purely to keep that function
// short; it has no logic of its own beyond field selection.
// secretDataReady reports whether item's config Secret has data to reveal
// (see fetchSecretDataYAML), without carrying the data itself: that's
// fetched separately, on demand, by handleKontinuumSecretDownload.
// identityThumbprint/identityIssuedAt/hasIdentity are item's own zone
// identity, if any (see zoneNameForKontinuum/fetchZoneIdentity) — hasIdentity
// is false for the ControlPlane (hub) Kontinuum, which has no zone identity
// of its own, so the gRPC card's thumbprint row is omitted for it.
func kontinuumDetailData(
	item v1alpha2.Kontinuum, secretDataReady bool, version string, authEnabled bool,
	identityThumbprint string, identityIssuedAt time.Time, hasIdentity bool,
) map[string]any {
	cfg := item.Status.Config

	data := map[string]any{
		dataKeyTitle:                item.Name,
		dataKeyActiveMenu:           pageRegistry,
		dataKeyVersion:              version,
		dataKeyAuthEnabled:          authEnabled,
		dataKeyName:                 item.Name,
		dataKeyNamespace:            item.Namespace,
		"Role":                      item.Status.Role,
		dataKeyRegion:               item.Spec.Region,
		"Zone":                      item.Spec.Zone,
		"LastHeartbeat":             formatAge(item.Status.LastHeartbeatTime.Time),
		dataKeyAge:                  formatAge(item.CreationTimestamp.Time),
		"APIVersion":                v1alpha2.GroupVersion().String(),
		"InstanceVersion":           item.Status.Version,
		"Addr":                      cfg.Server.Addr,
		"StorageBackend":            storageBackendName(cfg.Server.Storage),
		"StorageTarget":             cfg.Server.Storage,
		"SecretName":                item.Status.SecretRef.Name,
		"SecretNamespace":           item.Status.SecretRef.Namespace,
		"SecretDataReady":           secretDataReady,
		"LogLevel":                  cfg.Log.Level,
		"LogFormat":                 cfg.Log.Format,
		"DNSDomain":                 cfg.Server.DNS.Domain,
		"GRPCEndpoint":              cfg.Server.GRPC.Endpoint,
		"GRPCInsecureTLSSkipVerify": cfg.Server.GRPC.InsecureTLSSkipVerify,
		"OIDCEnabled":               cfg.OIDC.Enabled,
		"OIDCIssuerURL":             cfg.OIDC.IssuerURL,
		"OIDCClientID":              cfg.OIDC.ClientID,
		"OIDCRedirectURL":           cfg.OIDC.RedirectURL,
		"OIDCAdminGroups":           cfg.OIDC.AdminGroups,
	}

	data["HasIdentity"] = hasIdentity
	if hasIdentity {
		data["GRPCThumbprint"] = identityThumbprint
		data["GRPCIdentityIssuedAge"] = formatAge(identityIssuedAt)
	}

	return data
}

// fetchSecretDataYAML fetches the Secret backing ref (the Kontinuum detail
// page's status.secretRef) through kontinuums — the same identity-scoped
// client handleKontinuumDetail/handleKontinuumSecretDownload already used to
// fetch the Kontinuum object itself, so a viewer sees the config secret's
// contents exactly when RBAC would let them `kubectl get secret` it
// directly, with no separate authorization path to keep in sync. Returns
// ("", true) when there is no secret to show or it can no longer be found,
// since either just means the caller renders without a reveal panel rather
// than that the request failed. The bool result is false when the caller
// should stop: fetchSecretDataYAML has already written the error (or
// forbidden-redirect) response itself.
func (r *Router) fetchSecretDataYAML(
	writer http.ResponseWriter, request *http.Request,
	kontinuums KontinuumClient, ref v1alpha2.KontinuumSecretReference,
) (string, bool) {
	if ref.Name == "" {
		return "", true
	}

	var secret corev1.Secret

	key := client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}

	err := kontinuums.Get(request.Context(), key, &secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", true
		}

		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return "", false
		}

		http.Error(writer, "failed to get kontinuum config secret: "+err.Error(), http.StatusBadGateway)

		return "", false
	}

	secretDataYAML, err := secretDataToYAML(secret.Data)
	if err != nil {
		http.Error(writer, "failed to render kontinuum config secret: "+err.Error(), http.StatusInternalServerError)

		return "", false
	}

	return secretDataYAML, true
}

// secretDataToYAML renders a Secret's decoded Data map as sorted YAML
// key/value pairs. client-go already base64-decodes Data's values on the way
// in, so this only needs to retype []byte to string before marshaling —
// sigs.k8s.io/yaml marshals through encoding/json, which sorts map keys, so
// the output is deterministic without an explicit sort here.
func secretDataToYAML(data map[string][]byte) (string, error) {
	if len(data) == 0 {
		return "", nil
	}

	strData := make(map[string]string, len(data))
	for key, value := range data {
		strData[key] = string(value)
	}

	out, err := yaml.Marshal(strData)
	if err != nil {
		return "", fmt.Errorf("failed to marshal secret data as yaml: %w", err)
	}

	return string(out), nil
}

// roleRuleRow is one rbacv1.PolicyRule rendered as a rule's badges on a
// Role/ClusterRole row — see roleRow.
type roleRuleRow struct {
	APIGroups []string
	Resources []string
	Verbs     []string
}

// roleRuleRowsFrom maps rules to their display form, preserving order.
func roleRuleRowsFrom(rules []rbacv1.PolicyRule) []roleRuleRow {
	rows := make([]roleRuleRow, 0, len(rules))
	for _, rule := range rules {
		rows = append(rows, roleRuleRow{APIGroups: rule.APIGroups, Resources: rule.Resources, Verbs: rule.Verbs})
	}

	return rows
}

// handleConnect is GET /app/connect's handler — it renders the kubectl
// access card that used to live on the registry page (see issue #89: it's
// not registry-specific data, and this page is where API keys/federated
// identity credentials will land too). AuthEnabled drives the same
// OIDC-vs-no-auth branching handleRegistryKubeconfigDownload's own doc
// describes, so the served kubeconfig always matches what this page says
// about it.
func (r *Router) handleConnect(writer http.ResponseWriter, request *http.Request) {
	r.render(writer, request, pageConnect, map[string]any{
		dataKeyTitle:       "Connect",
		dataKeyActiveMenu:  "connect",
		dataKeyVersion:     r.version,
		dataKeyAuthEnabled: r.authEnabled,
	})
}

// roleRow is a Role or ClusterRole rendered as a row on the IAM "Roles"
// tabs — see handleIAMNamespaceRoles/handleIAMClusterRoles. Namespace is
// empty for a ClusterRole.
type roleRow struct {
	Name      string
	Namespace string
	Rules     []roleRuleRow
	Age       string
	// Managed is true for the "kontinuum-admin" ClusterRole
	// pkg/domain/adminrbac's own reconcile loop owns and recreates within
	// its own interval (default 30s) if deleted — see listClusterRoles'
	// own doc for why deleting it here is hidden rather than just
	// pointless. Always false for a namespaced Role, which this
	// controller never manages.
	Managed bool
}

// roleNames extracts every roles' Name, in the same order — used to
// populate the "Add role binding" modal's role-ref dropdown.
func roleNames(roles []roleRow) []string {
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}

	return names
}

// roleBindingRow is a RoleBinding or ClusterRoleBinding rendered as a row
// on the IAM "Role bindings" tabs — see
// handleIAMNamespaceRoleBindings/handleIAMClusterRoleBindings. Namespace is
// empty for a ClusterRoleBinding. Managed is true for ClusterRoleBindings
// kontinuum's admin-group controller manages (see pkg/domain/adminrbac) —
// rendered as a distinct read-only subsection on the cluster bindings page
// rather than mixed in with everything else.
type roleBindingRow struct {
	Name        string
	Namespace   string
	Subjects    []string
	RoleRefKind string
	RoleRefName string
	Age         string
	Managed     bool
}

// subjectStrings renders subjects as "Kind: Name" display strings.
func subjectStrings(subjects []rbacv1.Subject) []string {
	out := make([]string, 0, len(subjects))
	for _, subject := range subjects {
		out = append(out, subject.Kind+": "+subject.Name)
	}

	return out
}

// listNamespaceRoles lists the Roles in namespace and maps them to rows,
// sorted by name. On error, it writes the appropriate HTTP response itself
// (same as renderRegistry) and returns a non-nil error so the caller knows
// not to render the page.
func (r *Router) listNamespaceRoles(
	writer http.ResponseWriter, request *http.Request, kontinuums KontinuumClient, namespace string,
) ([]roleRow, error) {
	var list rbacv1.RoleList

	err := kontinuums.List(request.Context(), &list, client.InNamespace(namespace))
	if err != nil {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return nil, fmt.Errorf("forbidden: %w", err)
		}

		http.Error(writer, "failed to list roles: "+err.Error(), http.StatusBadGateway)

		return nil, fmt.Errorf("failed to list roles: %w", err)
	}

	roles := make([]roleRow, 0, len(list.Items))
	for _, item := range list.Items {
		roles = append(roles, roleRow{
			Name: item.Name, Namespace: item.Namespace,
			Rules: roleRuleRowsFrom(item.Rules), Age: formatAge(item.CreationTimestamp.Time),
		})
	}

	sort.Slice(roles, func(i, j int) bool { return roles[i].Name < roles[j].Name })

	return roles, nil
}

// listNamespaceRoleBindings lists the RoleBindings in namespace and maps
// them to rows, sorted by name — see listNamespaceRoles' own doc for the
// error-handling contract.
func (r *Router) listNamespaceRoleBindings(
	writer http.ResponseWriter, request *http.Request, kontinuums KontinuumClient, namespace string,
) ([]roleBindingRow, error) {
	var list rbacv1.RoleBindingList

	err := kontinuums.List(request.Context(), &list, client.InNamespace(namespace))
	if err != nil {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return nil, fmt.Errorf("forbidden: %w", err)
		}

		http.Error(writer, "failed to list role bindings: "+err.Error(), http.StatusBadGateway)

		return nil, fmt.Errorf("failed to list role bindings: %w", err)
	}

	bindings := make([]roleBindingRow, 0, len(list.Items))
	for _, item := range list.Items {
		bindings = append(bindings, roleBindingRow{
			Name: item.Name, Namespace: item.Namespace, Subjects: subjectStrings(item.Subjects),
			RoleRefKind: item.RoleRef.Kind, RoleRefName: item.RoleRef.Name,
			Age: formatAge(item.CreationTimestamp.Time),
		})
	}

	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Name < bindings[j].Name })

	return bindings, nil
}

// listClusterRoles lists every ClusterRole and maps them to rows, sorted by
// name — see listNamespaceRoles' own doc for the error-handling contract.
// Unfiltered: kontinuum doesn't bootstrap upstream's default ClusterRoles
// (see pkg/domain/adminrbac's own doc), so this list stays short even
// without narrowing it to admin-group-managed ones the way the old IAM page
// did. Managed marks the "kontinuum-admin" ClusterRole that controller
// owns (see roleRow.Managed's own doc) — the same ManagedBy label
// listClusterRoleBindings already keys off for its own Managed field.
func (r *Router) listClusterRoles(
	writer http.ResponseWriter, request *http.Request, kontinuums KontinuumClient,
) ([]roleRow, error) {
	var list rbacv1.ClusterRoleList

	err := kontinuums.List(request.Context(), &list)
	if err != nil {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return nil, fmt.Errorf("forbidden: %w", err)
		}

		http.Error(writer, "failed to list cluster roles: "+err.Error(), http.StatusBadGateway)

		return nil, fmt.Errorf("failed to list cluster roles: %w", err)
	}

	roles := make([]roleRow, 0, len(list.Items))
	for _, item := range list.Items {
		roles = append(roles, roleRow{
			Name: item.Name, Rules: roleRuleRowsFrom(item.Rules), Age: formatAge(item.CreationTimestamp.Time),
			Managed: item.Labels[v1alpha2.LabelManagedBy] == adminrbac.ManagedByValue,
		})
	}

	sort.Slice(roles, func(i, j int) bool { return roles[i].Name < roles[j].Name })

	return roles, nil
}

// listClusterRoleBindings lists every ClusterRoleBinding and maps them to
// rows, sorted by name — see listNamespaceRoles' own doc for the
// error-handling contract. Unlike the old handleIAM/listAdminGroupBindings
// this replaces, the list is unfiltered — every ClusterRoleBinding is
// shown, with Managed marking the ones kontinuum's admin-group controller
// owns (see roleBindingRow's own doc) rather than hiding everything else.
func (r *Router) listClusterRoleBindings(
	writer http.ResponseWriter, request *http.Request, kontinuums KontinuumClient,
) ([]roleBindingRow, error) {
	var list rbacv1.ClusterRoleBindingList

	err := kontinuums.List(request.Context(), &list)
	if err != nil {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return nil, fmt.Errorf("forbidden: %w", err)
		}

		http.Error(writer, "failed to list cluster role bindings: "+err.Error(), http.StatusBadGateway)

		return nil, fmt.Errorf("failed to list cluster role bindings: %w", err)
	}

	bindings := make([]roleBindingRow, 0, len(list.Items))
	for _, item := range list.Items {
		bindings = append(bindings, roleBindingRow{
			Name: item.Name, Subjects: subjectStrings(item.Subjects),
			RoleRefKind: item.RoleRef.Kind, RoleRefName: item.RoleRef.Name,
			Age:     formatAge(item.CreationTimestamp.Time),
			Managed: item.Labels[v1alpha2.LabelManagedBy] == adminrbac.ManagedByValue,
		})
	}

	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Name < bindings[j].Name })

	return bindings, nil
}

// handleIAMNamespaceRoles is GET
// /app/rbac.authorization.k8s.io/v1/namespaces/{ns}/roles's handler.
func (r *Router) handleIAMNamespaceRoles(writer http.ResponseWriter, request *http.Request) {
	r.renderIAMNamespace(writer, request, "roles")
}

// handleIAMNamespaceRoleBindings is GET
// /app/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings's handler.
func (r *Router) handleIAMNamespaceRoleBindings(writer http.ResponseWriter, request *http.Request) {
	r.renderIAMNamespace(writer, request, "bindings")
}

// renderIAMNamespace renders pageIAMNamespace for view ("roles" or
// "bindings"), listing the namespaced Role/RoleBinding objects for the
// current tenant. Like the old handleIAM, it skips every Kubernetes call
// when OIDC isn't configured — see iam_namespace_content.html's own notice
// for why: every request already has full access, so there's nothing
// meaningful to show or manage.
func (r *Router) renderIAMNamespace(writer http.ResponseWriter, request *http.Request, view string) {
	namespace := request.PathValue("ns")

	data := map[string]any{
		"Title": "Tenant", "ActiveMenu": "iam-namespace", "Version": r.version,
		"AuthEnabled": r.authEnabled, "Namespace": namespace, "View": view,
	}

	roles, ok := r.loadIAMNamespace(writer, request, namespace, view, data)
	if !ok {
		return
	}

	maps.Copy(data, r.roleAddFormData(namespace, roleAddFields{}, "", ""))
	maps.Copy(data, r.roleBindingAddFormData(namespace, roleNames(roles), roleBindingAddFields{}, "", ""))

	r.render(writer, request, pageIAMNamespace, data)
}

// loadIAMNamespace populates data with the namespaced Role list (and, for
// view "bindings", the RoleBinding list too) renderIAMNamespace needs —
// split out purely to keep that function's nesting/complexity down. It
// skips every Kubernetes call and returns (nil, true) when OIDC isn't
// configured — see renderIAMNamespace's own doc for why. ok is false only
// when this has already written the response itself (an error or a
// forbidden-redirect), same contract as listNamespaceRoles.
func (r *Router) loadIAMNamespace(
	writer http.ResponseWriter, request *http.Request, namespace, view string, data map[string]any,
) ([]roleRow, bool) {
	if !r.authEnabled {
		return nil, true
	}

	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return nil, false
	}

	roles, err := r.listNamespaceRoles(writer, request, kontinuums, namespace)
	if err != nil {
		return nil, false
	}

	data["Roles"] = roles

	if view != "bindings" {
		return roles, true
	}

	bindings, err := r.listNamespaceRoleBindings(writer, request, kontinuums, namespace)
	if err != nil {
		return nil, false
	}

	data["RoleBindings"] = bindings

	return roles, true
}

// handleIAMClusterRoles is GET /app/rbac.authorization.k8s.io/v1/clusterroles's handler.
func (r *Router) handleIAMClusterRoles(writer http.ResponseWriter, request *http.Request) {
	r.renderIAMCluster(writer, request, "roles")
}

// handleIAMClusterRoleBindings is GET /app/rbac.authorization.k8s.io/v1/clusterrolebindings's
// handler.
func (r *Router) handleIAMClusterRoleBindings(writer http.ResponseWriter, request *http.Request) {
	r.renderIAMCluster(writer, request, "bindings")
}

// renderIAMCluster renders pageIAMCluster for view ("roles" or "bindings")
// — see renderIAMNamespace's own doc, which this mirrors at cluster scope.
func (r *Router) renderIAMCluster(writer http.ResponseWriter, request *http.Request, view string) {
	data := map[string]any{
		"Title": "Global", "ActiveMenu": "iam-cluster", "Version": r.version,
		"AuthEnabled": r.authEnabled, "View": view,
	}

	roles, ok := r.loadIAMCluster(writer, request, view, data)
	if !ok {
		return
	}

	maps.Copy(data, r.clusterRoleAddFormData(roleAddFields{}, "", ""))
	maps.Copy(data, r.clusterRoleBindingAddFormData(roleNames(roles), roleBindingAddFields{}, "", ""))

	r.render(writer, request, pageIAMCluster, data)
}

// loadIAMCluster is loadIAMNamespace's cluster-scoped counterpart.
func (r *Router) loadIAMCluster(
	writer http.ResponseWriter, request *http.Request, view string, data map[string]any,
) ([]roleRow, bool) {
	if !r.authEnabled {
		return nil, true
	}

	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return nil, false
	}

	roles, err := r.listClusterRoles(writer, request, kontinuums)
	if err != nil {
		return nil, false
	}

	data["Roles"] = roles

	if view != "bindings" {
		return roles, true
	}

	bindings, err := r.listClusterRoleBindings(writer, request, kontinuums)
	if err != nil {
		return nil, false
	}

	data["RoleBindings"] = bindings

	return roles, true
}

// maxRoleAddFormBytes bounds the "Add role"/"Add role binding" forms' own
// body — a handful of short values (plus a modest number of rule rows), not
// a bulk upload.
const maxRoleAddFormBytes = 1 << 16

// ruleFieldPattern matches the rule builder's "rules[N][field]" form field
// names — see role_add_modal.html's own doc for how its "+ Add rule" script
// produces them, and parseRuleFields for how they're read back.
var ruleFieldPattern = regexp.MustCompile(`^rules\[(\d+)]\[(apiGroups|resources|verbs)]$`)

// roleRuleFields is one rule row's raw submitted values, preserved as-is
// (not yet split into a rbacv1.PolicyRule) so a rejected submission can be
// redisplayed exactly as typed — see buildPolicyRules for the conversion
// used once a submission is accepted.
type roleRuleFields struct {
	APIGroups string
	Resources string
	Verbs     []string
}

// parseRuleFields extracts the rule builder's row values from a parsed
// form's PostForm, in ascending index order regardless of what order the
// browser submitted them in.
func parseRuleFields(form map[string][]string) []roleRuleFields {
	rows := make(map[int]*roleRuleFields)

	for key, values := range form {
		matches := ruleFieldPattern.FindStringSubmatch(key)
		if matches == nil {
			continue
		}

		index, err := strconv.Atoi(matches[1])
		if err != nil {
			continue
		}

		row, ok := rows[index]
		if !ok {
			row = &roleRuleFields{}
			rows[index] = row
		}

		applyRuleField(row, matches[2], values)
	}

	indices := make([]int, 0, len(rows))
	for index := range rows {
		indices = append(indices, index)
	}

	sort.Ints(indices)

	out := make([]roleRuleFields, 0, len(indices))
	for _, index := range indices {
		out = append(out, *rows[index])
	}

	return out
}

// applyRuleField stores one "rules[N][field]" form value into row — split
// out of parseRuleFields purely to keep that function's cyclomatic
// complexity down.
func applyRuleField(row *roleRuleFields, field string, values []string) {
	switch field {
	case "apiGroups":
		if len(values) > 0 {
			row.APIGroups = values[0]
		}
	case "resources":
		if len(values) > 0 {
			row.Resources = values[0]
		}
	case "verbs":
		row.Verbs = values
	}
}

// splitAPIGroups splits s (comma-separated) into apiGroups entries — a
// blank field means the core API group, rbacv1.PolicyRule's own convention
// for APIGroups: [""].
func splitAPIGroups(s string) []string {
	if strings.TrimSpace(s) == "" {
		return []string{""}
	}

	return splitCommaList(s)
}

// splitCommaList splits s on commas, trims whitespace from each part, and
// drops empty parts.
func splitCommaList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}

		out = append(out, trimmed)
	}

	return out
}

// buildPolicyRules converts the rule builder's raw rows into
// rbacv1.PolicyRules, ready to attach to a new Role/ClusterRole.
func buildPolicyRules(rules []roleRuleFields) []rbacv1.PolicyRule {
	out := make([]rbacv1.PolicyRule, 0, len(rules))
	for _, rule := range rules {
		out = append(out, rbacv1.PolicyRule{
			APIGroups: splitAPIGroups(rule.APIGroups),
			Resources: splitCommaList(rule.Resources),
			Verbs:     rule.Verbs,
		})
	}

	return out
}

// hasVerb reports whether verbs contains verb — used by the rule builder's
// verb checkboxes to restore their checked state when redisplaying a
// rejected "Add role" submission.
func hasVerb(verbs []string, verb string) bool {
	return slices.Contains(verbs, verb)
}

// roleAddFields is "Add role"/"Add cluster role"'s parsed form fields.
type roleAddFields struct {
	name  string
	rules []roleRuleFields
}

// roleAddFormData is the "role-add-modal-body" fragment's template data for
// the namespaced Role form, following zoneAddFormData's own pattern.
// RoleAddURL points the form's hx-post at the right route for namespace, so
// the same "role-add-modal-body" template works for both the namespaced and
// cluster-scoped forms.
func (r *Router) roleAddFormData(namespace string, fields roleAddFields, createdRole, formErr string) map[string]any {
	return map[string]any{
		"RoleAddURL": "/app/rbac.authorization.k8s.io/v1/namespaces/" + namespace + "/roles", "RoleKind": "role",
		"RoleName": fields.name, "Rules": fields.rules,
		"CreatedRole": createdRole, "RoleError": formErr,
	}
}

// clusterRoleAddFormData is roleAddFormData's cluster-scoped counterpart.
func (r *Router) clusterRoleAddFormData(fields roleAddFields, createdRole, formErr string) map[string]any {
	return map[string]any{
		"RoleAddURL": "/app/rbac.authorization.k8s.io/v1/clusterroles", "RoleKind": "cluster role",
		"RoleName": fields.name, "Rules": fields.rules,
		"CreatedRole": createdRole, "RoleError": formErr,
	}
}

// renderRoleAddModalBody renders just the "role-add-modal-body" fragment
// (not a full page via layout.html) from page's own template tree — see
// renderZoneAddModalBody, the precedent this follows.
func (r *Router) renderRoleAddModalBody(writer http.ResponseWriter, page string, data map[string]any) {
	var buf bytes.Buffer

	err := r.pages[page].ExecuteTemplate(&buf, "role-add-modal-body", data)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)

		return
	}

	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(writer)
}

// handleRoleAdd is POST
// /app/rbac.authorization.k8s.io/v1/namespaces/{ns}/roles's handler — it
// creates the submitted Role in the current tenant namespace. On success it
// swaps the modal body to a success message; on failure it re-renders the
// form with the submitted values preserved and an error message — see
// handleZoneAdd, the precedent this follows.
func (r *Router) handleRoleAdd(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRoleAddFormBytes)

	err := request.ParseForm()
	if err != nil {
		http.Error(writer, "failed to parse form: "+err.Error(), http.StatusBadRequest)

		return
	}

	namespace := request.PathValue("ns")
	fields := roleAddFields{
		name: strings.TrimSpace(request.PostFormValue("name")), rules: parseRuleFields(request.PostForm),
	}

	if fields.name == "" {
		r.renderRoleAddModalBody(writer, pageIAMNamespace, r.roleAddFormData(namespace, fields, "", "Name is required."))

		return
	}

	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: fields.name, Namespace: namespace},
		Rules:      buildPolicyRules(fields.rules),
	}

	err = kontinuums.Create(request.Context(), role)
	if err != nil {
		r.renderRoleAddModalBody(writer, pageIAMNamespace, r.roleAddFormData(namespace, fields, "", err.Error()))

		return
	}

	r.renderRoleAddModalBody(writer, pageIAMNamespace, r.roleAddFormData(namespace, roleAddFields{}, role.Name, ""))
}

// handleClusterRoleAdd is POST /app/rbac.authorization.k8s.io/v1/clusterroles's handler — see
// handleRoleAdd, its namespaced counterpart.
func (r *Router) handleClusterRoleAdd(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRoleAddFormBytes)

	err := request.ParseForm()
	if err != nil {
		http.Error(writer, "failed to parse form: "+err.Error(), http.StatusBadRequest)

		return
	}

	fields := roleAddFields{
		name: strings.TrimSpace(request.PostFormValue("name")), rules: parseRuleFields(request.PostForm),
	}

	if fields.name == "" {
		r.renderRoleAddModalBody(writer, pageIAMCluster, r.clusterRoleAddFormData(fields, "", "Name is required."))

		return
	}

	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: fields.name},
		Rules:      buildPolicyRules(fields.rules),
	}

	err = kontinuums.Create(request.Context(), role)
	if err != nil {
		r.renderRoleAddModalBody(writer, pageIAMCluster, r.clusterRoleAddFormData(fields, "", err.Error()))

		return
	}

	r.renderRoleAddModalBody(writer, pageIAMCluster, r.clusterRoleAddFormData(roleAddFields{}, role.Name, ""))
}

// roleBindingAddFields is "Add role binding"/"Add cluster role binding"'s
// parsed form fields.
type roleBindingAddFields struct {
	name             string
	subjectKind      string
	subjectName      string
	subjectNamespace string
	roleRefName      string
}

// validateRoleBindingFields returns a human-readable error if fields is
// incomplete/invalid, or "" if it's ready to submit.
func validateRoleBindingFields(fields roleBindingAddFields) string {
	validKind := fields.subjectKind == rbacv1.UserKind ||
		fields.subjectKind == rbacv1.GroupKind || fields.subjectKind == rbacv1.ServiceAccountKind

	switch {
	case fields.name == "":
		return "Name is required."
	case fields.subjectName == "":
		return "Subject name is required."
	case fields.roleRefName == "":
		return "A role is required."
	case !validKind:
		return "Subject kind must be User, Group, or ServiceAccount."
	case fields.subjectKind == rbacv1.ServiceAccountKind && fields.subjectNamespace == "":
		return "Subject namespace is required for ServiceAccount subjects."
	default:
		return ""
	}
}

// buildSubject converts fields' subject into a rbacv1.Subject, following
// the same {Kind, APIGroup: rbacv1.GroupName} shape adminrbac's own
// createBinding uses for User/Group subjects — ServiceAccount subjects
// carry no APIGroup but do carry an explicit Namespace instead (Kubernetes
// never defaults it from the binding's own namespace).
func buildSubject(fields roleBindingAddFields) rbacv1.Subject {
	subject := rbacv1.Subject{Kind: fields.subjectKind, Name: fields.subjectName}

	if fields.subjectKind == rbacv1.ServiceAccountKind {
		subject.Namespace = fields.subjectNamespace
	} else {
		subject.APIGroup = rbacv1.GroupName
	}

	return subject
}

// roleBindingAddFormData is the "rolebinding-add-modal-body" fragment's
// template data for the namespaced RoleBinding form — see roleAddFormData's
// own doc for why AddURL is computed per scope.
func (r *Router) roleBindingAddFormData(
	namespace string, roleOptions []string, fields roleBindingAddFields, createdBinding, formErr string,
) map[string]any {
	return map[string]any{
		"RoleBindingAddURL": "/app/rbac.authorization.k8s.io/v1/namespaces/" + namespace + "/rolebindings",
		"RoleBindingKind":   "role binding", "Namespace": namespace,
		"RoleOptions": roleOptions,
		"BindingName": fields.name, "SubjectKind": fields.subjectKind,
		"SubjectName": fields.subjectName, "SubjectNamespace": fields.subjectNamespace, "RoleRefName": fields.roleRefName,
		"CreatedBinding": createdBinding, "BindingError": formErr,
	}
}

// clusterRoleBindingAddFormData is roleBindingAddFormData's cluster-scoped
// counterpart.
func (r *Router) clusterRoleBindingAddFormData(
	roleOptions []string, fields roleBindingAddFields, createdBinding, formErr string,
) map[string]any {
	return map[string]any{
		"RoleBindingAddURL": "/app/rbac.authorization.k8s.io/v1/clusterrolebindings",
		"RoleBindingKind":   "cluster role binding",
		"RoleOptions":       roleOptions,
		"BindingName":       fields.name, "SubjectKind": fields.subjectKind,
		"SubjectName": fields.subjectName, "SubjectNamespace": fields.subjectNamespace, "RoleRefName": fields.roleRefName,
		"CreatedBinding": createdBinding, "BindingError": formErr,
	}
}

// renderRoleBindingAddModalBody renders just the "rolebinding-add-modal-body"
// fragment — see renderRoleAddModalBody, the precedent this follows.
func (r *Router) renderRoleBindingAddModalBody(writer http.ResponseWriter, page string, data map[string]any) {
	var buf bytes.Buffer

	err := r.pages[page].ExecuteTemplate(&buf, "rolebinding-add-modal-body", data)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)

		return
	}

	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(writer)
}

// handleRoleBindingAdd is POST
// /app/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings's handler —
// it creates the submitted RoleBinding in the current tenant namespace,
// referencing an existing namespaced Role. See handleRoleAdd for the
// success/failure response shape this follows.
func (r *Router) handleRoleBindingAdd(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRoleAddFormBytes)

	err := request.ParseForm()
	if err != nil {
		http.Error(writer, "failed to parse form: "+err.Error(), http.StatusBadRequest)

		return
	}

	namespace := request.PathValue("ns")
	fields := roleBindingAddFields{
		name:             strings.TrimSpace(request.PostFormValue("name")),
		subjectKind:      request.PostFormValue("subject-kind"),
		subjectName:      strings.TrimSpace(request.PostFormValue("subject-name")),
		subjectNamespace: strings.TrimSpace(request.PostFormValue("subject-namespace")),
		roleRefName:      request.PostFormValue("role-ref"),
	}

	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	roles, err := r.listNamespaceRoles(writer, request, kontinuums, namespace)
	if err != nil {
		return
	}

	roleOptions := roleNames(roles)

	if formErr := validateRoleBindingFields(fields); formErr != "" {
		r.renderRoleBindingAddModalBody(writer, pageIAMNamespace,
			r.roleBindingAddFormData(namespace, roleOptions, fields, "", formErr))

		return
	}

	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: fields.name, Namespace: namespace},
		Subjects:   []rbacv1.Subject{buildSubject(fields)},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: fields.roleRefName},
	}

	err = kontinuums.Create(request.Context(), binding)
	if err != nil {
		r.renderRoleBindingAddModalBody(writer, pageIAMNamespace,
			r.roleBindingAddFormData(namespace, roleOptions, fields, "", err.Error()))

		return
	}

	r.renderRoleBindingAddModalBody(writer, pageIAMNamespace,
		r.roleBindingAddFormData(namespace, roleOptions, roleBindingAddFields{}, binding.Name, ""))
}

// handleClusterRoleBindingAdd is POST /app/rbac.authorization.k8s.io/v1/clusterrolebindings's
// handler — see handleRoleBindingAdd, its namespaced counterpart.
func (r *Router) handleClusterRoleBindingAdd(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRoleAddFormBytes)

	err := request.ParseForm()
	if err != nil {
		http.Error(writer, "failed to parse form: "+err.Error(), http.StatusBadRequest)

		return
	}

	fields := roleBindingAddFields{
		name:             strings.TrimSpace(request.PostFormValue("name")),
		subjectKind:      request.PostFormValue("subject-kind"),
		subjectName:      strings.TrimSpace(request.PostFormValue("subject-name")),
		subjectNamespace: strings.TrimSpace(request.PostFormValue("subject-namespace")),
		roleRefName:      request.PostFormValue("role-ref"),
	}

	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	roles, err := r.listClusterRoles(writer, request, kontinuums)
	if err != nil {
		return
	}

	roleOptions := roleNames(roles)

	if formErr := validateRoleBindingFields(fields); formErr != "" {
		r.renderRoleBindingAddModalBody(writer, pageIAMCluster,
			r.clusterRoleBindingAddFormData(roleOptions, fields, "", formErr))

		return
	}

	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: fields.name},
		Subjects:   []rbacv1.Subject{buildSubject(fields)},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: fields.roleRefName},
	}

	err = kontinuums.Create(request.Context(), binding)
	if err != nil {
		r.renderRoleBindingAddModalBody(writer, pageIAMCluster,
			r.clusterRoleBindingAddFormData(roleOptions, fields, "", err.Error()))

		return
	}

	r.renderRoleBindingAddModalBody(writer, pageIAMCluster,
		r.clusterRoleBindingAddFormData(roleOptions, roleBindingAddFields{}, binding.Name, ""))
}

// handleDeleteRole is DELETE
// /app/rbac.authorization.k8s.io/v1/namespaces/{ns}/roles/{name}'s handler
// — deletes the named Role, then redirects back to the namespace's own
// Roles tab, the same delete-then-redirect shape as
// handleDeleteInstanceObject (see deleteAndRedirect).
func (r *Router) handleDeleteRole(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	namespace := request.PathValue("ns")
	target := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: request.PathValue("name"), Namespace: namespace},
	}

	r.deleteAndRedirect(writer, request, kontinuums, target,
		"role", "/app/rbac.authorization.k8s.io/v1/namespaces/"+namespace+"/roles")
}

// handleDeleteRoleBinding is DELETE
// /app/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings/{name}'s
// handler — see handleDeleteRole, its Role counterpart.
func (r *Router) handleDeleteRoleBinding(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	namespace := request.PathValue("ns")
	target := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: request.PathValue("name"), Namespace: namespace},
	}

	r.deleteAndRedirect(writer, request, kontinuums, target,
		"role binding", "/app/rbac.authorization.k8s.io/v1/namespaces/"+namespace+"/rolebindings")
}

// handleDeleteClusterRole is DELETE
// /app/rbac.authorization.k8s.io/v1/clusterroles/{name}'s handler — see
// handleDeleteRole, its namespaced counterpart.
func (r *Router) handleDeleteClusterRole(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	target := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: request.PathValue("name")},
	}

	r.deleteAndRedirect(writer, request, kontinuums, target,
		"cluster role", "/app/rbac.authorization.k8s.io/v1/clusterroles")
}

// handleDeleteClusterRoleBinding is DELETE
// /app/rbac.authorization.k8s.io/v1/clusterrolebindings/{name}'s handler —
// see handleDeleteClusterRole, its RoleBinding counterpart.
func (r *Router) handleDeleteClusterRoleBinding(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	target := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: request.PathValue("name")},
	}

	r.deleteAndRedirect(writer, request, kontinuums, target,
		"cluster role binding", "/app/rbac.authorization.k8s.io/v1/clusterrolebindings")
}

// instanceRow is one api/v1alpha2.Instance object rendered as a row on the
// /app/kontinuum.sh/v1alpha2/namespaces/{ns}/instances list — see pageInstances' own
// doc for why this isn't named "instance", which already means something
// else in this file.
type instanceRow struct {
	Name         string
	Namespace    string
	TalosVersion string
	// Condition/ConditionOK reflect whichever of status.conditions most
	// recently transitioned (see latestCondition) — an Instance can carry
	// several (Discovered, then once claimed: Configured, Joined, a
	// per-member Ready), and several of those latch permanently once set
	// (see pkg/domain/instance's own Reconciler), so pinning to one fixed
	// type risks showing a stale badge while a later condition is what
	// actually changed most recently. Mirrors zoneRow's identical
	// Condition/ConditionOK fields.
	Condition   string
	ConditionOK bool
	ClaimedBy   string
	Age         string
	// Deleting is whether this Instance's own DeletionTimestamp is set —
	// see zoneRow.Deleting's own doc for why the template shows this
	// instead of a stale Condition value while InstanceResetFinalizer is
	// still resetting it.
	Deleting bool
}

// instanceRowFrom builds one instances-list row from item.
func instanceRowFrom(item v1alpha2.Instance) instanceRow {
	row := instanceRow{
		Name:         item.Name,
		Namespace:    item.Namespace,
		TalosVersion: item.Status.Talos.Version,
		ClaimedBy:    item.Labels[v1alpha2.LabelClaimedBy],
		Age:          formatAge(item.CreationTimestamp.Time),
		Deleting:     !item.DeletionTimestamp.IsZero(),
	}

	if cond := latestCondition(item.Status.Conditions); cond != nil {
		row.Condition = cond.Type
		row.ConditionOK = cond.Status == metav1.ConditionTrue
	}

	return row
}

// handleInstances is GET /app/kontinuum.sh/v1alpha2/namespaces/{ns}/instances's
// handler — it lists Instance CRD objects (api/v1alpha2.Instance:
// bare-metal or provider-backed machines InstancePool/TalosCluster claim
// from — see issue #24's architecture) in the {ns} tenant's own namespace,
// as a read-only browse page. Claiming/unclaiming isn't exposed here, per
// issue #52's explicit scope. Instance became namespaced in issue #63's
// architecture specifically so a tenant can bring their own hardware into
// their own namespace — {ns} is which tenant's own instances this page
// shows, chosen via nav.html's tenant switcher (see addTenantSwitcherData).
func (r *Router) handleInstances(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	namespace := request.PathValue("ns")

	var list v1alpha2.InstanceList

	err = kontinuums.List(request.Context(), &list, client.InNamespace(namespace))
	if err != nil {
		// Forbidden means the signed-in identity is authenticated but not
		// authorized — the session itself isn't the problem, but there's
		// no reason to keep it either, so send the caller back to sign in
		// rather than leave them stuck on a page they can't use.
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		http.Error(writer, "failed to list instances: "+err.Error(), http.StatusBadGateway)

		return
	}

	rows := make([]instanceRow, 0, len(list.Items))
	for _, item := range list.Items {
		rows = append(rows, instanceRowFrom(item))
	}

	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

	data := map[string]any{
		dataKeyTitle:       "Instances",
		dataKeyActiveMenu:  "instances",
		dataKeyVersion:     r.version,
		dataKeyAuthEnabled: r.authEnabled,
		dataKeyNamespace:   namespace,
		dataKeyInstances:   rows,
	}

	// The "Add instance" modal's own fragment data — always empty on a
	// plain page load, since only a submission (see handleInstanceAdd) ever
	// carries a preserved/error/success state — see renderRegistry's
	// identical maps.Copy of its own "Add zone" modal data.
	maps.Copy(data, r.instanceAddFormData(namespace, "", "", ""))

	r.render(writer, request, pageInstances, data)
}

// maxInstanceAddFormBytes bounds the "Add instance" form's request body —
// its only field is a short address, never a bulk upload — same rationale
// as maxZoneAddFormBytes.
const maxInstanceAddFormBytes = 1 << 16

// instanceAddFormData is the "instance-add-modal-body" fragment's template
// data — namespace/address is the (possibly just-submitted) form state,
// createdInstance/formErr surface a just-completed submission's outcome.
// Rendered twice: once embedded in the instances page's own initial render
// (always empty — see handleInstances), and again by handleInstanceAdd on
// every submission.
func (r *Router) instanceAddFormData(namespace, address, createdInstance, formErr string) map[string]any {
	return map[string]any{
		dataKeyNamespace:  namespace,
		"Address":         address,
		"CreatedInstance": createdInstance,
		"Error":           formErr,
	}
}

// renderInstanceAddModalBody renders just the "instance-add-modal-body"
// fragment (not a full page via layout.html) — the instances page's own
// dialog swaps #instance-add-modal-body's innerHTML with this on every form
// submission, so the modal never navigates away from the instances page —
// mirrors renderZoneAddModalBody exactly.
func (r *Router) renderInstanceAddModalBody(writer http.ResponseWriter, data map[string]any) {
	var buf bytes.Buffer

	err := r.pages[pageInstances].ExecuteTemplate(&buf, "instance-add-modal-body", data)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)

		return
	}

	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(writer)
}

// handleInstanceAdd is POST
// /app/kontinuum.sh/v1alpha2/namespaces/{ns}/instances/add's handler — it registers
// a standalone Instance in the {ns} tenant's own namespace via the shared
// pkg/domain/instance.Add, left unclaimed until something claims it (see
// issue #81 and zone.AddOptions.ExistingInstanceName's own doc for one such
// consumer — the "Add zone" modal's own instance-picker). On success it
// swaps the modal body to a success message; on failure it re-renders the
// form with the submitted address preserved and an error message. Either
// way the response stays a fragment — the instances page underneath is
// never navigated away from — mirrors handleZoneAdd exactly.
func (r *Router) handleInstanceAdd(writer http.ResponseWriter, request *http.Request) {
	// Every field here is a short address, never a bulk upload — bound the
	// body before ParseForm reads it into memory.
	request.Body = http.MaxBytesReader(writer, request.Body, maxInstanceAddFormBytes)

	err := request.ParseForm()
	if err != nil {
		http.Error(writer, "failed to parse form: "+err.Error(), http.StatusBadRequest)

		return
	}

	namespace := request.PathValue("ns")
	address := request.PostFormValue("address")

	// zonesFor, despite its name, is just this Router's per-request-identity
	// factory for a full controller-runtime client.Client — see
	// ZoneClientFactory's own doc. instancedomain.Add needs Create, which
	// kontinuumsFor's own narrower KontinuumClient interface doesn't expose,
	// so this reuses zonesFor exactly as handleZoneAdd does for its own
	// writes, rather than widening KontinuumClient for one caller.
	zones, err := r.zonesFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	created, err := instancedomain.Add(request.Context(), zones, instancedomain.AddOptions{
		Namespace: namespace, Address: address,
	})
	if err != nil {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		r.renderInstanceAddModalBody(writer, r.instanceAddFormData(namespace, address, "", err.Error()))

		return
	}

	r.renderInstanceAddModalBody(writer, r.instanceAddFormData(namespace, "", created.Name, ""))
}

// instanceInterfaceRow is one discovered network interface, shown on the
// instance detail page's own interfaces table.
type instanceInterfaceRow struct {
	Name       string
	MACAddress string
	Addresses  string
}

// instanceConditionRow is one status.conditions entry, shown on the instance
// detail page's own conditions table.
type instanceConditionRow struct {
	Type    string
	Status  string
	OK      bool
	Reason  string
	Message string
	Age     string
}

// instanceDiskRow is one discovered disk, shown on the instance detail
// page's own hardware section — see issue #76.
type instanceDiskRow struct {
	DevPath    string
	PrettySize string
	Model      string
	Serial     string
	Transport  string
	Rotational bool
}

// instanceCPURow is one discovered processor socket, shown on the instance
// detail page's own hardware section — see issue #76.
type instanceCPURow struct {
	Manufacturer string
	ProductName  string
	Architecture string
	CoreCount    uint32
	ThreadCount  uint32
	MaxSpeedMHz  uint32
}

// instanceMemoryRow is one discovered memory module, shown on the instance
// detail page's own hardware section — see issue #76.
type instanceMemoryRow struct {
	SizeMiB      uint32
	Manufacturer string
	Speed        uint32
	Serial       string
}

// instanceInterfaceRowsFrom builds instanceDetailData's own Interfaces rows
// — factored out purely to keep that function under this repo's own
// funlen limit, same reason every other instance*RowsFrom helper below
// exists.
func instanceInterfaceRowsFrom(interfaces []v1alpha2.InstanceInterfaceStatus) []instanceInterfaceRow {
	rows := make([]instanceInterfaceRow, 0, len(interfaces))
	for _, iface := range interfaces {
		rows = append(rows, instanceInterfaceRow{
			Name:       iface.Name,
			MACAddress: iface.MACAddress,
			Addresses:  strings.Join(iface.Addresses, ", "),
		})
	}

	return rows
}

// instanceConditionRowsFrom builds instanceDetailData's own Conditions rows
// — see instanceInterfaceRowsFrom's own doc.
func instanceConditionRowsFrom(conditions []metav1.Condition) []instanceConditionRow {
	rows := make([]instanceConditionRow, 0, len(conditions))
	for _, cond := range conditions {
		rows = append(rows, instanceConditionRow{
			Type:    cond.Type,
			Status:  string(cond.Status),
			OK:      cond.Status == metav1.ConditionTrue,
			Reason:  cond.Reason,
			Message: capitalizeFirst(cond.Message),
			Age:     formatAge(cond.LastTransitionTime.Time),
		})
	}

	return rows
}

// instanceDiskRowsFrom builds instanceDetailData's own Disks rows — see
// instanceInterfaceRowsFrom's own doc.
func instanceDiskRowsFrom(disks []v1alpha2.InstanceDiskStatus) []instanceDiskRow {
	rows := make([]instanceDiskRow, 0, len(disks))
	for _, disk := range disks {
		rows = append(rows, instanceDiskRow{
			DevPath:    disk.DevPath,
			PrettySize: disk.PrettySize,
			Model:      disk.Model,
			Serial:     disk.Serial,
			Transport:  disk.Transport,
			Rotational: disk.Rotational,
		})
	}

	return rows
}

// instanceCPURowsFrom builds instanceDetailData's own CPUs rows — see
// instanceInterfaceRowsFrom's own doc.
func instanceCPURowsFrom(cpus []v1alpha2.InstanceCPUStatus) []instanceCPURow {
	rows := make([]instanceCPURow, 0, len(cpus))
	for _, cpu := range cpus {
		rows = append(rows, instanceCPURow{
			Manufacturer: cpu.Manufacturer,
			ProductName:  cpu.ProductName,
			Architecture: cpu.Architecture,
			CoreCount:    cpu.CoreCount,
			ThreadCount:  cpu.ThreadCount,
			MaxSpeedMHz:  cpu.MaxSpeedMHz,
		})
	}

	return rows
}

// instanceMemoryRowsFrom builds instanceDetailData's own Memory rows — see
// instanceInterfaceRowsFrom's own doc.
func instanceMemoryRowsFrom(modules []v1alpha2.InstanceMemoryStatus) []instanceMemoryRow {
	rows := make([]instanceMemoryRow, 0, len(modules))
	for _, module := range modules {
		rows = append(rows, instanceMemoryRow{
			SizeMiB:      module.SizeMiB,
			Manufacturer: module.Manufacturer,
			Speed:        module.Speed,
			Serial:       module.Serial,
		})
	}

	return rows
}

// handleInstanceDetail is GET
// /app/kontinuum.sh/v1alpha2/namespaces/{ns}/instances/{name}'s handler — it shows
// one Instance CRD object's discovery result: Talos version, discovered
// network interfaces, and status.conditions, plus which InstancePool (if
// any) has claimed it (see api/v1alpha2.LabelClaimedBy — claiming isn't
// recorded anywhere else on Instance itself). No link is rendered to the
// claiming InstancePool: that CRD has no UI page of its own yet (tracked
// separately, see issue #52's "explicitly out of scope").
func (r *Router) handleInstanceDetail(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	var item v1alpha2.Instance

	key := client.ObjectKey{Name: request.PathValue("name"), Namespace: request.PathValue("ns")}

	err = kontinuums.Get(request.Context(), key, &item)
	if err != nil {
		if apierrors.IsNotFound(err) {
			http.NotFound(writer, request)

			return
		}

		// Forbidden means the signed-in identity is authenticated but not
		// authorized — the session itself isn't the problem, but there's
		// no reason to keep it either, so send the caller back to sign in
		// rather than leave them stuck on a page they can't use.
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		http.Error(writer, "failed to get instance: "+err.Error(), http.StatusBadGateway)

		return
	}

	r.render(writer, request, pageInstanceDetail, instanceDetailData(item, r.version, r.authEnabled))
}

// instanceDetailData builds handleInstanceDetail's template data from item —
// factored out purely to keep that function short, same as
// kontinuumDetailData does for the Kontinuum detail page above.
func instanceDetailData(item v1alpha2.Instance, version string, authEnabled bool) map[string]any {
	interfaces := instanceInterfaceRowsFrom(item.Status.Interfaces)
	conditions := instanceConditionRowsFrom(sortConditionsNewestFirst(item.Status.Conditions))
	disks := instanceDiskRowsFrom(item.Status.Disks)
	cpus := instanceCPURowsFrom(item.Status.CPUs)
	memory := instanceMemoryRowsFrom(item.Status.Memory)

	discoverySource := "Bare metal (spec.interfaces)"
	if item.Spec.ProviderRef != nil {
		discoverySource = fmt.Sprintf("%s/%s", item.Spec.ProviderRef.Kind, item.Spec.ProviderRef.Name)
	}

	data := map[string]any{
		dataKeyTitle:        item.Name,
		dataKeyActiveMenu:   "instances",
		dataKeyVersion:      version,
		dataKeyAuthEnabled:  authEnabled,
		dataKeyName:         item.Name,
		dataKeyNamespace:    item.Namespace,
		dataKeyAge:          formatAge(item.CreationTimestamp.Time),
		dataKeyTalosVersion: item.Status.Talos.Version,
		"DiscoverySource":   discoverySource,
		"Hostname":          item.Annotations[instancedomain.AnnotationHostname],
		"SerialNumber":      item.Status.SerialNumber,
		"ClaimedBy":         item.Labels[v1alpha2.LabelClaimedBy],
		"Labels":            sortedLabels(item.Labels),
		"Interfaces":        interfaces,
		"Disks":             disks,
		"CPUs":              cpus,
		"Memory":            memory,
		dataKeyConditions:   conditions,
		dataKeyDeleting:     !item.DeletionTimestamp.IsZero(),
	}

	// The title-bar badge shows the same condition instanceRowFrom picks
	// for this Instance's own row on the list page (see latestCondition),
	// not a fixed Discovered lookup — so a viewer sees one consistent
	// condition for a given Instance whether they're looking at the list
	// or this detail page.
	if cond := latestCondition(item.Status.Conditions); cond != nil {
		data["Condition"] = cond.Type
		data["ConditionOK"] = cond.Status == metav1.ConditionTrue
	}

	return data
}

// handleDeleteInstanceObject is DELETE
// /app/kontinuum.sh/v1alpha2/namespaces/{ns}/instances/{name}'s handler — it deletes
// the Instance CRD object named by the {name} path value and sends the
// browser back to the instances list via HX-Redirect. Not to be confused
// with handleDeleteInstance above, which deletes a Kontinuum — see
// pageInstances' own doc for why the two stay separate despite the
// similar-sounding name.
//
// If this Instance is claimed by a pool some TalosCluster references,
// deleting it here sets its own deletionTimestamp, which
// taloscluster.InstanceResetReconciler's own finalizer (see
// taloscluster.InstanceResetFinalizer) picks up to reset its node back to
// Talos maintenance mode before the object is actually allowed to go away
// — so the object may keep existing, still visible on the instances list,
// for a while after this call returns. An unclaimed Instance (nothing to
// reset) is removed immediately.
func (r *Router) handleDeleteInstanceObject(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	namespace := request.PathValue("ns")

	target := &v1alpha2.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: request.PathValue("name"), Namespace: namespace},
	}

	r.deleteAndRedirect(writer, request, kontinuums, target,
		"instance", "/app/kontinuum.sh/v1alpha2/namespaces/"+namespace+"/instances")
}

// deleteAndRedirect deletes target through kontinuums and, on success (or if
// it was already gone), sends the browser to redirectPath via the
// Hx-Redirect response header — the shared body behind
// handleDeleteInstanceObject and handleDeleteTalosCluster below, which
// differ only in which object kind they delete and where they redirect
// to afterwards. kind names target's own kind in the bad-gateway error
// message (e.g. "instance", "taloscluster").
func (r *Router) deleteAndRedirect(
	writer http.ResponseWriter, request *http.Request,
	kontinuums KontinuumClient, target client.Object, kind, redirectPath string,
) {
	err := kontinuums.Delete(request.Context(), target)
	if err != nil && !apierrors.IsNotFound(err) {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		http.Error(writer, "failed to delete "+kind+": "+err.Error(), http.StatusBadGateway)

		return
	}

	writer.Header().Set("Hx-Redirect", redirectPath)
	writer.WriteHeader(http.StatusOK)
}

// talosKubeconfigSecretKey is the key a TalosCluster's own kubeconfig is
// stored under on its status.secretRef Secret — mirrors
// pkg/domain/taloscluster/secrets.go's own unexported kubeconfigKey. Kept as
// a separate copy (rather than exporting that package's constant) since this
// is the only thing pkg/ui needs from that package, and pulling in the
// domain package itself would be a much bigger dependency than one string.
const talosKubeconfigSecretKey = "kubeconfig"

// talosClusterRow is a TalosCluster object rendered as a row on the list
// page — see handleTalosClusters. Condition/ConditionOK reflect whichever
// of status.conditions most recently transitioned (see latestCondition),
// not a fixed "Ready" lookup — mirrors zoneRow's identical fields, and for
// the same reason instanceRow moved off a fixed lookup: Ready and
// ControlPlaneReady both latch permanently once true (see
// pkg/domain/taloscluster's own Reconciler), so pinning to "Ready" risks
// showing a stale badge while a later condition is what actually changed
// most recently.
type talosClusterRow struct {
	Name              string
	Namespace         string
	ControlPlanePool  string
	TalosVersion      string
	KubernetesVersion string
	Condition         string
	ConditionOK       bool
	Age               string
	// Deleting is whether this TalosCluster's own DeletionTimestamp is set
	// — see zoneRow.Deleting's own doc for why the template shows this
	// instead of a stale Condition value during teardown.
	Deleting bool
}

// handleTalosClusters is GET /app/talosclusters's handler — it lists every
// TalosCluster and renders the list page, following the same "browse/inspect
// a real object, not a form" precedent as handleIAM (see issue #53's own
// reference to #38).
func (r *Router) handleTalosClusters(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	namespace := request.PathValue("ns")

	var list v1alpha2.TalosClusterList

	err = kontinuums.List(request.Context(), &list, client.InNamespace(namespace))
	if err != nil {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		http.Error(writer, "failed to list taloscluster instances: "+err.Error(), http.StatusBadGateway)

		return
	}

	clusters := make([]talosClusterRow, 0, len(list.Items))
	for _, item := range list.Items {
		row := talosClusterRow{
			Name:              item.Name,
			Namespace:         item.Namespace,
			ControlPlanePool:  item.Spec.ControlPlane.PoolRef.Name,
			TalosVersion:      item.Spec.Talos.Version,
			KubernetesVersion: item.Spec.Kubernetes.Version,
			Age:               formatAge(item.CreationTimestamp.Time),
			Deleting:          !item.DeletionTimestamp.IsZero(),
		}

		if cond := latestCondition(item.Status.Conditions); cond != nil {
			row.Condition = cond.Type
			row.ConditionOK = cond.Status == metav1.ConditionTrue
		}

		clusters = append(clusters, row)
	}

	sort.Slice(clusters, func(i, j int) bool { return clusters[i].Name < clusters[j].Name })

	r.render(writer, request, pageTalosClusters, map[string]any{
		dataKeyTitle:       "Clusters",
		dataKeyActiveMenu:  "talosclusters",
		dataKeyVersion:     r.version,
		dataKeyAuthEnabled: r.authEnabled,
		dataKeyNamespace:   namespace,
		"TalosClusters":    clusters,
	})
}

// conditionOfType returns the entry in conditions whose Type matches
// conditionType, or nil if there is none — used instead of
// renderRegistry/listZoneRows' own latestCondition where the UI wants one
// specific, named condition rather than whichever changed most recently.
func conditionOfType(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for index := range conditions {
		if conditions[index].Type == conditionType {
			return &conditions[index]
		}
	}

	return nil
}

// poolRow is an InstancePool referenced by a TalosCluster's control plane or
// a named worker pool, rendered on the detail page's pool breakdown. Name is
// "Control plane" for the control-plane row, which has no name of its own
// (see TalosClusterMemberSpec) — every other row is a worker pool, named per
// TalosClusterWorkerSpec.Name. Found is false when the referenced
// InstancePool doesn't exist yet (or this viewer's RBAC hides it) — a
// TalosCluster only ever references a pool by name (see
// InstancePoolReference's own doc), so that's "not provisioned yet," not a
// page-load failure.
type poolRow struct {
	Name          string
	PoolRef       string
	ReadyReplicas int32
	Replicas      int32
	Found         bool
}

// fetchPoolRow looks up the InstancePool named poolName and summarizes it as
// a poolRow — see that type's own doc for why a missing pool isn't treated
// as an error. Errors other than NotFound (e.g. Forbidden) are folded into
// the same "not found" rendering rather than failing the whole detail page:
// kontinuum's RBAC model is all-or-nothing per admin group (see handleIAM's
// own doc), so a viewer who can already read the TalosCluster itself is
// never expected to be selectively denied one of its InstancePools.
func fetchPoolRow(ctx context.Context, kontinuums KontinuumClient, namespace, rowName, poolName string) poolRow {
	var pool v1alpha2.InstancePool

	key := client.ObjectKey{Name: poolName, Namespace: namespace}

	err := kontinuums.Get(ctx, key, &pool)
	if err != nil {
		return poolRow{Name: rowName, PoolRef: poolName}
	}

	return poolRow{
		Name: rowName, PoolRef: poolName, Found: true,
		ReadyReplicas: pool.Status.ReadyReplicas, Replicas: pool.Spec.Replicas,
	}
}

// conditionRow is one status.conditions entry rendered on the detail page's
// full condition list.
type conditionRow struct {
	Type    string
	Status  string
	OK      bool
	Reason  string
	Message string
	Age     string
}

// fetchOwningZone looks up the Zone sharing cluster's own name. zone-add
// creates Zone/InstancePool/TalosCluster all under one shared
// <region>-<zone> name (see pkg/domain/zone/add.go's BuildAddObjects) — the
// same name-matching the Zone controller itself relies on to find "its"
// TalosCluster (see pkg/domain/zone/controller.go's mapTalosClusterToZone).
// Returns a zero Zone, ok=true when none is found: a TalosCluster doesn't
// require an owning Zone to exist (e.g. one created outside the zone-add
// flow), so that's "nothing to show," not a failure. ok is false only when
// fetchOwningZone has already written the error (or forbidden-redirect)
// response itself.
func (r *Router) fetchOwningZone(
	writer http.ResponseWriter, request *http.Request, kontinuums KontinuumClient, namespace, clusterName string,
) (v1alpha2.Zone, bool) {
	var zone v1alpha2.Zone

	key := client.ObjectKey{Name: clusterName, Namespace: namespace}

	err := kontinuums.Get(request.Context(), key, &zone)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return v1alpha2.Zone{}, true
		}

		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return v1alpha2.Zone{}, false
		}

		http.Error(writer, "failed to get owning zone: "+err.Error(), http.StatusBadGateway)

		return v1alpha2.Zone{}, false
	}

	return zone, true
}

// fetchTalosClusterKubeconfig fetches the kubeconfig stored under ref (a
// TalosCluster's own status.secretRef) — see talosKubeconfigSecretKey's own
// doc for the key it reads. A cluster with no secretRef yet, or whose Secret
// doesn't yet carry a kubeconfig (bootstrap still in progress), returns
// (nil, true): that's not a request failure, just "not ready yet". The bool
// result is false only when fetchTalosClusterKubeconfig has already written
// the error (or forbidden-redirect) response itself — mirrors
// fetchSecretDataYAML's identical contract.
func (r *Router) fetchTalosClusterKubeconfig(
	writer http.ResponseWriter, request *http.Request, kontinuums KontinuumClient, ref v1alpha2.SecretReference,
) ([]byte, bool) {
	if ref.Name == "" {
		return nil, true
	}

	var secret corev1.Secret

	err := kontinuums.Get(request.Context(), client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}, &secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, true
		}

		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return nil, false
		}

		http.Error(writer, "failed to get taloscluster kubeconfig secret: "+err.Error(), http.StatusBadGateway)

		return nil, false
	}

	return secret.Data[talosKubeconfigSecretKey], true
}

// fetchTalosCluster fetches the TalosCluster named name — shared by
// handleTalosClusterDetail and handleTalosClusterKubeconfigDownload, which
// both start from "look up this one TalosCluster" before doing their own,
// different thing with it. ok is false only when fetchTalosCluster has
// already written the response itself: NotFound, a forbidden-redirect, or a
// bad-gateway error.
func (r *Router) fetchTalosCluster(
	writer http.ResponseWriter, request *http.Request, kontinuums KontinuumClient, namespace, name string,
) (v1alpha2.TalosCluster, bool) {
	var cluster v1alpha2.TalosCluster

	key := client.ObjectKey{Name: name, Namespace: namespace}

	err := kontinuums.Get(request.Context(), key, &cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			http.NotFound(writer, request)

			return v1alpha2.TalosCluster{}, false
		}

		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return v1alpha2.TalosCluster{}, false
		}

		http.Error(writer, "failed to get taloscluster: "+err.Error(), http.StatusBadGateway)

		return v1alpha2.TalosCluster{}, false
	}

	return cluster, true
}

// talosClusterDetailData builds handleTalosClusterDetail's template data
// from cluster and its already-fetched zone/kubeconfig (see
// fetchOwningZone/fetchTalosClusterKubeconfig) — factored out of
// handleTalosClusterDetail purely to keep that function short, mirroring
// kontinuumDetailData's identical role for the Kontinuum detail page.
// revealed is handleTalosClusterDetail's own ?reveal=true query parameter,
// passed straight through as KubeconfigRevealed — see
// taloscluster_content.html's own reveal-panel/reveal-panel-script calls
// for why the page renders itself already open when set, instead of
// always starting masked and fixing that up client-side.
func talosClusterDetailData(
	ctx context.Context, kontinuums KontinuumClient, cluster v1alpha2.TalosCluster, zone v1alpha2.Zone,
	kubeconfig []byte, revealed bool, version string, authEnabled bool,
) map[string]any {
	pools := make([]poolRow, 0, len(cluster.Spec.Workers)+1)
	pools = append(pools,
		fetchPoolRow(ctx, kontinuums, cluster.Namespace, "Control plane", cluster.Spec.ControlPlane.PoolRef.Name))

	for _, worker := range cluster.Spec.Workers {
		pools = append(pools, fetchPoolRow(ctx, kontinuums, cluster.Namespace, worker.Name, worker.PoolRef.Name))
	}

	sortedConditions := sortConditionsNewestFirst(cluster.Status.Conditions)
	conditions := make([]conditionRow, 0, len(sortedConditions))

	for _, cond := range sortedConditions {
		conditions = append(conditions, conditionRow{
			Type: cond.Type, Status: string(cond.Status), OK: cond.Status == metav1.ConditionTrue,
			Reason: cond.Reason, Message: capitalizeFirst(cond.Message), Age: formatAge(cond.LastTransitionTime.Time),
		})
	}

	data := map[string]any{
		dataKeyTitle:         cluster.Name,
		dataKeyActiveMenu:    "talosclusters",
		dataKeyVersion:       version,
		dataKeyAuthEnabled:   authEnabled,
		dataKeyName:          cluster.Name,
		dataKeyNamespace:     cluster.Namespace,
		dataKeyTalosVersion:  cluster.Spec.Talos.Version,
		"KubernetesVersion":  cluster.Spec.Kubernetes.Version,
		dataKeyAge:           formatAge(cluster.CreationTimestamp.Time),
		"Pools":              pools,
		dataKeyConditions:    conditions,
		dataKeyDeleting:      !cluster.DeletionTimestamp.IsZero(),
		"KubeconfigReady":    len(kubeconfig) > 0,
		"KubeconfigRevealed": revealed,
		"HasZone":            zone.Name != "",
		"ZoneObjectName":     zone.Name,
		"ZoneRegion":         zone.Spec.Region,
		"ZoneName":           zone.Spec.Zone,
		"Labels":             sortedLabels(cluster.Labels),
	}

	if readyCond := conditionOfType(cluster.Status.Conditions, "Ready"); readyCond != nil {
		data["Ready"] = string(readyCond.Status)
		data["ReadyOK"] = readyCond.Status == metav1.ConditionTrue
	}

	return data
}

// handleTalosClusterDetail is GET /app/talosclusters/{name}'s handler — one
// TalosCluster's control-plane/worker pool breakdown, versions, full
// condition list, and (see fetchTalosClusterKubeconfig) whether a
// kubeconfig is available to download, without ever fetching or rendering
// that kubeconfig's own contents into the page — see
// handleTalosClusterKubeconfigDownload for the actual download. An
// optional ?reveal=true query parameter (see talosClusterDetailData's own
// KubeconfigRevealed field) renders the kubeconfig's reveal panel already
// open, so a page reload or the 15s auto-refresh's own hx-get (see
// taloscluster_content.html) can round-trip through this same parameter
// and stay open without a client-side flash between states.
func (r *Router) handleTalosClusterDetail(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	cluster, found := r.fetchTalosCluster(writer, request, kontinuums, request.PathValue("ns"), request.PathValue("name"))
	if !found {
		return
	}

	zone, found := r.fetchOwningZone(writer, request, kontinuums, cluster.Namespace, cluster.Name)
	if !found {
		return
	}

	kubeconfig, found := r.fetchTalosClusterKubeconfig(writer, request, kontinuums, cluster.Status.SecretRef)
	if !found {
		return
	}

	revealed := request.URL.Query().Get("reveal") == "true"

	data := talosClusterDetailData(
		request.Context(), kontinuums, cluster, zone, kubeconfig, revealed, r.version, r.authEnabled,
	)

	r.render(writer, request, pageTalosCluster, data)
}

// handleTalosClusterKubeconfigDownload is GET
// /app/talosclusters/{name}/kubeconfig's handler — it streams the cluster's
// kubeconfig (see fetchTalosClusterKubeconfig) straight to the response,
// serving two different callers from taloscluster_content.html: the
// `<a download>` link (browser save-as-file, via Content-Disposition below)
// and reveal-panel-script's own on-demand fetch — triggered by a "Reveal"
// click, or automatically when the page itself already rendered the panel
// open (see handleTalosClusterDetail's own ?reveal=true) — which renders
// the response text inline instead. Either way,
// this TalosCluster kubeconfig — real cluster-admin credentials pulled from
// a Secret, not a value this process can regenerate on demand — is never
// part of handleTalosClusterDetail's own server-rendered page response; it
// only ever reaches the browser through an explicit, separate request the
// viewer triggers themselves, unlike settings_content.html's synthetic
// OIDC-login kubeconfig, which that page's initial render embeds directly.
// See issue #53's own explicit requirement, which this on-demand fetch
// deliberately relaxes (the credential can now reach the page's DOM) while
// still keeping it out of anything cached or logged as the page's own
// response body.
func (r *Router) handleTalosClusterKubeconfigDownload(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	cluster, found := r.fetchTalosCluster(writer, request, kontinuums, request.PathValue("ns"), request.PathValue("name"))
	if !found {
		return
	}

	kubeconfig, found := r.fetchTalosClusterKubeconfig(writer, request, kontinuums, cluster.Status.SecretRef)
	if !found {
		return
	}

	if len(kubeconfig) == 0 {
		http.NotFound(writer, request)

		return
	}

	writer.Header().Set("Content-Type", "application/yaml")
	writer.Header().Set("Content-Disposition", `attachment; filename="`+cluster.Name+`-kubeconfig.yaml"`)
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(kubeconfig)
}

// handleDeleteTalosCluster is DELETE
// /app/kontinuum.sh/v1alpha2/namespaces/{ns}/talosclusters/{name}'s handler — it
// deletes the TalosCluster object named by the {name} path value and sends
// the browser back to the clusters list via HX-Redirect.
//
// Deleting here only sets TalosCluster's own deletionTimestamp — Reconciler's
// own finalizer (see TalosClusterFinalizer) stops touching the cluster's
// members and removes itself immediately, with no wait of its own: unlike
// Zone's teardown, this alone does not release or reset anything still
// claimed by the cluster's own control-plane/worker pools (see
// TalosClusterFinalizer's own doc, and zone.reconcileTeardown, which relies
// on Zone's own ownership of those Instances to cascade that separately).
func (r *Router) handleDeleteTalosCluster(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	namespace := request.PathValue("ns")

	target := &v1alpha2.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: request.PathValue("name"), Namespace: namespace},
	}

	r.deleteAndRedirect(writer, request, kontinuums, target,
		"taloscluster", "/app/kontinuum.sh/v1alpha2/namespaces/"+namespace+"/talosclusters")
}

// handleRegistryKubeconfigDownload is GET /app/registry/kubeconfig's
// handler — it serves the synthetic OIDC-login kubeconfig
// registry_content.html's own kubectl-access card shows, computed by
// kubeconfig() (kontinuum's default is no authentication at all, not "no
// access," so there's always a working kubeconfig here, not just when OIDC
// happens to be enabled). Serves two different callers: the page's
// `<a download>` link, and its "Reveal" button, which fetches this same
// endpoint via JS only when clicked and renders the response text inline —
// mirrors handleTalosClusterKubeconfigDownload's identical dual role for a
// different kubeconfig.
func (r *Router) handleRegistryKubeconfigDownload(writer http.ResponseWriter, request *http.Request) {
	content := kubeconfig(requestOrigin(request), request.Host, r.cfg.OIDC.IssuerURL, r.cfg.OIDC.ClientID)

	writer.Header().Set("Content-Type", "application/yaml")
	writer.Header().Set("Content-Disposition", `attachment; filename="kontinuum-kubeconfig.yaml"`)
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte(content))
}

// render executes page's template tree against data, first merging in
// nav.html's own tenant-switcher fields (see addTenantSwitcherData) — every
// page shares the same nav, so this is the one place that needs to run for
// all of them, rather than every handler building it itself.
func (r *Router) render(writer http.ResponseWriter, request *http.Request, page string, data map[string]any) {
	r.addTenantSwitcherData(request, data)

	var buf bytes.Buffer

	err := r.pages[page].ExecuteTemplate(&buf, "layout.html", data)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)

		return
	}

	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(writer)
}

// addTenantSwitcherData merges nav.html's own "CurrentTenant"/"Tenants"
// fields into data. CurrentTenant is request's own {ns} path value — set on
// the instances routes (see RegisterRoutes), empty everywhere else, which
// nav.html falls back to defaultTenantNamespace for. Tenants lists every
// namespace the caller's own identity can see, best-effort: a listing
// failure (e.g. Forbidden — this viewer's RBAC just doesn't cover
// namespace-listing, deny-by-default per pkg/domain/adminrbac's own model)
// leaves Tenants empty rather than failing the whole page over what's only
// ever a supplementary nav control, never the page's own requested data.
func (r *Router) addTenantSwitcherData(request *http.Request, data map[string]any) {
	currentTenant := request.PathValue("ns")
	if currentTenant == "" {
		currentTenant = defaultTenantNamespace
	}

	data["CurrentTenant"] = currentTenant

	namespaces, err := r.namespacesFor(request.Context())
	if err != nil {
		return
	}

	list, err := namespaces.List(request.Context(), metav1.ListOptions{})
	if err != nil || list == nil {
		return
	}

	names := make([]string, 0, len(list.Items))
	for _, ns := range list.Items {
		names = append(names, ns.Name)
	}

	sort.Strings(names)

	data["Tenants"] = names
}
