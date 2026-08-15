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
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/auth"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/adminrbac"
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
// list, and delete registered kontinuum instances (see pkg/domain/registry,
// which owns the kontinuums.kontinuum.sh CRD and the objects it acts on).
// It is satisfied by a controller-runtime client.Client.
type KontinuumClient interface {
	Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error
	List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error
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
	// /app/kontinuum.sh/namespaces/{ns}/instances and
	// .../instances/{name} — the Instance CRD (api/v1alpha2.Instance, a
	// bare-metal or provider-backed machine InstancePool/TalosCluster claims
	// from). Not to be confused with pageKontinuum above: a registered
	// Kontinuum server process, a completely different type despite the
	// similar-sounding name — see issue #52, which is why the two stay
	// separate pages under separate routes (/instances vs /kontinuums)
	// rather than sharing one. The URL's own kontinuum.sh/namespaces/{ns}
	// shape (rather than a flat /app/instances) is what issue #63's
	// architecture needs: Instance became a namespaced CRD there, one
	// tenant's own namespace at a time — see the nav's tenant switcher
	// (renderTenantSwitcher) for how {ns} is chosen.
	pageInstances      = "instances"
	pageInstanceDetail = "instance-detail"
	pageTalosClusters  = "talosclusters"
	pageTalosCluster   = "taloscluster"
	pageIAM            = "iam"
	pageSettings       = "settings"
)

// defaultTenantNamespace is where GET /app and /app/home land a caller who
// hasn't picked a tenant yet — v1alpha2.DefaultSecretNamespace
// ("kontinuum-system"), the one namespace guaranteed to exist and be worth
// looking at on every install (see pkg/domain/zone.Add's own doc).
const defaultTenantNamespace = v1alpha2.DefaultSecretNamespace

// defaultInstancesPath is defaultTenantNamespace's own instances list URL —
// shared by handleAppRoot/handleHome's redirects and nav.html's own
// "Instances" link default (see renderTenantSwitcher).
const defaultInstancesPath = "/app/kontinuum.sh/namespaces/" + defaultTenantNamespace + "/instances"

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
		"templates/components/icon_cluster.html",
		"templates/components/icon_kubernetes.html",
		"templates/components/icon_shield.html",
		"templates/components/icon_settings.html",
		"templates/components/icon_logout.html",
		"templates/components/icon_book_open_text.html",
		"templates/components/icon_external_link.html",
	}

	files := make([]string, 0, len(shared)+len(content))
	files = append(files, shared...)
	files = append(files, content...)

	// dict is the only template func made available to every page's
	// template tree — see templateDict's own doc for why.
	funcs := template.FuncMap{"dict": templateDict}

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
// and zonesFor. cfg is shown on the settings page and is expected to
// already be redacted (see config.Config.Redact) — Router does not redact it
// itself. The "Add zone" form leaves zonedomain.AddOptions.Domain empty for
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
	pages := map[string]*template.Template{
		pageRegistry: mustParsePage("templates/registry_content.html",
			"templates/components/icon_trash.html", "templates/components/icon_globe.html",
			"templates/components/zone_add_modal.html"),
		pageKontinuum: mustParsePage("templates/kontinuum_content.html",
			"templates/components/icon_chevron_left.html", "templates/components/icon_eye.html",
			"templates/components/icon_eye_off.html", "templates/components/icon_copy.html",
			"templates/components/icon_check.html", "templates/components/icon_key.html",
			"templates/components/icon_info.html", "templates/components/icon_download.html",
			"templates/components/reveal_panel.html", "templates/components/reveal_panel_script.html"),
		pageInstances: mustParsePage("templates/instances_content.html"),
		pageInstanceDetail: mustParsePage("templates/instance_detail_content.html",
			"templates/components/icon_chevron_left.html", "templates/components/icon_info.html",
			"templates/components/icon_ethernet_port.html", "templates/components/icon_list_checks.html"),
		pageTalosClusters: mustParsePage("templates/talosclusters_content.html"),
		pageTalosCluster: mustParsePage("templates/taloscluster_content.html",
			"templates/components/icon_chevron_left.html", "templates/components/icon_info.html",
			"templates/components/icon_list_checks.html",
			"templates/components/icon_key.html", "templates/components/icon_download.html",
			"templates/components/icon_eye.html", "templates/components/icon_eye_off.html",
			"templates/components/icon_copy.html", "templates/components/icon_check.html",
			"templates/components/reveal_panel.html", "templates/components/reveal_panel_script.html",
			"templates/components/copy_snippet.html"),
		pageIAM: mustParsePage("templates/iam_content.html",
			"templates/components/icon_key.html", "templates/components/icon_info.html"),
		pageSettings: mustParsePage("templates/settings_content.html",
			"templates/components/icon_copy.html", "templates/components/icon_download.html",
			"templates/components/icon_eye.html", "templates/components/icon_eye_off.html",
			"templates/components/icon_terminal.html", "templates/components/icon_check.html",
			"templates/components/reveal_panel.html", "templates/components/reveal_panel_script.html",
			"templates/components/copy_snippet.html"),
	}

	return &Router{
		namespacesFor:     namespacesFor,
		kontinuumsFor:     kontinuumsFor,
		zonesFor:          zonesFor,
		pages:             pages,
		version:           version,
		cfg:               cfg,
		authEnabled:       authEnabled,
		invalidateSession: invalidateSession,
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
// defaultInstancesPath. protect wraps the /app/home and /app/settings
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

	mux.Handle("GET "+vendorURLPrefix, vendorHandler())
	mux.HandleFunc("GET /{$}", handleRoot)
	mux.HandleFunc("GET /app", appRoot)
	mux.HandleFunc("GET /app/home", protect(handleAppHome))
	mux.HandleFunc("GET /app/kontinuum.sh/namespaces/{ns}/kontinuums", protect(r.renderRegistry))
	mux.HandleFunc("GET /app/kontinuum.sh/namespaces/{ns}/kontinuums/{name}", protect(r.handleKontinuumDetail))
	mux.HandleFunc("GET /app/kontinuum.sh/namespaces/{ns}/kontinuums/{name}/secret",
		protect(r.handleKontinuumSecretDownload))
	mux.HandleFunc("DELETE /app/kontinuum.sh/namespaces/{ns}/kontinuums/{name}", protect(r.handleDeleteInstance))
	mux.HandleFunc("POST /app/zones/add", protect(r.handleZoneAdd))
	mux.HandleFunc("GET /app/kontinuum.sh/namespaces/{ns}/instances", protect(r.handleInstances))
	mux.HandleFunc("GET /app/kontinuum.sh/namespaces/{ns}/instances/{name}", protect(r.handleInstanceDetail))
	mux.HandleFunc("GET /app/kontinuum.sh/namespaces/{ns}/talosclusters", protect(r.handleTalosClusters))
	mux.HandleFunc("GET /app/kontinuum.sh/namespaces/{ns}/talosclusters/{name}", protect(r.handleTalosClusterDetail))
	mux.HandleFunc("GET /app/kontinuum.sh/namespaces/{ns}/talosclusters/{name}/kubeconfig",
		protect(r.handleTalosClusterKubeconfigDownload))
	mux.HandleFunc("GET /app/iam", protect(r.handleIAM))
	mux.HandleFunc("GET /app/settings", protect(r.handleSettings))
	mux.HandleFunc("GET /app/settings/kubeconfig", protect(r.handleSettingsKubeconfigDownload))
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
	Name      string
	Region    string
	Age       string
	Condition string
	// ConditionOK is whether Condition's own status is True — the zones
	// table's badge template colors on this rather than parsing Condition's
	// "Type=Status" string back apart.
	ConditionOK bool
	Message     string
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
		"Title":       "Registry",
		"ActiveMenu":  "registry",
		"Version":     r.version,
		"Namespace":   namespace,
		"Instances":   instances,
		"Zones":       zones,
		"AuthEnabled": r.authEnabled,
	}

	// The "Add zone" modal's own fragment data — always empty on a plain
	// page load, since only a submission (see handleZoneAdd) ever carries
	// a preserved/error/success state. Merged in here so the same
	// "zone-add-modal-body" template works whether it's embedded on
	// initial page load or swapped in on submit.
	maps.Copy(data, r.zoneAddFormData(zoneAddFields{}, "", ""))

	r.render(writer, request, pageRegistry, data)
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

	err = zones.List(request.Context(), &list)
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
		row := zoneRow{Name: item.Name, Region: item.Spec.Region, Age: formatAge(item.CreationTimestamp.Time)}

		if cond := latestCondition(item.Status.Conditions); cond != nil {
			row.Condition = cond.Type + "=" + string(cond.Status)
			row.ConditionOK = cond.Status == metav1.ConditionTrue
			row.Message = capitalizeFirst(cond.Message)
		}

		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

	return rows, nil
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

// maxZoneAddFormBytes bounds the "Add zone" form's request body — every
// field is a short identifier/address, so this is generous, not tight.
const maxZoneAddFormBytes = 1 << 16

// zoneAddFormData is the "zone-add-modal-body" fragment's template data —
// fields is the (possibly just-submitted) form state, createdZone/formErr
// surface a just-completed submission's outcome. Rendered three times: once
// embedded in the registry page's own initial render (always empty — see
// renderRegistry), and again by handleZoneAdd on every submission.
func (r *Router) zoneAddFormData(fields zoneAddFields, createdZone, formErr string) map[string]any {
	return map[string]any{
		"Region":            fields.region,
		"Zone":              fields.zone,
		"TalosAddress":      fields.talosAddress,
		"TalosVersion":      fields.talosVersion,
		"KubernetesVersion": fields.kubernetesVersion,
		"CreatedZone":       createdZone,
		"Error":             formErr,
	}
}

// zoneAddFields is "Add zone"'s parsed form fields.
type zoneAddFields struct {
	region            string
	zone              string
	talosAddress      string
	talosVersion      string
	kubernetesVersion string
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
	}

	zones, err := r.zonesFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	createdZone, err := zonedomain.Add(request.Context(), zones, zonedomain.AddOptions{
		Region:            fields.region,
		Zone:              fields.zone,
		TalosAddress:      fields.talosAddress,
		TalosVersion:      fields.talosVersion,
		KubernetesVersion: fields.kubernetesVersion,
	})
	if err != nil {
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		r.renderZoneAddModalBody(writer, r.zoneAddFormData(fields, "", err.Error()))

		return
	}

	r.renderZoneAddModalBody(writer, r.zoneAddFormData(zoneAddFields{}, createdZone.Name, ""))
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

	r.render(writer, request, pageKontinuum, kontinuumDetailData(item, secretDataYAML != "", r.version, r.authEnabled))
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
func kontinuumDetailData(
	item v1alpha2.Kontinuum, secretDataReady bool, version string, authEnabled bool,
) map[string]any {
	cfg := item.Status.Config

	return map[string]any{
		"Title":           item.Name,
		"ActiveMenu":      "registry",
		"Version":         version,
		"AuthEnabled":     authEnabled,
		"Name":            item.Name,
		"Namespace":       item.Namespace,
		"Role":            item.Status.Role,
		"Region":          item.Spec.Region,
		"Zone":            item.Spec.Zone,
		"LastHeartbeat":   formatAge(item.Status.LastHeartbeatTime.Time),
		"Age":             formatAge(item.CreationTimestamp.Time),
		"APIVersion":      v1alpha2.GroupVersion().String(),
		"InstanceVersion": item.Status.Version,
		"Addr":            cfg.Server.Addr,
		"StorageBackend":  storageBackendName(cfg.Server.Storage),
		"StorageTarget":   cfg.Server.Storage,
		"SecretName":      item.Status.SecretRef.Name,
		"SecretNamespace": item.Status.SecretRef.Namespace,
		"SecretDataReady": secretDataReady,
		"LogLevel":        cfg.Log.Level,
		"LogFormat":       cfg.Log.Format,
		"OIDCEnabled":     cfg.OIDC.Enabled,
		"OIDCIssuerURL":   cfg.OIDC.IssuerURL,
		"OIDCClientID":    cfg.OIDC.ClientID,
		"OIDCRedirectURL": cfg.OIDC.RedirectURL,
		"OIDCAdminGroups": cfg.OIDC.AdminGroups,
	}
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

// binding is a group-to-role grant rendered as a row on the IAM page — see
// handleIAM. It reflects a live rbacv1.ClusterRoleBinding kontinuum's
// admin-group controller manages (see pkg/domain/adminrbac): Subject is the
// OIDC group, read back from the binding's adminrbac.AdminGroupAnnotation
// rather than its name, since OIDC group names aren't always valid
// Kubernetes object names — see adminrbac's own doc. Name and Age identify
// the underlying ClusterRoleBinding object itself.
type binding struct {
	Name    string
	Subject string
	Role    string
	Age     string
}

// handleIAM is GET /app/iam's handler. When OIDC is configured, it lists
// the live ClusterRoleBindings kontinuum's admin-group controller manages
// (see pkg/domain/adminrbac) — real RBAC objects a cluster-admin can also
// inspect with `kubectl get clusterrolebindings`, rather than rows
// recomputed from cfg.OIDC.AdminGroups.
func (r *Router) handleIAM(writer http.ResponseWriter, request *http.Request) {
	var bindings []binding

	if r.authEnabled {
		var err error

		bindings, err = r.listAdminGroupBindings(writer, request)
		if err != nil {
			return
		}
	}

	r.render(writer, request, pageIAM, map[string]any{
		"Title":       "IAM",
		"ActiveMenu":  "iam",
		"Version":     r.version,
		"AuthEnabled": r.authEnabled,
		"Bindings":    bindings,
	})
}

// listAdminGroupBindings lists the ClusterRoleBindings labeled as managed by
// adminrbac.ManagedByValue and maps them to binding rows, sorted by
// subject. On error, it writes the appropriate HTTP response itself (same
// as renderRegistry) and returns a non-nil error so the caller knows not to
// render the page.
func (r *Router) listAdminGroupBindings(writer http.ResponseWriter, request *http.Request) ([]binding, error) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return nil, err
	}

	var list rbacv1.ClusterRoleBindingList

	err = kontinuums.List(request.Context(), &list,
		client.MatchingLabels{v1alpha2.LabelManagedBy: adminrbac.ManagedByValue})
	if err != nil {
		// Forbidden means the signed-in identity is authenticated but not
		// authorized — the session itself isn't the problem, but there's
		// no reason to keep it either, so send the caller back to sign in
		// rather than leave them stuck on a page they can't use.
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return nil, fmt.Errorf("forbidden: %w", err)
		}

		http.Error(writer, "failed to list admin group bindings: "+err.Error(), http.StatusBadGateway)

		return nil, fmt.Errorf("failed to list admin group bindings: %w", err)
	}

	bindings := make([]binding, 0, len(list.Items))
	for _, item := range list.Items {
		bindings = append(bindings, binding{
			Name:    item.Name,
			Subject: item.Annotations[adminrbac.AdminGroupAnnotation],
			Role:    item.RoleRef.Name,
			Age:     formatAge(item.CreationTimestamp.Time),
		})
	}

	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Subject < bindings[j].Subject })

	return bindings, nil
}

// instanceRow is one api/v1alpha2.Instance object rendered as a row on the
// /app/kontinuum.sh/namespaces/{ns}/instances list — see pageInstances' own
// doc for why this isn't named "instance", which already means something
// else in this file.
type instanceRow struct {
	Name         string
	Namespace    string
	TalosVersion string
	Discovered   bool
	Reason       string
	ClaimedBy    string
	Age          string
}

// instanceRowFrom builds one instances-list row from item.
func instanceRowFrom(item v1alpha2.Instance) instanceRow {
	row := instanceRow{
		Name:         item.Name,
		Namespace:    item.Namespace,
		TalosVersion: item.Status.Talos.Version,
		ClaimedBy:    item.Labels[v1alpha2.LabelClaimedBy],
		Age:          formatAge(item.CreationTimestamp.Time),
	}

	if cond := meta.FindStatusCondition(item.Status.Conditions, instancedomain.DiscoveredConditionType); cond != nil {
		row.Discovered = cond.Status == metav1.ConditionTrue
		row.Reason = cond.Reason
	}

	return row
}

// handleInstances is GET /app/kontinuum.sh/namespaces/{ns}/instances's
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

	r.render(writer, request, pageInstances, map[string]any{
		"Title":       "Instances",
		"ActiveMenu":  "instances",
		"Version":     r.version,
		"AuthEnabled": r.authEnabled,
		"Namespace":   namespace,
		"Instances":   rows,
	})
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

// handleInstanceDetail is GET
// /app/kontinuum.sh/namespaces/{ns}/instances/{name}'s handler — it shows
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
	interfaces := make([]instanceInterfaceRow, 0, len(item.Status.Interfaces))
	for _, iface := range item.Status.Interfaces {
		interfaces = append(interfaces, instanceInterfaceRow{
			Name:       iface.Name,
			MACAddress: iface.MACAddress,
			Addresses:  strings.Join(iface.Addresses, ", "),
		})
	}

	conditions := make([]instanceConditionRow, 0, len(item.Status.Conditions))
	for _, cond := range item.Status.Conditions {
		conditions = append(conditions, instanceConditionRow{
			Type:    cond.Type,
			Status:  string(cond.Status),
			OK:      cond.Status == metav1.ConditionTrue,
			Reason:  cond.Reason,
			Message: capitalizeFirst(cond.Message),
			Age:     formatAge(cond.LastTransitionTime.Time),
		})
	}

	discoverySource := "Bare metal (spec.interfaces)"
	if item.Spec.ProviderRef != nil {
		discoverySource = fmt.Sprintf("%s/%s", item.Spec.ProviderRef.Kind, item.Spec.ProviderRef.Name)
	}

	return map[string]any{
		"Title":           item.Name,
		"ActiveMenu":      "instances",
		"Version":         version,
		"AuthEnabled":     authEnabled,
		"Name":            item.Name,
		"Namespace":       item.Namespace,
		"Age":             formatAge(item.CreationTimestamp.Time),
		"TalosVersion":    item.Status.Talos.Version,
		"Discovered":      meta.IsStatusConditionTrue(item.Status.Conditions, instancedomain.DiscoveredConditionType),
		"DiscoverySource": discoverySource,
		"ClaimedBy":       item.Labels[v1alpha2.LabelClaimedBy],
		"Interfaces":      interfaces,
		"Conditions":      conditions,
	}
}

// talosKubeconfigSecretKey is the key a TalosCluster's own kubeconfig is
// stored under on its status.secretRef Secret — mirrors
// pkg/domain/taloscluster/secrets.go's own unexported kubeconfigKey. Kept as
// a separate copy (rather than exporting that package's constant) since this
// is the only thing pkg/ui needs from that package, and pulling in the
// domain package itself would be a much bigger dependency than one string.
const talosKubeconfigSecretKey = "kubeconfig"

// talosClusterRow is a TalosCluster object rendered as a row on the list
// page — see handleTalosClusters.
type talosClusterRow struct {
	Name              string
	Namespace         string
	ControlPlanePool  string
	TalosVersion      string
	KubernetesVersion string
	Ready             string
	ReadyOK           bool
	Age               string
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
		}

		if cond := conditionOfType(item.Status.Conditions, "Ready"); cond != nil {
			row.Ready = string(cond.Status)
			row.ReadyOK = cond.Status == metav1.ConditionTrue
		}

		clusters = append(clusters, row)
	}

	sort.Slice(clusters, func(i, j int) bool { return clusters[i].Name < clusters[j].Name })

	r.render(writer, request, pageTalosClusters, map[string]any{
		"Title":         "Clusters",
		"ActiveMenu":    "talosclusters",
		"Version":       r.version,
		"AuthEnabled":   r.authEnabled,
		"Namespace":     namespace,
		"TalosClusters": clusters,
	})
}

// conditionOfType returns the entry in conditions whose Type matches
// conditionType, or nil if there is none — used instead of
// renderRegistry/listZoneRows' own latestCondition where the UI wants one
// specific, named condition (e.g. "Ready") rather than whichever changed
// most recently.
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

	conditions := make([]conditionRow, 0, len(cluster.Status.Conditions))
	for _, cond := range cluster.Status.Conditions {
		conditions = append(conditions, conditionRow{
			Type: cond.Type, Status: string(cond.Status), OK: cond.Status == metav1.ConditionTrue,
			Reason: cond.Reason, Message: capitalizeFirst(cond.Message), Age: formatAge(cond.LastTransitionTime.Time),
		})
	}

	data := map[string]any{
		"Title":              cluster.Name,
		"ActiveMenu":         "talosclusters",
		"Version":            version,
		"AuthEnabled":        authEnabled,
		"Name":               cluster.Name,
		"Namespace":          cluster.Namespace,
		"TalosVersion":       cluster.Spec.Talos.Version,
		"KubernetesVersion":  cluster.Spec.Kubernetes.Version,
		"Age":                formatAge(cluster.CreationTimestamp.Time),
		"Pools":              pools,
		"Conditions":         conditions,
		"KubeconfigReady":    len(kubeconfig) > 0,
		"KubeconfigRevealed": revealed,
		"HasZone":            zone.Name != "",
		"ZoneRegion":         zone.Spec.Region,
		"ZoneName":           zone.Spec.Zone,
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

func (r *Router) handleSettings(writer http.ResponseWriter, request *http.Request) {
	data := map[string]any{
		"Title":       "Settings",
		"ActiveMenu":  "settings",
		"Version":     r.version,
		"AuthEnabled": r.authEnabled,
	}

	r.render(writer, request, pageSettings, data)
}

// handleSettingsKubeconfigDownload is GET /app/settings/kubeconfig's
// handler — it serves the same synthetic OIDC-login kubeconfig
// settings_content.html's own kubectl-access card shows, computed by
// kubeconfig() the same way handleSettings itself used to embed directly
// into the page (kontinuum's default is no authentication at all, not "no
// access," so there's always a working kubeconfig here, not just when OIDC
// happens to be enabled). This is now its own endpoint instead, serving two
// different callers: the page's `<a download>` link, and its "Reveal"
// button, which fetches this same endpoint via JS only when clicked and
// renders the response text inline — mirrors
// handleTalosClusterKubeconfigDownload's identical dual role for a
// different kubeconfig.
func (r *Router) handleSettingsKubeconfigDownload(writer http.ResponseWriter, request *http.Request) {
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
