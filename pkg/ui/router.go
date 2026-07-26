// Package ui exposes an HTMX + Tailwind based web UI for kontinuum, mounted
// at /app. It renders kontinuum's Kubernetes-style resources as friendlier
// domain concepts — for now, namespaces are shown as tenants.
package ui

import (
	"bytes"
	"context"
	"embed"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/auth"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
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

// KontinuumClient is the subset of the Kubernetes API the UI needs to list
// and delete registered kontinuum instances (see pkg/domain/registry, which
// owns the kontinuums.kontinuum.sh CRD and the objects it acts on). It is
// satisfied by a controller-runtime client.Client.
type KontinuumClient interface {
	List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error
	Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error
}

// KontinuumClientFactory builds a KontinuumClient scoped to ctx — see
// NamespaceListerFactory, which follows the same per-request-identity
// pattern.
type KontinuumClientFactory func(ctx context.Context) (KontinuumClient, error)

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
	pageHome     = "home"
	pageTopology = "topology"
	pageSettings = "settings"
)

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
		"templates/components/icon_tenants.html",
		"templates/components/icon_topology.html",
		"templates/components/icon_settings.html",
		"templates/components/icon_logout.html",
	}

	files := make([]string, 0, len(shared)+len(content))
	files = append(files, shared...)
	files = append(files, content...)

	return template.Must(template.New("").ParseFS(templatesFS, files...))
}

// Router handles HTTP routing for the /app UI.
type Router struct {
	namespacesFor     NamespaceListerFactory
	kontinuumsFor     KontinuumClientFactory
	pages             map[string]*template.Template
	version           string
	cfg               config.Config
	authEnabled       bool
	invalidateSession SessionInvalidator
}

// NewRouter creates a new UI router backed by namespacesFor and
// kontinuumsFor. cfg is shown on the settings page and is expected to
// already be redacted (see config.Redact) — Router does not redact it
// itself. authEnabled shows or hides the nav's logout link; pass true only
// when a /app/logout route is actually registered (see pkg/auth), since
// otherwise the link would 404. invalidateSession may be nil (see
// SessionInvalidator).
func NewRouter(
	namespacesFor NamespaceListerFactory, kontinuumsFor KontinuumClientFactory,
	version string, cfg config.Config, authEnabled bool, invalidateSession SessionInvalidator,
) *Router {
	pages := map[string]*template.Template{
		pageHome: mustParsePage("templates/home_content.html"),
		pageTopology: mustParsePage("templates/topology_content.html",
			"templates/components/icon_trash.html"),
		pageSettings: mustParsePage("templates/settings_content.html",
			"templates/components/icon_copy.html", "templates/components/icon_download.html",
			"templates/components/icon_eye.html", "templates/components/icon_eye_off.html",
			"templates/components/icon_terminal.html", "templates/components/icon_server.html",
			"templates/components/icon_shield.html", "templates/components/icon_check.html"),
	}

	return &Router{
		namespacesFor:     namespacesFor,
		kontinuumsFor:     kontinuumsFor,
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
// /app/home. protect wraps the /app/home and /app/settings handlers; nil
// leaves them unprotected. See pkg/auth for kontinuum's OIDC login flow,
// which supplies both when OIDC is configured.
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
	mux.HandleFunc("GET /app/home", protect(r.handleHome))
	mux.HandleFunc("GET /app/topology", protect(r.renderTopology))
	mux.HandleFunc("DELETE /app/topology/{name}", protect(r.handleDeleteInstance))
	mux.HandleFunc("GET /app/settings", protect(r.handleSettings))
}

func handleRoot(writer http.ResponseWriter, request *http.Request) {
	if !acceptsHTML(request) {
		http.NotFound(writer, request)

		return
	}

	http.Redirect(writer, request, "/app", http.StatusFound)
}

func handleAppRoot(writer http.ResponseWriter, request *http.Request) {
	http.Redirect(writer, request, "/app/home", http.StatusFound)
}

// acceptsHTML reports whether request's Accept header prefers HTML, the
// signal real browsers send for top-level navigations. Kubernetes API
// clients ask for application/json or the apidiscovery media types instead.
func acceptsHTML(request *http.Request) bool {
	return strings.Contains(request.Header.Get("Accept"), "text/html")
}

// tenant is a namespace rendered as a tenant row in the UI.
type tenant struct {
	Name   string
	Status string
	Age    string
}

func (r *Router) handleHome(writer http.ResponseWriter, request *http.Request) {
	namespaces, err := r.namespacesFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	list, err := namespaces.List(request.Context(), metav1.ListOptions{})
	if err != nil {
		// Forbidden means the signed-in identity is authenticated but not
		// authorized — the session itself isn't the problem, but there's
		// no reason to keep it either, so send the caller back to sign in
		// rather than leave them stuck on a page they can't use.
		if apierrors.IsForbidden(err) && r.invalidateSession != nil {
			r.invalidateSession(writer, request, auth.MapError(err))

			return
		}

		http.Error(writer, "failed to list namespaces: "+err.Error(), http.StatusBadGateway)

		return
	}

	tenants := make([]tenant, 0, len(list.Items))
	for _, ns := range list.Items {
		tenants = append(tenants, tenant{
			Name:   ns.Name,
			Status: string(ns.Status.Phase),
			Age:    formatAge(ns.CreationTimestamp.Time),
		})
	}

	sort.Slice(tenants, func(i, j int) bool { return tenants[i].Name < tenants[j].Name })

	r.render(writer, pageHome, map[string]any{
		"Title":       "Tenants",
		"ActiveMenu":  "home",
		"Version":     r.version,
		"Tenants":     tenants,
		"AuthEnabled": r.authEnabled,
	})
}

// instance is a Kontinuum object rendered as a topology row in the UI.
type instance struct {
	Name   string
	Role   string
	Region string
	Zone   string
	Age    string
}

// handleDeleteInstance deletes the Kontinuum object named by the {name}
// path value, then re-renders the topology page — the same response a GET
// would produce — so the htmx button that triggers this (hx-select'ing
// #topology-content out of the response, same as the page's own polling)
// shows the updated list immediately instead of waiting for the next poll.
// deleteDebounce is how long handleDeleteInstance waits after a successful
// Delete before re-rendering the topology page. A live instance re-registers
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

	target := &v1alpha2.Kontinuum{ObjectMeta: metav1.ObjectMeta{Name: request.PathValue("name")}}

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

	r.renderTopology(writer, request)
}

// renderTopology is GET /app/topology's handler — it lists Kontinuum
// instances and renders the topology page. Also called by
// handleDeleteInstance after a delete, so both the page's own polling and a
// delete action produce byte-identical #topology-content.
func (r *Router) renderTopology(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	var list v1alpha2.KontinuumList

	err = kontinuums.List(request.Context(), &list)
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
			Name:   item.Name,
			Role:   item.Status.Role,
			Region: item.Spec.Region,
			Zone:   item.Spec.Zone,
			Age:    formatAge(item.Status.LastHeartbeatTime.Time),
		})
	}

	sort.Slice(instances, func(i, j int) bool { return instances[i].Name < instances[j].Name })

	r.render(writer, pageTopology, map[string]any{
		"Title":       "Topology",
		"ActiveMenu":  "topology",
		"Version":     r.version,
		"Instances":   instances,
		"AuthEnabled": r.authEnabled,
	})
}

func (r *Router) handleSettings(writer http.ResponseWriter, request *http.Request) {
	data := map[string]any{
		"Title":           "Settings",
		"ActiveMenu":      "settings",
		"Version":         r.version,
		"Addr":            r.cfg.Server.Addr,
		"StorageBackend":  storageBackendName(r.cfg.Server.Storage),
		"StorageTarget":   r.cfg.Server.Storage,
		"LogLevel":        r.cfg.Log.Level,
		"LogFormat":       r.cfg.Log.Format,
		"AuthEnabled":     r.authEnabled,
		"OIDCIssuerURL":   r.cfg.OIDC.IssuerURL,
		"OIDCClientID":    r.cfg.OIDC.ClientID,
		"OIDCAdminGroups": r.cfg.OIDC.AdminGroups,
	}

	if r.authEnabled {
		data["Kubeconfig"] = kubeconfig(requestOrigin(request), request.Host, r.cfg.OIDC.IssuerURL, r.cfg.OIDC.ClientID)
	}

	r.render(writer, pageSettings, data)
}

func (r *Router) render(writer http.ResponseWriter, page string, data any) {
	var buf bytes.Buffer

	err := r.pages[page].ExecuteTemplate(&buf, "layout.html", data)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)

		return
	}

	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(writer)
}
