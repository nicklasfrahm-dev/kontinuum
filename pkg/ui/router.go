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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

//go:embed templates/*.html templates/components/*.html
var templatesFS embed.FS

const (
	pageHome     = "home"
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
	namespacesFor NamespaceListerFactory
	pages         map[string]*template.Template
	version       string
	cfg           config.Config
	authEnabled   bool
}

// NewRouter creates a new UI router backed by namespacesFor. cfg is shown on
// the settings page and is expected to already be redacted (see
// config.Redact) — Router does not redact it itself. authEnabled shows or
// hides the nav's logout link; pass true only when a /app/logout route is
// actually registered (see pkg/auth), since otherwise the link would 404.
func NewRouter(namespacesFor NamespaceListerFactory, version string, cfg config.Config, authEnabled bool) *Router {
	pages := map[string]*template.Template{
		pageHome: mustParsePage("templates/home_content.html"),
		pageSettings: mustParsePage("templates/settings_content.html",
			"templates/components/icon_copy.html", "templates/components/icon_download.html",
			"templates/components/icon_eye.html", "templates/components/icon_eye_off.html",
			"templates/components/icon_terminal.html", "templates/components/icon_server.html",
			"templates/components/icon_shield.html", "templates/components/icon_check.html"),
	}

	return &Router{
		namespacesFor: namespacesFor,
		pages:         pages,
		version:       version,
		cfg:           cfg,
		authEnabled:   authEnabled,
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

	mux.HandleFunc("GET /{$}", handleRoot)
	mux.HandleFunc("GET /app", appRoot)
	mux.HandleFunc("GET /app/home", protect(r.handleHome))
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
