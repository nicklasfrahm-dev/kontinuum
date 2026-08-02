// Package ui exposes an HTMX + Tailwind based web UI for kontinuum, mounted
// at /app. It renders kontinuum's Kubernetes-style resources as friendlier
// domain concepts — for now, namespaces are shown as tenants.
package ui

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

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
	pageRegistry = "registry"
	pageInstance = "instance"
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
		"templates/components/icon_registry.html",
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
// already be redacted (see config.Config.Redact) — Router does not redact it
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
		pageRegistry: mustParsePage("templates/registry_content.html",
			"templates/components/icon_trash.html"),
		pageInstance: mustParsePage("templates/instance_content.html",
			"templates/components/icon_server.html", "templates/components/icon_shield.html",
			"templates/components/icon_chevron_left.html", "templates/components/icon_eye.html",
			"templates/components/icon_eye_off.html", "templates/components/icon_copy.html",
			"templates/components/icon_check.html", "templates/components/icon_key.html",
			"templates/components/icon_info.html"),
		pageSettings: mustParsePage("templates/settings_content.html",
			"templates/components/icon_copy.html", "templates/components/icon_download.html",
			"templates/components/icon_eye.html", "templates/components/icon_eye_off.html",
			"templates/components/icon_terminal.html", "templates/components/icon_check.html"),
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
	mux.HandleFunc("GET /app/kontinuums", protect(r.renderRegistry))
	mux.HandleFunc("GET /app/kontinuums/{name}", protect(r.handleInstanceDetail))
	mux.HandleFunc("DELETE /app/kontinuums/{name}", protect(r.handleDeleteInstance))
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

// instance is a Kontinuum object rendered as a registry row in the UI.
type instance struct {
	Name          string
	Role          string
	Region        string
	Zone          string
	LastHeartbeat string
	Age           string
	APIVersion    string
	Version       string
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
			Name:          item.Name,
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

	r.render(writer, pageRegistry, map[string]any{
		"Title":       "Registry",
		"ActiveMenu":  "registry",
		"Version":     r.version,
		"Instances":   instances,
		"AuthEnabled": r.authEnabled,
	})
}

// handleInstanceDetail is GET /app/kontinuums/{name}'s handler — it shows one
// Kontinuum instance's own settings (status.config, status.secretRef),
// sourced from the shared Kontinuum object store rather than this
// process's own local config, so it renders the same regardless of which
// instance's UI you happen to be browsing from.
func (r *Router) handleInstanceDetail(writer http.ResponseWriter, request *http.Request) {
	kontinuums, err := r.kontinuumsFor(request.Context())
	if err != nil {
		http.Error(writer, "failed to build kubernetes client: "+err.Error(), http.StatusInternalServerError)

		return
	}

	var item v1alpha2.Kontinuum

	name := request.PathValue("name")

	err = kontinuums.Get(request.Context(), client.ObjectKey{Name: name}, &item)
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

	r.render(writer, pageInstance, instanceDetailData(item, secretDataYAML, r.version, r.authEnabled))
}

// instanceDetailData builds handleInstanceDetail's template data from item
// and its already-fetched secretDataYAML (see fetchSecretDataYAML) —
// factored out of handleInstanceDetail purely to keep that function short;
// it has no logic of its own beyond field selection.
func instanceDetailData(item v1alpha2.Kontinuum, secretDataYAML, version string, authEnabled bool) map[string]any {
	cfg := item.Status.Config

	return map[string]any{
		"Title":           item.Name,
		"ActiveMenu":      "registry",
		"Version":         version,
		"AuthEnabled":     authEnabled,
		"Name":            item.Name,
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
		"SecretDataYAML":  secretDataYAML,
		"LogLevel":        cfg.Log.Level,
		"LogFormat":       cfg.Log.Format,
		"OIDCEnabled":     cfg.OIDC.Enabled,
		"OIDCIssuerURL":   cfg.OIDC.IssuerURL,
		"OIDCClientID":    cfg.OIDC.ClientID,
		"OIDCRedirectURL": cfg.OIDC.RedirectURL,
		"OIDCAdminGroups": cfg.OIDC.AdminGroups,
	}
}

// fetchSecretDataYAML fetches the Secret backing ref (the instance page's
// status.secretRef) through kontinuums — the same identity-scoped client
// handleInstanceDetail already used to fetch the Kontinuum object itself, so
// a viewer sees the config secret's contents exactly when RBAC would let
// them `kubectl get secret` it directly, with no separate authorization path
// to keep in sync. Returns ("", true) when there is no secret to show or it
// can no longer be found, since either just means the instance page renders
// without a reveal panel rather than that the request failed. The bool
// result is false when the caller should stop: fetchSecretDataYAML has
// already written the error (or forbidden-redirect) response itself.
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

func (r *Router) handleSettings(writer http.ResponseWriter, request *http.Request) {
	// kubeconfig itself branches on whether OIDC is configured (see its own
	// doc) — kontinuum's default is no authentication at all, not "no
	// access," so there's always a working kubeconfig to show here, not
	// just when OIDC happens to be enabled.
	data := map[string]any{
		"Title":       "Settings",
		"ActiveMenu":  "settings",
		"Version":     r.version,
		"AuthEnabled": r.authEnabled,
		"Kubeconfig":  kubeconfig(requestOrigin(request), request.Host, r.cfg.OIDC.IssuerURL, r.cfg.OIDC.ClientID),
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
