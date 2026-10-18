package ui

import (
	"net"
	"net/http"
	"net/url"
	"strings"
)

// appPathPrefix is the path every UI route lives under.
const appPathPrefix = "/app"

// Mux is the subset of *http.ServeMux RegisterRoutes registers onto —
// satisfied by *http.ServeMux itself and by AppMux.
type Mux interface {
	Handle(pattern string, handler http.Handler)
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

// AppMux registers routes onto a server's shared mux, sending every route
// under /app through FrontendRedirect first when a frontend host is set —
// see NewAppMux.
type AppMux struct {
	outer *http.ServeMux
	app   *http.ServeMux
}

// NewAppMux wraps outer, the server's shared mux. With frontendHost empty
// it registers every route onto outer unchanged. Otherwise /app routes go
// onto a mux of their own, mounted on outer at /app and /app/ behind
// FrontendRedirect, so the UI and its login flow are only ever served on
// frontendHost — the session cookie lives on a single origin — while the
// API stays reachable on every zone's own hostname.
func NewAppMux(outer *http.ServeMux, frontendHost string) *AppMux {
	if frontendHost == "" {
		return &AppMux{outer: outer}
	}

	app := http.NewServeMux()
	handler := FrontendRedirect(frontendHost, app)

	outer.Handle(appPathPrefix, handler)
	outer.Handle(appPathPrefix+"/", handler)

	return &AppMux{outer: outer, app: app}
}

// Handle registers handler for pattern — see NewAppMux for where it lands.
func (m *AppMux) Handle(pattern string, handler http.Handler) {
	if m.app != nil && isAppPattern(pattern) {
		m.app.Handle(pattern, handler)

		return
	}

	m.outer.Handle(pattern, handler)
}

// HandleFunc registers handler for pattern — see NewAppMux for where it
// lands.
func (m *AppMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	m.Handle(pattern, http.HandlerFunc(handler))
}

// isAppPattern reports whether a ServeMux pattern ("[METHOD ]path") is
// under /app.
func isAppPattern(pattern string) bool {
	path := pattern
	if _, rest, found := strings.Cut(pattern, " "); found {
		path = rest
	}

	return path == appPathPrefix || strings.HasPrefix(path, appPathPrefix+"/")
}

// FrontendRedirect redirects every request whose Host isn't frontendHost to
// the same path on https://<frontendHost> with a 308, which keeps the
// method and body, and serves requests already on frontendHost with next.
func FrontendRedirect(frontendHost string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if requestHost(request) == frontendHost {
			next.ServeHTTP(writer, request)

			return
		}

		// Only the path and query are taken from the request; the scheme and
		// host are fixed, so this can't redirect anywhere but frontendHost.
		target := url.URL{
			Scheme: "https", Host: frontendHost, Path: request.URL.Path, RawPath: request.URL.RawPath,
			RawQuery: request.URL.RawQuery,
		}

		writer.Header().Set("Location", target.String())
		writer.WriteHeader(http.StatusPermanentRedirect)
	})
}

// requestHost returns request's Host header without any port.
func requestHost(request *http.Request) string {
	host, _, err := net.SplitHostPort(request.Host)
	if err != nil {
		return request.Host
	}

	return host
}
