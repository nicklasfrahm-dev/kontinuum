package ui_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nicklasfrahm/kontinuum/pkg/ui"
)

const testFrontendHost = "kontinuum.example.com"

// serveVia sends method target with Host host through mux.
func serveVia(t *testing.T, mux *http.ServeMux, method, host, target string) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequestWithContext(t.Context(), method, target, nil)
	request.Host = host

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)

	return recorder
}

// newFrontendMux mounts one UI route and one API route through
// ui.NewAppMux for frontendHost.
func newFrontendMux(frontendHost string) *http.ServeMux {
	outer := http.NewServeMux()
	appMux := ui.NewAppMux(outer, frontendHost)

	ok := func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusOK) }

	appMux.HandleFunc("GET /app/home", ok)
	appMux.HandleFunc("POST /app/zones/add", ok)
	appMux.HandleFunc("GET /apis/example", ok)

	return outer
}

func TestAppMuxRedirectsUIToFrontendHost(t *testing.T) {
	t.Parallel()

	mux := newFrontendMux(testFrontendHost)

	recorder := serveVia(t, mux, http.MethodGet, "1a.eu.example.com", "/app/home?tab=zones")
	assert.Equal(t, http.StatusPermanentRedirect, recorder.Code)
	assert.Equal(t, "https://"+testFrontendHost+"/app/home?tab=zones", recorder.Header().Get("Location"))

	// 308 keeps the method, so a form posted to a zone hostname still
	// lands as a POST.
	recorder = serveVia(t, mux, http.MethodPost, "1a.eu.example.com:443", "/app/zones/add")
	assert.Equal(t, http.StatusPermanentRedirect, recorder.Code)

	recorder = serveVia(t, mux, http.MethodGet, testFrontendHost, "/app/home")
	assert.Equal(t, http.StatusOK, recorder.Code)

	recorder = serveVia(t, mux, http.MethodGet, testFrontendHost+":443", "/app/home")
	assert.Equal(t, http.StatusOK, recorder.Code, "a port on the frontend host is still the frontend host")
}

func TestAppMuxLeavesAPIOnZoneHostnames(t *testing.T) {
	t.Parallel()

	recorder := serveVia(t, newFrontendMux(testFrontendHost), http.MethodGet, "1a.eu.example.com", "/apis/example")
	assert.Equal(t, http.StatusOK, recorder.Code)
}

func TestAppMuxWithoutFrontendHostServesEveryHost(t *testing.T) {
	t.Parallel()

	recorder := serveVia(t, newFrontendMux(""), http.MethodGet, "1a.eu.example.com", "/app/home")
	assert.Equal(t, http.StatusOK, recorder.Code)
}
