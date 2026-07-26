package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/kommodity-io/kommodity/pkg/libkapi"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/auth"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
	"github.com/nicklasfrahm/kontinuum/pkg/logging"
	"github.com/nicklasfrahm/kontinuum/pkg/ui"
)

const shutdownTimeout = 10 * time.Second

// NewServeCmd builds the serve command, which starts the Kubernetes-style
// API server.
func NewServeCmd() *cobra.Command {
	defaults := config.Defaults()

	var addr = defaults.Server.Addr

	var storage = defaults.Server.Storage

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the Kubernetes-style API server",
		// Runtime errors (listener failures, storage errors) shouldn't print
		// the command usage alongside the error.
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServe(cmd, addr, storage)
		},
	}

	cmd.Flags().StringVar(&addr, "addr", defaults.Server.Addr,
		"Listener address (e.g. \":8080\")")
	cmd.Flags().StringVar(&storage, "storage", defaults.Server.Storage,
		"Storage connection string (e.g. sqlite://kontinuum.db, postgres://...)")

	return cmd
}

// runServe loads config, builds the libkapi server, and runs it until a
// signal is received or an unrecoverable error occurs.
func runServe(cmd *cobra.Command, addr string, storage string) error {
	cfg, logger, err := loadServeConfig(cmd, addr, storage)
	if err != nil {
		return err
	}

	// sigChan catches SIGINT and SIGTERM so we can log which signal was
	// received before initiating shutdown.
	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	authOpts, oidcHandler, err := configureOIDC(ctx, cfg, logger)
	if err != nil {
		return err
	}

	server, err := buildServer(cfg, logger, authOpts, oidcHandler)
	if err != nil {
		return err
	}

	logger.Info("Kontinuum starting", "addr", cfg.Server.Addr, "storage", cfg.Server.Storage)

	// Run the server in a goroutine so we can watch for signals on the
	// main goroutine and log which signal was received.
	serveErr := make(chan error, 1)

	go func() {
		serveErr <- server.ListenAndServe(ctx)
	}()

	// A select, not a plain <-sigChan: ListenAndServe can fail before any
	// signal ever arrives (e.g. the listener address is already in use),
	// and that error only surfaces on serveErr. Blocking on sigChan alone
	// would leave the process hanging forever on a startup failure instead
	// of exiting with it.
	select {
	case err = <-serveErr:
		if err != nil {
			return fmt.Errorf("server exited with error: %w", err)
		}

		return nil
	case sig := <-sigChan:
		logger.Info("Received signal, shutting down", "signal", sig.String())
	}

	cancel()

	err = shutdownServer(server, logger)
	if err != nil {
		<-serveErr

		return err
	}

	err = <-serveErr
	if err != nil {
		return fmt.Errorf("server exited with error: %w", err)
	}

	return nil
}

// loadServeConfig loads config from environment variables, applies flag
// overrides, and creates the logger.
func loadServeConfig(cmd *cobra.Command, addr string, storage string) (*config.Config, *slog.Logger, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Flags override config (env vars) when explicitly set.
	if cmd.Flags().Changed("addr") {
		cfg.Server.Addr = addr
	}

	if cmd.Flags().Changed("storage") {
		cfg.Server.Storage = storage
	}

	level, err := logging.ParseLevel(cfg.Log.Level)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse log level: %w", err)
	}

	format, err := logging.ParseFormat(cfg.Log.Format)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse log format: %w", err)
	}

	logger := logging.New(level, format, os.Stdout)

	return cfg, logger, nil
}

// buildServer creates the libkapi server with custom handlers. authOpts and
// oidcHandler come from configureOIDC; oidcHandler is nil when OIDC is not
// configured.
func buildServer(
	cfg *config.Config, logger *slog.Logger, authOpts []libkapi.Option, oidcHandler *auth.Handler,
) (*libkapi.Server, error) {
	// oidcHandler is nil when OIDC isn't configured — leave invalidateSession
	// nil too rather than binding a method value to a nil receiver.
	var invalidateSession ui.SessionInvalidator
	if oidcHandler != nil {
		invalidateSession = oidcHandler.InvalidateSession
	}

	scheme := runtime.NewScheme()

	err := v1alpha2.AddToScheme(scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to register kontinuum.sh/v1alpha2 scheme: %w", err)
	}

	// v1alpha1 must be in the same scheme too — not because anything here
	// constructs v1alpha1 objects, but because the conversion webhook
	// handler (registered by registry.Controller.SetupWithManager) resolves
	// both GVKs against this scheme to convert between them.
	err = v1alpha1.AddToScheme(scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to register kontinuum.sh/v1alpha1 scheme: %w", err)
	}

	// core/v1 is needed too: mgr.GetClient() (built off this same scheme —
	// see registryOptions/WithScheme) is what Heartbeat uses to create the
	// Namespace and Secret backing status.secretRef, and a
	// controller-runtime client can't handle a type its scheme doesn't
	// recognize even though the server itself already serves core/v1.
	err = corev1.AddToScheme(scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to register core/v1 scheme: %w", err)
	}

	uiRouter := ui.NewRouter(
		namespaceListerFactory(cfg.Server.Addr), kontinuumListerFactory(cfg.Server.Addr, scheme),
		version, config.Redact(*cfg), oidcHandler != nil, invalidateSession)

	registryOpts, err := registryOptions(cfg, logger, scheme)
	if err != nil {
		return nil, err
	}

	opts := slices.Concat([]libkapi.Option{
		libkapi.WithAddr(cfg.Server.Addr),
		libkapi.WithStorage(cfg.Server.Storage),
		libkapi.WithLogger(logger),
		libkapi.WithHTTPHandlerFactory(customHandlers(uiRouter, oidcHandler)),
	}, authOpts, registryOpts)

	// Storage is resolved against a background context so the backend
	// is only torn down by Server.Shutdown, not by the signal context
	// that drives ListenAndServe.
	server, err := libkapi.New(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to build server: %w", err)
	}

	return server, nil
}

// registryOptions builds the libkapi options that wire kontinuum's server
// registry (see pkg/domain/registry) onto the Server: WithScheme registers
// Kontinuum's GVK so the registry controller's watches (and EnsureCRD's own
// client) resolve; WithPostStartHook ensures the kontinuums.kontinuum.sh CRD
// exists as soon as the listener is up, before the controller manager
// starts — see registry.EnsureCRD's doc for why that timing matters;
// WithWebhookServer provisions the TLS-serving webhook the CRD's
// v1alpha1<->v1alpha2 conversion (registered by Controller.SetupWithManager)
// answers on; and WithController hands the TTL reconciler and heartbeat
// runnable off to libkapi's own Manager lifecycle, started once the server
// is serving, stopped before the HTTP listener closes on Shutdown.
func registryOptions(cfg *config.Config, logger *slog.Logger, scheme *runtime.Scheme) ([]libkapi.Option, error) {
	role, err := registry.Role(cfg.Server.Region, cfg.Server.Zone)
	if err != nil {
		return nil, fmt.Errorf("failed to determine server registry role: %w", err)
	}

	// Provisioned here, before ListenAndServe, rather than left to
	// libkapi's own (later, internal) webhook cert generation — see
	// registry.EnsureConversionWebhookCert's doc for why the ordering
	// matters: the CRD's conversion webhook clientConfig needs this cert's
	// bytes on its very first apply.
	caBundle, err := registry.EnsureConversionWebhookCert()
	if err != nil {
		return nil, fmt.Errorf("failed to provision conversion webhook certificate: %w", err)
	}

	registryLogger := logger.With("component", "registry")

	controller := registry.NewController(registry.Config{
		Role:          role,
		Region:        cfg.Server.Region,
		Zone:          cfg.Server.Zone,
		Logger:        registryLogger,
		Version:       version,
		Storage:       cfg.Server.Storage,
		DisplayConfig: displayConfig(cfg),
	})

	ensureCRD := func(ctx context.Context, loopbackConfig *rest.Config) error {
		return registry.EnsureCRD(ctx, loopbackConfig, caBundle, registryLogger)
	}

	return []libkapi.Option{
		libkapi.WithScheme(scheme),
		libkapi.WithPostStartHook(ensureCRD),
		libkapi.WithController(controller),
		libkapi.WithWebhookServer(libkapi.WebhookConfig{Port: registry.ConversionWebhookPort}),
	}, nil
}

// displayConfig builds the non-confidential configuration snapshot written
// to status.config on every heartbeat — see v1alpha2.KontinuumConfigStatus.
// cfg.Log and cfg.OIDC are already that same status type (see
// pkg/config.Config's doc), so they're copied through as-is; only Server
// needs translating — Addr copies straight across, Storage is redacted
// (config.RedactStorage) rather than omitted, since the unredacted original
// still goes into the Secret status.secretRef points to (see
// registry.Config.Storage) — and OIDC.Enabled needs deriving, since
// pkg/config.Load never sets it (see KontinuumOIDCConfigStatus's doc).
func displayConfig(cfg *config.Config) v1alpha2.KontinuumConfigStatus {
	oidc := cfg.OIDC
	oidc.Enabled = oidc.IssuerURL != ""

	return v1alpha2.KontinuumConfigStatus{
		Server: v1alpha2.KontinuumServerConfigStatus{
			Addr:    cfg.Server.Addr,
			Storage: config.RedactStorage(cfg.Server.Storage),
		},
		Log:  cfg.Log,
		OIDC: oidc,
	}
}

// namespaceListerFactory builds a ui.NamespaceListerFactory that calls back
// into this same server over loopback HTTP, authenticated as whatever
// identity ctx carries (see pkg/auth.TokenFromContext). This way the UI's
// own namespace listing runs as the signed-in browser user — subject to the
// same authorizer as any other client — instead of through a separate,
// privileged internal client.
func namespaceListerFactory(addr string) ui.NamespaceListerFactory {
	return func(ctx context.Context) (ui.NamespaceLister, error) {
		restCfg := &rest.Config{Host: localBaseURL(addr)}

		if token := auth.TokenFromContext(ctx); token != "" {
			restCfg.BearerToken = token
		}

		clientset, err := kubernetes.NewForConfig(restCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to build in-process kubernetes client: %w", err)
		}

		return clientset.CoreV1().Namespaces(), nil
	}
}

// kontinuumListerFactory builds a ui.KontinuumClientFactory that calls back
// into this same server over loopback HTTP, authenticated as whatever
// identity ctx carries — see namespaceListerFactory, which this mirrors.
// scheme must already have kontinuum.sh/v1alpha2 registered (see
// v1alpha2.AddToScheme) so the controller-runtime client can resolve
// Kontinuum's GVK.
func kontinuumListerFactory(addr string, scheme *runtime.Scheme) ui.KontinuumClientFactory {
	return func(ctx context.Context) (ui.KontinuumClient, error) {
		restCfg := &rest.Config{Host: localBaseURL(addr)}

		if token := auth.TokenFromContext(ctx); token != "" {
			restCfg.BearerToken = token
		}

		c, err := ctrlclient.New(restCfg, ctrlclient.Options{Scheme: scheme})
		if err != nil {
			return nil, fmt.Errorf("failed to build in-process kontinuum client: %w", err)
		}

		return c, nil
	}
}

// configureOIDC builds the resource-server bearer-token authenticator and
// admin-group authorizer, plus the browser PKCE login handler, from
// cfg.OIDC. Both return values are nil when cfg.OIDC.IssuerURL is empty,
// matching kontinuum's default of no authentication. The issuer's discovery
// document is fetched from ctx, so startup fails fast if the issuer is
// unreachable or misconfigured.
//
// Authorization is deny-by-default: only system:masters, service accounts,
// and the groups listed in cfg.OIDC.AdminGroups get access — see
// libkapi.WithAdminAuthorizer. Server startup fails if AdminGroups is empty,
// since an OIDC deployment with no admin groups configured would lock
// everyone out.
func configureOIDC(
	ctx context.Context, cfg *config.Config, logger *slog.Logger,
) ([]libkapi.Option, *auth.Handler, error) {
	if cfg.OIDC.IssuerURL == "" {
		return nil, nil, nil
	}

	oidcHandler, err := auth.NewHandler(ctx, auth.Config{
		IssuerURL:   cfg.OIDC.IssuerURL,
		ClientID:    cfg.OIDC.ClientID,
		RedirectURL: cfg.OIDC.RedirectURL,
	}, logger.With("component", "oidc"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to configure oidc login flow: %w", err)
	}

	authOpts := []libkapi.Option{
		libkapi.WithOIDC(libkapi.OIDCConfig{
			IssuerURL: cfg.OIDC.IssuerURL,
			ClientID:  cfg.OIDC.ClientID,
		}),
		libkapi.WithAdminAuthorizer(libkapi.AdminAuthorizerConfig{
			AdminGroups: cfg.OIDC.AdminGroups,
		}),
	}

	return authOpts, oidcHandler, nil
}

// shutdownServer gracefully stops the HTTP listener, the apiserver's
// background run loop, and the storage backend.
func shutdownServer(server *libkapi.Server, logger *slog.Logger) error {
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	err := server.Shutdown(shutdownCtx)
	if err != nil && !errors.Is(err, libkapi.ErrServerNotStarted) {
		logger.Error("Graceful shutdown failed", "error", err)

		return fmt.Errorf("failed to shutdown server: %w", err)
	}

	return nil
}

// customHandlers mounts the /app UI alongside the built API server. Any
// request that does not match a registered route falls through to the
// Kubernetes API server's own handler. oidcHandler is nil when OIDC is not
// configured, leaving the UI unprotected.
func customHandlers(uiRouter *ui.Router, oidcHandler *auth.Handler) libkapi.HTTPHandlerFactory {
	return func(mux *http.ServeMux) error {
		var appRoot http.HandlerFunc

		var protect func(http.HandlerFunc) http.HandlerFunc

		if oidcHandler != nil {
			appRoot = oidcHandler.HandleApp
			protect = oidcHandler.Protect
			mux.HandleFunc("GET /app/login", oidcHandler.HandleLogin)
			mux.HandleFunc("GET /app/logout", oidcHandler.HandleLogout)
		}

		uiRouter.RegisterRoutes(mux, appRoot, protect)

		return nil
	}
}

// localBaseURL derives the loopback URL the in-process Kubernetes client
// uses to reach the server the UI is mounted on, e.g. ":8080" ->
// "http://127.0.0.1:8080". A missing or wildcard host (":8080",
// "0.0.0.0:8080") is rewritten to the loopback address since the listener
// isn't guaranteed to be reachable there.
func localBaseURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	return "http://" + net.JoinHostPort(host, port)
}
