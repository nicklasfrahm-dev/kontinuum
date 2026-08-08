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
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha1"
	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/auth"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/addon"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/adminrbac"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/instancepool"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/registry"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/taloscluster"
	"github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
	"github.com/nicklasfrahm/kontinuum/pkg/logging"
	"github.com/nicklasfrahm/kontinuum/pkg/ui"
)

const shutdownTimeout = 10 * time.Second

// NewServeCmd builds the serve command, which starts the Kubernetes-style
// API server.
func NewServeCmd() *cobra.Command {
	defaults := &config.Config{}
	defaults.Defaults()

	addr := defaults.Server.Addr

	storage := defaults.Server.Storage

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

	// Deliberately not cancel()ing ctx here: ListenAndServe derives its own
	// internal runCtx from ctx, and libkapi's Server watches runCtx with a
	// shutdown goroutine that closes the HTTP listener the moment it's
	// canceled — independently of, and unsequenced with, Shutdown's own
	// "stop the controller manager (running registry.Heartbeat, whose
	// deregister call needs a live listener to reach itself over) → run
	// pre-shutdown hooks → only then close the listener" ordering. Canceling
	// ctx early raced that internal watcher ahead of Shutdown's own
	// sequencing, closing the listener out from under an in-flight
	// deregister call. Shutdown cancels the same underlying context itself,
	// at the correct point in that sequence — nothing here needs to. The
	// deferred cancel() above still runs once runServe returns, as a safety
	// net, but by then Shutdown has already finished.
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

	anonymous, err := cfg.ValidateAuthentication()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to validate authentication config: %w", err)
	}

	if anonymous {
		logger.Warn("Starting with anonymous access allowed",
			"reason", "KONTINUUM_INSECURE_ALLOW_ANONYMOUS=true and no OIDC issuer configured")
	}

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

	// rbac.authorization.k8s.io/v1 is needed for the same reason as core/v1
	// above: adminrbac.Runnable's client.Client (built off this scheme) and
	// the UI's own per-request client (kontinuumListerFactory, which the IAM
	// page also uses to list ClusterRoleBindings — see handleIAM) both
	// resolve ClusterRole/ClusterRoleBinding's GVKs against it.
	err = rbacv1.AddToScheme(scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to register rbac.authorization.k8s.io/v1 scheme: %w", err)
	}

	uiRouter := ui.NewRouter(
		namespaceListerFactory(cfg.Server.Addr), kontinuumListerFactory(cfg.Server.Addr, scheme),
		zoneClientFactory(cfg.Server.Addr, scheme),
		version, cfg.Redact(), oidcHandler != nil, invalidateSession)

	registryOpts, err := registryOptions(cfg, logger, scheme)
	if err != nil {
		return nil, err
	}

	// Deliberately not passing libkapi.WithGarbageCollector here, despite
	// several domain controllers already setting owner references that
	// assume something cascades on them (see pkg/domain/zone/add.go,
	// pkg/domain/addon/resources.go, pkg/domain/taloscluster/secrets.go):
	// enabling it took the whole controller manager down, repeatedly
	// failing "conversion webhook for kontinuum.sh/v1alpha2, Kind=Kontinuum
	// ... connection refused" — its own discovery/informer machinery
	// appears to hit Kontinuum's v1alpha1/v1alpha2 conversion webhook
	// (registered by registry.Controller.SetupWithManager) in a way this
	// hasn't been root-caused yet. Revisit once that's understood; until
	// then, owner references are correct metadata (kubectl tree already
	// reads them) that nothing acts on.
	opts := slices.Concat([]libkapi.Option{
		libkapi.WithAddr(cfg.Server.Addr),
		libkapi.WithStorage(cfg.Server.Storage),
		libkapi.WithLogger(logger),
		libkapi.WithServerFactory(customHandlers(uiRouter, oidcHandler)),
	}, authOpts, registryOpts, instanceOptions(logger), instancePoolOptions(logger), talosClusterOptions(logger),
		addonOptions(logger), zoneOptions(cfg, logger), adminRBACOptions(cfg, logger))

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

// instanceOptions builds the libkapi options that wire the zone-join
// build-out's first controller (see pkg/domain/instance) onto the Server:
// WithPostStartHook ensures the four new CRDs (Zone, Instance, InstancePool,
// TalosCluster) exist as soon as the listener is up, before the controller
// manager starts — mirroring registryOptions' own ensureCRD timing, since
// instance.EnsureCRDs has the identical ordering requirement. WithController
// hands the Instance discovery reconciler off to libkapi's own Manager
// lifecycle. Unlike registryOptions, this needs no WithScheme call (scheme
// already carries these kinds — see buildServer's v1alpha2.AddToScheme) and
// no webhook server (none of these four kinds have a conversion webhook).
func instanceOptions(logger *slog.Logger) []libkapi.Option {
	instanceLogger := logger.With("component", "instance")

	controller := instance.NewController(instance.Config{Logger: instanceLogger})

	ensureCRDs := func(ctx context.Context, loopbackConfig *rest.Config) error {
		return instance.EnsureCRDs(ctx, loopbackConfig, instanceLogger)
	}

	return []libkapi.Option{
		libkapi.WithPostStartHook(ensureCRDs),
		libkapi.WithController(controller),
	}
}

// instancePoolOptions builds the libkapi options that wire the InstancePool
// claim reconciler (see pkg/domain/instancepool) onto the Server. No
// WithPostStartHook is needed — instancepool.kontinuum.sh's CRD is already
// ensured by instanceOptions' own ensureCRDs call.
func instancePoolOptions(logger *slog.Logger) []libkapi.Option {
	controller := instancepool.NewController(instancepool.Config{Logger: logger.With("component", "instancepool")})

	return []libkapi.Option{libkapi.WithController(controller)}
}

// talosClusterOptions builds the libkapi options that wire the
// TalosCluster bootstrap/addons reconciler (see pkg/domain/taloscluster)
// onto the Server. No WithPostStartHook is needed —
// talosclusters.kontinuum.sh's CRD is already ensured by instanceOptions'
// own ensureCRDs call.
func talosClusterOptions(logger *slog.Logger) []libkapi.Option {
	controller := taloscluster.NewController(taloscluster.Config{Logger: logger.With("component", "taloscluster")})

	return []libkapi.Option{libkapi.WithController(controller)}
}

// addonOptions builds the libkapi options that wire the Addon install/
// health-probe reconciler (see pkg/domain/addon) onto the Server. No
// WithPostStartHook is needed — addons.kontinuum.sh's CRD is already
// ensured by instanceOptions' own ensureCRDs call.
func addonOptions(logger *slog.Logger) []libkapi.Option {
	controller := addon.NewController(addon.Config{Logger: logger.With("component", "addon")})

	return []libkapi.Option{libkapi.WithController(controller)}
}

// zoneImage is the kontinuum container image zoneOptions deploys onto
// every joined zone's downstream cluster — matches the image ci.yml pushes
// (see .github/workflows/ci.yml), tagged with this repo's own build
// version so a zone runs the exact same version its own hub does.
const zoneImageRepo = "ghcr.io/nicklasfrahm/kontinuum"

// zoneOptions builds the libkapi options that wire the Zone downstream-
// install reconciler (see pkg/domain/zone) onto the Server. No
// WithPostStartHook is needed — zones.kontinuum.sh's CRD is already
// ensured by instanceOptions' own ensureCRDs call.
func zoneOptions(cfg *config.Config, logger *slog.Logger) []libkapi.Option {
	controller := zone.NewController(zone.Config{
		Logger:     logger.With("component", "zone"),
		ACMEEmail:  cfg.ACME.Email,
		ACMEServer: cfg.ACME.Server,
		Image:      zoneImageRepo + ":" + version,
	})

	return []libkapi.Option{libkapi.WithController(controller)}
}

// adminRBACOptions builds the libkapi options that wire the admin-group
// RBAC reconciler (see pkg/domain/adminrbac) onto the Server: it keeps a
// ClusterRoleBinding for every group in cfg.OIDC.AdminGroups pointing at a
// cluster-admin-equivalent ClusterRole, which the RBAC authorizer (see
// configureOIDC) actually evaluates on every request — see issue #41. Only
// wired when OIDC is configured; with no OIDC there's no notion of an admin
// group to bind, and no authorizer is wired either (see configureOIDC).
func adminRBACOptions(cfg *config.Config, logger *slog.Logger) []libkapi.Option {
	if cfg.OIDC.IssuerURL == "" {
		return nil
	}

	controller := adminrbac.NewController(adminrbac.Config{
		Logger:      logger.With("component", "adminrbac"),
		AdminGroups: cfg.OIDC.AdminGroups,
	})

	return []libkapi.Option{libkapi.WithController(controller)}
}

// displayConfig builds the non-confidential configuration snapshot written
// to status.config on every heartbeat. cfg.Redact() is Config itself with
// Server.Storage's credentials stripped (the unredacted original still goes
// into the Secret status.secretRef points to — see registry.Config.Storage)
// — since Config is defined directly in terms of v1alpha2.KontinuumConfigStatus
// (see pkg/config.Config's doc), the redacted copy converts straight across
// with no field-by-field copying. Only OIDC.Enabled needs deriving first,
// since pkg/config.Load never sets it (see KontinuumOIDCConfigStatus's doc).
func displayConfig(cfg *config.Config) v1alpha2.KontinuumConfigStatus {
	redacted := cfg.Redact()
	redacted.OIDC.Enabled = redacted.OIDC.IssuerURL != ""

	return v1alpha2.KontinuumConfigStatus(redacted)
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

// zoneClientFactory builds a ui.ZoneClientFactory that calls back into this
// same server over loopback HTTP, authenticated as whatever identity ctx
// carries — see kontinuumListerFactory, which this mirrors exactly except
// for its return type: ui.ZoneClientFactory hands the raw client.Client
// straight to pkg/domain/zone.Add rather than a narrowed interface.
func zoneClientFactory(addr string, scheme *runtime.Scheme) ui.ZoneClientFactory {
	return func(ctx context.Context) (ctrlclient.Client, error) {
		restCfg := &rest.Config{Host: localBaseURL(addr)}

		if token := auth.TokenFromContext(ctx); token != "" {
			restCfg.BearerToken = token
		}

		c, err := ctrlclient.New(restCfg, ctrlclient.Options{Scheme: scheme})
		if err != nil {
			return nil, fmt.Errorf("failed to build in-process zone client: %w", err)
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
// Authorization is deny-by-default: libkapi.WithRBACAuthorizer's real RBAC
// authorizer is tried first (evaluating the ClusterRoleBindings
// pkg/domain/adminrbac reconciles, plus any hand-authored
// Role/RoleBinding/ClusterRole/ClusterRoleBinding), falling back to
// system:masters, service accounts, and the groups listed in
// cfg.OIDC.AdminGroups. Server startup fails if AdminGroups is empty, since
// an OIDC deployment with no admin groups configured would lock everyone
// out.
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
		libkapi.WithRBACAuthorizer(libkapi.RBACAuthorizerConfig{AdminGroups: cfg.OIDC.AdminGroups}),
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
func customHandlers(uiRouter *ui.Router, oidcHandler *auth.Handler) libkapi.ServerFactory {
	return func(c *libkapi.Ctx) error {
		mux := c.HTTPMux()

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
