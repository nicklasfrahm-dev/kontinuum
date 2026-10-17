package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	"github.com/nicklasfrahm/kontinuum/pkg/config"
	instancedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/instance"
	zonedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
	"github.com/nicklasfrahm/kontinuum/pkg/fleet"
)

const (
	// defaultFleetFile is the only file up reads without -f. kontinuum.yaml
	// is deliberately not discovered: that name used to be this repo's
	// kubeconfig.
	defaultFleetFile = "kontinuum.yml"
	// defaultKubeconfigOut is where a newly created fleet's kubeconfig goes.
	defaultKubeconfigOut = "kubeconfig.yaml"
	// defaultUpTimeout bounds how long up waits for the fleet to converge.
	defaultUpTimeout = 30 * time.Minute
	// defaultUpPollInterval is how often up checks on the fleet.
	defaultUpPollInterval = 5 * time.Second
	// kubeconfigFileMode keeps the written kubeconfig private.
	kubeconfigFileMode = 0o600
	// globalLeaseName is zonelease.GlobalKey's Lease — see
	// waitForHandOff.
	globalLeaseName = "zonelock-global"
	// exitCodePending is up --dry-run's exit code for pending changes or
	// drift, so a scheduled dry run works as a drift alarm.
	exitCodePending = 2
)

var (
	// ErrPending is RunUp's error for a dry run with changes or drift
	// pending — see exitCodePending.
	ErrPending = errors.New("dry run: changes or drift pending")
	// ErrStorageNotDirect is RunUp's error for storage that doesn't resolve
	// to direct mode, the only one up supports.
	ErrStorageNotDirect = errors.New("kontinuum up needs storage every zone can reach")
	// ErrDuplicateAddress is RunUp's error for one node declared twice in
	// the fleet, after hostname resolution.
	ErrDuplicateAddress = errors.New("instance address is declared more than once")
	// ErrNodeNotInMaintenance is RunUp's error for a new zone's instance
	// that isn't a Talos node in maintenance mode: it belongs to something
	// else, or its state was lost.
	ErrNodeNotInMaintenance = errors.New(
		"not a talos node in maintenance mode: already configured but not part of this fleet, or unreachable")
	// ErrNotConverged is RunUp's error when the fleet doesn't converge
	// within --timeout.
	ErrNotConverged = errors.New("fleet did not converge")
)

// ExitError carries a process exit code other than 1 — see cmd/kontinuum.
type ExitError struct {
	Code int
	Err  error
}

// Error implements error.
func (e *ExitError) Error() string {
	return e.Err.Error()
}

// Unwrap returns the wrapped error.
func (e *ExitError) Unwrap() error {
	return e.Err
}

// UpFlags is up's parsed flag set. Flags only ever cover how up runs:
// what the fleet looks like comes from kontinuum.yml alone.
type UpFlags struct {
	Files         []string
	DryRun        bool
	Prune         bool
	KubeconfigOut string
	Timeout       time.Duration
}

// UpHub is a running in-process hub — see UpDeps.StartHub.
type UpHub interface {
	// Client talks to the hub's own loopback apiserver.
	Client() client.Client
	// Identity is the hub's zonelease holder identity.
	Identity() string
	// Stop shuts the hub down.
	Stop() error
}

// UpDeps are RunUp's seams to the outside world.
type UpDeps struct {
	// StartHub starts an in-process hub against cfg's storage, running the
	// domain controllers only when controllers is set.
	StartHub func(ctx context.Context, cfg *config.Config, controllers bool) (UpHub, error)
	// Probe fails unless address is a Talos node in maintenance mode.
	Probe func(ctx context.Context, address string) error
	// Resolver resolves instance hostnames. Nil means net.DefaultResolver.
	Resolver instancedomain.Resolver
	// StorageResolver resolves the storage host. Nil means
	// net.DefaultResolver.
	StorageResolver config.StorageResolver
	// Sources resolves ${env:} and ${file:} references.
	Sources fleet.Sources
	// Version is the CLI's own version.
	Version string
	// PollInterval defaults to defaultUpPollInterval when zero.
	PollInterval time.Duration
}

// NewUpCmd builds the up command.
func NewUpCmd() *cobra.Command {
	flags := UpFlags{}

	cmd := &cobra.Command{
		Use:   "up",
		Short: "Converge a fleet to a declarative kontinuum.yml",
		Long: "Converge a fleet to a declarative kontinuum.yml: create it on fresh machines, resume an " +
			"interrupted run, add missing zones to a running fleet and report drift. Running it again with " +
			"nothing to do is a no-op. It never prompts, and deletes nothing without --prune.",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return RunUp(cmd.Context(), cmd.OutOrStdout(), flags, productionUpDeps())
		},
	}

	cmd.Flags().StringSliceVarP(&flags.Files, "file", "f", []string{defaultFleetFile},
		"kontinuum.yml to converge to; several (comma-separated or repeated) are merged left to right, "+
			"the right-most winning")
	cmd.Flags().BoolVar(&flags.DryRun, "dry-run", false,
		"Print the merged fleet and the plan, change nothing, and exit 2 if changes or drift are pending")
	cmd.Flags().BoolVar(&flags.Prune, "prune", false,
		"Also remove zones running in the fleet but missing from kontinuum.yml")
	cmd.Flags().StringVar(&flags.KubeconfigOut, "kubeconfig-out", defaultKubeconfigOut,
		"Where a newly created fleet's kubeconfig is written, mode 0600")
	cmd.Flags().DurationVar(&flags.Timeout, "timeout", defaultUpTimeout,
		"How long to wait for the fleet to converge")

	return cmd
}

// loadedFleet is a validated kontinuum.yml and what it maps onto.
type loadedFleet struct {
	fleet   *fleet.Fleet
	cfg     *config.Config
	desired []fleet.DesiredZone
}

// RunUp converges the fleet to flags.Files — see docs/workflows/up.md.
func RunUp(ctx context.Context, out io.Writer, flags UpFlags, deps UpDeps) error {
	if deps.PollInterval <= 0 {
		deps.PollInterval = defaultUpPollInterval
	}

	doc, err := fleet.Load(flags.Files)
	if err != nil {
		return fmt.Errorf("failed to load fleet: %w", err)
	}

	if flags.DryRun {
		err = printMergedFleet(out, doc)
		if err != nil {
			return err
		}
	}

	loaded, err := loadFleet(ctx, doc, deps)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, flags.Timeout)
	defer cancel()

	reader, err := deps.StartHub(ctx, loaded.cfg, false)
	if err != nil {
		return fmt.Errorf("failed to reach storage: %w", err)
	}

	plan, err := planFleet(ctx, reader.Client(), loaded, deps.Version, flags.Prune)
	if err != nil {
		return errors.Join(err, stopHub(reader))
	}

	plan.Print(out)

	if flags.DryRun || !plan.Changes() {
		return finishWithoutChanges(reader, plan, flags.DryRun)
	}

	return applyPlan(ctx, out, flags, deps, loaded, plan, reader)
}

// finishWithoutChanges stops reader for a dry run or a run with nothing to
// apply, returning ErrPending for a dry run with anything pending.
func finishWithoutChanges(reader UpHub, plan fleet.Plan, dryRun bool) error {
	err := stopHub(reader)
	if err != nil {
		return err
	}

	if dryRun && plan.Pending() {
		return &ExitError{Code: exitCodePending, Err: ErrPending}
	}

	return nil
}

// applyPlan creates and prunes zones. Against a fleet running another
// version it only applies the zone objects, through reader, without ever
// running this CLI's own controllers against it. Otherwise it swaps reader
// for a hub running every controller, waits for the fleet to converge and
// for a zone peer to take over fleet-wide work, and writes the kubeconfig
// of a fleet it created.
func applyPlan(
	ctx context.Context, out io.Writer, flags UpFlags, deps UpDeps, loaded *loadedFleet, plan fleet.Plan, reader UpHub,
) error {
	err := checkMaintenanceMode(ctx, deps.Probe, plan.Create)
	if err != nil {
		return errors.Join(err, stopHub(reader))
	}

	if plan.Upgrade(deps.Version) {
		_, _ = fmt.Fprintln(out, "The fleet runs another version: applying zone objects only, without running controllers.")

		return errors.Join(applyZones(ctx, reader.Client(), loaded, plan), stopHub(reader))
	}

	err = stopHub(reader)
	if err != nil {
		return err
	}

	since := time.Now()

	hub, err := deps.StartHub(ctx, loaded.cfg, true)
	if err != nil {
		return fmt.Errorf("failed to start in-process hub: %w", err)
	}

	err = converge(ctx, out, deps, loaded, plan, hub, since)
	if err != nil {
		return errors.Join(err, stopHub(hub))
	}

	err = stopHub(hub)
	if err != nil {
		return err
	}

	if plan.NewFleet {
		return writeKubeconfig(out, flags.KubeconfigOut, loaded)
	}

	return nil
}

// stopHub stops hub.
func stopHub(hub UpHub) error {
	err := hub.Stop()
	if err != nil {
		return fmt.Errorf("failed to stop in-process hub: %w", err)
	}

	return nil
}

// converge applies plan through hub and waits for the fleet to settle.
func converge(
	ctx context.Context, out io.Writer, deps UpDeps, loaded *loadedFleet, plan fleet.Plan, hub UpHub, since time.Time,
) error {
	err := applyZones(ctx, hub.Client(), loaded, plan)
	if err != nil {
		return err
	}

	err = waitForFleet(ctx, out, hub.Client(), loaded.desired, plan.Prune, deps.PollInterval)
	if err != nil {
		return err
	}

	return waitForHandOff(ctx, out, hub.Client(), hub.Identity(), since, deps.PollInterval)
}

// printMergedFleet prints the merged fleet with references unexpanded.
func printMergedFleet(out io.Writer, doc *fleet.Document) error {
	rendered, err := doc.Render()
	if err != nil {
		return fmt.Errorf("failed to render merged fleet: %w", err)
	}

	_, err = fmt.Fprintf(out, "%s---\n", rendered)
	if err != nil {
		return fmt.Errorf("failed to print merged fleet: %w", err)
	}

	return nil
}

// loadFleet expands and validates doc, checks the storage resolves to
// direct mode, and resolves every instance address, rejecting a node
// declared twice anywhere in the fleet.
func loadFleet(ctx context.Context, doc *fleet.Document, deps UpDeps) (*loadedFleet, error) {
	parsed, err := doc.Expand(deps.Sources)
	if err != nil {
		return nil, fmt.Errorf("failed to expand fleet: %w", err)
	}

	err = fleet.Validate(parsed, deps.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to validate fleet: %w", err)
	}

	cfg, err := parsed.ToConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to map fleet onto configuration: %w", err)
	}

	err = requireDirectStorage(ctx, deps.StorageResolver, cfg)
	if err != nil {
		return nil, err
	}

	desired, err := resolveDesired(ctx, deps.Resolver, parsed.Spec.Zones)
	if err != nil {
		return nil, err
	}

	return &loadedFleet{fleet: parsed, cfg: cfg, desired: desired}, nil
}

// requireDirectStorage resolves cfg's storage mode and rejects anything but
// direct mode, naming the override for split-horizon DNS.
func requireDirectStorage(ctx context.Context, resolver config.StorageResolver, cfg *config.Config) error {
	mode, err := config.ResolveStorageMode(ctx, resolver, cfg.Server.Storage,
		config.StorageMode(cfg.Server.ZoneStorageMode))
	if err != nil {
		return fmt.Errorf("failed to resolve storage mode: %w", err)
	}

	if mode != config.StorageModeDirect {
		return fmt.Errorf("%w: spec.storage.uri resolves to %s mode (a private address, sqlite, or an "+
			"unresolvable host); behind split-horizon DNS, set spec.storage.mode: Direct", ErrStorageNotDirect, mode)
	}

	return nil
}

// resolveDesired resolves every zone's instance addresses, the way zone
// add does, and rejects one node declared twice in the fleet — in two
// zones, or twice in one — naming both places.
func resolveDesired(
	ctx context.Context, resolver instancedomain.Resolver, zones []fleet.Zone,
) ([]fleet.DesiredZone, error) {
	desired := make([]fleet.DesiredZone, 0, len(zones))
	seen := map[string]string{}

	for _, zone := range zones {
		resolved := fleet.DesiredZone{Zone: zone, IPs: make([]string, 0, len(zone.Instances))}

		for index, instance := range zone.Instances {
			resolvedIP, _, err := instancedomain.ResolveAddress(ctx, resolver, instance.Address)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve zone %s instance %q: %w", zone.Name(), instance.Address, err)
			}

			place := fmt.Sprintf("zone %s instances[%d] (%s)", zone.Name(), index, instance.Address)
			if first, duplicate := seen[resolvedIP]; duplicate {
				return nil, fmt.Errorf("%w: %s is declared by %s and %s", ErrDuplicateAddress, resolvedIP, first, place)
			}

			seen[resolvedIP] = place

			resolved.IPs = append(resolved.IPs, resolvedIP)
		}

		desired = append(desired, resolved)
	}

	return desired, nil
}

// planFleet observes the fleet through hubClient and plans against it.
func planFleet(
	ctx context.Context, hubClient client.Client, loaded *loadedFleet, version string, prune bool,
) (fleet.Plan, error) {
	observed, err := observeFleet(ctx, hubClient)
	if err != nil {
		return fleet.Plan{}, err
	}

	return fleet.NewPlan(loaded.desired, loaded.cfg, version, observed, prune), nil
}

// observeFleet reads every zone, its seed Instances' addresses and every
// registered Kontinuum from the fleet's shared storage. Storage that can't
// be read is an error, never "no fleet".
func observeFleet(ctx context.Context, hubClient client.Client) (fleet.Observed, error) {
	var zones v1alpha2.ZoneList

	err := hubClient.List(ctx, &zones, client.InNamespace(v1alpha2.KontinuumSystemNamespace))
	if err != nil {
		return fleet.Observed{}, fmt.Errorf("failed to list zones: %w", err)
	}

	observed := fleet.Observed{}

	for _, zoneObj := range zones.Items {
		var instances v1alpha2.InstanceList

		err = hubClient.List(ctx, &instances, client.InNamespace(v1alpha2.KontinuumSystemNamespace),
			client.MatchingLabels{v1alpha2.LabelRegion: zoneObj.Spec.Region, v1alpha2.LabelZone: zoneObj.Spec.Zone})
		if err != nil {
			return fleet.Observed{}, fmt.Errorf("failed to list instances of zone %s: %w", zoneObj.Name, err)
		}

		ips := make([]string, 0, len(instances.Items))
		for _, instance := range instances.Items {
			if len(instance.Spec.Interfaces) > 0 {
				ips = append(ips, instance.Spec.Interfaces[0])
			}
		}

		observed.Zones = append(observed.Zones, fleet.ObservedZone{
			Name: zoneObj.Name, Region: zoneObj.Spec.Region, Zone: zoneObj.Spec.Zone, IPs: ips,
		})
	}

	var kontinuums v1alpha2.KontinuumList

	err = hubClient.List(ctx, &kontinuums)
	if err != nil {
		return fleet.Observed{}, fmt.Errorf("failed to list kontinuums: %w", err)
	}

	observed.Kontinuums = kontinuums.Items

	return observed, nil
}

// checkMaintenanceMode probes every instance of every zone about to be
// created, before anything is, and names each one that isn't a Talos node
// in maintenance mode rather than bootstrapping a partial zone.
func checkMaintenanceMode(
	ctx context.Context, probe func(context.Context, string) error, zones []fleet.DesiredZone,
) error {
	var (
		mutex    sync.Mutex
		group    sync.WaitGroup
		failures []string
	)

	for _, zone := range zones {
		for _, address := range zone.IPs {
			group.Go(func() {
				err := probe(ctx, address)
				if err == nil {
					return
				}

				mutex.Lock()
				defer mutex.Unlock()

				failures = append(failures, fmt.Sprintf("zone %s: %s: %s", zone.Name(), address, err.Error()))
			})
		}
	}

	group.Wait()

	if len(failures) == 0 {
		return nil
	}

	sort.Strings(failures)

	return fmt.Errorf("%w:\n  %s", ErrNodeNotInMaintenance, strings.Join(failures, "\n  "))
}

// applyZones creates plan.Create's zones the way zone add does, and
// removes plan.Prune's through the zone remove path: deleting the Zone,
// whose finalizer tears its cluster down.
func applyZones(ctx context.Context, hubClient client.Client, loaded *loadedFleet, plan fleet.Plan) error {
	spec := loaded.fleet.Spec

	for _, zone := range plan.Create {
		_, err := zonedomain.Add(ctx, hubClient, zonedomain.AddOptions{
			Region:            zone.Region,
			Zone:              zone.Zone.Zone,
			Domain:            spec.DNS.Domain,
			TalosAddresses:    zone.IPs,
			TalosVersion:      spec.Talos.Version,
			KubernetesVersion: spec.Kubernetes.Version,
		})
		if err != nil {
			return fmt.Errorf("failed to add zone %s: %w", zone.Name(), err)
		}
	}

	for _, name := range plan.Prune {
		zoneObj := &v1alpha2.Zone{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace}}

		err := client.IgnoreNotFound(hubClient.Delete(ctx, zoneObj))
		if err != nil {
			return fmt.Errorf("failed to remove zone %s: %w", name, err)
		}
	}

	return nil
}

// waitForFleet waits until every desired zone reports Ready and every
// pruned zone is gone, printing each zone's status as it changes.
func waitForFleet(
	ctx context.Context, out io.Writer, hubClient client.Client, desired []fleet.DesiredZone, pruned []string,
	interval time.Duration,
) error {
	printed := map[string]string{}

	return poll(ctx, interval, func() (bool, error) {
		converged := true

		for _, zone := range desired {
			ready, status, err := zoneStatus(ctx, hubClient, zone.Name())
			if err != nil {
				return false, err
			}

			if printed[zone.Name()] != status {
				_, _ = fmt.Fprintf(out, "zone %s: %s\n", zone.Name(), status)
				printed[zone.Name()] = status
			}

			converged = converged && ready
		}

		for _, name := range pruned {
			err := hubClient.Get(ctx, client.ObjectKey{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace},
				&v1alpha2.Zone{})
			if !apierrors.IsNotFound(err) {
				converged = false
			}
		}

		return converged, nil
	})
}

// zoneStatus reports whether name's Zone is Ready and a one-line status.
func zoneStatus(ctx context.Context, hubClient client.Client, name string) (bool, string, error) {
	var zoneObj v1alpha2.Zone

	err := hubClient.Get(ctx, client.ObjectKey{Name: name, Namespace: v1alpha2.KontinuumSystemNamespace}, &zoneObj)
	if apierrors.IsNotFound(err) {
		return false, "waiting for the zone to appear", nil
	}

	if err != nil {
		return false, "", fmt.Errorf("failed to get zone %s: %w", name, err)
	}

	cond := meta.FindStatusCondition(zoneObj.Status.Conditions, zonedomain.ReadyConditionType)
	if cond == nil {
		return false, "waiting for status", nil
	}

	return cond.Status == metav1.ConditionTrue, fmt.Sprintf("%s (%s) %s", cond.Status, cond.Reason, cond.Message), nil
}

// waitForHandOff waits until a zone peer — any holder other than up's own
// identity — has renewed the fleet-wide lease since up started. up's own
// hub is never granted that lease (see zonelease.Identity.Temporary), so
// this proves the fleet reconciles fleet-wide state with up gone, rather
// than assuming it.
func waitForHandOff(
	ctx context.Context, out io.Writer, hubClient client.Client, identity string, since time.Time,
	interval time.Duration,
) error {
	_, _ = fmt.Fprintln(out, "Waiting for a zone peer to run a fleet-wide reconcile...")

	return poll(ctx, interval, func() (bool, error) {
		var lease coordinationv1.Lease

		err := hubClient.Get(ctx, client.ObjectKey{Name: globalLeaseName, Namespace: v1alpha2.KontinuumSystemNamespace},
			&lease)
		if apierrors.IsNotFound(err) {
			return false, nil
		}

		if err != nil {
			return false, fmt.Errorf("failed to get fleet-wide lease: %w", err)
		}

		holder := ""
		if lease.Spec.HolderIdentity != nil {
			holder = *lease.Spec.HolderIdentity
		}

		renewed := lease.Spec.RenewTime != nil && lease.Spec.RenewTime.After(since)

		return holder != "" && holder != identity && renewed, nil
	})
}

// poll runs check every interval until it reports done, fails, or ctx
// expires — the last reported as ErrNotConverged.
func poll(ctx context.Context, interval time.Duration, check func() (bool, error)) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		done, err := check()
		if err != nil {
			return err
		}

		if done {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrNotConverged, ctx.Err())
		case <-ticker.C:
		}
	}
}

// writeKubeconfig writes a kubeconfig with one context per zone peer,
// <zone>.<region>.<domain>, to path with mode 0600. With OIDC, every
// context authenticates through kubectl oidc-login against the fleet's
// public client, like the UI's own kubeconfig download.
func writeKubeconfig(out io.Writer, path string, loaded *loadedFleet) error {
	kubeconfig := BuildFleetKubeconfig(loaded.fleet)

	data, err := clientcmd.Write(*kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to encode kubeconfig: %w", err)
	}

	err = os.WriteFile(path, data, kubeconfigFileMode)
	if err != nil {
		return fmt.Errorf("failed to write kubeconfig: %w", err)
	}

	_, _ = fmt.Fprintf(out, "Wrote kubeconfig for %d zones to %s\n", len(loaded.fleet.Spec.Zones), path)

	return nil
}

// oidcUserName names the kubeconfig user BuildFleetKubeconfig's contexts
// share when the fleet uses OIDC.
const oidcUserName = "oidc"

// BuildFleetKubeconfig builds a kubeconfig with one context per zone of
// parsed, named <zone>.<region>.<domain> and pointing at it — the API stays
// on the zone hostnames even with a frontend host. The first zone's context
// is the current one.
func BuildFleetKubeconfig(parsed *fleet.Fleet) *clientcmdapi.Config {
	kubeconfig := clientcmdapi.NewConfig()

	user := ""
	if oidc := parsed.Spec.Auth.OIDC; oidc != nil {
		user = oidcUserName

		clientID := oidc.ClientID
		if clientID == "" {
			clientID = "kontinuum"
		}

		kubeconfig.AuthInfos[user] = &clientcmdapi.AuthInfo{Exec: &clientcmdapi.ExecConfig{
			APIVersion: "client.authentication.k8s.io/v1beta1",
			Command:    "kubectl",
			Args: []string{
				"oidc-login", "get-token", "--oidc-issuer-url=" + oidc.IssuerURL, "--oidc-client-id=" + clientID,
				"--oidc-extra-scope=email", "--oidc-extra-scope=groups",
			},
			InteractiveMode: clientcmdapi.NeverExecInteractiveMode,
		}}
	}

	names := make([]string, 0, len(parsed.Spec.Zones))

	for _, zone := range parsed.Spec.Zones {
		name := zone.Zone + "." + zone.Region + "." + parsed.Spec.DNS.Domain
		names = append(names, name)

		kubeconfig.Clusters[name] = &clientcmdapi.Cluster{Server: "https://" + name}
		kubeconfig.Contexts[name] = &clientcmdapi.Context{Cluster: name, AuthInfo: user}
	}

	if len(names) > 0 {
		kubeconfig.CurrentContext = names[0]
	}

	return kubeconfig
}
