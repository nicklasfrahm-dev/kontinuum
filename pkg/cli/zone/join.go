package zone

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	zonedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

// domainEnvVar is read directly from the operator's own shell environment,
// not via pkg/config: pkg/config.Load is only ever called by `kontinuum
// serve` (a hub/worker process reading its own server-side config); "zone
// join" is a client-side command with no server config of its own, and
// Zone.spec.domain's CEL validation requires a non-empty value at create
// time, so it has to come from somewhere the CLI itself can read.
const domainEnvVar = "KONTINUUM_DOMAIN"

const defaultWaitTimeout = 15 * time.Minute

// errMissingDomainEnv is a static sentinel — err113 flags a dynamically
// constructed errors.New/fmt.Errorf call without a wrapped static error.
var errMissingDomainEnv = fmt.Errorf("%s must be set in the environment before running zone join", domainEnvVar)

// JoinFlags is "zone join"'s parsed flag set.
type JoinFlags struct {
	Region            string
	Zone              string
	TalosAddress      string
	TalosVersion      string
	KubernetesVersion string
	Kubeconfig        string
	Context           string
	Wait              bool
	Timeout           time.Duration
}

// NewJoinCmd builds the "zone join" command.
func NewJoinCmd() *cobra.Command {
	flags := JoinFlags{Timeout: defaultWaitTimeout}

	cmd := &cobra.Command{
		Use:   "join",
		Short: "Join a new zone to this kontinuum control plane",
		// Runtime errors (kubeconfig resolution, API errors) shouldn't
		// print the command usage alongside the error.
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return RunZoneJoin(cmd, flags, buildHubClient)
		},
	}

	cmd.Flags().StringVar(&flags.Region, "region", "", "Region this zone belongs to")
	cmd.Flags().StringVar(&flags.Zone, "zone", "", "This zone's own name within --region")
	cmd.Flags().StringVar(&flags.TalosAddress, "talos-address", "",
		"Talos maintenance-mode address of the seed node")
	cmd.Flags().StringVar(&flags.TalosVersion, "talos-version", "",
		"Talos version (defaults to the cluster controller's own pinned default)")
	cmd.Flags().StringVar(&flags.KubernetesVersion, "kubernetes-version", "",
		"Kubernetes version (defaults to the cluster controller's own pinned default)")
	cmd.Flags().StringVar(&flags.Kubeconfig, "kubeconfig", "",
		"Path to the hub kubeconfig (defaults to $KUBECONFIG or ~/.kube/config)")
	cmd.Flags().StringVar(&flags.Context, "context", "",
		"kubeconfig context naming the hub (defaults to current-context)")
	cmd.Flags().BoolVar(&flags.Wait, "wait", false,
		"Block until the zone reports Installed, printing live status")
	cmd.Flags().DurationVar(&flags.Timeout, "timeout", defaultWaitTimeout,
		"How long --wait waits before giving up")

	for _, name := range []string{"region", "zone", "talos-address"} {
		_ = cmd.MarkFlagRequired(name)
	}

	return cmd
}

// hubClientBuilder builds a client.Client against the hub apiserver — the
// seam RunZoneJoin's tests inject a fake through, in place of the
// production buildHubClient.
type hubClientBuilder func(kubeconfigPath, contextOverride string) (client.Client, error)

// RunZoneJoin is "zone join"'s implementation: it reads KONTINUUM_DOMAIN
// from the environment, builds a client against the hub apiserver, and
// creates the zone's four hub-side objects via the shared
// pkg/domain/zone.Apply fan-out — the same function a future UI join form
// calls, so the two never construct these objects differently.
func RunZoneJoin(cmd *cobra.Command, flags JoinFlags, buildClient hubClientBuilder) error {
	domain := os.Getenv(domainEnvVar)
	if domain == "" {
		return errMissingDomainEnv
	}

	hubClient, err := buildClient(flags.Kubeconfig, flags.Context)
	if err != nil {
		return err
	}

	createdZone, err := zonedomain.Apply(cmd.Context(), hubClient, zonedomain.JoinOptions{
		Region:            flags.Region,
		Zone:              flags.Zone,
		Domain:            domain,
		TalosAddress:      flags.TalosAddress,
		TalosVersion:      flags.TalosVersion,
		KubernetesVersion: flags.KubernetesVersion,
	})
	if err != nil {
		return fmt.Errorf("failed to join zone: %w", err)
	}

	_, err = fmt.Fprintf(cmd.OutOrStdout(), "Created zone %q (region=%s zone=%s)\n",
		createdZone.Name, flags.Region, flags.Zone)
	if err != nil {
		return fmt.Errorf("failed to print join result: %w", err)
	}

	if !flags.Wait {
		return nil
	}

	return waitForZone(cmd, hubClient, createdZone.Name, flags.Timeout)
}

// errWaitTimedOut is returned when --wait's timeout expires before the zone
// reports Installed.
var errWaitTimedOut = errors.New("timed out waiting for zone to become installed")
