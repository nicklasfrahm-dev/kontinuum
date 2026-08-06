package zone

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	zonedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

const defaultWaitTimeout = 15 * time.Minute

// AddFlags is "zone add"'s parsed flag set.
type AddFlags struct {
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

// NewAddCmd builds the "zone add" command.
func NewAddCmd() *cobra.Command {
	flags := AddFlags{Timeout: defaultWaitTimeout}

	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add a new zone to this kontinuum control plane",
		// Runtime errors (kubeconfig resolution, API errors) shouldn't
		// print the command usage alongside the error.
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return RunZoneAdd(cmd, flags, buildHubClient)
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
// seam RunZoneAdd's tests inject a fake through, in place of the
// production buildHubClient.
type hubClientBuilder func(kubeconfigPath, contextOverride string) (client.Client, error)

// RunZoneAdd is "zone add"'s implementation: it builds a client against
// the hub apiserver and creates the zone's four hub-side objects via the
// shared pkg/domain/zone.Add fan-out — the same function the UI's "Add
// zone" form calls, so the two never construct these objects differently.
// Domain is left for Add itself to infer from an already-registered
// Kontinuum (see zonedomain.AddOptions.Domain's own doc) — the CLI has no
// domain of its own to supply.
func RunZoneAdd(cmd *cobra.Command, flags AddFlags, buildClient hubClientBuilder) error {
	hubClient, err := buildClient(flags.Kubeconfig, flags.Context)
	if err != nil {
		return err
	}

	createdZone, err := zonedomain.Add(cmd.Context(), hubClient, zonedomain.AddOptions{
		Region:            flags.Region,
		Zone:              flags.Zone,
		TalosAddress:      flags.TalosAddress,
		TalosVersion:      flags.TalosVersion,
		KubernetesVersion: flags.KubernetesVersion,
	})
	if err != nil {
		return fmt.Errorf("failed to add zone: %w", err)
	}

	_, err = fmt.Fprintf(cmd.OutOrStdout(), "Created zone %q (region=%s zone=%s)\n",
		createdZone.Name, flags.Region, flags.Zone)
	if err != nil {
		return fmt.Errorf("failed to print add result: %w", err)
	}

	if !flags.Wait {
		return nil
	}

	return waitForZone(cmd, hubClient, createdZone.Name, flags.Timeout)
}

// errWaitTimedOut is returned when --wait's timeout expires before the zone
// reports Installed.
var errWaitTimedOut = errors.New("timed out waiting for zone to become installed")
