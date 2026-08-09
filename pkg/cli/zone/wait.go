package zone

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nicklasfrahm/kontinuum/api/v1alpha2"
	zonedomain "github.com/nicklasfrahm/kontinuum/pkg/domain/zone"
)

const pollInterval = 2 * time.Second

// waitForZone polls name's Zone every pollInterval, printing a single
// progressively-updating status line (the condition that most recently
// transitioned) until it reports InstalledConditionType=True or timeout
// elapses. The Zone reconciler's non-terminal states retry forever by
// design (matching every other controller in this repo), so there is no
// other signal to treat as "stop and report failure" — timeout is the only
// bound.
func waitForZone(cmd *cobra.Command, hubClient client.Client, name string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()

	out := cmd.OutOrStdout()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var lastLine string

	for {
		installed, line, err := pollZoneOnce(ctx, hubClient, name)
		if err != nil {
			// The timeout can expire mid-poll (inside the Get call itself,
			// not just between polls) — surface the same clean timeout
			// message either way, rather than a raw wrapped client error
			// that happens to mention "context deadline exceeded".
			if ctx.Err() != nil {
				_, _ = fmt.Fprintln(out)

				return fmt.Errorf("%w: last status was %q", errWaitTimedOut, lastLine)
			}

			return err
		}

		if line != lastLine {
			printStatusLine(out, line)

			lastLine = line
		}

		if installed {
			_, _ = fmt.Fprintln(out)

			return nil
		}

		select {
		case <-ctx.Done():
			_, _ = fmt.Fprintln(out)

			return fmt.Errorf("%w: last status was %q", errWaitTimedOut, lastLine)
		case <-ticker.C:
		}
	}
}

// pollZoneOnce fetches name's Zone once, reporting whether it's Installed
// and a one-line human-readable summary of its most-recently-transitioned
// condition.
func pollZoneOnce(ctx context.Context, hubClient client.Client, name string) (bool, string, error) {
	var zoneObj v1alpha2.Zone

	err := hubClient.Get(ctx, client.ObjectKey{Name: name, Namespace: v1alpha2.DefaultSecretNamespace}, &zoneObj)
	if apierrors.IsNotFound(err) {
		return false, "waiting for zone to appear...", nil
	}

	if err != nil {
		return false, "", fmt.Errorf("failed to get zone %q: %w", name, err)
	}

	cond := latestCondition(zoneObj.Status.Conditions)
	if cond == nil {
		return false, "waiting for zone status...", nil
	}

	installed := cond.Type == zonedomain.InstalledConditionType && cond.Status == metav1.ConditionTrue
	line := fmt.Sprintf("%s=%s (%s): %s", cond.Type, cond.Status, cond.Reason, cond.Message)

	return installed, line, nil
}

// latestCondition returns whichever of conditions most recently
// transitioned, or nil if conditions is empty.
func latestCondition(conditions []metav1.Condition) *metav1.Condition {
	var latest *metav1.Condition

	for index := range conditions {
		cond := &conditions[index]
		if latest == nil || cond.LastTransitionTime.After(latest.LastTransitionTime.Time) {
			latest = cond
		}
	}

	return latest
}

// printStatusLine overwrites the previous status line in place via a
// carriage return — no external TUI dependency needed for a single
// progressively-updating line.
func printStatusLine(out io.Writer, line string) {
	_, _ = fmt.Fprintf(out, "\r\033[K%s", line)
}
