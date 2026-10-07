// partial_discovery.go — #517.
//
// THE DEFECT. discovery.ServerPreferredResources is a PARTIAL-RESULT API: when
// one API group fails it returns every HEALTHY group's resources *alongside*
// *discovery.ErrGroupDiscoveryFailed (client-go@v0.35.3
// discovery/discovery_client.go:597-601, and withRetries at :706-720 hands that
// pair straight back after its retries). Discover treated the non-nil error as
// fatal and discarded `lists` entirely, and handlers/list.go turned it into a
// 500 — so ONE stale aggregated APIService broke GET /list for EVERY category,
// including resources living in perfectly healthy groups. A stale aggregated
// APIService (metrics-server, a custom aggregated API) is the common failure
// mode of aggregated APIs, not an exotic one.
//
// WHAT THE ERROR CARRIES. ErrGroupDiscoveryFailed.Groups is a
// map[schema.GroupVersion]error (discovery_client.go:434-437): it NAMES each
// group/version that failed and the cause. That is the actionable datum — it
// identifies the stale APIService — so it is surfaced, not swallowed.
//
// DISCRIMINATION. discovery.GroupDiscoveryFailedErrorGroups(err)
// (discovery_client.go:462-470) is used rather than
// discovery.IsGroupDiscoveryFailedError(err) (:457-460): the former unwraps via
// errors.As AND yields Groups in one call, while the latter is a bare type
// assertion that a single fmt.Errorf("%w") anywhere in the chain would defeat.
//
// VISIBILITY (decision 1 of the issue). A degraded read that LOOKS complete is
// the under-report shape this repo keeps finding, so the degradation is reported
// three ways and never only by a shorter list:
//   - Discover returns *PartialDiscoveryError next to the healthy resources, so
//     the caller cannot be unaware of it;
//   - handlers/list.go logs it at WARN naming the failed group/versions;
//   - the snowplow_discovery expvar family counts it.
//
// WHY A COUNTER IS WARRANTED, AND WHY IT HAS A DENOMINATOR. A bare
// partial_total would read 0 both when discovery is healthy and when /list is
// never called — a counter whose zero reads as health is not a detector. So the
// family publishes `total` (every Discover call) next to `partial_total` and
// `fatal_total`: partial_total == 0 ∧ total > 0 is real health, total == 0 says
// only "no /list traffic". `failed_groups` carries the group/versions of the
// most recent degradation so an operator can name the stale APIService from
// /debug/vars without tailing logs.
//
// NOT GATED ON cache.Disabled(), unlike the sibling family in
// cached_client_metrics.go:41-49. That gate exists because the SA-discovery
// singleton is a cache-path mechanism and its keys must not appear under
// CACHE_ENABLED=false (the transparent-fallback contract). This family is not a
// cache key: GET /list performs discovery either way — list.go:67 records an
// apiserver fallthrough and goes to the apiserver regardless — so gating it
// would blind the detector in exactly the configuration where /list ALWAYS hits
// discovery.
//
// IDENTITY-FREE. The only strings published here are cluster API group/versions
// from discovery (e.g. "metrics.k8s.io/v1beta1"). Discovery is identity-free, so
// no caller identity, group membership or endpoint reaches /debug/vars from this
// file.
package dynamic

import (
	"errors"
	"expvar"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// PartialDiscoveryError reports that discovery completed for SOME API groups
// and FAILED for others.
//
// It is returned ALONGSIDE a usable result: the resources of every healthy
// group. It is a DEGRADED signal, not a failure. A caller that maps it to a 5xx
// reintroduces #517; discriminate it with AsPartialDiscovery, serve what came
// back, and report the degradation.
type PartialDiscoveryError struct {
	// Groups are the group/versions whose discovery failed, each mapped to its
	// cause, exactly as client-go reported them
	// (discovery.ErrGroupDiscoveryFailed.Groups).
	Groups map[schema.GroupVersion]error
}

// FailedGroupVersions returns the failed group/versions as a sorted
// "group/version" slice, for logging and for the expvar family.
func (e *PartialDiscoveryError) FailedGroupVersions() []string {
	return sortedGroupVersions(e.Groups)
}

// Error names every failed group/version and its cause. The count is explicit
// so a reader cannot mistake this for a total discovery failure.
func (e *PartialDiscoveryError) Error() string {
	parts := make([]string, 0, len(e.Groups))
	for gv, cause := range e.Groups {
		parts = append(parts, fmt.Sprintf("%s: %v", gv, cause))
	}
	sort.Strings(parts)
	return fmt.Sprintf("partial discovery: %d API group/version(s) failed and were skipped: %s",
		len(e.Groups), strings.Join(parts, ", "))
}

// AsPartialDiscovery reports whether err is a partial-discovery degradation —
// i.e. whether the resources returned with it are usable — and hands back the
// failed groups. It unwraps, so a caller that wraps Discover's error keeps the
// discrimination.
func AsPartialDiscovery(err error) (*PartialDiscoveryError, bool) {
	var pd *PartialDiscoveryError
	if errors.As(err, &pd) {
		return pd, true
	}
	return nil, false
}

func sortedGroupVersions(groups map[schema.GroupVersion]error) []string {
	out := make([]string, 0, len(groups))
	for gv := range groups {
		out = append(out, gv.String())
	}
	sort.Strings(out)
	return out
}

// --- the snowplow_discovery expvar family -----------------------------------

// DiscoveryStats is the scrape shape of the snowplow_discovery expvar key.
type DiscoveryStats struct {
	// Total is every Discover call — the DENOMINATOR that makes a zero
	// Partial/Fatal readable as health rather than as absence of traffic.
	Total int64 `json:"total"`
	// Partial counts Discover calls honoured as DEGRADED: a healthy subset was
	// returned and some group/versions were skipped.
	Partial int64 `json:"partial_total"`
	// Fatal counts Discover calls that returned no usable result at all (a
	// transport/auth failure, or a group failure with nothing healthy left).
	// These are the calls GET /list still answers 500 for.
	Fatal int64 `json:"fatal_total"`
	// FailedGroups are the group/versions of the most recent degradation,
	// sorted. Empty until one is observed.
	FailedGroups []string `json:"failed_groups"`
}

var (
	discoveryTotal        atomic.Int64
	discoveryPartialTotal atomic.Int64
	discoveryFatalTotal   atomic.Int64
	// discoveryFailedGroups holds a []string; atomic.Value keeps the scrape
	// race-free against concurrent /list requests.
	discoveryFailedGroups atomic.Value
)

func recordDiscoveryCall() { discoveryTotal.Add(1) }

func recordPartialDiscovery(groups map[schema.GroupVersion]error) {
	discoveryPartialTotal.Add(1)
	storeFailedGroups(groups)
}

func recordFatalDiscovery(groups map[schema.GroupVersion]error) {
	discoveryFatalTotal.Add(1)
	storeFailedGroups(groups)
}

func storeFailedGroups(groups map[schema.GroupVersion]error) {
	if len(groups) == 0 {
		// A transport failure names no group: keep the last known set rather
		// than blanking the one datum that identifies a stale APIService.
		return
	}
	discoveryFailedGroups.Store(sortedGroupVersions(groups))
}

// DiscoveryStatsSnapshot reads the discovery counters. Lazy — called at scrape
// time by the expvar.Func below, and by the #517 arms.
func DiscoveryStatsSnapshot() DiscoveryStats {
	failed, _ := discoveryFailedGroups.Load().([]string)
	return DiscoveryStats{
		Total:        discoveryTotal.Load(),
		Partial:      discoveryPartialTotal.Load(),
		Fatal:        discoveryFatalTotal.Load(),
		FailedGroups: append([]string(nil), failed...),
	}
}

// discoveryMetricsOnce guards expvar.Publish, which panics on a duplicate key.
var discoveryMetricsOnce sync.Once

func init() { registerDiscoveryMetrics() }

func registerDiscoveryMetrics() {
	discoveryMetricsOnce.Do(func() {
		expvar.Publish("snowplow_discovery", expvar.Func(func() any {
			return DiscoveryStatsSnapshot()
		}))
	})
}
