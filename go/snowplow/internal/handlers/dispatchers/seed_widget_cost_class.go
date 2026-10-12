// seed_widget_cost_class.go — #594: the boot seed's widget cost class.
//
// THE DEFECT THIS SHIPS AGAINST. On 057/1.12.42 the boot seed ran its whole
// pipGlobalTimeout (seed_ms 480019 against a 480000 cap) and was cut off with
// nav_units_aborted = 1504 of 5962. #591 deleted #403's pre-resolve widget-seed
// skip — correctly, because that skip was keyed on `userAccessFilter` and so
// skipped exactly the composition-data widgets #561 exists to warm — and that
// re-enabled 1570 (widget × cohort) units on this cluster. 130 of those make a
// real external HTTP fetch, 20 of them to an endpoint measured at 3.8–14.0 s.
//
// WHAT THIS CHANGES: THE ORDER, NOT THE SET. Within an identity rank, widget
// units whose apiRef RESTAction declares an EndpointRef sort AFTER the ones
// that do not; first-nav walk order (NavOrder) is preserved inside each class.
// So when the budget runs out, what is left unseeded is the population that
//
//   - can never publish (no dep edges — which is precisely why
//     setRefreshKeyHeaderUnlessExternal already withholds its refresh-key
//     header), and
//   - carries the bounded external TTL, so a boot-warmed cell expires long
//     before most of those units are ever navigated to,
//
// instead of an arbitrary suffix of the walk.
//
// WHY THIS IS NOT #403'S SKIP RETURNING. #403 DROPPED units; this orders them.
// Nothing is lost when the budget allows, so no later change can quietly come
// to depend on these units being absent — the failure mode recorded in #591,
// where a waiver turned out to be load-bearing on the gate being removed. The
// predicate is also a different, structural one (does the RA dispatch outside
// the apiserver) rather than `userAccessFilter`, which was never a cost signal
// and covered 157 widgets here, 144 of them cheap and internal.
//
// EXACTNESS AND FAILURE DIRECTION. apiref.DeclaresExternalEndpoint makes the
// same informer-only objects.Get and the same conversion apiref.Resolve makes,
// and answers false whenever it cannot prove the declaration. A false means the
// unit keeps its ordinary first-nav position — the pre-#594 behaviour — so a
// missing or denied RESTAction degrades to today's order rather than to a
// wrong one.

package dispatchers

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// seedWidgetIsExternal reports that this widget's seed resolve would dispatch
// against an endpoint outside the apiserver, so its unit belongs at the back of
// its identity rank. See the file header.
func seedWidgetIsExternal(ctx context.Context, w *unstructured.Unstructured) bool {
	if w == nil {
		return false
	}
	ref, err := widgets.GetApiRef(w.Object)
	if err != nil {
		return false
	}
	return apiref.DeclaresExternalEndpoint(ctx, ref)
}

// seedWidgetIsExternalFn is the test seam over the cost-class probe, so a
// falsifier can drive the ordering without standing up informer-backed
// RESTActions. Production never reassigns it.
var seedWidgetIsExternalFn = seedWidgetIsExternal
