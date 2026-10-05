// seed_widget_uaf_precheck.go — #403: skip a widget seed unit whose widgets
// cell the UAF gate is certain to decline.
//
// WHY. The widget seed (seedOneWidget) used to resolve every (widget × cohort)
// unit and only then consult declineWidgetUAFPut. For a widget whose static
// spec.apiRef names a RESTAction that DECLARES a userAccessFilter, the decline is
// already certain before the resolve: widgets.Resolve always calls resolveApiRef
// → apiref.Resolve, which bumps the UAFTouchedSink from the RA's declaration as
// soon as its objects.Get succeeds (bumpUAFSinkIfDeclared), and the gate then
// declines the Put for every identity. On 057 that is 908 resolve-then-discard
// units in every boot's readiness window (snowplow_widgets_uaf_put_declined_total
// at the latch, identical across boots since 10-02).
//
// NON-LOSSY (zero cold navigations). The skipped resolve's only product was the
// widgets cell, which is declined in 100% of these cases; the dep Record, the
// seeded-set Mark and the 4a full-list pin all sit after that Put and never ran
// for a declined unit. The identity-free apistage content cells the resolve could
// touch are warmed under the SA by the Step 7.5 content prewarm, whose harvest is
// exactly each widget's spec.apiRef (phase1_content_prewarm.go), and the GVRs
// inside the RA are registered by the SA discovery walk's own widgets.Resolve —
// the same accepted latency-transfer argument as the restactions seed's
// pre-resolve UAF skip (seedOneRestaction). The pass-scoped SeedResolveMemo entry
// is only consumed by widgets sharing the same apiRef RA, which take this skip
// too.
//
// EXACTNESS. apiref.DeclaresUserAccessFilter makes the SAME objects.Get under the
// SAME cohort ctx and the SAME conversion as apiref.Resolve, and answers false
// whenever it cannot prove the bump. A false falls through to the unchanged
// resolve, so a denied/missing RA is still classified by the resolve (rbac_deny /
// operational counters unchanged) and a nested RA→RA chain is still caught by the
// post-resolve sink gate.

package dispatchers

import (
	"context"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// pipSeedWidgetUAFPreResolveSkipTotal counts widget seed units skipped BEFORE
// the resolve because the apiRef'd RA declares a userAccessFilter. Every one is
// also counted by snowplow_widgets_uaf_put_declined_total (the Put it replaces
// was declined); this one proves the skip, not the resolve, did it. Exposed as
// snowplow_phase1_seed_widget_uaf_preresolve_skip_total (expvar + OTLP).
var pipSeedWidgetUAFPreResolveSkipTotal atomic.Uint64

// Phase1SeedWidgetUAFPreResolveSkips is the OTLP / test accessor.
func Phase1SeedWidgetUAFPreResolveSkips() uint64 { return pipSeedWidgetUAFPreResolveSkipTotal.Load() }

// seedWidgetApiRefDeclaresUAF reports that the widget's resolve is certain to be
// UAF-declined for the identity on ctx (see the file header).
func seedWidgetApiRefDeclaresUAF(ctx context.Context, w *unstructured.Unstructured) bool {
	if w == nil {
		return false
	}
	ref, err := widgets.GetApiRef(w.Object)
	if err != nil {
		return false
	}
	return apiref.DeclaresUserAccessFilter(ctx, ref)
}

// seedWidgetApiRefDeclaresUAFFn is the test seam over the pre-check (the parity
// falsifier disables it to replay the pre-#403 path). Production never
// reassigns it.
var seedWidgetApiRefDeclaresUAFFn = seedWidgetApiRefDeclaresUAF
