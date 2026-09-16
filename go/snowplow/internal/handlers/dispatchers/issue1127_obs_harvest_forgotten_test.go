// issue1127_obs_harvest_forgotten_test.go — 1.12.7 observability:
// snowplow_phase1_harvest_forgotten_total must count harvested entries ACTUALLY
// REMOVED, per harvester, and must not count hook invocations.
//
// WHY THE DISTINCTION IS THE WHOLE ARM. Almost every gone verdict the dep
// tracker derives names a coordinate that was never harvested. A counter wired
// to the hook would therefore climb steadily on a healthy cluster while telling
// an operator nothing about whether any replay was stopped — the same failure
// mode the repair counters were designed to avoid. The counter is only useful
// if a non-zero value means a specific harvested copy stopped being re-resolved
// into L1, so that is what these arms pin.
//
// RED: move either increment from the harvester's forgetCoordinate to the hook
// body in registerHarvesterGoneForgetHook, and the unharvested-coordinate arm
// fails immediately.
package dispatchers

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func obsWidgetGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
}

// obsHarvestWidget harvests one widget under a given pagination tuple through
// the production harvest path.
func obsHarvestWidget(h *navWidgetHarvester, ns, name string, perPage, page int) {
	w := &unstructured.Unstructured{}
	w.SetNamespace(ns)
	w.SetName(name)
	w.SetGroupVersionKind(schema.GroupVersionKind{
		Group: obsWidgetGVR().Group, Version: obsWidgetGVR().Version, Kind: "Flex",
	})
	h.harvestNavWidget(w, obsWidgetGVR(), perPage, page, perPage, page)
}

func TestIssue1127Obs_HarvestForgotten_CountsRemovalsNotHookFirings(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetGoneForgetHooksForTest()
	t.Cleanup(cache.ResetGoneForgetHooksForTest)

	const ns = "krateo-system"
	nav := newNavWidgetHarvester()
	content := newContentPrewarmHarvester()
	// One widget harvested under TWO pagination tuples, so the removal count
	// and the coordinate count differ and the arm can tell them apart.
	obsHarvestWidget(nav, ns, "doomed-flex", -1, -1)
	obsHarvestWidget(nav, ns, "doomed-flex", 5, 1)
	obsHarvestWidget(nav, ns, "live-flex", -1, -1)

	registerHarvesterGoneForgetHook(rePrewarmDeps{navHarv: nav, harvester: content})

	// ── a verdict for a coordinate that was NEVER harvested ──────────────
	before := harvestForgottenNavTotal.Load()
	cache.Deps().OnDelete(obsWidgetGVR(), ns, "never-harvested")
	if got := harvestForgottenNavTotal.Load(); got != before {
		t.Fatalf("RED (1.12.7 obs): a gone verdict for a coordinate that was never harvested moved "+
			"harvest_forgotten_total by %d. The counter is wired to the hook rather than to the "+
			"removal, so on a healthy cluster it would climb with every deletion while proving "+
			"nothing about whether a replay was stopped", got-before)
	}

	// ── a verdict for a HARVESTED coordinate, two entries ────────────────
	before = harvestForgottenNavTotal.Load()
	cache.Deps().OnDelete(obsWidgetGVR(), ns, "doomed-flex")
	if got := harvestForgottenNavTotal.Load() - before; got != 2 {
		t.Fatalf("RED (1.12.7 obs): forgetting a widget harvested under 2 pagination tuples moved "+
			"harvest_forgotten_total by %d, want 2 — the counter must report entries actually "+
			"dropped, which is the number of replays stopped, not coordinates named", got)
	}

	// ── a repeat verdict for the same, now-absent coordinate ─────────────
	before = harvestForgottenNavTotal.Load()
	cache.Deps().OnDelete(obsWidgetGVR(), ns, "doomed-flex")
	if got := harvestForgottenNavTotal.Load(); got != before {
		t.Fatalf("RED (1.12.7 obs): a REPEAT verdict for an already-forgotten coordinate moved the "+
			"counter by %d. A re-delivered event would inflate it without any replay being "+
			"stopped", got-before)
	}
}

// TestIssue1127Obs_HarvestForgotten_SplitsPerHarvester — the two harvesters
// fail independently, so a forget landing on one and not the other must be
// visible. A single merged counter cannot show that.
func TestIssue1127Obs_HarvestForgotten_SplitsPerHarvester(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetGoneForgetHooksForTest()
	t.Cleanup(cache.ResetGoneForgetHooksForTest)

	const ns = "krateo-system"
	nav := newNavWidgetHarvester()
	content := newContentPrewarmHarvester()
	obsHarvestWidget(nav, ns, "split-flex", -1, -1)

	// Harvest a RESTAction apiRef through the production path: a widget CR
	// carrying spec.apiRef is what the walk feeds the content harvester.
	w := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": obsWidgetGVR().Group + "/" + obsWidgetGVR().Version,
		"kind":       "Flex",
		"metadata":   map[string]any{"name": "split-flex", "namespace": ns},
		"spec": map[string]any{
			"apiRef": map[string]any{"name": "split-ra", "namespace": ns},
		},
	}}
	content.harvestApiRef(w)
	if len(content.snapshot()) != 1 {
		t.Fatalf("premise: the apiRef was not harvested (snapshot=%d)", len(content.snapshot()))
	}

	registerHarvesterGoneForgetHook(rePrewarmDeps{navHarv: nav, harvester: content})

	// A widget verdict must move ONLY the nav counter.
	navBefore, apiBefore := harvestForgottenNavTotal.Load(), harvestForgottenApiRefTotal.Load()
	cache.Deps().OnDelete(obsWidgetGVR(), ns, "split-flex")
	if got := harvestForgottenNavTotal.Load() - navBefore; got != 1 {
		t.Fatalf("1.12.7 obs: widget verdict moved the nav counter by %d, want 1", got)
	}
	if got := harvestForgottenApiRefTotal.Load(); got != apiBefore {
		t.Fatalf("RED (1.12.7 obs): a WIDGET verdict moved the apiRef counter by %d. The split exists "+
			"so an operator can see which harvester acted; cross-talk destroys that", got-apiBefore)
	}

	// A RESTAction verdict must move ONLY the apiRef counter.
	navBefore, apiBefore = harvestForgottenNavTotal.Load(), harvestForgottenApiRefTotal.Load()
	cache.Deps().OnDelete(restActionGVR, ns, "split-ra")
	if got := harvestForgottenApiRefTotal.Load() - apiBefore; got != 1 {
		t.Fatalf("RED (1.12.7 obs): a RESTAction verdict moved the apiRef counter by %d, want 1 — "+
			"the content-prewarm harvester's removals are not being counted", got)
	}
	if got := harvestForgottenNavTotal.Load(); got != navBefore {
		t.Fatalf("1.12.7 obs: a RESTAction verdict moved the nav counter by %d, want 0", got-navBefore)
	}
}
