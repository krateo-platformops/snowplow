// seed_terminal_put_gen_guard_394_test.go — #394 behavioural arms: the
// POST-readyz seed terminal Put must not resurrect a cell that was REMOVED
// during the seed's resolve; the boot seed must stay a plain Put.
//
// Every arm drives the REAL seed primitives (seedOneWidget, seedOneRestaction
// and its real resolve+Put tail seedRestactionResolveAndPutProd) over the REAL
// resolved-L1 store under a real RBAC snapshot. Only the resolver edge is
// seamed (widgetsResolveFn; seedObjectsGetFn for the RESTAction fetch), and the
// REMOVAL is the store's own removal funnel: DeleteForTest → deleteForDep, or a
// genuine LRU eviction (RESOLVED_CACHE_MAX_ENTRIES=1). The keepwarm and
// gvr-discovered arms run through the REAL seedScopeYielding loop, so the
// one-shot re-seed in the seedWidgetTarget / seedRestactionTarget closures is
// exercised too.
//
// This file uses ONLY symbols that exist on origin/main, so it compiles there
// and its arms go RED there (main plain-Puts and resurrects; no re-seed).
package dispatchers

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const s394RAName = "s394-seed-ra"

// s394RATargetGVR is an arbitrary RESTAction target GVR for the loop's
// restaction target enumeration (restActionTargetGVRFn is seamed to return it).
var s394RATargetGVR = schema.GroupVersionResource{Group: "core.krateo.io", Version: "v1", Resource: "s394targets"}

// s394Cohort is the ONE cohort every arm seeds under: the user + the group the
// a1 two-tenant fixture's shared CRB grants on BOTH the widget and the
// RESTAction GVR, so both classes derive a live, non-empty-BindingUID key.
func s394Cohort() seedTarget {
	return seedTarget{BindingUID: "uid-portal-shared", Username: a1Alice, Groups: []string{a1Group}, CollapsedBindings: 1}
}

func s394WidgetEntry() navWidgetEntry {
	return navWidgetEntry{
		W:          h1WidgetUnstructured(map[string]any{}),
		GVR:        h1WidgetGVR,
		PerPage:    -1,
		Page:       -1,
		KeyPerPage: -1,
		KeyPage:    -1,
	}
}

func s394RARef() templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: s394RAName, Namespace: h1NS},
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}
}

// s394FetchedRA is the RESTAction the seamed fetch hands back: no api steps
// (the resolve is hermetic) and a jq filter that stamps WHICH resolve produced
// the body, so the cell's bytes say whether the pre-removal resolve or the
// re-seed won.
func s394FetchedRA(bodyTag string) objects.Result {
	return objects.Result{
		GVR: restActionGVR,
		Unstructured: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": restActionGVR.Group + "/" + restActionGVR.Version,
			"kind":       "RESTAction",
			"metadata":   map[string]any{"name": s394RAName, "namespace": h1NS},
			"spec":       map[string]any{"api": []any{}, "filter": `"` + bodyTag + `"`},
		}},
	}
}

// s394Keys derives the two production keys exactly as the primitives do.
func s394Keys(t *testing.T) (widgetKey, raKey string, handle cacheHandle, wInputs, raInputs *cache.ResolvedKeyInputs) {
	t.Helper()
	cctx := withCohortSeedContext(context.Background(), s394Cohort(), endpoints.Endpoint{}, nil)
	e := s394WidgetEntry()
	widgetKey, handle, wInputs = dispatchCacheLookupKey(cctx, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource,
		h1NS, h1WName, -1, -1, effectiveKeyExtras(cctx, e.W.Object, nil))
	raKey, _, raInputs = dispatchCacheLookupKey(cctx, "restactions",
		restActionGVR.Group, restActionGVR.Version, restActionGVR.Resource,
		h1NS, s394RAName, -1, -1, nil)
	if handle == nil || widgetKey == "" || raKey == "" || wInputs == nil || wInputs.BindingUID == "" ||
		raInputs == nil || raInputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: the cohort must derive live, non-empty-BindingUID widgets + restactions keys "+
			"(else the primitives short-circuit at the #95 guard and no arm can fail); widgetKey=%q raKey=%q", widgetKey, raKey)
	}
	return widgetKey, raKey, handle, wInputs, raInputs
}

// s394Harness wires the resolver-edge seams and counts the resolves.
type s394Harness struct {
	widgetKey, raKey string
	handle           cacheHandle
	widgetResolves   int
	raResolves       int
	// removeOn reports whether the n-th resolve (1-based) of a class must remove
	// that class's cell DURING the resolve (after the seed captured its guard).
	removeOn func(n int) bool
	// remove is the removal itself (default: the real deleteForDep funnel).
	remove func(key string)
	// keyMismatch records a resolve whose ctx carried an L1 key other than the
	// precomputed one (the removal would then target the wrong cell).
	keyMismatch string
}

func (h *s394Harness) install(t *testing.T) {
	t.Helper()
	if h.remove == nil {
		h.remove = func(key string) { cache.ResolvedCache().DeleteForTest(key) }
	}
	origW, origGet, origTail := widgetsResolveFn, seedObjectsGetFn, seedRestactionResolveAndPutFn
	t.Cleanup(func() {
		widgetsResolveFn, seedObjectsGetFn, seedRestactionResolveAndPutFn = origW, origGet, origTail
	})
	widgetsResolveFn = func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		h.widgetResolves++
		if got := cache.L1KeyFromContext(ctx); got != h.widgetKey {
			h.keyMismatch = "widget resolve ctx key " + got
		}
		if h.removeOn(h.widgetResolves) {
			h.remove(h.widgetKey)
		}
		out := h1WidgetUnstructured(map[string]any{})
		if err := unstructured.SetNestedField(out.Object, "widget-resolve-"+itoaLine(h.widgetResolves), "status", "widgetData", "rows"); err != nil {
			t.Fatalf("widget seam: %v", err)
		}
		return out, nil
	}
	seedObjectsGetFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
		return s394FetchedRA("ra-resolve-" + itoaLine(h.raResolves+1))
	}
	// The REAL tail (seedRestactionResolveAndPutProd) runs the real Resolve and
	// the real terminal Put; the wrapper only injects the removal AFTER the
	// guard was captured in seedOneRestaction and BEFORE the tail's Put.
	seedRestactionResolveAndPutFn = func(
		ctx, resCtx context.Context, cr *templatesv1.RESTAction, ref templatesv1.ObjectReference,
		authnNS, key string, handle cacheHandle, inputs *cache.ResolvedKeyInputs, got objects.Result,
		stageErrSink *cache.StageErrorSink, extTouchedSink *cache.ExternalTouchedSink,
	) error {
		h.raResolves++
		if key != h.raKey {
			h.keyMismatch = "restaction tail key " + key
		}
		if h.removeOn(h.raResolves) {
			h.remove(h.raKey)
		}
		return seedRestactionResolveAndPutProd(ctx, resCtx, cr, ref, authnNS, key, handle, inputs, got, stageErrSink, extTouchedSink)
	}
}

// s394PreSeed writes the pre-removal cell a post-readyz seed is about to
// re-resolve. age backdates CreatedAt (keepwarm only re-resolves cells at or
// beyond the age-skip threshold).
func s394PreSeed(handle cacheHandle, key string, inputs *cache.ResolvedKeyInputs, age time.Duration) {
	handle.Put(key, &cache.ResolvedEntry{
		RawJSON:   []byte(`{"body":"pre-removal"}`),
		Inputs:    inputs,
		CreatedAt: time.Now().Add(-age),
	})
}

func s394Body(handle cacheHandle, key string) (string, bool) {
	e, ok := handle.Get(key)
	if !ok || e == nil {
		return "", false
	}
	return string(e.RawJSON), true
}

// s394RunLoop drives the REAL seedScopeYielding over one widget + one
// RESTAction for the single cohort in the given post-readyz mode.
func s394RunLoop(t *testing.T, mode seedScopeMode) {
	t.Helper()
	prevEnum, prevTGVR := enumeratePrewarmTargetsForGVRFn, restActionTargetGVRFn
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn, restActionTargetGVRFn = prevEnum, prevTGVR })
	c := s394Cohort()
	enumeratePrewarmTargetsForGVRFn = func(g schema.GroupVersionResource, _ string) []cache.PrewarmTarget {
		return []cache.PrewarmTarget{{
			BindingUID:        c.BindingUID,
			Subject:           cache.SubjectIdentity{Username: c.Username, Groups: c.Groups},
			GVR:               g,
			Verb:              "list",
			CollapsedBindings: c.CollapsedBindings,
		}}
	}
	restActionTargetGVRFn = func(_ context.Context, _ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
		return s394RATargetGVR, true
	}
	if err := seedScopeYielding(context.Background(), []templatesv1.ObjectReference{s394RARef()},
		[]navWidgetEntry{s394WidgetEntry()}, endpoints.Endpoint{}, nil, h1NS, mode); err != nil {
		t.Fatalf("seedScopeYielding(%v) returned %v; want nil", mode, err)
	}
}

// TestS394_PostReadyzSeed_RemovalDuringResolve_NotResurrected_ReseededOnce is
// arms 1 (keepwarm) and 2 (gvr-discovered).
//
// FIXTURE: both cells (widget + restaction) are resident with a pre-removal
// body (backdated past the keepwarm age-skip threshold, so keepwarm re-resolves
// them). The FIRST resolve of each class removes its own cell through the real
// deleteForDep funnel — after the seed captured its guard, before its Put.
//
//	GREEN (fix): the first terminal PutIfGen is REFUSED (refusal counter +2),
//	  the closure re-seeds ONCE in the same mode (2 resolves per class), and the
//	  cell holds the RE-SEED's body ("…-2"); no operational failure recorded.
//	RED (origin/main): the plain Put RESURRECTS the cell with the body resolved
//	  before the removal ("…-1"), one resolve per class, refusal counter +0.
//
// The "refused twice" sub-arm removes on EVERY resolve (during the re-seed: a
// concurrent fill of the absent cell, then a second DELETE): the fix leaves the cell
// absent after exactly two resolves (one re-seed, then Info + leave alone);
// main resurrects it.
func TestS394_PostReadyzSeed_RemovalDuringResolve_NotResurrected_ReseededOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode seedScopeMode
	}{
		{"keepwarm", seedModeKeepwarm},
		{"gvr-discovered", seedModeGVRDiscovered},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, sub := range []struct {
				name     string
				removeOn func(n int) bool
				twice    bool
			}{
				{"removed-once", func(n int) bool { return n == 1 }, false},
				{"removed-every-resolve", func(int) bool { return true }, true},
			} {
				t.Run(sub.name, func(t *testing.T) {
					engineLatchTestMu.Lock()
					defer engineLatchTestMu.Unlock()
					zeroCustomerInFlight()
					quietLoggingE(t)
					resetFirstNavLatchForTest()
					t.Cleanup(resetFirstNavLatchForTest)

					a1BuildTwoTenantWatcher(t)
					widgetKey, raKey, handle, wIn, raIn := s394Keys(t)
					age := keepwarmAgeSkipThreshold() + keepwarmSweepInterval()/2
					s394PreSeed(handle, widgetKey, wIn, age)
					s394PreSeed(handle, raKey, raIn, age)

					h := &s394Harness{widgetKey: widgetKey, raKey: raKey, handle: handle, removeOn: sub.removeOn}
					if sub.twice {
						// A removal needs a resident cell: at re-seed entry the cell is already
						// gone (the first removal), so model a concurrent writer that fills it
						// during the re-seed resolve and a second DELETE that removes it again.
						h.remove = func(key string) {
							if _, resident := handle.Get(key); !resident {
								inputs := wIn
								if key == raKey {
									inputs = raIn
								}
								handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"body":"concurrent"}`), Inputs: inputs})
							}
							cache.ResolvedCache().DeleteForTest(key)
						}
					}
					h.install(t)

					refusedBefore := cache.ResolvedCache().Stats().PutRefusedGenerationMovedTotal
					opFailBefore := pipSeedOperationalFailTotal.Load()

					s394RunLoop(t, tc.mode)

					if h.keyMismatch != "" {
						t.Fatalf("PRECONDITION: the seed ran under a different key than the precomputed one (%s) — "+
							"the removal hit the wrong cell and this arm proves nothing", h.keyMismatch)
					}
					refused := cache.ResolvedCache().Stats().PutRefusedGenerationMovedTotal - refusedBefore
					opFail := pipSeedOperationalFailTotal.Load() - opFailBefore

					for _, cl := range []struct {
						class    string
						key      string
						resolves int
						reseed   string
					}{
						{"widget", widgetKey, h.widgetResolves, "widget-resolve-2"},
						{"restaction", raKey, h.raResolves, "ra-resolve-2"},
					} {
						body, present := s394Body(handle, cl.key)
						if sub.twice {
							if present {
								t.Errorf("#394 RED (%s/%s): the %s cell was REMOVED during the seed resolve and is "+
									"present again (%q) — the post-readyz seed's terminal Put resurrected it",
									tc.name, sub.name, cl.class, body)
							}
						} else {
							if present && strings.Contains(body, cl.reseed[:len(cl.reseed)-1]+"1") {
								t.Errorf("#394 RED (%s): the %s cell was REMOVED during the seed resolve and was "+
									"RESURRECTED with the body resolved before the removal (%q) — the terminal Put "+
									"is not generation-guarded", tc.name, cl.class, body)
							} else if !present || !strings.Contains(body, cl.reseed) {
								t.Errorf("#394 (%s): after a refused terminal Put the one-shot re-seed must fill the "+
									"%s cell with ITS OWN fresh resolve (%q); present=%v body=%q",
									tc.name, cl.class, cl.reseed, present, body)
							}
						}
						if cl.resolves != 2 {
							t.Errorf("#394 (%s/%s): %s resolves = %d, want 2 (the refused seed + exactly ONE inline "+
								"re-seed; 1 means no re-seed ran, >2 means the re-seed is not one-shot)",
								tc.name, sub.name, cl.class, cl.resolves)
						}
					}
					wantRefused := uint64(2) // one refusal per class
					if sub.twice {
						wantRefused = 4 // seed + re-seed refused, per class
					}
					if refused != wantRefused {
						t.Errorf("#394 (%s/%s): put_refused_generation_moved delta = %d, want %d", tc.name, sub.name, refused, wantRefused)
					}
					if opFail != 0 {
						t.Errorf("#394 (%s/%s): a refused terminal Put must NOT be classified as an operational "+
							"failure (failedSet → finalizeBootReEnqueue redrives a plain-Put BOOT scope); delta=%d",
							tc.name, sub.name, opFail)
					}
				})
			}
		})
	}
}

// TestS394_BootSeed_LRUEvictionDuringResolve_StillPlainPuts is arm 3: the boot
// seed keeps the #323 pre-readyz exemption — a plain Put, so a boot re-fill of
// a cell LRU-evicted during its resolve is NOT over-refused.
//
// FIXTURE: RESOLVED_CACHE_MAX_ENTRIES=1. The target cell is absent at seed
// entry (boot does not fresh-skip). During the resolve another write lands the
// target and a filler Put LRU-evicts it (removeElementLocked → the generation
// moves). The boot seed's terminal Put must still land.
//
// Main passes this too (main plain-Puts everywhere); its RED is the
// "guard boot too" mutation (seedTerminalGuardFor without the boot branch),
// under which the boot Put is refused and the cell stays cold.
func TestS394_BootSeed_LRUEvictionDuringResolve_StillPlainPuts(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_MAX_ENTRIES", "1")
	// #408 — this arm is the PRE-readyz boot exemption; post-readyz boot is guarded.
	cache.ResetPhase1DoneForTest()
	t.Cleanup(cache.ResetPhase1DoneForTest)
	quietLoggingE(t)
	a1BuildTwoTenantWatcher(t)
	widgetKey, raKey, handle, wIn, raIn := s394Keys(t)

	lruEvict := func(key string, inputs *cache.ResolvedKeyInputs) {
		handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"body":"concurrent"}`), Inputs: inputs})
		handle.Put("s394-filler", &cache.ResolvedEntry{RawJSON: []byte(`{}`)}) // cap 1 → evicts key
		if _, ok := handle.Get(key); ok {
			t.Fatalf("PRECONDITION: the filler Put did not LRU-evict %q (RESOLVED_CACHE_MAX_ENTRIES not honoured?)", key)
		}
	}
	h := &s394Harness{widgetKey: widgetKey, raKey: raKey, handle: handle, removeOn: func(int) bool { return true }}
	h.remove = func(key string) {
		if key == widgetKey {
			lruEvict(key, wIn)
		} else {
			lruEvict(key, raIn)
		}
	}
	h.install(t)

	cctx := withCohortSeedContext(context.Background(), s394Cohort(), endpoints.Endpoint{}, nil)
	if err := seedOneWidget(cctx, s394WidgetEntry(), h1NS, seedModeBoot); err != nil {
		t.Fatalf("boot seedOneWidget returned %v; want nil (boot never refuses)", err)
	}
	if body, ok := s394Body(handle, widgetKey); !ok || !strings.Contains(body, "widget-resolve-1") {
		t.Fatalf("#394 arm 3 RED (boot over-refused): the boot widget seed did not plain-Put into the LRU-evicted "+
			"cell (present=%v body=%q). Boot is pre-readyz and exempt (#323) — guarding it leaves boot cells cold.", ok, body)
	}
	if err := seedOneRestaction(cctx, "cohort-s394", s394RARef(), h1NS, seedModeBoot); err != nil {
		t.Fatalf("boot seedOneRestaction returned %v; want nil (boot never refuses)", err)
	}
	if body, ok := s394Body(handle, raKey); !ok || !strings.Contains(body, "ra-resolve-1") {
		t.Fatalf("#394 arm 3 RED (boot over-refused): the boot restaction seed did not plain-Put into the "+
			"LRU-evicted cell (present=%v body=%q).", ok, body)
	}
	if h.widgetResolves != 1 || h.raResolves != 1 {
		t.Fatalf("PRECONDITION: each boot primitive must resolve exactly once (widget=%d restaction=%d)", h.widgetResolves, h.raResolves)
	}
}
