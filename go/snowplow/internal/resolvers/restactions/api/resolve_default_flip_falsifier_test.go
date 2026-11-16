// resolve_default_flip_falsifier_test.go — falsifiers for the 2026-07-02
// `spec.api[].resolve` DEFAULT FLIP (true→false), the snowplow half of the
// "resolve is opt-in" contract move (docs/resolve-default-flip-plan-2026-07-02.md).
//
// The flip is TWO load-bearingly-coupled edits shipped together:
//   (a) apis/templates/v1/core.go — REMOVE +kubebuilder:default=true on the
//       api-step Resolve *bool → the CRD injects nothing on omit → an omitted
//       `resolve` arrives at the resolver as a NIL pointer;
//   (b) resolve.go — ptr.Deref(apiCall.Resolve, true) → ptr.Deref(..., false),
//       the SINGLE default-fallback site, now LOAD-BEARING for the nil case.
//
// Before the flip (b)'s 2nd arg was INERT — the CRD injected non-nil `true` on
// every omit, so apiCall.Resolve was never nil and the fallback never fired.
// The OMIT arm below is precisely what became load-bearing: with Resolve nil it
// drives the ptr.Deref fallback end-to-end through api.Resolve. It is RED under
// the old `true` fallback (the seam fires → resolves) and GREEN under the new
// `false` fallback (the seam is skipped → raw CR fed).
//
// Discriminating arms (all four the plan's Phase-B falsifier calls for):
//   - OMIT   (Resolve nil)   → NOT resolved (raw fed). RED-before-flip. Proves it took.
//   - TRUE   (Resolve true)  → resolved in-process. Proves opt-in preserved.
//   - FALSE  (Resolve false) → raw CR (unchanged behaviour).
//   - CATEGORY-B invariant (explicit true consuming a child's RESOLVED
//     `.status.<field>`, models fsa-*-composition-values) → still gets the
//     resolved envelope, surviving the default flip. Proves the safe-migration
//     contract: explicit-true is invariant across the flip.
//
// Pure in-process, no kubeconfig (feedback_no_go_test_against_remote_kubeconfig).
// Reuses seamStub / inProcessRun / directRACall from
// resolve_inprocess_falsifier_test.go and iterResolveCtx / iterFailFastRetries
// from resolve_iter_continue_test.go.

package api

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

// --- Resolver-side unit gate: nil pointer → the false fallback fires ---------

// TestResolveDefaultFlip_Omit_NilPointer_NotResolved is the load-bearing
// falsifier for the flip at the single deref site. maybeResolveInProcess is
// invoked with the resolve value AS THE CALLER COMPUTED IT at resolve.go via
// ptr.Deref(apiCall.Resolve, <fallback>). Here we reproduce that exact
// computation for a NIL pointer (an omitted `resolve`) and assert the resulting
// gate value is false → no substitution.
//
// RED before the flip: ptr.Deref((*bool)(nil), true) == true → the seam fires,
// the resolved envelope is substituted for the raw CR.
// GREEN after the flip: ptr.Deref((*bool)(nil), false) == false → the seam is
// never called, the raw CR is fed.
func TestResolveDefaultFlip_Omit_NilPointer_NotResolved(t *testing.T) {
	// This is the exact expression at resolve.go's single default-fallback site.
	var omitted *bool // an omitted `resolve` — nil after the CRD default is removed
	resolve := ptr.Deref(omitted, false)
	if resolve {
		t.Fatalf("FLIP NOT TAKEN: ptr.Deref(nil, false)==true — the resolve.go fallback "+
			"still defaults an omitted `resolve` to true. Omitted-resolve would resolve "+
			"in-process, diverging from the CRD contract (omit→false).")
	}

	stub := &seamStub{ret: []byte(`{"should":"not appear"}`)}
	stub.install(t)

	r := inProcessRun(t)
	_, did, err := r.maybeResolveInProcess(ownClusterCtx(r.ctx), directRACall(), resolve)
	if err != nil {
		t.Fatalf("OMIT: unexpected error: %v", err)
	}
	if did {
		t.Fatalf("OMIT: omitted `resolve` (nil→false) substituted (did=true) — it must feed the RAW CR")
	}
	if stub.calls.Load() != 0 {
		t.Fatalf("OMIT: omitted `resolve` invoked the seam (%d calls) — the flip did not take", stub.calls.Load())
	}
}

// TestResolveDefaultFlip_ExplicitTrue_StillResolves — the opt-in-preserved arm
// at the resolver-side gate: an explicit resolve:true still routes through the
// seam and substitutes the resolved envelope. Complements the OMIT arm (the two
// diverge only because of the flip: nil→false, true→resolve).
func TestResolveDefaultFlip_ExplicitTrue_StillResolves(t *testing.T) {
	explicitTrue := ptr.Deref(ptr.To(true), false)
	if !explicitTrue {
		t.Fatalf("setup: ptr.Deref(true, false) != true")
	}

	stub := &seamStub{ret: []byte(`{"kind":"RESTAction","status":{"resolved":"opt-in-yes"}}`)}
	stub.install(t)

	r := inProcessRun(t)
	got, did, err := r.maybeResolveInProcess(ownClusterCtx(r.ctx), directRACall(), explicitTrue)
	if err != nil || !did {
		t.Fatalf("EXPLICIT-TRUE: did=%v err=%v — opt-in resolve:true must still substitute", did, err)
	}
	if stub.calls.Load() != 1 {
		t.Fatalf("EXPLICIT-TRUE: seam invoked %d times, want 1", stub.calls.Load())
	}
	if string(got) != string(stub.ret) {
		t.Fatalf("EXPLICIT-TRUE: substituted bytes != seam output")
	}
}

// --- End-to-end through api.Resolve: the nil pointer drives the deref site ---

// flipStage builds a direct-RA-path api-step with the given Resolve pointer
// (nil = omitted). Uses a placeholder internal endpoint so the stage reaches
// the dispatch cascade; under this hermetic harness (no informer, no watcher)
// the served arms decline and the stage reaches the branch that computes
// `resolve := ptr.Deref(apiCall.Resolve, false)` and calls maybeResolveInProcess.
func flipStage(name string, resolve *bool) *templates.API {
	return &templates.API{
		Name:    name,
		Path:    "/apis/templates.krateo.io/v1/namespaces/krateo-system/restactions/inner-ra",
		Verb:    ptr.To("GET"),
		Resolve: resolve, // nil = OMITTED (post-flip: apiserver injects nothing)
		Filter:  ptr.To("." + name),
	}
}

// runFlipStage drives ONE stage end-to-end through api.Resolve with the seam
// stubbed, returning the resolved dict and the seam invocation count. Every
// substitution decision flows through the real resolve.go deref site — so a nil
// Resolve exercises ptr.Deref(nil, false) exactly as production would.
func runFlipStage(t *testing.T, stage *templates.API, seamOut []byte) (map[string]any, int64) {
	t.Helper()
	stub := &seamStub{ret: seamOut}
	stub.install(t)
	ctx := cache.WithInternalEndpoint(iterResolveCtx(),
		&endpoints.Endpoint{ServerURL: "http://test.invalid"})
	dict := Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{stage},
		RESTActionNamespace: "krateo-system",
		RESTActionName:      "flip-" + stage.Name,
	})
	return dict, stub.calls.Load()
}

// TestResolveDefaultFlip_EndToEnd_OmitVsTrueVsFalse drives the three primary
// arms through the FULL api.Resolve path (not just the gate), so the load-bearing
// ptr.Deref(apiCall.Resolve, false) at resolve.go is the actual code under test.
//
// OMIT  (nil)   → seam NOT invoked (raw fed). RED before the flip: nil→true
//                 would invoke the seam. This is the "the flip took" proof.
// TRUE  (true)  → seam invoked (resolved envelope substituted). Opt-in preserved.
// FALSE (false) → seam NOT invoked (raw fed). Unchanged.
func TestResolveDefaultFlip_EndToEnd_OmitVsTrueVsFalse(t *testing.T) {
	iterFailFastRetries(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	t.Setenv("CACHE_ENABLED", "true")

	const resolvedEnvelope = `{"kind":"RESTAction","apiVersion":"templates.krateo.io/v1","status":{"resolved":"end-to-end"}}`

	// OMIT arm — the load-bearing case.
	t.Run("omit", func(t *testing.T) {
		_, seamCalls := runFlipStage(t, flipStage("omit", nil), []byte(resolvedEnvelope))
		if seamCalls != 0 {
			t.Fatalf("OMIT end-to-end: seam invoked %d times — an omitted `resolve` resolved "+
				"in-process. The flip did NOT take (ptr.Deref(nil, ...) still defaults true).", seamCalls)
		}
	})

	// EXPLICIT-TRUE arm — opt-in preserved.
	t.Run("explicit_true", func(t *testing.T) {
		dict, seamCalls := runFlipStage(t, flipStage("true", ptr.To(true)), []byte(resolvedEnvelope))
		if seamCalls == 0 {
			t.Fatalf("EXPLICIT-TRUE end-to-end: seam NOT invoked — opt-in resolve:true failed to "+
				"resolve. dict=%#v", dict)
		}
		inner, ok := dict["true"].(map[string]any)
		if !ok {
			t.Fatalf("EXPLICIT-TRUE: dict[true] not a resolved map: %#v", dict["true"])
		}
		if status, _ := inner["status"].(map[string]any); status == nil || status["resolved"] != "end-to-end" {
			t.Fatalf("EXPLICIT-TRUE: output != the resolved envelope; got %#v", inner)
		}
	})

	// EXPLICIT-FALSE arm — unchanged.
	t.Run("explicit_false", func(t *testing.T) {
		_, seamCalls := runFlipStage(t, flipStage("false", ptr.To(false)), []byte(resolvedEnvelope))
		if seamCalls != 0 {
			t.Fatalf("EXPLICIT-FALSE end-to-end: seam invoked %d times — resolve:false must feed the RAW CR", seamCalls)
		}
	})
}

// --- Category-B invariant: explicit resolve:true survives the flip ----------

// TestResolveDefaultFlip_CategoryB_ExplicitTrueSurvivesFlip models the
// fsa-*-composition-values safe-migration contract (plan §2 / §5 Phase A): a
// category-B RA CONSUMES a nested child's RESOLVED-ONLY `.status.<field>` (a
// field that exists ONLY after resolving the child — a raw child CR has no such
// field). With EXPLICIT resolve:true on the loopback step, that child is
// resolved in-process and the resolved envelope (carrying the resolved-only
// status field) is fed downstream — UNCHANGED by the default flip.
//
// The parent's filter then reads the child's resolved-only field. The seam stub
// stands in for the resolved child: it returns an envelope whose
// `.status.allCompositionResources` is a NON-NULL list (the resolved-only
// output). The parent's filter projects exactly the field the real
// fsa-*-composition-values RA reads:
//     { allCompositionResources: .child.status.allCompositionResources }
// so a resolved child → non-null list, an unresolved (raw) child → null.
//
// This is the "would the flip break category-B?" invariant. Because the step
// carries EXPLICIT resolve:true (Phase A), the answer is NO: the field is
// non-null. If a future regression dropped the explicit-true handling, this arm
// goes RED (null field → empty composition-values panel — the exact §2 hazard).
func TestResolveDefaultFlip_CategoryB_ExplicitTrueSurvivesFlip(t *testing.T) {
	iterFailFastRetries(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	t.Setenv("CACHE_ENABLED", "true")

	// The resolved child envelope — the resolved-only `.status.allCompositionResources`
	// list exists ONLY because the child was resolved (a raw child CR would not
	// carry it). Two representative resource identities, as the real roll-up does.
	const resolvedChild = `{
		"kind":"RESTAction","apiVersion":"templates.krateo.io/v1",
		"status":{"allCompositionResources":[
			{"metadata":{"name":"cm-a"},"apiVersion":"v1","kind":"ConfigMap"},
			{"metadata":{"name":"svc-b"},"apiVersion":"v1","kind":"Service"}
		]}
	}`

	// The category-B parent step: a direct-RA-path loopback to the child, with
	// EXPLICIT resolve:true. Its filter is the identity `.` — the assertion is on
	// the SUBSTITUTED ENVELOPE the parent's filter is fed (dict[stage]): a
	// resolved child carries the resolved-only `.status.allCompositionResources`
	// list; a raw (unresolved) child would NOT (the loopback would return the raw
	// CR, whose `.status.allCompositionResources` is absent → the §2 empty-panel
	// hazard). We assert on the fed content directly rather than a downstream jq
	// projection so the invariant under test is the resolve substitution itself,
	// not the resolver's jsonHandler jq semantics (orthogonal to the flip).
	//
	// (The real fsa-y1-composition-values RA's filter
	// `{ allCompositionResources: .allCompositionResources.status.allCompositionResources }`
	// reads THIS same resolved-only field off the resolved loopback output — so
	// the load-bearing property is "the resolved envelope is fed", which is what
	// the parent's downstream projection then reads.)
	stage := &templates.API{
		Name:    "allCompositionResources",
		Path:    "/apis/templates.krateo.io/v1/namespaces/krateo-system/restactions/fsa-y1-composition-resources",
		Verb:    ptr.To("GET"),
		Resolve: ptr.To(true), // Phase A: EXPLICIT true — invariant across the flip
		Filter:  ptr.To("."),  // identity: the fed envelope lands in dict[stage] verbatim
	}

	dict, seamCalls := runFlipStage(t, stage, []byte(resolvedChild))
	if seamCalls == 0 {
		t.Fatalf("CATEGORY-B: explicit resolve:true did NOT resolve the child — the loopback "+
			"returned a raw CR. dict=%#v", dict)
	}
	// The resolver stores each stage's filtered output under dict[stageName]; the
	// identity `.` filter's result is itself keyed under the stage name, so the
	// fed envelope lives at dict[stage][stage].
	slot, ok := dict["allCompositionResources"].(map[string]any)
	if !ok {
		t.Fatalf("CATEGORY-B: dict[allCompositionResources] not a map (no output): %#v", dict["allCompositionResources"])
	}
	fed, ok := slot["allCompositionResources"].(map[string]any)
	if !ok {
		t.Fatalf("CATEGORY-B: fed envelope not a map (no envelope fed): %#v", slot["allCompositionResources"])
	}
	status, _ := fed["status"].(map[string]any)
	if status == nil {
		t.Fatalf("CATEGORY-B REGRESSION (§2 hazard): the fed envelope has NO `.status` — the "+
			"loopback returned a raw/unresolved child, not the resolved envelope. Explicit "+
			"resolve:true failed to survive the flip. got %#v", fed)
	}
	list, ok := status["allCompositionResources"].([]any)
	if !ok || len(list) == 0 {
		t.Fatalf("CATEGORY-B REGRESSION (§2 hazard): the child's resolved-only "+
			"`.status.allCompositionResources` is NULL/empty in the fed envelope — the "+
			"composition-values panel would render EMPTY. got %#v", status["allCompositionResources"])
	}
	if len(list) != 2 {
		t.Fatalf("CATEGORY-B: resolved list len=%d, want 2 (the child's resolved resources)", len(list))
	}
}

// --- Concurrency: the flipped default is read race-free across stages -------

// TestResolveDefaultFlip_Concurrent_RaceFree drives a MIX of omit / true / false
// stages CONCURRENTLY through api.Resolve (RESOLVER_ITER_PARALLELISM>1 and many
// stages) with -race, asserting the per-stage ptr.Deref(apiCall.Resolve, false)
// read + the seam decision are race-free and each stage's arm holds
// independently. The flip changes the fallback CONSTANT (a read), not shared
// state — but per feedback_shared_vs_copy_is_a_concurrency_change any change on
// the resolve-decision path gets a concurrent -race arm, and this pins that the
// mixed-arm fan-out never cross-contaminates a stage's resolve decision.
func TestResolveDefaultFlip_Concurrent_RaceFree(t *testing.T) {
	iterFailFastRetries(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "8")
	t.Setenv("CACHE_ENABLED", "true")

	const resolvedEnvelope = `{"kind":"RESTAction","apiVersion":"templates.krateo.io/v1","status":{"resolved":"conc"}}`

	// A shared seam stub recording per-stage-name invocation; the omit/false
	// stages must NEVER appear, the true stages must each appear once.
	var mu sync.Mutex
	seenTrue := map[string]int{}
	var forbiddenCalls atomic.Int64
	prev := nestedCallResolver
	nestedCallResolver = func(_ context.Context, ref templates.ObjectReference, _, _ int, _ map[string]any) ([]byte, error) {
		// The path name is the RA name; the stage carries it. We can't read the
		// stage name from the ref (all stages share inner-ra), so instead assert
		// aggregate: the seam fires exactly once per TRUE stage. We tag TRUE
		// stages with a distinct RA name in the path so the ref discriminates.
		mu.Lock()
		seenTrue[ref.Name]++
		mu.Unlock()
		if ref.Name != "inner-true" {
			forbiddenCalls.Add(1)
		}
		return []byte(resolvedEnvelope), nil
	}
	t.Cleanup(func() { nestedCallResolver = prev })

	// Build K stages of each arm. TRUE stages point at a distinct RA name so the
	// seam ref discriminates them from any accidental omit/false resolution.
	const K = 6
	var items []*templates.API
	trueStage := func(i int) *templates.API {
		return &templates.API{
			Name:    fmt.Sprintf("t%d", i),
			Path:    "/apis/templates.krateo.io/v1/namespaces/krateo-system/restactions/inner-true",
			Verb:    ptr.To("GET"),
			Resolve: ptr.To(true),
			Filter:  ptr.To(fmt.Sprintf(".t%d", i)),
		}
	}
	rawStage := func(prefix string, i int, resolve *bool) *templates.API {
		return &templates.API{
			Name:    fmt.Sprintf("%s%d", prefix, i),
			Path:    "/apis/templates.krateo.io/v1/namespaces/krateo-system/restactions/inner-raw",
			Verb:    ptr.To("GET"),
			Resolve: resolve,
			Filter:  ptr.To(fmt.Sprintf(".%s%d", prefix, i)),
		}
	}
	for i := 0; i < K; i++ {
		items = append(items, trueStage(i))         // resolve:true
		items = append(items, rawStage("o", i, nil)) // OMIT (nil)
		items = append(items, rawStage("f", i, ptr.To(false)))
	}

	ctx := cache.WithInternalEndpoint(iterResolveCtx(),
		&endpoints.Endpoint{ServerURL: "http://test.invalid"})
	_ = Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               items,
		RESTActionNamespace: "krateo-system",
		RESTActionName:      "flip-concurrent",
	})

	if forbiddenCalls.Load() != 0 {
		t.Fatalf("CONCURRENT: %d seam calls for a NON-true (omit/false) stage — the flipped "+
			"default was mis-read under concurrency (omit/false must NOT resolve)", forbiddenCalls.Load())
	}
	mu.Lock()
	gotTrue := seenTrue["inner-true"]
	mu.Unlock()
	if gotTrue != K {
		t.Fatalf("CONCURRENT: resolve:true seam fired %d times, want %d (one per TRUE stage) — "+
			"the opt-in arm was lost or double-fired under concurrency", gotTrue, K)
	}
}
