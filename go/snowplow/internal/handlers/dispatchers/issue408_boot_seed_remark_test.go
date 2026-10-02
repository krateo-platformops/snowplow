// issue408_boot_seed_remark_test.go — #408 arms: the boot seed terminal Put gets
// #375 remark coverage before /readyz and the #394 gen guard after it.
//
// THE GAP (main 759a59d9). seedModeBoot plain-Puts (seed_terminal_put_guard.go)
// and so does widget_content's boot branch. A boot resolve records its dep edges
// on resCtx BEFORE its Put, so a dep event in that window dirty-marks a key that
// is not resident yet. The refresher skips that mark as no-entry, and the plain
// Put then stores the pre-event body with no mark pending. Separately, the boot
// scope keeps seeding the RA content tail AFTER the first-nav latch flips /readyz
// (phase1_walk.go engineSeed select → MarkPhase1Done; prewarm_engine_boot.go RA
// tail), so boot-mode plain Puts also land post-readyz, where #394 requires a
// gen guard.
//
// Every arm drives the REAL seed primitives (seedOneWidget; seedOneRestaction and
// its real tail seedRestactionResolveAndPutProd) or the real
// populateWidgetContentL1, over the real resolved-L1 store and the real dep
// tracker (Record → OnUpdate → bumpCoordinateGen). Only the resolver edge is
// seamed (the s394 harness).
//
// RED on main: arm 1 sees 0 remarks (plain Put, no remark); arm 2 sees the
// removed cell resurrected (plain Put post-readyz).
package dispatchers

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var i408DepGVR = schema.GroupVersionResource{Group: "seed.example.io", Version: "v1", Resource: "i408deps"}

// i408RemarkRecorder counts #375 remarks per key and reason, and the nil-sink
// (unguarded) Puts, for the duration of one test.
type i408RemarkRecorder struct {
	mu        sync.Mutex
	remarks   map[string]map[string]int
	unguarded []string
}

func newI408RemarkRecorder(t *testing.T) *i408RemarkRecorder {
	t.Helper()
	r := &i408RemarkRecorder{remarks: map[string]map[string]int{}}
	t.Cleanup(cache.SetDepGenRemarkObserverForTest(func(k, reason string) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.remarks[k] == nil {
			r.remarks[k] = map[string]int{}
		}
		r.remarks[k][reason]++
	}))
	cache.ResetUnguardedPutTotalForTest()
	t.Cleanup(cache.SetUnguardedPutHookForTest(func(k string) {
		r.mu.Lock()
		r.unguarded = append(r.unguarded, k)
		r.mu.Unlock()
	}))
	return r
}

func (r *i408RemarkRecorder) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, by := range r.remarks {
		for _, c := range by {
			n += c
		}
	}
	return n
}

func (r *i408RemarkRecorder) of(key string) map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]int{}
	for k, v := range r.remarks[key] {
		out[k] = v
	}
	return out
}

// TestIssue408_PreReadyzBootSeed_DepEventWhileNonResident_PlainPutRemarksOnce is
// arm 1 (both seed primitives) plus the quiet-boot zero-remark control.
//
// Inside each boot resolve the seed records a dep on resCtx and that dep then
// changes (a real OnUpdate) while the cell is still NOT resident, so the
// dirty-mark fan-out targets a key the refresher would skip. The plain terminal
// Put must land (boot never refuses) AND remark the key EXACTLY once as
// "moved". The quiet sub-arm runs the same boot with no dep event: 0 remarks,
// 0 unguarded Puts.
func TestIssue408_PreReadyzBootSeed_DepEventWhileNonResident_PlainPutRemarksOnce(t *testing.T) {
	for _, tc := range []struct {
		name  string
		churn bool
	}{
		{"dep-event-while-non-resident_remarks-exactly-once", true},
		{"quiet-boot_zero-remarks", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache.ResetPhase1DoneForTest()
			t.Cleanup(cache.ResetPhase1DoneForTest)
			quietLoggingE(t)
			a1BuildTwoTenantWatcher(t)
			widgetKey, raKey, handle, _, _ := s394Keys(t)
			rec := newI408RemarkRecorder(t)

			var nonResidentAtEvent, sinkless []string
			churnInside := func(ctx context.Context, key, name string) {
				if _, _, ok := cache.DepGenSinkForTest(ctx); !ok {
					sinkless = append(sinkless, key)
				}
				if !tc.churn {
					return
				}
				cache.Deps().Record(ctx, key, i408DepGVR, "i408-ns", name)
				cache.Deps().OnUpdate(i408DepGVR, "i408-ns", name)
				if !cache.ResolvedCache().Has(key) {
					nonResidentAtEvent = append(nonResidentAtEvent, key)
				}
			}
			widgetResolves, raResolves := 0, 0
			origW, origGet, origTail := widgetsResolveFn, seedObjectsGetFn, seedRestactionResolveAndPutFn
			t.Cleanup(func() {
				widgetsResolveFn, seedObjectsGetFn, seedRestactionResolveAndPutFn = origW, origGet, origTail
			})
			widgetsResolveFn = func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
				widgetResolves++
				churnInside(ctx, widgetKey, "w")
				return h1WidgetUnstructured(map[string]any{}), nil
			}
			seedObjectsGetFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
				return s394FetchedRA("i408-ra-body")
			}
			seedRestactionResolveAndPutFn = func(
				ctx, resCtx context.Context, cr *templatesv1.RESTAction, ref templatesv1.ObjectReference,
				authnNS, key string, handle cacheHandle, inputs *cache.ResolvedKeyInputs, got objects.Result,
				stageErrSink *cache.StageErrorSink, extTouchedSink *cache.ExternalTouchedSink,
			) error {
				raResolves++
				churnInside(resCtx, key, "ra")
				return seedRestactionResolveAndPutProd(ctx, resCtx, cr, ref, authnNS, key, handle, inputs, got, stageErrSink, extTouchedSink)
			}

			cctx := withCohortSeedContext(context.Background(), s394Cohort(), endpoints.Endpoint{}, nil)
			if err := seedOneWidget(cctx, s394WidgetEntry(), h1NS, seedModeBoot); err != nil {
				t.Fatalf("boot seedOneWidget: %v (boot never refuses pre-readyz)", err)
			}
			if err := seedOneRestaction(cctx, "cohort-i408", s394RARef(), h1NS, seedModeBoot); err != nil {
				t.Fatalf("boot seedOneRestaction: %v (boot never refuses pre-readyz)", err)
			}

			if widgetResolves != 1 || raResolves != 1 {
				t.Fatalf("PRECONDITION: each boot primitive must resolve once (cell absent at entry); widget=%d ra=%d",
					widgetResolves, raResolves)
			}
			if len(sinkless) != 0 {
				t.Fatalf("PRECONDITION: the boot resolve ctx must carry the #375 dep-gen sink; sinkless=%v", sinkless)
			}
			for _, k := range []string{widgetKey, raKey} {
				if _, ok := s394Body(handle, k); !ok {
					t.Fatalf("PRECONDITION: the pre-readyz boot plain Put must land (no refusal); %q absent", k)
				}
			}
			if n := cache.UnguardedPutTotal(); n != 0 || len(rec.unguarded) != 0 {
				t.Errorf("#408: unguarded_put_total delta=%d (%v) — the boot Put ran without the resolve's sink",
					n, rec.unguarded)
			}
			if !tc.churn {
				if n := rec.total(); n != 0 {
					t.Errorf("#408: a quiet boot (no dep event) must remark nothing; got %v", rec.remarks)
				}
				return
			}
			if len(nonResidentAtEvent) != 2 {
				t.Fatalf("PRECONDITION: the dep event must hit each key while NOT resident (the window a refresher "+
					"mark is skipped as no-entry); non-resident at event = %v", nonResidentAtEvent)
			}
			for _, cl := range []struct{ class, key string }{{"widget", widgetKey}, {"restaction", raKey}} {
				if got := rec.of(cl.key); got["moved"] != 1 || len(got) != 1 {
					t.Errorf("#408 RED (%s): a pre-readyz boot plain Put whose dep moved during the resolve, while the "+
						"key was not resident, must remark the key EXACTLY once as 'moved'; got %v. Without it the "+
						"refresher skipped the mark as no-entry and the cell holds the pre-event body with no mark pending.",
						cl.class, got)
				}
			}
		})
	}
}

// TestIssue408_PreReadyzWidgetContentBoot_DepEventRemarksOnce is arm 1 for the
// widget_content boot branch: the real populateWidgetContentL1 pre-readyz, on a
// resolve ctx (WithL1KeyContext) that recorded a dep which then moved.
func TestIssue408_PreReadyzWidgetContentBoot_DepEventRemarksOnce(t *testing.T) {
	cache.ResetPhase1DoneForTest()
	t.Cleanup(cache.ResetPhase1DoneForTest)
	quietLoggingE(t)
	a1BuildTwoTenantWatcher(t)
	rec := newI408RemarkRecorder(t)

	key, _ := widgetContentL1Key(h1WidgetGVR, h1NS, h1WName, -1, -1)
	if key == "" {
		t.Fatal("PRECONDITION: widgetContent key derivation returned \"\"")
	}
	in := h1WidgetUnstructured(map[string]any{})
	res := h1WidgetUnstructured(map[string]any{})

	// quiet first: no dep event → no remark.
	quiet := cache.WithL1KeyContext(context.Background(), key)
	cache.Deps().Record(quiet, key, i408DepGVR, "i408-ns", "wc-quiet")
	populateWidgetContentL1(quiet, h1WidgetGVR, in, -1, -1, res, 0)
	if _, ok := cache.ResolvedCache().Get(key); !ok {
		t.Fatal("PRECONDITION: the pre-readyz widget_content boot Put must land")
	}
	if n := rec.total(); n != 0 {
		t.Fatalf("#408: a quiet widget_content boot Put must remark nothing; got %v", rec.remarks)
	}

	cache.ResolvedCache().DeleteForTest(key)
	ctx := cache.WithL1KeyContext(context.Background(), key)
	cache.Deps().Record(ctx, key, i408DepGVR, "i408-ns", "wc")
	cache.Deps().OnUpdate(i408DepGVR, "i408-ns", "wc")
	if cache.ResolvedCache().Has(key) {
		t.Fatal("PRECONDITION: the content key must be non-resident at the dep event")
	}
	populateWidgetContentL1(ctx, h1WidgetGVR, in, -1, -1, res, 0)
	if _, ok := cache.ResolvedCache().Get(key); !ok {
		t.Fatal("PRECONDITION: the pre-readyz widget_content boot Put must land (never refused)")
	}
	if got := rec.of(key); got["moved"] != 1 || len(got) != 1 {
		t.Errorf("#408 RED (widget_content boot): the plain Put whose dep moved during the resolve must remark "+
			"the content key EXACTLY once as 'moved'; got %v", got)
	}
	if len(rec.unguarded) != 0 {
		t.Errorf("#408: no nil-sink Put expected; got %v", rec.unguarded)
	}
}

// TestIssue408_PostReadyzBootSeed_RemovalDuringResolve_Refused is arm 2: a
// boot-mode seed running AFTER /readyz (the RA content tail after the first-nav
// latch) whose cell is removed mid-resolve must be REFUSED, not resurrected.
// Two shapes: captured post-readyz, and captured pre-readyz with /readyz
// flipping during the resolve (the generation captured BEFORE the flip guards
// the Put).
//
// The cell is absent at seed entry (boot skips a resident cell), so the removal
// is a concurrent fill followed by a real DELETE (deleteForDep), which moves the
// generation.
func TestIssue408_PostReadyzBootSeed_RemovalDuringResolve_Refused(t *testing.T) {
	for _, tc := range []struct {
		name        string
		flipMidSeed bool // MarkPhase1Done inside the resolve instead of before the seed
	}{
		{"captured-post-readyz", false},
		{"captured-pre-readyz_put-post-readyz", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache.ResetPhase1DoneForTest()
			t.Cleanup(cache.ResetPhase1DoneForTest)
			quietLoggingE(t)
			a1BuildTwoTenantWatcher(t)
			widgetKey, raKey, handle, wIn, raIn := s394Keys(t)
			h := &s394Harness{widgetKey: widgetKey, raKey: raKey, handle: handle, removeOn: func(n int) bool { return n == 1 }}
			h.remove = func(key string) {
				if tc.flipMidSeed {
					cache.MarkPhase1Done()
				}
				inputs := wIn
				if key == raKey {
					inputs = raIn
				}
				handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"body":"concurrent"}`), Inputs: inputs})
				cache.ResolvedCache().DeleteForTest(key)
			}
			h.install(t)
			if !tc.flipMidSeed {
				cache.MarkPhase1Done()
			}
			refusedBefore := cache.ResolvedCache().Stats().PutRefusedGenerationMovedTotal

			cctx := withCohortSeedContext(context.Background(), s394Cohort(), endpoints.Endpoint{}, nil)
			wErr := seedOneWidget(cctx, s394WidgetEntry(), h1NS, seedModeBoot)
			if tc.flipMidSeed {
				cache.ResetPhase1DoneForTest() // the RA unit is captured pre-readyz too
			}
			raErr := seedOneRestaction(cctx, "cohort-i408", s394RARef(), h1NS, seedModeBoot)

			if h.keyMismatch != "" {
				t.Fatalf("PRECONDITION: seed ran under a different key (%s)", h.keyMismatch)
			}
			if h.widgetResolves != 1 || h.raResolves != 1 {
				t.Fatalf("PRECONDITION: each primitive must resolve once; widget=%d ra=%d", h.widgetResolves, h.raResolves)
			}
			for _, cl := range []struct {
				class string
				key   string
				err   error
			}{{"widget", widgetKey, wErr}, {"restaction", raKey, raErr}} {
				if body, present := s394Body(handle, cl.key); present {
					t.Errorf("#408 RED (%s/%s): a post-readyz boot-mode seed resurrected a cell REMOVED during its "+
						"resolve (%q) — the boot terminal Put is not gen-guarded after /readyz", tc.name, cl.class, body)
				}
				if !errors.Is(cl.err, errSeedTerminalPutRefused) {
					t.Errorf("#408 (%s/%s): the refused Put must surface errSeedTerminalPutRefused (the engine closure "+
						"re-seeds once on it); got %v", tc.name, cl.class, cl.err)
				}
			}
			if d := cache.ResolvedCache().Stats().PutRefusedGenerationMovedTotal - refusedBefore; d != 2 {
				t.Errorf("#408 (%s): put_refused_generation_moved delta = %d, want 2 (one per class)", tc.name, d)
			}
		})
	}
}
