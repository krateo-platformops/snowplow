// issue375_x_394_keepwarm_seed_test.go — the #394 × #375 COMPOSITION arm.
//
// #394 (PR #404) turned the post-readyz seed terminal Put into a PutIfGen. Its own
// guard is REMOVAL-only: the resolved-L1 generation does not move on a write, so a
// refresher ReplaceIfGen landing mid-seed is overwritten by the seed's (older) body.
// That refresh-overwrite case closes by composition with #375: remarkIfDepsMoved runs
// inside the IfGen Put methods, so a dep that moved after the seed's startSeq re-marks
// the key and the refresher re-resolves it at mark latency.
//
// The composition holds ONLY if the terminal Put runs under the seed's resCtx (built by
// WithL1KeyContext, so it carries the dep-gen sink + startSeq). On the outer ctx the
// sink is nil: every accepted keepwarm / gvr-discovered seed Put would be a nil-sink
// drift (unguarded_put_total++ plus a fail-fresh remark = refresher amplification).
//
// Both sub-arms drive the REAL seedScopeYielding loop in seedModeKeepwarm over one
// widget + one RESTAction (the #394 s394 fixtures: real seedOneWidget, real
// seedOneRestaction + seedRestactionResolveAndPutProd tail, real resolved-L1 store).
// Only the resolver edge is seamed.
//
//   - dep-moved: inside each resolve the seed reads a dep (Record on resCtx) and the dep
//     then churns (a real OnUpdate → bumpCoordinateGen), i.e. AFTER startSeq. Each
//     accepted terminal PutIfGen must remark its key EXACTLY once, as "moved" (never
//     "nil_sink").
//   - no-churn sweep: the same keepwarm sweep with no dep churn. unguarded_put_total
//     delta == 0 and remarks == 0 (zero amplification).
//
// NEUTERS: terminal Put on the OUTER ctx (seedTerminalPut(ctx, …)) → nil sink → both
// sub-arms RED; terminal Put reverted to a plain Put → dep-moved RED (no remark).

package dispatchers

import (
	"context"
	"strings"
	"sync"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var x394DepGVR = schema.GroupVersionResource{Group: "seed.example.io", Version: "v1", Resource: "x394deps"}

func TestIssue375x394_KeepwarmSeedTerminalPut_ComposesWithDepGenGuard(t *testing.T) {
	for _, tc := range []struct {
		name  string
		churn bool
	}{
		{"dep-moved-after-startSeq_remarks-exactly-once", true},
		{"no-churn-sweep_zero-unguarded-zero-remarks", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
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

			var mu sync.Mutex
			remarks := map[string]map[string]int{} // key → reason → n
			t.Cleanup(cache.SetDepGenRemarkObserverForTest(func(k, reason string) {
				mu.Lock()
				defer mu.Unlock()
				if remarks[k] == nil {
					remarks[k] = map[string]int{}
				}
				remarks[k][reason]++
			}))
			var unguardedKeys []string
			cache.ResetUnguardedPutTotalForTest()
			t.Cleanup(cache.SetUnguardedPutHookForTest(func(k string) {
				mu.Lock()
				unguardedKeys = append(unguardedKeys, k)
				mu.Unlock()
			}))

			// churnInside reads a dep under the resolve ctx and then churns it, so the
			// bump lands AFTER the sink's startSeq. Asserts the resolve ctx has a sink.
			sinkless := []string{}
			churnInside := func(ctx context.Context, key, name string) {
				if _, _, ok := cache.DepGenSinkForTest(ctx); !ok {
					sinkless = append(sinkless, key)
				}
				if !tc.churn {
					return
				}
				cache.Deps().Record(ctx, key, x394DepGVR, "x394-ns", name)
				cache.Deps().OnUpdate(x394DepGVR, "x394-ns", name)
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
				return s394FetchedRA("x394-ra-body")
			}
			seedRestactionResolveAndPutFn = func(
				ctx, resCtx context.Context, cr *templatesv1.RESTAction, ref templatesv1.ObjectReference,
				authnNS, key string, handle cacheHandle, inputs *cache.ResolvedKeyInputs, got objects.Result,
				stageErrSink *cache.StageErrorSink, extTouchedSink *cache.ExternalTouchedSink,
			) error {
				raResolves++
				if key != raKey {
					t.Errorf("PRECONDITION: restaction tail ran under key %q, want the precomputed %q", key, raKey)
				}
				churnInside(resCtx, raKey, "ra")
				return seedRestactionResolveAndPutProd(ctx, resCtx, cr, ref, authnNS, key, handle, inputs, got, stageErrSink, extTouchedSink)
			}

			refusedBefore := cache.ResolvedCache().Stats().PutRefusedGenerationMovedTotal
			s394RunLoop(t, seedModeKeepwarm)

			// Non-vacuity: keepwarm re-resolved BOTH aged cells exactly once and the
			// guarded terminal Puts were ACCEPTED (the remark only runs on accept).
			if widgetResolves != 1 || raResolves != 1 {
				t.Fatalf("PRECONDITION: keepwarm must re-resolve each aged cell once; widget=%d restaction=%d",
					widgetResolves, raResolves)
			}
			if d := cache.ResolvedCache().Stats().PutRefusedGenerationMovedTotal - refusedBefore; d != 0 {
				t.Fatalf("PRECONDITION: no removal ran, so no terminal PutIfGen may be refused; refused delta=%d", d)
			}
			for _, k := range []string{widgetKey, raKey} {
				body, ok := s394Body(handle, k)
				if !ok || strings.Contains(body, "pre-removal") {
					t.Fatalf("PRECONDITION: the keepwarm seed's terminal Put must replace the aged body; present=%v body=%q", ok, body)
				}
			}
			if len(sinkless) != 0 {
				t.Fatalf("#375×#394 RED: the seed resolve ctx carried no dep-gen sink for %v", sinkless)
			}

			mu.Lock()
			defer mu.Unlock()
			if n := cache.UnguardedPutTotal(); n != 0 || len(unguardedKeys) != 0 {
				t.Errorf("#375×#394 RED: unguarded_put_total delta=%d (keys %v) across a keepwarm sweep — the seed "+
					"terminal PutIfGen ran without the resolve's dep-gen sink (outer ctx instead of resCtx): every "+
					"accepted keepwarm seed Put becomes a fail-fresh remark", n, unguardedKeys)
			}
			if tc.churn {
				for _, cl := range []struct{ class, key string }{{"widget", widgetKey}, {"restaction", raKey}} {
					got := remarks[cl.key]
					if got["moved"] != 1 || got["nil_sink"] != 0 {
						t.Errorf("#375×#394 RED (%s): an accepted keepwarm seed PutIfGen whose dep moved after startSeq "+
							"must remark its key EXACTLY once as 'moved'; got %v", cl.class, got)
					}
				}
				return
			}
			total := 0
			for _, byReason := range remarks {
				for _, n := range byReason {
					total += n
				}
			}
			if total != 0 {
				t.Errorf("#375×#394 RED: a keepwarm sweep with NO dep churn must remark nothing (zero amplification); "+
					"got %v", remarks)
			}
		})
	}
}
