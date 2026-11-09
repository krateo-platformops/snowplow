// issue1265_customer_serves_test.go — #1265 P0: the served-cold counter
// (snowplow_customer_serves_total / _cold_total), X-Snowplow-Serve-Outcome and
// the snowplow.serve.* span attributes, driven through the REAL restactions and
// widgets ServeHTTP with only the four H1 seams swapped (dispatch_seams.go).
//
// THE ORACLE IS NOT THE INSTRUMENT. Every arm counts the live resolves the
// handler ran through the resolve seam itself, and checks the counter against
// that count and against the requests the arm issued — never against another
// reading of the counter.
//
//   - C1 detection: every way a customer request can end up waiting on a live
//     resolve reads cold with its cause, +1 per request (and a put-declined
//     widget is cold on EVERY visit).
//   - C2 conservation: a mix of requests: Σ outcomes == requests issued, and
//     cold == live resolves.
//   - C3 exclusion (#376/#552): refresher, seed, demand revalidation, stale
//     re-run, background and nested self-loopbacks, export pages: each runs a
//     resolve and moves nothing, and carries no header.
//   - C4 stale is warm (#710): a hit on a dirty or stale-derived cell is stale,
//     never cold; a D2 stale-INPUT serve on a cell miss is cold (ruling Q6).
package dispatchers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/workclass"
)

// --- counter reading -------------------------------------------------------

// s1265Snap reads every served and cold cell, keyed "<handler>|<outcome>" and
// "cold|<handler>|<cause>". Every served cell must carry class_warm=none (P0).
func s1265Snap(t *testing.T) map[string]uint64 {
	t.Helper()
	out := map[string]uint64{}
	for _, c := range CustomerServesSnapshot() {
		if c.ClassWarm != classWarmNone {
			t.Fatalf("class_warm=%q on %s|%s: P0 emits only %q", c.ClassWarm, c.Handler, c.Outcome, classWarmNone)
		}
		out[c.Handler+"|"+c.Outcome] = c.Count
	}
	for _, c := range CustomerColdServesSnapshot() {
		out["cold|"+c.Handler+"|"+c.Cause] = c.Count
	}
	return out
}

// s1265Delta is the non-zero movement since before.
func s1265Delta(t *testing.T, before map[string]uint64) map[string]uint64 {
	t.Helper()
	d := map[string]uint64{}
	for k, v := range s1265Snap(t) {
		if v != before[k] {
			d[k] = v - before[k]
		}
	}
	return d
}

func s1265AssertDelta(t *testing.T, what string, before, want map[string]uint64) {
	t.Helper()
	got := s1265Delta(t, before)
	if len(got) != len(want) {
		t.Fatalf("%s: counter moved %v, want exactly %v", what, got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s: counter moved %v, want exactly %v", what, got, want)
		}
	}
}

// --- the two handlers, one harness ------------------------------------------

type s1265Target struct {
	name    string // the handler label
	gvr     schema.GroupVersionResource
	crName  string
	cr      func() *unstructured.Unstructured
	handler func() http.Handler
}

var s1265Targets = []s1265Target{
	{name: "restactions", gvr: h1RAGVR, crName: h1RAName, cr: h1RAUnstructured, handler: RESTAction},
	{name: "widgets", gvr: h1WidgetGVR, crName: h1WName, cr: func() *unstructured.Unstructured {
		return h1WidgetUnstructured(map[string]any{})
	}, handler: Widgets},
}

// s1265Env is one arm's seamed collaborators.
type s1265Env struct {
	tg       s1265Target
	resolves atomic.Int64 // the oracle: live resolves the handler ran
	allow    atomic.Bool
	// requesterDialled makes the fetch report a requester-credential read,
	// which the Put-gate declines (an every-visit-cold widget or RESTAction).
	requesterDialled atomic.Bool
	// onResolve runs inside the resolve seam (D2 arms note a stale input).
	onResolve func(ctx context.Context)
	// resolveErr, when set, is the error the resolve seam returns.
	resolveErr error
}

func s1265Setup(t *testing.T, tg s1265Target) *s1265Env {
	t.Helper()
	// The per-identity cell is the one under test: keep the identity-free
	// content layer out of the plain arms (the content-hit arms turn it on).
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "false")
	h1BuildWatcher(t)
	e := &s1265Env{tg: tg}
	e.allow.Store(true)
	cr := tg.cr()
	r1 := setFetchObjectForTest(func(req *http.Request) objects.Result {
		return objects.Result{GVR: tg.gvr, Unstructured: cr.DeepCopy(), RequesterDialled: e.requesterDialled.Load()}
	})
	r2 := setCheckDispatchRBACForTest(func(context.Context, schema.GroupVersionResource, string) bool { return e.allow.Load() })
	r3 := setRestactionsResolveForTest(func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
		e.resolves.Add(1)
		if e.onResolve != nil {
			e.onResolve(ctx)
		}
		if e.resolveErr != nil {
			return nil, e.resolveErr
		}
		out := &templatesv1.RESTAction{}
		out.SetName(h1RAName)
		out.SetNamespace(h1NS)
		return out, nil
	})
	r4 := setWidgetsResolveForTest(func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
		e.resolves.Add(1)
		if e.onResolve != nil {
			e.onResolve(ctx)
		}
		if e.resolveErr != nil {
			return nil, e.resolveErr
		}
		return h1WidgetUnstructured(map[string]any{}), nil
	})
	t.Cleanup(func() { r4(); r3(); r2(); r1() })
	return e
}

// key is the per-identity key a request under ctx derives.
func (e *s1265Env) key(t *testing.T, ctx context.Context) (string, cacheHandle, *cache.ResolvedKeyInputs) {
	t.Helper()
	var extras map[string]any
	if e.tg.name == "widgets" {
		extras = effectiveKeyExtras(ctx, e.tg.cr().Object, nil)
	}
	return dispatchCacheLookupKey(ctx, e.tg.name, e.tg.gvr.Group, e.tg.gvr.Version, e.tg.gvr.Resource,
		h1NS, e.tg.crName, -1, -1, extras)
}

// warmCell stores a cell under the request's key and returns the key.
func (e *s1265Env) warmCell(t *testing.T, ctx context.Context, entry *cache.ResolvedEntry) string {
	t.Helper()
	key, handle, inputs := e.key(t, ctx)
	if handle == nil || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("setup: expected a live, servable key; key=%q handle=%v inputs=%+v", key, handle != nil, inputs)
	}
	entry.Inputs = inputs
	putFixture(handle, key, entry)
	return key
}

func (e *s1265Env) serve(ctx context.Context, edit func(*http.Request)) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(ctx)
	if edit != nil {
		edit(req)
	}
	e.tg.handler().ServeHTTP(rec, req)
	return rec
}

func s1265Header(t *testing.T, what string, rec *httptest.ResponseRecorder, want string) {
	t.Helper()
	if got := rec.Header().Get(ServeOutcomeHeader); got != want {
		t.Fatalf("%s: %s = %q, want %q (status %d)", what, ServeOutcomeHeader, got, want, rec.Code)
	}
}

func (e *s1265Env) wantResolves(t *testing.T, what string, want int64) {
	t.Helper()
	if got := e.resolves.Load(); got != want {
		t.Fatalf("%s: the handler ran %d live resolves, want %d", what, got, want)
	}
}

// --- C1: every live path is detected ----------------------------------------

func TestIssue1265_C1_EveryLiveServeIsCountedColdWithItsCause(t *testing.T) {
	for _, tg := range s1265Targets {
		h := tg.name
		cold := func(cause string) map[string]uint64 {
			return map[string]uint64{h + "|cold": 1, "cold|" + h + "|" + cause: 1}
		}

		t.Run(h+"/no_cell", func(t *testing.T) {
			e := s1265Setup(t, tg)
			before := s1265Snap(t)
			rec := e.serve(h1ReqCtx(h1User), nil)
			e.wantResolves(t, "a miss", 1)
			s1265Header(t, "a miss", rec, "cold")
			s1265AssertDelta(t, "a miss", before, cold(coldCauseNoCell))
		})

		t.Run(h+"/expired_ttl", func(t *testing.T) {
			t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "60")
			e := s1265Setup(t, tg)
			ctx := h1ReqCtx(h1User)
			e.warmCell(t, ctx, &cache.ResolvedEntry{RawJSON: []byte(`{"old":1}` + "\n")})
			t.Cleanup(cache.ResolvedCache().SetClockForTest(func() time.Time { return time.Now().Add(2 * time.Minute) }))
			before := s1265Snap(t)
			rec := e.serve(ctx, nil)
			e.wantResolves(t, "a lookup that finds the cell past its TTL", 1)
			s1265Header(t, "expired", rec, "cold")
			s1265AssertDelta(t, "expired", before, cold(coldCauseExpiredTTL))
		})

		t.Run(h+"/evicted_maxage", func(t *testing.T) {
			t.Setenv("RESOLVED_CACHE_TTL_SECONDS", strconv.Itoa(10*24*3600))
			t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "120")
			e := s1265Setup(t, tg)
			ctx := h1ReqCtx(h1User)
			e.warmCell(t, ctx, &cache.ResolvedEntry{RawJSON: []byte(`{"old":1}` + "\n")})
			t.Cleanup(cache.ResolvedCache().SetClockForTest(func() time.Time { return time.Now().Add(5 * time.Minute) }))
			before := s1265Snap(t)
			rec := e.serve(ctx, nil)
			e.wantResolves(t, "a lookup that finds the cell past the max age", 1)
			s1265Header(t, "past max age", rec, "cold")
			s1265AssertDelta(t, "past max age", before, cold(coldCauseEvictedMaxAge))
		})

		// A requester the bindings do not name derives a "" first-match
		// BindingUID: #95 never serves that key. (The dispatch gate is the
		// seam here; the key mint's EvaluateRBAC is the real one.)
		t.Run(h+"/ineligible_key", func(t *testing.T) {
			e := s1265Setup(t, tg)
			ctx := h1ReqCtx("mallory")
			if _, handle, inputs := e.key(t, ctx); handle == nil || serveFromCacheEligible(inputs) {
				t.Fatalf("setup: mallory must derive a minted but unservable key")
			}
			before := s1265Snap(t)
			rec := e.serve(ctx, nil)
			e.wantResolves(t, "an unservable key", 1)
			s1265Header(t, "ineligible", rec, "cold")
			s1265AssertDelta(t, "ineligible", before, cold(coldCauseIneligibleKey))
		})

		t.Run(h+"/dry_run", func(t *testing.T) {
			e := s1265Setup(t, tg)
			before := s1265Snap(t)
			rec := e.serve(cache.WithInert(h1ReqCtx(h1User)), nil)
			e.wantResolves(t, "a dry run", 1)
			s1265Header(t, "dry run", rec, "cold")
			s1265AssertDelta(t, "dry run", before, cold(coldCauseDryRun))
		})

		t.Run(h+"/cache_off_l1", func(t *testing.T) {
			e := s1265Setup(t, tg)
			t.Setenv("RESOLVED_CACHE_ENABLED", "false")
			cache.ResetResolvedCacheForTest()
			before := s1265Snap(t)
			rec := e.serve(h1ReqCtx(h1User), nil)
			e.wantResolves(t, "L1 off", 1)
			s1265Header(t, "L1 off", rec, "cold")
			s1265AssertDelta(t, "L1 off", before, cold(coldCauseCacheOff))
		})

		t.Run(h+"/cache_off", func(t *testing.T) {
			e := s1265Setup(t, tg)
			t.Setenv("CACHE_ENABLED", "false")
			cache.ResetResolvedCacheForTest()
			before := s1265Snap(t)
			rec := e.serve(h1ReqCtx(h1User), nil)
			e.wantResolves(t, "cache off", 1)
			s1265Header(t, "cache off", rec, "cold")
			s1265AssertDelta(t, "cache off", before, cold(coldCauseCacheOff))
		})

		// A response whose Put is declined stores nothing: every visit waits.
		t.Run(h+"/put_declined_every_visit", func(t *testing.T) {
			e := s1265Setup(t, tg)
			e.requesterDialled.Store(true)
			ctx := h1ReqCtx(h1User)
			before := s1265Snap(t)
			for i := 0; i < 3; i++ {
				rec := e.serve(ctx, nil)
				s1265Header(t, "a declined visit", rec, "cold")
			}
			e.wantResolves(t, "three declined visits", 3)
			key, handle, _ := e.key(t, ctx)
			if _, ok := handle.Get(key); ok {
				t.Fatal("setup: the requester-credential read must decline the Put")
			}
			s1265AssertDelta(t, "three declined visits", before,
				map[string]uint64{h + "|cold": 3, "cold|" + h + "|" + coldCauseNoCell: 3})
		})

		// Ruling Q6: on a cell miss, a resolve fed a known-stale apistage input
		// (D2) still made the user wait on the refilter and the jq: cold,
		// no_cell. Its stored cell is stale-derived, so the NEXT visit is a
		// stale hit (C4): warm.
		t.Run(h+"/d2_stale_input_then_stale_hit", func(t *testing.T) {
			e := s1265Setup(t, tg)
			t.Cleanup(cache.ResetRefresherForTest) // quiesces the committed debt's replace
			e.onResolve = func(ctx context.Context) {
				if !cache.StaleServeDisabled(ctx) {
					cache.StaleServeSinkFromContext(ctx).Note("s1265-content-cell")
				}
			}
			ctx := h1ReqCtx(h1User)
			before := s1265Snap(t)
			rec := e.serve(ctx, nil)
			s1265Header(t, "a D2 stale-input serve", rec, "cold")
			key, handle, _ := e.key(t, ctx)
			got, ok := handle.GetNoTouch(key)
			if !ok || !got.StaleDerived {
				t.Fatalf("setup: the D2 pass must store a stale-derived cell (ok=%v)", ok)
			}
			s1265AssertDelta(t, "a D2 stale-input serve", before, cold(coldCauseNoCell))
			e.onResolve = nil
			before = s1265Snap(t)
			rec = e.serve(ctx, nil)
			s1265Header(t, "a hit on the stale-derived cell", rec, "stale")
			e.wantResolves(t, "the D2 serve then a hit", 1)
			s1265AssertDelta(t, "a hit on the stale-derived cell", before, map[string]uint64{h + "|stale": 1})
		})

		// #710 PR-2b: a stale-derived pass whose Put is declined is withdrawn and
		// the request re-runs. Two resolves, ONE customer call, ONE cold.
		t.Run(h+"/withdrawn_stale_pass_counts_once", func(t *testing.T) {
			e := s1265Setup(t, tg)
			e.requesterDialled.Store(true)
			e.onResolve = func(ctx context.Context) {
				if !cache.StaleServeDisabled(ctx) {
					cache.StaleServeSinkFromContext(ctx).Note("s1265-content-cell")
				}
			}
			before := s1265Snap(t)
			rec := e.serve(h1ReqCtx(h1User), nil)
			e.wantResolves(t, "a withdrawn pass and its re-run", 2)
			s1265Header(t, "a withdrawn pass", rec, "cold")
			s1265AssertDelta(t, "a withdrawn pass", before, cold(coldCauseNoCell))
		})

		t.Run(h+"/refused", func(t *testing.T) {
			e := s1265Setup(t, tg)
			e.allow.Store(false)
			before := s1265Snap(t)
			rec := e.serve(h1ReqCtx(h1User), nil)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("setup: a denied dispatch must 403; got %d", rec.Code)
			}
			e.wantResolves(t, "a denial", 0)
			s1265Header(t, "a denial (no serve decision)", rec, "")
			s1265AssertDelta(t, "a denial", before, map[string]uint64{h + "|refused": 1})
		})
	}
}

// --- C4: stale is warm -------------------------------------------------------

func TestIssue1265_C4_AStaleServeReadsStaleNeverCold(t *testing.T) {
	for _, tg := range s1265Targets {
		h := tg.name

		t.Run(h+"/fresh_hit", func(t *testing.T) {
			e := s1265Setup(t, tg)
			ctx := h1ReqCtx(h1User)
			e.warmCell(t, ctx, &cache.ResolvedEntry{RawJSON: []byte(`{"warm":1}` + "\n")})
			before := s1265Snap(t)
			rec := e.serve(ctx, nil)
			e.wantResolves(t, "a hit", 0)
			s1265Header(t, "a fresh hit", rec, "fresh")
			s1265AssertDelta(t, "a fresh hit", before, map[string]uint64{h + "|fresh": 1})
		})

		t.Run(h+"/dirty_hit", func(t *testing.T) {
			e := s1265Setup(t, tg)
			cache.ResetRefresherForTest()
			t.Cleanup(cache.ResetRefresherForTest)
			ctx := h1ReqCtx(h1User)
			key := e.warmCell(t, ctx, &cache.ResolvedEntry{RawJSON: []byte(`{"warm":1}` + "\n")})
			// A dep event marks the cell; no refresher worker runs, so the
			// invalidation→fresh window stays open while the hit is served.
			cache.DirtyMarkForTest(key)
			if !cache.InputsMovedSincePut(key) {
				t.Fatal("setup: the dirty mark must open the cell's window")
			}
			before := s1265Snap(t)
			rec := e.serve(ctx, nil)
			e.wantResolves(t, "a dirty hit", 0)
			s1265Header(t, "a dirty hit", rec, "stale")
			s1265AssertDelta(t, "a dirty hit", before, map[string]uint64{h + "|stale": 1})
		})

		t.Run(h+"/stale_derived_hit", func(t *testing.T) {
			e := s1265Setup(t, tg)
			ctx := h1ReqCtx(h1User)
			e.warmCell(t, ctx, &cache.ResolvedEntry{RawJSON: []byte(`{"warm":1}` + "\n"), StaleDerived: true})
			before := s1265Snap(t)
			rec := e.serve(ctx, nil)
			e.wantResolves(t, "a stale-derived hit", 0)
			s1265Header(t, "a stale-derived hit", rec, "stale")
			s1265AssertDelta(t, "a stale-derived hit", before, map[string]uint64{h + "|stale": 1})
		})
	}

	// The identity-free widgetContent shell: the widgets handler's third serve.
	contentArm := func(t *testing.T, dirty bool, want string) {
		e := s1265Setup(t, s1265Targets[1])
		t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
		cache.ResetRefresherForTest()
		t.Cleanup(cache.ResetRefresherForTest)
		ctx := h1ReqCtx(h1User)
		cr := h1WidgetUnstructured(map[string]any{})
		contentKey, contentHandle, _ := dispatchWidgetContentKey(ctx,
			h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource, h1NS, h1WName, -1, -1,
			effectiveKeyExtras(ctx, cr.Object, nil))
		if contentHandle == nil {
			t.Fatal("setup: expected a live widgetContent handle")
		}
		putFixture(contentHandle, contentKey, &cache.ResolvedEntry{
			RawJSON: []byte(`{"status":{"widgetData":{"x":1},"resourcesRefs":{"items":[]}}}`)})
		if dirty {
			cache.DirtyMarkForTest(contentKey)
			if !cache.InputsMovedSincePut(contentKey) {
				t.Fatal("setup: the dirty mark must open the shell's window")
			}
		}
		before := s1265Snap(t)
		rec := e.serve(ctx, nil)
		e.wantResolves(t, "a content hit", 0)
		s1265Header(t, "a content hit", rec, want)
		s1265AssertDelta(t, "a content hit", before, map[string]uint64{"widgets|" + want: 1})
	}
	t.Run("widgets/content_hit_fresh", func(t *testing.T) { contentArm(t, false, "fresh") })
	t.Run("widgets/content_hit_dirty", func(t *testing.T) { contentArm(t, true, "stale") })
}

// --- C3: traffic that is not a top-level customer call is never counted -----

func TestIssue1265_C3_NonCustomerTrafficIsNeverCounted(t *testing.T) {
	for _, tg := range s1265Targets {
		h := tg.name
		// Each arm must RUN a resolve through the real handler (non-vacuous),
		// move no counter cell and stamp no header.
		silent := func(t *testing.T, e *s1265Env, what string, rec *httptest.ResponseRecorder, before map[string]uint64) {
			t.Helper()
			if e.resolves.Load() == 0 {
				t.Fatalf("%s: no resolve ran: the arm proves nothing", what)
			}
			if rec != nil {
				s1265Header(t, what, rec, "")
			}
			s1265AssertDelta(t, what, before, map[string]uint64{})
		}

		t.Run(h+"/background_self_loopback", func(t *testing.T) {
			e := s1265Setup(t, tg)
			before := s1265Snap(t)
			rec := e.serve(h1ReqCtx(h1User), func(r *http.Request) {
				// What a seed's / refresher's stage sends to snowplow itself.
				r.Header.Set(workclass.Header, workclass.Seed.String())
				r.Header.Set(cache.BackgroundHeader, "1")
				r.Header.Set(cache.NestedDepthHeader, "1")
			})
			silent(t, e, "a background self-loopback", rec, before)
		})

		t.Run(h+"/background_hop_without_work_class", func(t *testing.T) {
			e := s1265Setup(t, tg)
			before := s1265Snap(t)
			rec := e.serve(h1ReqCtx(h1User), func(r *http.Request) { r.Header.Set(cache.BackgroundHeader, "1") })
			silent(t, e, "a background hop", rec, before)
		})

		// Each marker alone excludes: the work class a background caller's
		// self-loopback continues (workclass.Header), with no other marker.
		t.Run(h+"/background_work_class_alone", func(t *testing.T) {
			e := s1265Setup(t, tg)
			before := s1265Snap(t)
			rec := e.serve(h1ReqCtx(h1User), func(r *http.Request) { r.Header.Set(workclass.Header, workclass.Refresh.String()) })
			silent(t, e, "a background work-class hop", rec, before)
		})

		t.Run(h+"/customer_nested_self_loopback", func(t *testing.T) {
			e := s1265Setup(t, tg)
			before := s1265Snap(t)
			rec := e.serve(h1ReqCtx(h1User), func(r *http.Request) { r.Header.Set(cache.NestedDepthHeader, "1") })
			silent(t, e, "a customer's nested self-loopback", rec, before)
		})

		t.Run(h+"/export_redispatch", func(t *testing.T) {
			e := s1265Setup(t, tg)
			before := s1265Snap(t)
			rec := e.serve(cache.WithExportRead(h1ReqCtx(h1User)), nil)
			silent(t, e, "an export page", rec, before)
		})

		t.Run(h+"/stale_rerun_pass", func(t *testing.T) {
			e := s1265Setup(t, tg)
			before := s1265Snap(t)
			rec := e.serve(cache.WithStaleServeDisabled(h1ReqCtx(h1User)), nil)
			silent(t, e, "a stale re-run pass", rec, before)
		})

		// The production demand-revalidation attempt (demandRevalidateOnce):
		// a detached re-entry of the handler that served a known-stale hit.
		t.Run(h+"/demand_revalidation", func(t *testing.T) {
			e := s1265Setup(t, tg)
			ctx := h1ReqCtx(h1User)
			key, _, _ := e.key(t, ctx)
			before := s1265Snap(t)
			req := httptest.NewRequest("GET", "/call", nil).WithContext(ctx)
			demandRevalidateOnce(tg.handler(), req, key, time.Now(), "s1265")
			silent(t, e, "a demand revalidation", nil, before)
		})

		// The real refresher: a dirty mark dequeued by the production pool, its
		// handler the production resolveAndPopulateL1 (the resolve-once seam is
		// the oracle; the refresher never enters ServeHTTP).
		t.Run(h+"/refresher", func(t *testing.T) {
			i187RefresherEnv(t)
			e := s1265Setup(t, tg)
			cache.ResetRefresherForTest()
			ctx := h1ReqCtx(h1User)
			key := e.warmCell(t, ctx, &cache.ResolvedEntry{RawJSON: []byte(`{"prior":1}` + "\n")})
			var refreshes atomic.Int64
			t.Cleanup(setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
				refreshes.Add(1)
				e.resolves.Add(1)
				return []byte(`{"fresh":1}` + "\n"), nil
			}))
			before := s1265Snap(t)
			invocations, stop := i1126Loop(t, h, nil, nil)
			t.Cleanup(stop)
			completed0 := cache.RefresherStatsByStat()["completed"]
			cache.DirtyMarkForTest(key)
			i1126WaitFor(t, 15*time.Second, "the refresh dequeue settled", func() bool {
				return invocations.Load() >= 1 && cache.RefresherStatsByStat()["completed"] > completed0
			})
			if refreshes.Load() == 0 {
				t.Fatal("setup: the refresher's handler never re-resolved the cell")
			}
			silent(t, e, "a refresher refresh", nil, before)
		})
	}

	// The real seed (cohort / keep-warm units go through seedOneWidget).
	t.Run("widgets/seed", func(t *testing.T) {
		e := s1265Setup(t, s1265Targets[1])
		entry := navWidgetEntry{W: h1WidgetUnstructured(map[string]any{}), GVR: h1WidgetGVR, Pagination: unpaginatedPrewarm}
		seedCtx := withCohortSeedContext(context.Background(),
			seedTarget{Username: h1User, Groups: []string{"devs"}}, endpoints.Endpoint{}, nil)
		before := s1265Snap(t)
		if err := seedOneWidget(seedCtx, entry, h1NS, seedModeBoot); err != nil {
			t.Fatalf("seedOneWidget: %v", err)
		}
		if e.resolves.Load() == 0 {
			t.Fatal("setup: the seed never reached the resolve: the arm proves nothing")
		}
		s1265AssertDelta(t, "a seed", before, map[string]uint64{})
	})
}

// --- C2: conservation over a mix --------------------------------------------

func TestIssue1265_C2_EveryCustomerCallIsCountedOnceAndColdEqualsLiveResolves(t *testing.T) {
	for _, tg := range s1265Targets {
		h := tg.name
		t.Run(h, func(t *testing.T) {
			e := s1265Setup(t, tg)
			ctx := h1ReqCtx(h1User)
			key, handle, _ := e.key(t, ctx)
			before := s1265Snap(t)
			issued, refused := 0, 0
			store := handle.(*cache.ResolvedCacheStore)
			// A deterministic mix: misses, hits, declined (every-visit-cold)
			// visits, evictions and denials, interleaved on co-prime periods.
			for i := 0; i < 60; i++ {
				if i%3 == 0 {
					store.DeleteForTest(key) // the next allowed serve misses
				}
				e.requesterDialled.Store(i%4 == 1) // a declined Put stores nothing
				e.allow.Store(i%5 != 3)
				rec := e.serve(ctx, nil)
				issued++
				if rec.Code == http.StatusForbidden {
					refused++
					s1265Header(t, "a denial", rec, "")
					continue
				}
				if got := rec.Header().Get(ServeOutcomeHeader); got != "fresh" && got != "cold" {
					t.Fatalf("serve %d: %s = %q, want fresh or cold", i, ServeOutcomeHeader, got)
				}
			}
			d := s1265Delta(t, before)
			var total uint64
			for _, o := range serveOutcomeNames {
				total += d[h+"|"+o]
			}
			if total != uint64(issued) {
				t.Fatalf("Σ outcomes = %d over %d customer calls: %v", total, issued, d)
			}
			if d[h+"|refused"] != uint64(refused) {
				t.Fatalf("refused = %d, want %d", d[h+"|refused"], refused)
			}
			if d[h+"|cold"] != uint64(e.resolves.Load()) {
				t.Fatalf("cold = %d but the handler ran %d live resolves: %v", d[h+"|cold"], e.resolves.Load(), d)
			}
			var causes uint64
			for _, c := range coldCauses {
				causes += d["cold|"+h+"|"+c]
			}
			if causes != d[h+"|cold"] {
				t.Fatalf("Σ causes = %d, cold = %d", causes, d[h+"|cold"])
			}
			if d[h+"|fresh"] == 0 || d[h+"|cold"] == 0 || refused == 0 {
				t.Fatalf("the mix did not exercise every outcome: %v", d)
			}
		})
	}
}

// --- the span attribute ------------------------------------------------------

func TestIssue1265_ServerSpanCarriesTheOutcomeAndNoIdentity(t *testing.T) {
	for _, tg := range s1265Targets {
		h := tg.name
		t.Run(h, func(t *testing.T) {
			e := s1265Setup(t, tg)
			exp := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSampler(sdktrace.AlwaysSample()))
			t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
			serve := func() map[attribute.Key]string {
				ctx, span := tp.Tracer("t").Start(h1ReqCtx(h1User), "GET /call")
				e.serve(ctx, nil)
				span.End()
				spans := exp.GetSpans()
				out := map[attribute.Key]string{}
				for _, kv := range spans[len(spans)-1].Attributes {
					out[kv.Key] = kv.Value.Emit()
				}
				return out
			}
			got := serve() // miss
			if got[spanAttrServeOutcome] != "cold" || got[spanAttrServeColdCause] != coldCauseNoCell || len(got) != 2 {
				t.Fatalf("a cold serve's span attributes = %v, want exactly %s=cold, %s=%s",
					got, spanAttrServeOutcome, spanAttrServeColdCause, coldCauseNoCell)
			}
			got = serve() // hit
			if got[spanAttrServeOutcome] != "fresh" || len(got) != 1 {
				t.Fatalf("a fresh serve's span attributes = %v, want exactly %s=fresh", got, spanAttrServeOutcome)
			}
			for _, v := range got {
				if v == h1User {
					t.Fatalf("the span carries the requester's identity: %v", got)
				}
			}
		})
	}
}

// TestIssue1265_ClosedLabelSets pins the label vocabulary the OTLP mirror and
// the bench read: every cause and outcome cell is pre-populated (a 0 is a
// reading), and class_warm emits only "none" before P3.
func TestIssue1265_ClosedLabelSets(t *testing.T) {
	served := CustomerServesSnapshot()
	if len(served) != len(serveHandlerNames)*len(serveOutcomeNames) {
		t.Fatalf("served cells = %d, want handlers × outcomes", len(served))
	}
	cold := CustomerColdServesSnapshot()
	if len(cold) != len(serveHandlerNames)*len(coldCauses) {
		t.Fatalf("cold cells = %d, want handlers × causes", len(cold))
	}
	want := []string{"no_cell", "expired_ttl", "evicted_maxage", "ineligible_key", "dry_run", "cache_off"}
	if len(coldCauses) != len(want) {
		t.Fatalf("causes = %v, want %v", coldCauses, want)
	}
	for i := range want {
		if coldCauses[i] != want[i] {
			t.Fatalf("causes = %v, want %v", coldCauses, want)
		}
	}
	if coldCauseIndex("joined") != -1 {
		t.Fatal("joined is P2's cause; P0 must not emit it")
	}
}
