package dispatchers

// work_origin_census_1260_test.go — #1260: every driver's root names its work
// origin, so every line logged on its context carries work_origin.
//
// THE CENSUS CHAIN. A driver is something that starts work on a context of its
// own. The #720/#728 census (nested_bound_driver_ctx_720_test.go) already makes
// every production resolve call site in the module name its driver, and every
// background driver own an arm in driverCtxArms720 that builds its REAL
// production context. This census hangs off that set: every key of
// driverCtxArms720 must have a row in workOriginByDriver1260, and every context
// its arm builds must classify (cache.WorkOrigin, the decorator's classifier) as
// that row's origin. So a NEW background driver fails #720 until it has an arm,
// then fails here until its root names an origin — it cannot land as lines
// with neither trace_id nor work_origin.
//
// The drivers #720 does not see (they are not resolve call sites) are proven
// here on their real builders too: a demand revalidation and a background hop.
// The two D2 roots (restactions/api, staleReplaceRoot) are proven, with their
// own workclass.Background census, in
// internal/resolvers/restactions/api/work_origin_stale_replace_1260_test.go;
// the export re-dispatch in internal/handlers/work_origin_export_1260_test.go;
// the request root (main.go, newServerHandler) in
// main_work_origin_1260_test.go.
//
// UNTAGGED on purpose, like the #563 and #720 censuses: it runs in ordinary CI.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// workOriginByDriver1260 — the work origin each #720 background driver's real
// context must carry. The three prewarm-path drivers share the #214 marker, so
// they share its name.
var workOriginByDriver1260 = map[string]string{
	"refresher":        cache.BackgroundOriginRefresher,
	"cohort_seed":      cache.BackgroundOriginCohortSeed,
	"engine_boot":      cache.BackgroundOriginPrewarmEngineBoot,
	"boot_walk":        cache.RefDenialOriginPrewarmPath,
	"content_prewarm":  cache.RefDenialOriginPrewarmPath,
	"pagination_drain": cache.RefDenialOriginPrewarmPath,
}

func TestIssue1260_EveryBackgroundDriverRootNamesItsWorkOrigin(t *testing.T) {
	if o, ok := cache.WorkOrigin(driverRoot720()); ok {
		t.Fatalf("PRECONDITION: an unmarked root must name no origin, got %q — every arm below would be vacuous", o)
	}
	names := make([]string, 0, len(driverCtxArms720))
	for n := range driverCtxArms720 {
		names = append(names, n)
	}
	sort.Strings(names)
	closed := map[string]bool{}
	for _, o := range cache.WorkOrigins {
		closed[o] = true
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			want, ok := workOriginByDriver1260[name]
			if !ok {
				t.Fatalf("#1260 census: background driver %q (driverCtxArms720) has no work origin row — stamp "+
					"its root with a marker cache.WorkOrigin names, and add the row here", name)
			}
			if !closed[want] {
				t.Fatalf("#1260 census: row %q names %q, outside the closed set cache.WorkOrigins", name, want)
			}
			for i, ctx := range driverCtxArms720[name](t) {
				got, ok := cache.WorkOrigin(ctx)
				if !ok || got != want {
					t.Errorf("#1260: driver %s ctx #%d has work origin (%q, %v), want %q — its lines would not "+
						"say which driver logged them", name, i, got, ok, want)
				}
			}
		})
	}
	for name := range workOriginByDriver1260 {
		if _, ok := driverCtxArms720[name]; !ok {
			t.Errorf("#1260 census: stale row %q — no such driver in driverCtxArms720; remove it", name)
		}
	}
}

// A demand revalidation replays a customer's request detached; its attempt's
// context (demandRevalidateOnce, the production builder) names it, not
// "request", though it keeps the request's values.
func TestIssue1260_DemandRevalidationRootNamesItself(t *testing.T) {
	quiesceDefaultSummaryForTest(t)
	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	req = req.WithContext(cache.WithRequestWork(req.Context()))
	var got string
	var ok bool
	demandRevalidateOnce(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, ok = cache.WorkOrigin(r.Context())
	}), req, "L1_issue1260_demand", time.Now(), "s1260")
	if !ok || got != cache.NonCustomerDemandRevalidation {
		t.Fatalf("#1260: a demand revalidation's ctx has work origin (%q, %v), want %q", got, ok, cache.NonCustomerDemandRevalidation)
	}
}

// A background resolve's self-loopback enters as a request; the ingest
// (reseedBackgroundFromHeaders, run first in ServeHTTP) names it a hop.
func TestIssue1260_BackgroundHopNamesItself(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	req.Header.Set(cache.BackgroundHeader, "1")
	req = req.WithContext(xcontext.BuildContext(cache.WithRequestWork(req.Context()),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "hop-1260"})))
	got, ok := cache.WorkOrigin(reseedBackgroundFromHeaders(req).Context())
	if !ok || got != cache.NonCustomerBackgroundHop {
		t.Fatalf("#1260: a background hop's ctx has work origin (%q, %v), want %q", got, ok, cache.NonCustomerBackgroundHop)
	}
	// Control: the same request without the header stays a request.
	plain := httptest.NewRequest(http.MethodGet, "/call", nil)
	plain = plain.WithContext(xcontext.BuildContext(cache.WithRequestWork(context.Background()),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "hop-1260"})))
	if got, _ := cache.WorkOrigin(reseedBackgroundFromHeaders(plain).Context()); got != cache.WorkOriginRequest {
		t.Fatalf("CONTROL: a request with no hop header has work origin %q, want request", got)
	}
}
