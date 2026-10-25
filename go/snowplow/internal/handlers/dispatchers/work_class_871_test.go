package dispatchers

// work_class_871_test.go — #871 (measurement only): the dispatchers' work
// classes. Each arm installs a probe in the unit's body through an existing
// seam and reads the label the Go runtime recorded on the goroutine (goroutine
// profile), on a goroutine the unit starts, and on CPU samples (CPU profile).
//
//	C    a restactions / widgets /call is snowplow_work=customer (one unit)
//	DR   a demand revalidation re-entering ServeHTTP is fill_bg (one unit),
//	     and its attempt hands ServeHTTP a fill_bg ctx although its template is
//	     a customer request
//	RR   a stale serve's re-run stays inside its customer unit (no new unit)
//	LB   a trusted self-loopback carrying a background class continues it
//	     (labelled, not counted); an untrusted or customer-valued one does not
//	P    a prewarm-engine scope is prewarm
//	S    a seed target is seed, nested in its scope, which is restored after
//	D    a pagination-drain job and a re-collection pass are drain

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/response"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/testsupport/workclassprobe"
	"github.com/krateo-platformops/snowplow/internal/workclass"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const wc871Spin = 150 * time.Millisecond

// wc871Serve runs h.ServeHTTP(req) with a probe in the fetch-the-CR seam (the
// first thing the dispatch does after its label is set) under a CPU profile.
func wc871Serve(t *testing.T, h http.Handler, req *http.Request) (*workclassprobe.Probe, map[string]int) {
	t.Helper()
	p := &workclassprobe.Probe{Child: true, Spin: wc871Spin}
	t.Cleanup(setFetchObjectForTest(func(r *http.Request) objects.Result {
		p.Observe(r.Context())
		return objects.Result{Err: &response.Status{Code: http.StatusNotFound, Message: "work-class probe"}}
	}))
	got := workclassprobe.CPUClasses(t, func() { h.ServeHTTP(httptest.NewRecorder(), req) })
	return p, got
}

func wc871RequireUnit(t *testing.T, p *workclassprobe.Probe, got map[string]int, want string) {
	t.Helper()
	workclassprobe.RequireAll(t, "dispatch goroutine", p.Self(), want)
	workclassprobe.RequireAll(t, "goroutine started by the dispatch", p.Children(), want)
	workclassprobe.RequireAll(t, "dispatch request ctx", p.Ctx(), want)
	if got[want] == 0 || len(got) != 1 {
		t.Fatalf("CPU samples of the dispatch: %v, want only %q", got, want)
	}
}

func wc871Started(c workclass.Class) uint64 { return workclass.Snapshot()[c.String()].Started }

func wc871Handlers() map[string]http.Handler {
	return map[string]http.Handler{"restactions": &restActionHandler{}, "widgets": &widgetsHandler{}}
}

func TestWorkClass871_C_CallIsOneCustomerUnit(t *testing.T) {
	for name, h := range wc871Handlers() {
		t.Run(name, func(t *testing.T) {
			before := wc871Started(workclass.Customer)
			req := httptest.NewRequest("GET", "/call?resource="+name+"&name=x&namespace=y", nil)
			p, got := wc871Serve(t, h, req)
			wc871RequireUnit(t, p, got, "customer")
			if d := wc871Started(workclass.Customer) - before; d != 1 {
				t.Fatalf("customer units started moved by %d, want 1", d)
			}
			if after := workclassprobe.GoroutineClass(); after != workclassprobe.None {
				t.Fatalf("the serving goroutine still carries %q after ServeHTTP", after)
			}
		})
	}
}

func TestWorkClass871_DR_DemandRevalidationIsOneFillBGUnit(t *testing.T) {
	for name, h := range wc871Handlers() {
		t.Run(name, func(t *testing.T) {
			bf, bc := wc871Started(workclass.FillBG), wc871Started(workclass.Customer)
			ctx, settle := cache.BeginDemandRevalidation(context.Background(), "k-871-dr-"+name, time.Now())
			defer settle()
			req := httptest.NewRequest("GET", "/call?resource="+name+"&name=x&namespace=y", nil).WithContext(ctx)
			p, got := wc871Serve(t, h, req)
			wc871RequireUnit(t, p, got, "fill_bg")
			if df, dc := wc871Started(workclass.FillBG)-bf, wc871Started(workclass.Customer)-bc; df != 1 || dc != 0 {
				t.Fatalf("fill_bg started +%d, customer started +%d; want +1 / +0", df, dc)
			}
		})
	}
}

// TestWorkClass871_DR_AttemptCtxIsFillBGThoughTheTemplateIsACustomers: the
// demand template is a clone of the customer's labelled request; the attempt's
// ServeHTTP restores its goroutine to the ctx it is handed, so that ctx must be
// fill_bg, not the template's customer.
func TestWorkClass871_DR_AttemptCtxIsFillBGThoughTheTemplateIsACustomers(t *testing.T) {
	var seen string
	h := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { seen = workclass.Of(r.Context()).String() })
	tpl := httptest.NewRequest("GET", "/call?resource=restactions&name=x&namespace=y", nil)
	tpl = tpl.WithContext(workclass.Label(tpl.Context(), workclass.Customer))
	demandRevalidateOnce(h, tpl, "k-871-dr-tpl", time.Now(), "probe")
	if seen != "fill_bg" {
		t.Fatalf("the demand attempt handed ServeHTTP a ctx labelled %q, want fill_bg", seen)
	}
}

func TestWorkClass871_RR_StaleRerunStaysInItsCustomerUnit(t *testing.T) {
	h := &restActionHandler{}
	before := wc871Started(workclass.Customer)
	p := &workclassprobe.Probe{Child: true}
	t.Cleanup(setFetchObjectForTest(func(r *http.Request) objects.Result {
		p.Observe(r.Context())
		return objects.Result{Err: &response.Status{Code: http.StatusNotFound, Message: "work-class probe"}}
	}))
	workclass.Do(context.Background(), workclass.Customer, func(ctx context.Context) {
		req := httptest.NewRequest("GET", "/call?resource=restactions&name=x&namespace=y", nil)
		h.ServeHTTP(httptest.NewRecorder(), req.WithContext(cache.WithStaleServeDisabled(ctx)))
	})
	workclassprobe.RequireAll(t, "re-run goroutine", p.Self(), "customer")
	workclassprobe.RequireAll(t, "re-run ctx", p.Ctx(), "customer")
	if d := wc871Started(workclass.Customer) - before; d != 1 {
		t.Fatalf("customer units started moved by %d, want 1 (the re-run is not a new unit)", d)
	}
}

func TestWorkClass871_LB_TrustedLoopbackContinuesTheCallersClass(t *testing.T) {
	cases := []struct {
		name, user, header, want string
		customerUnit             bool
	}{
		{"trusted prewarm hop", "seed-user", "prewarm", "prewarm", false},
		{"trusted refresh hop", "seed-user", "refresh", "refresh", false},
		{"untrusted hop is a customer", "", "prewarm", "customer", true},
		{"a customer value is not a background class", "seed-user", "customer", "customer", true},
		{"garbage is ignored", "seed-user", "everything", "customer", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bc, bp := wc871Started(workclass.Customer), wc871Started(workclass.Prewarm)
			req := buildLoopbackReq(tc.user, testSelfHost, "", "")
			req.Header.Set(workclass.Header, tc.header)
			p, got := wc871Serve(t, &restActionHandler{}, req)
			wc871RequireUnit(t, p, got, tc.want)
			dc, dp := wc871Started(workclass.Customer)-bc, wc871Started(workclass.Prewarm)-bp
			if tc.customerUnit != (dc == 1) || dp != 0 {
				t.Fatalf("customer started +%d, prewarm started +%d (a continued hop counts no unit)", dc, dp)
			}
		})
	}
}

func TestWorkClass871_P_EngineScopeIsPrewarm(t *testing.T) {
	drainAllCustomerInFlight()
	e := newTestEngine()
	p := &workclassprobe.Probe{Child: true, Spin: wc871Spin}
	e.scopeHandler = func(ctx context.Context, _ prewarmScope) error {
		p.Observe(ctx)
		return nil
	}
	before := wc871Started(workclass.Prewarm)
	e.queue.Add(prewarmScope{kind: scopeKindKeepwarm})
	s, _ := e.queue.Get()
	got := workclassprobe.CPUClasses(t, func() { e.processScope(context.Background(), s) })
	workclassprobe.RequireAll(t, "scope goroutine", p.Self(), "prewarm")
	workclassprobe.RequireAll(t, "goroutine started by the scope", p.Children(), "prewarm")
	workclassprobe.RequireAll(t, "scope ctx", p.Ctx(), "prewarm")
	if got["prewarm"] == 0 || len(got) != 1 {
		t.Fatalf("CPU samples of the scope: %v, want only prewarm", got)
	}
	if d := wc871Started(workclass.Prewarm) - before; d != 1 {
		t.Fatalf("prewarm units started moved by %d, want 1", d)
	}
	if after := workclassprobe.GoroutineClass(); after != workclassprobe.None {
		t.Fatalf("the engine worker still carries %q after the scope", after)
	}
}

func TestWorkClass871_S_SeedTargetIsSeedNestedInItsScope(t *testing.T) {
	p := &workclassprobe.Probe{Child: true, Spin: wc871Spin}
	var after string
	got := workclassprobe.CPUClasses(t, func() {
		workclass.Do(context.Background(), workclass.Prewarm, func(ctx context.Context) {
			_ = measuredSeedUnit(ctx, true, func(ctx context.Context) error {
				p.Observe(ctx)
				return nil
			})
			after = workclassprobe.GoroutineClass()
		})
	})
	workclassprobe.RequireAll(t, "seed goroutine", p.Self(), "seed")
	workclassprobe.RequireAll(t, "goroutine started by the seed", p.Children(), "seed")
	workclassprobe.RequireAll(t, "seed ctx", p.Ctx(), "seed")
	if got["seed"] == 0 || len(got) != 1 {
		t.Fatalf("CPU samples of the seed target: %v, want only seed", got)
	}
	if after != "prewarm" {
		t.Fatalf("after the seed target the scope carries %q, want prewarm restored", after)
	}
}

// BenchmarkServeWorkClass871: the whole per-/call cost of the customer label
// (the request copy included), against the bare request for scale.
func BenchmarkServeWorkClass871(b *testing.B) {
	req := httptest.NewRequest("GET", "/call?resource=restactions&name=x&namespace=y", nil)
	b.Run("label", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			r, end := beginServeWorkClass(req)
			_ = r
			end()
		}
	})
}

// wc871PageSeams swaps the drain's page seams for a probe and returns it.
func wc871PageSeams(t *testing.T, page1 func(page int) *unstructured.Unstructured) *workclassprobe.Probe {
	t.Helper()
	p := &workclassprobe.Probe{Child: true, Spin: wc871Spin}
	prevFetch, prevResolve := paginationFetchPageFn, paginationResolvePageFn
	t.Cleanup(func() { paginationFetchPageFn, paginationResolvePageFn = prevFetch, prevResolve })
	const ns, name = "krateo-system", "compositions-page-datagrid"
	paginationFetchPageFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
		return fetchOKResult(ns, name)
	}
	paginationResolvePageFn = func(ctx context.Context, opts widgets.ResolveOptions) (*unstructured.Unstructured, error) {
		p.Observe(ctx)
		return page1(opts.Page), nil
	}
	return p
}

func TestWorkClass871_D_DrainJobIsDrain(t *testing.T) {
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()
	oldCap := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = 2
	t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })
	const ns, name = "krateo-system", "compositions-page-datagrid"
	p := wc871PageSeams(t, func(page int) *unstructured.Unstructured {
		return nonRBACSensitivePageEnvelope(ns, name, page, false)
	})
	before := wc871Started(workclass.Drain)
	got := workclassprobe.CPUClasses(t, func() {
		drainApiRefPaginationJobs(context.Background(), []apiRefPaginationJob{{
			In: newUnstructuredWidget(ns, name), GVR: drainPageCellGVR(), Page1Res: fakePage1Driven(),
			Depth: 1, PerPage: 5, KeyPerPage: -1, KeyPage: -1, AuthnNS: ns,
		}}, endpoints.Endpoint{}, nil)
	})
	workclassprobe.RequireAll(t, "drain page goroutine", p.Self(), "drain")
	workclassprobe.RequireAll(t, "goroutine started by the page", p.Children(), "drain")
	workclassprobe.RequireAll(t, "drain page ctx", p.Ctx(), "drain")
	if got["drain"] == 0 || len(got) != 1 {
		t.Fatalf("CPU samples of the drain: %v, want only drain", got)
	}
	if d := wc871Started(workclass.Drain) - before; d != 1 {
		t.Fatalf("drain units started moved by %d, want 1 (one job)", d)
	}
}

func TestWorkClass871_D_RecollectPassIsDrain(t *testing.T) {
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()
	t.Cleanup(recollectDelayForTest(0))
	c := newApiRefPaginationCollector()
	job := fakeApiRefPaginationJob("krateo-system", "compositions-page-datagrid")
	status := job.Page1Res.Object["status"].(map[string]any)
	status["resourcesRefs"].(map[string]any)["slice"].(map[string]any)["continue"] = false
	c.collect(job)
	if c.pendingRecollectCount() != 1 {
		t.Fatalf("PRECONDITION: %d pending re-collection candidates, want 1", c.pendingRecollectCount())
	}
	p := wc871PageSeams(t, func(int) *unstructured.Unstructured { return fakeContinuingPage1() })
	got := workclassprobe.CPUClasses(t, func() { recollectPendingApiRefPaginationJobs(context.Background(), c, nil) })
	workclassprobe.RequireAll(t, "re-collection goroutine", p.Self(), "drain")
	workclassprobe.RequireAll(t, "goroutine started by the re-collection", p.Children(), "drain")
	workclassprobe.RequireAll(t, "re-collection ctx", p.Ctx(), "drain")
	if got["drain"] == 0 || len(got) != 1 {
		t.Fatalf("CPU samples of the re-collection: %v, want only drain", got)
	}
}
