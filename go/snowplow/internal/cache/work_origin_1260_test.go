package cache

// work_origin_1260_test.go — #1260: WorkOrigin, the classifier the log decorator
// stamps as work_origin. The driver ROOTS are proven in dispatchers
// (work_origin_census_1260_test.go) on their real production contexts; this arm
// pins the classifier itself: every marker names its driver, the most specific
// root wins, an unmarked context names nothing, and the set is closed.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIssue1260_WorkOrigin_EachMarkerNamesItsDriver(t *testing.T) {
	bg := context.Background()
	demand, _ := BeginDemandRevalidation(bg, "L1_1260", time.Now())
	cases := []struct {
		name string
		ctx  context.Context
		want string // "" = no work_origin
	}{
		{"nil", nil, ""},
		{"unmarked (boot, informer callback, plumbing)", bg, ""},
		{"request", WithRequestWork(bg), WorkOriginRequest},
		{"refresher", WithBackgroundResolveOrigin(bg, BackgroundOriginRefresher), BackgroundOriginRefresher},
		{"cohort seed", WithPrewarmPath(WithBackgroundResolveOrigin(bg, BackgroundOriginCohortSeed)), BackgroundOriginCohortSeed},
		{"prewarm engine boot", WithBackgroundResolveOrigin(bg, BackgroundOriginPrewarmEngineBoot), BackgroundOriginPrewarmEngineBoot},
		{"prewarm path (boot walk, content prewarm, pagination drain)", WithPrewarmPath(bg), RefDenialOriginPrewarmPath},
		{"background with no origin (the detector cell)", WithBackgroundResolve(bg), RefDenialOriginBackgroundUnattributed},
		{"demand revalidation", demand, NonCustomerDemandRevalidation},
		{"background hop", WithBackgroundHop(bg), NonCustomerBackgroundHop},
		{"export", WithExportRead(bg), NonCustomerExport},
		{"stale replace", WithStaleReplaceWork(bg), WorkOriginStaleReplace},

		// A request's own re-dispatches keep its context values: each names
		// itself, not "request".
		{"export of a request", WithExportRead(WithRequestWork(bg)), NonCustomerExport},
		{"demand revalidation replaying a request", func() context.Context {
			c, _ := BeginDemandRevalidation(WithRequestWork(bg), "L1_1260", time.Now())
			return c
		}(), NonCustomerDemandRevalidation},
		{"background hop entering as a request", WithBackgroundHop(WithRequestWork(bg)), NonCustomerBackgroundHop},
		// A D2 replace runs the refresher's own resolve, which stamps
		// "refresher"; the work is the replace.
		{"refresher resolve under a D2 replace", WithBackgroundResolveOrigin(WithStaleReplaceWork(bg), BackgroundOriginRefresher), WorkOriginStaleReplace},

		// A detached goroutine (WithDetachedWork, the workclass.Detach rule):
		// a background driver's keeps its name, a request's or an unattributed
		// caller's is named fresh.
		{"detached from a request", WithDetachedWork(bg, WithRequestWork(bg), WorkOriginClusterListPopulate), WorkOriginClusterListPopulate},
		{"detached from unattributed work", WithDetachedWork(bg, bg, WorkOriginClusterListPopulate), WorkOriginClusterListPopulate},
		{"detached from the refresher", WithDetachedWork(bg, WithBackgroundResolveOrigin(bg, BackgroundOriginRefresher), WorkOriginClusterListPopulate), BackgroundOriginRefresher},
		{"detached from a prewarm walk", WithDetachedWork(bg, WithPrewarmPath(bg), WorkOriginClusterListPopulate), RefDenialOriginPrewarmPath},
		{"detached from an export of a request", WithDetachedWork(bg, WithExportRead(WithRequestWork(bg)), WorkOriginClusterListPopulate), NonCustomerExport},
		{"detached from a D2 replace", WithDetachedWork(bg, WithStaleReplaceWork(bg), WorkOriginClusterListPopulate), WorkOriginStaleReplace},
		// A detached root outranks a marker its own work stamps later.
		{"refresher marker under a detached root", WithBackgroundResolveOrigin(WithDetachedWork(bg, WithRequestWork(bg), WorkOriginClusterListPopulate), BackgroundOriginRefresher), WorkOriginClusterListPopulate},
	}
	closed := map[string]bool{}
	for _, o := range WorkOrigins {
		if closed[o] {
			t.Fatalf("WorkOrigins lists %q twice", o)
		}
		closed[o] = true
	}
	seen := map[string]bool{}
	for _, c := range cases {
		got, ok := WorkOrigin(c.ctx)
		if ok != (c.want != "") || got != c.want {
			t.Errorf("%s: WorkOrigin = (%q, %v), want %q", c.name, got, ok, c.want)
		}
		if ok && !closed[got] {
			t.Errorf("%s: %q is not in the closed set WorkOrigins %v", c.name, got, WorkOrigins)
		}
		seen[got] = true
	}
	// Every member of the closed set is reachable from a marker above, so the
	// set holds no name nothing can produce.
	for _, o := range WorkOrigins {
		if !seen[o] {
			t.Errorf("WorkOrigins member %q is produced by no marker in this table", o)
		}
	}
}

// The middleware main.go installs at the server root marks the handler's ctx.
func TestIssue1260_RequestWorkMiddleware_MarksTheHandlersContext(t *testing.T) {
	var got string
	var ok bool
	h := RequestWorkMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, ok = WorkOrigin(r.Context())
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/call", nil))
	if !ok || got != WorkOriginRequest {
		t.Fatalf("a served request's ctx: WorkOrigin = (%q, %v), want %q", got, ok, WorkOriginRequest)
	}
}
