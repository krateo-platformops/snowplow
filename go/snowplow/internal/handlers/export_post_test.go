package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
)

// #970 — POST /export: the export with its extras in the request BODY, for a
// filter too large for the URL. It mirrors POST /call/read exactly (the body
// envelope and decoder, the read lane every page goes through), and like
// every export it never sends a write stage. Through the real chain with the
// cache on (seedExtrasHarness): handlers.ExportPOST's composition, observed
// between Export and the read lane by exportPOST's wrapper.

// callRead is the POST /call/read handler under the route's middleware
// prefix (main.go adds UserConfig and the scope in front of both routes).
func callRead() http.Handler {
	return middleware.BodyExtrasDecode(ReadDispatcher(dispatchers.All())(CallRead()))
}

// serveAs970 serves method target (with body) as userA (group devs), with
// the caller's endpoint at serverURL.
func serveAs970(h http.Handler, method, target, body, serverURL string) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rd)
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithAccessToken("token-userA"),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "userA", Groups: []string{"devs"}}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: serverURL}),
	))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// pageSeen is one re-dispatched page as the read lane received it.
type pageSeen struct {
	method, page, perPage, queryExtras, key string
	extras                                  map[string]any
}

func observePages(pages *[]pageSeen) func(http.Handler) http.Handler {
	return func(lane http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			lane.ServeHTTP(w, r)
			ex, _ := util.ExtrasFromContext(r.Context())
			q := r.URL.Query()
			*pages = append(*pages, pageSeen{
				method: r.Method, page: q.Get("page"), perPage: q.Get("perPage"),
				queryExtras: q.Get("extras"), key: w.Header().Get("X-Snowplow-Refresh-Key"), extras: ex,
			})
		})
	}
}

type row970 struct {
	ID   string `json:"id"`
	Team string `json:"team"`
}

func raRows(t *testing.T, rec *httptest.ResponseRecorder, what string) []row970 {
	t.Helper()
	var ra struct {
		Status struct {
			Rows []row970 `json:"rows"`
		} `json:"status"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &ra) != nil {
		t.Fatalf("%s: %d %s", what, rec.Code, rec.Body.String())
	}
	return ra.Status.Rows
}

const raQuery970 = "apiVersion=templates.krateo.io/v1&resource=restactions&namespace=" + extrasNS970 + "&name=team-rows"

// TestExport970_PostExportMatchesCallRead — for two filters in the body, a
// multi-page POST /export exports exactly the rows of POST /call/read with
// that body; every page is a POST through the read lane carrying the body's
// extras (never the query's: a stray ?extras= is ignored, as on
// /call/read), uses the key of a POST /call/read of that window and body,
// and is an L1 hit that stores no new cell.
func TestExport970_PostExportMatchesCallRead(t *testing.T) {
	seedExtrasRESTAction(t)
	dispatchers.RegisterL1LookupMetricsForTest()
	setExportPaging(t, 2, 100) // 4 rows per team: three pages

	var pages []pageSeen
	export := exportPOST(observePages(&pages))
	read := callRead()
	for _, team := range []string{"green", "blue"} {
		body := `{"extras":{"team":"` + team + `"}}`
		var want []row970
		keyByPage := map[string]string{}
		for p := 1; p <= 3; p++ {
			rec := serveAs970(read, http.MethodPost, "/call/read?"+raQuery970+"&page="+itoa(p)+"&perPage=2", body, "https://127.0.0.1:1")
			want = append(want, raRows(t, rec, team+" /call/read page "+itoa(p))...)
			keyByPage[itoa(p)] = rec.Header().Get("X-Snowplow-Refresh-Key")
		}
		if len(want) != 4 {
			t.Fatalf("%s: POST /call/read gave %d rows over 3 pages, want 4", team, len(want))
		}
		// A stray ?extras= naming the OTHER team must change nothing.
		stray := url.QueryEscape(`{"team":"red"}`)
		for _, target := range []string{"/export?" + raQuery970, "/export?" + raQuery970 + "&extras=" + stray} {
			pages = nil
			hitsBefore := l1Hits(t)
			storeBefore := cache.ResolvedCache().Len()
			rec := serveAs970(export, http.MethodPost, target+"&format=json&path=.status.rows", body, "https://127.0.0.1:1")
			var got []row970
			if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &got) != nil {
				t.Fatalf("%s %s: POST /export: %d %s", team, target, rec.Code, rec.Body.String())
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s %s: POST /export rows %+v, want POST /call/read's %+v", team, target, got, want)
			}
			if len(pages) < 2 {
				t.Fatalf("%s: the export walked %d page(s), want at least 2", team, len(pages))
			}
			for _, pg := range pages {
				if pg.method != http.MethodPost {
					t.Errorf("%s: page %s went to the lane as %s, want POST (the /call/read lane)", team, pg.page, pg.method)
				}
				if !reflect.DeepEqual(pg.extras, map[string]any{"team": team}) {
					t.Errorf("%s: page %s carried extras %v, want the body's", team, pg.page, pg.extras)
				}
				if pg.perPage != "2" || pg.key == "" || pg.key != keyByPage[pg.page] {
					t.Errorf("%s: page %s used key %q, POST /call/read of that window %q", team, pg.page, pg.key, keyByPage[pg.page])
				}
			}
			if hits := l1Hits(t) - hitsBefore; hits < float64(len(pages)) {
				t.Errorf("%s: %v of %d pages were served from the /call/read cells", team, hits, len(pages))
			}
			if n := cache.ResolvedCache().Len(); n != storeBefore {
				t.Errorf("%s: POST /export stored %d new cells; every page must reuse its window's", team, n-storeBefore)
			}
		}
	}
}

// TestExport970_PostExportBodyRefusedLikeCallRead — a body that is not the
// /call/read envelope (invalid JSON), and a body over /call/read's 1 MiB
// bound, get exactly /call/read's refusal: the same 400 and body, for an
// explicit window and for the full list, before any page is dispatched.
func TestExport970_PostExportBodyRefusedLikeCallRead(t *testing.T) {
	seedExtrasRESTAction(t)
	var pages []pageSeen
	export := exportPOST(observePages(&pages))
	read := callRead()
	oversize := `{"extras":{"team":"green","pad":"` + strings.Repeat("x", middleware.CallReadMaxBodyBytes) + `"}}`
	for _, c := range []struct{ name, body string }{
		{"invalid JSON", `{"extras":{"team":`},
		{"over the 1 MiB bound", oversize},
	} {
		for _, window := range []string{"&page=1&perPage=5", ""} {
			pages = nil
			want := serveAs970(read, http.MethodPost, "/call/read?"+raQuery970+window, c.body, "https://127.0.0.1:1")
			got := serveAs970(export, http.MethodPost, "/export?"+raQuery970+"&format=json"+window, c.body, "https://127.0.0.1:1")
			if want.Code != http.StatusBadRequest {
				t.Fatalf("%s: POST /call/read answered %d, want 400", c.name, want.Code)
			}
			if got.Code != want.Code || got.Body.String() != want.Body.String() {
				t.Errorf("%s window %q: POST /export answered %d %q, want /call/read's %d %q", c.name, window, got.Code, got.Body.String(), want.Code, want.Body.String())
			}
			if len(pages) != 0 {
				t.Errorf("%s window %q: %d page(s) dispatched after a refused body", c.name, window, len(pages))
			}
		}
	}
}

// apiserverRecorder is the caller's apiserver for the write arms: it records
// every request it receives and accepts all of them.
type apiserverRecorder struct {
	mu   sync.Mutex
	reqs []string
}

func (a *apiserverRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.reqs = append(a.reqs, r.Method+" "+r.URL.Path)
	a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-00","namespace":"team-a"},"data":{"team":"red"}}`))
}

func (a *apiserverRecorder) take() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := a.reqs
	a.reqs = nil
	return out
}

func writes(reqs []string) []string {
	var w []string
	for _, r := range reqs {
		if !strings.HasPrefix(r, http.MethodGet+" ") {
			w = append(w, r)
		}
	}
	return w
}

// TestExport970_ExportNeverSendsAWrite — an export is a read. A RESTAction
// that declares a write stage (POST, continueOnError) is exported by GET
// and by POST /export: the apiserver receives no write, the rows still
// export, and the refused stage is reported in the export's
// X-Snowplow-Stage-Outcomes, not silent. Control: the same RESTAction read
// by GET /call and by POST /call/read (a customer request) does send the
// write, so the recorder can see one.
func TestExport970_ExportNeverSendsAWrite(t *testing.T) {
	api := &apiserverRecorder{}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	seedExtrasHarness(t, srv.URL)

	q := "apiVersion=templates.krateo.io/v1&resource=restactions&namespace=" + extrasNS970 + "&name=" + writeRAName970
	extras := url.QueryEscape(`{"team":"green"}`)
	body := `{"extras":{"team":"green"}}`

	// Controls: a customer read sends the write.
	getCall := Dispatcher(dispatchers.All())(Call())
	want := raRows(t, serveAs970(getCall, http.MethodGet, "/call?"+q+"&extras="+extras, "", srv.URL), "GET /call")
	if w := writes(api.take()); len(w) == 0 {
		t.Fatal("control: GET /call sent no write: the recorder cannot observe one, the arm proves nothing")
	}
	raRows(t, serveAs970(callRead(), http.MethodPost, "/call/read?"+q, body, srv.URL), "POST /call/read")
	if w := writes(api.take()); len(w) == 0 {
		t.Fatal("control: POST /call/read sent no write")
	}

	for _, c := range []struct {
		method string
		h      http.Handler
		target string
		body   string
	}{
		{http.MethodGet, ExportGET(), "/export?" + q + "&extras=" + extras, ""},
		{http.MethodPost, ExportPOST(), "/export?" + q, body},
	} {
		rec := serveAs970(c.h, c.method, c.target+"&format=json&path=.status.rows", c.body, srv.URL)
		var got []row970
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &got) != nil {
			t.Fatalf("%s /export: %d %s", c.method, rec.Code, rec.Body.String())
		}
		if w := writes(api.take()); len(w) != 0 {
			t.Errorf("%s /export sent a write to the apiserver: %v", c.method, w)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s /export rows %+v, want the read's %+v", c.method, got, want)
		}
		var outcomes []map[string]any
		if err := json.Unmarshal([]byte(rec.Header().Get(util.HeaderStageOutcomes)), &outcomes); err != nil {
			t.Fatalf("%s /export %s = %q: %v", c.method, util.HeaderStageOutcomes, rec.Header().Get(util.HeaderStageOutcomes), err)
		}
		reported := false
		for _, o := range outcomes {
			reported = reported || o["name"] == "notify"
		}
		if !reported {
			t.Errorf("%s /export %s = %v: the refused write stage \"notify\" is not reported", c.method, util.HeaderStageOutcomes, outcomes)
		}
	}
}

// TestExport970_PostExportPassthroughIsForcedGET — a POST /export of a
// resource no resolve handler serves falls through to the read-only
// CallRead, exactly as POST /call/read does: the apiserver is asked with a
// GET, never a create.
func TestExport970_PostExportPassthroughIsForcedGET(t *testing.T) {
	api := &apiserverRecorder{}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	seedExtrasHarness(t, srv.URL)

	rec := serveAs970(ExportPOST(), http.MethodPost, "/export?apiVersion=v1&resource=configmaps&namespace=team-a&name=cm-00&format=json&page=1&perPage=5", `{"extras":{}}`, srv.URL)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /export of a configmap: %d %s", rec.Code, rec.Body.String())
	}
	reqs := api.take()
	if len(reqs) == 0 {
		t.Fatal("the apiserver saw no request: the passthrough was not exercised")
	}
	if w := writes(reqs); len(w) != 0 {
		t.Errorf("POST /export reached the apiserver with a write verb: %v (want GET only)", w)
	}
}
