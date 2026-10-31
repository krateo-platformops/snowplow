package handlers

// #970 round 6 — an export never writes, for every write shape, on every
// page, and leaves nothing a customer read is served from. Every arm runs in
// a FRESH harness where the request under test is the FIRST resolve: once a
// cluster-list cell is warm, a write iterator stage on a namespaced
// collection path is collapsed into a GET LIST (#1011, pre-existing), which
// would make "the export sent no write" vacuous. Each arm has a control: the
// same RESTAction read by a customer sends the write, so the recorder can
// see one.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func raQueryFor970(name string) string {
	return "apiVersion=templates.krateo.io/v1&resource=restactions&namespace=" + extrasNS970 + "&name=" + name
}

// exportLane970 is one way to export a RESTAction for team green.
type exportLane970 struct {
	name, method, target, body string
	h                          func() http.Handler
}

func exportLanes970(ra string) []exportLane970 {
	q := raQueryFor970(ra)
	return []exportLane970{
		{"GET /export", http.MethodGet, "/export?" + q + "&extras=" + url.QueryEscape(`{"team":"green"}`) + "&format=json&path=.status.rows", "", ExportGET},
		{"POST /export", http.MethodPost, "/export?" + q + "&format=json&path=.status.rows", `{"extras":{"team":"green"}}`, ExportPOST},
	}
}

// freshWriteHarness970 is a fresh cache-on harness whose caller endpoint
// records every request.
func freshWriteHarness970(t *testing.T) (*apiserverRecorder, string) {
	t.Helper()
	rec := &apiserverRecorder{}
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)
	seedExtrasHarness(t, srv.URL)
	return rec, srv.URL
}

func getCall970() http.Handler { return Dispatcher(dispatchers.All())(Call()) }

// TestExport970_NoWriteEscapesAnyStageShape — a write stage with
// continueOnError on and off, and an iterator write stage (one write per
// row of the read stage) with continueOnError on and off. Control: a
// customer GET /call sends the writes. GET and POST /export send none, and
// report the refused stage in X-Snowplow-Stage-Outcomes.
func TestExport970_NoWriteEscapesAnyStageShape(t *testing.T) {
	for _, ra := range []string{writeRAName970, writeNoCoeRAName970, writeIterRAName970, writeIterNoCoeRAName970} {
		t.Run(ra+"/control GET /call", func(t *testing.T) {
			apisrv, serverURL := freshWriteHarness970(t)
			rec := serveAs970(getCall970(), http.MethodGet, "/call?"+raQueryFor970(ra)+"&extras="+url.QueryEscape(`{"team":"green"}`), "", serverURL)
			w := writes(apisrv.take())
			t.Logf("%d writes=%d", rec.Code, len(w))
			if len(w) == 0 {
				t.Fatalf("control: GET /call sent no write (%d %s): the arm cannot observe one", rec.Code, rec.Body.String())
			}
		})
		for _, l := range exportLanes970(ra) {
			t.Run(ra+"/"+l.name, func(t *testing.T) {
				apisrv, serverURL := freshWriteHarness970(t)
				rec := serveAs970(l.h(), l.method, l.target, l.body, serverURL)
				if rec.Code != http.StatusOK {
					t.Fatalf("export: %d %s", rec.Code, rec.Body.String())
				}
				if w := writes(apisrv.take()); len(w) != 0 {
					t.Errorf("export sent %d write(s): %v", len(w), w)
				}
				if oc := rec.Header().Get(util.HeaderStageOutcomes); !strings.Contains(oc, `"name":"notify"`) {
					t.Errorf("export %s = %q: the refused write stage is not reported", util.HeaderStageOutcomes, oc)
				}
			})
		}
	}
}

// TestExport970_NoPageOfAFullListExportWrites — a full-list export walks
// three pages (page size 2, four green rows); no page sends the write, and
// every page's rows export. Control, after: a customer read of page 2 sends
// the write.
func TestExport970_NoPageOfAFullListExportWrites(t *testing.T) {
	for _, l := range exportLanes970(writeRAName970) {
		t.Run(l.name, func(t *testing.T) {
			apisrv, serverURL := freshWriteHarness970(t)
			setExportPaging(t, 2, 100)
			var pages []pageSeen
			h := Export(observePages(&pages)(Dispatcher(dispatchers.All())(Call())))
			if l.method == http.MethodPost {
				h = exportPOST(observePages(&pages))
			}
			rec := serveAs970(h, l.method, l.target, l.body, serverURL)
			var rows []row970
			if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &rows) != nil || len(rows) != 4 {
				t.Fatalf("export: %d rows=%d %s", rec.Code, len(rows), rec.Body.String())
			}
			if len(pages) != 3 {
				t.Fatalf("the export walked %d page(s), want 3: the arm does not reach a page after the first", len(pages))
			}
			if w := writes(apisrv.take()); len(w) != 0 {
				t.Errorf("a full-list export over %d pages sent %d write(s): %v", len(pages), len(w), w)
			}
			ctl := serveAs970(getCall970(), http.MethodGet, "/call?"+raQueryFor970(writeRAName970)+"&extras="+url.QueryEscape(`{"team":"green"}`)+"&page=2&perPage=2", "", serverURL)
			if w := writes(apisrv.take()); ctl.Code != http.StatusOK || len(w) == 0 {
				t.Fatalf("control: a customer read of page 2 sent no write (%d)", ctl.Code)
			}
		})
	}
}

// TestExport970_ExportNeverPrimesACellACallServes — an export, its write
// refused, stores no cell a customer read is later served from. Export
// FIRST (the first resolve of a fresh harness), then the customer's read of
// the export's own window, by the same lane: the read sends the write and
// is not an L1 hit, and the export stored no restactions cell. Then the
// export again, after that read: no write, no hit.
func TestExport970_ExportNeverPrimesACellACallServes(t *testing.T) {
	q := raQueryFor970(writeRAName970)
	window := "&page=1&perPage=" + itoa(exportPageSize)
	extras := url.QueryEscape(`{"team":"green"}`)
	body := `{"extras":{"team":"green"}}`
	for _, c := range []struct {
		name, method, exportTarget, readTarget, body string
		export, read                                 func() http.Handler
	}{
		{"GET", http.MethodGet, "/export?" + q + "&extras=" + extras + "&format=json&path=.status.rows", "/call?" + q + "&extras=" + extras + window, "", ExportGET, getCall970},
		{"POST", http.MethodPost, "/export?" + q + "&format=json&path=.status.rows", "/call/read?" + q + window, body, ExportPOST, callRead},
	} {
		t.Run(c.name, func(t *testing.T) {
			apisrv, serverURL := freshWriteHarness970(t)
			dispatchers.RegisterL1LookupMetricsForTest()

			rec := serveAs970(c.export(), c.method, c.exportTarget, c.body, serverURL)
			if rec.Code != http.StatusOK {
				t.Fatalf("export: %d %s", rec.Code, rec.Body.String())
			}
			if w := writes(apisrv.take()); len(w) != 0 {
				t.Errorf("export sent write(s) %v", w)
			}
			for _, k := range cache.ResolvedCache().KeysForTest() {
				if md, _ := cache.ResolvedCache().MetadataForKey(k); md.Resource == "restactions" {
					t.Errorf("the export stored a restactions cell (%s) of a RESTAction that declares a write", md.Path)
				}
			}

			hits := l1Hits(t)
			rd := serveAs970(c.read(), c.method, c.readTarget, c.body, serverURL)
			if rd.Code != http.StatusOK {
				t.Fatalf("customer read: %d %s", rd.Code, rd.Body.String())
			}
			if w := writes(apisrv.take()); len(w) == 0 {
				t.Errorf("the customer read after an export sent no write: served from the export's resolve")
			}
			if h := l1Hits(t) - hits; h != 0 {
				t.Errorf("the customer read after an export was an L1 hit (+%v)", h)
			}

			hits = l1Hits(t)
			rec = serveAs970(c.export(), c.method, c.exportTarget, c.body, serverURL)
			if rec.Code != http.StatusOK {
				t.Fatalf("export after the read: %d %s", rec.Code, rec.Body.String())
			}
			if w := writes(apisrv.take()); len(w) != 0 {
				t.Errorf("the export after a customer read sent write(s) %v", w)
			}
			if h := l1Hits(t) - hits; h != 0 {
				t.Errorf("the export after a customer read was an L1 hit (+%v)", h)
			}
		})
	}
}

// TestExport970_RefusalTextNamesTheExport — the refusal recorded under the
// write stage's errorKey can reach the exported rows (here the RESTAction
// filter exports it as the one row). It says what happened: an export
// never sends writes; it does not call the export background work.
// Control: a customer read sends the write and records no refusal.
func TestExport970_RefusalTextNamesTheExport(t *testing.T) {
	for _, l := range exportLanes970(writeErrorKeyRAName970) {
		t.Run(l.name, func(t *testing.T) {
			_, serverURL := freshWriteHarness970(t)
			rec := serveAs970(l.h(), l.method, l.target, l.body, serverURL)
			var rows []row970
			if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &rows) != nil || len(rows) != 1 {
				t.Fatalf("export: %d %s", rec.Code, rec.Body.String())
			}
			got := rows[0].Team
			if !strings.Contains(got, "an export never sends writes") || !strings.Contains(got, "verb POST was not sent") {
				t.Errorf("exported errorKey %q does not say the export never sends the write", got)
			}
			if strings.Contains(got, "background work") || strings.Contains(got, "customer request") {
				t.Errorf("exported errorKey %q calls the export background work", got)
			}
		})
	}
	t.Run("control GET /call", func(t *testing.T) {
		apisrv, serverURL := freshWriteHarness970(t)
		rows := raRows(t, serveAs970(getCall970(), http.MethodGet, "/call?"+raQueryFor970(writeErrorKeyRAName970), "", serverURL), "GET /call")
		if w := writes(apisrv.take()); len(w) == 0 {
			t.Fatal("control: GET /call sent no write")
		}
		if len(rows) != 1 || strings.Contains(rows[0].Team, "never") {
			t.Errorf("control rows %+v: a customer read recorded a refusal", rows)
		}
	})
}

// TestExport970_MergedOutcomesKeepTheHeaderBound — the merged
// X-Snowplow-Stage-Outcomes keeps the per-page contract: at most
// api.StageOutcomesMaxHeaderBytes, else the {"truncated":true,"failed":N}
// summary in the resolve's own shape. A page that was already a summary
// makes the merge a summary (its stages are unknown) and never drops the
// other pages' count.
func TestExport970_MergedOutcomesKeepTheHeaderBound(t *testing.T) {
	page := func(reason string, n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = `{"name":"stage-with-a-long-name-` + strconv.Itoa(i) + `","ok":false,"reason":"` + reason + `"}`
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	merged := func(values ...string) string {
		s := &stageOutcomeSet{}
		for _, v := range values {
			s.add(v)
		}
		h := http.Header{}
		s.set(h)
		return h.Get(util.HeaderStageOutcomes)
	}

	reasons := []string{"Error", "Forbidden", "NotFound", "Unauthorized"}
	var big []string
	for _, r := range reasons {
		v := page(r, 40)
		if len(v) > api.StageOutcomesMaxHeaderBytes {
			t.Fatalf("a %d-byte page value is not a valid per-page header", len(v))
		}
		big = append(big, v)
	}
	cases := []struct {
		name   string
		values []string
		want   string
	}{
		{"four valid pages over the bound together", big, api.TruncatedStageOutcomes(160)},
		{"a summarised page between two lists keeps their count", []string{page("Error", 3), api.TruncatedStageOutcomes(2), page("Forbidden", 3)}, api.TruncatedStageOutcomes(6)},
		{"a summarised page with the larger count", []string{page("Error", 3), api.TruncatedStageOutcomes(50)}, api.TruncatedStageOutcomes(50)},
		{"lists under the bound merge to the list", []string{page("Error", 2), page("Error", 2), "[]"}, page("Error", 2)},
		{"all pages succeeded", []string{"[]", "[]"}, "[]"},
	}
	for _, c := range cases {
		got := merged(c.values...)
		if got != c.want {
			t.Errorf("%s: merged %s = %.120q (%d bytes), want %q", c.name, util.HeaderStageOutcomes, got, len(got), c.want)
		}
		if len(got) > api.StageOutcomesMaxHeaderBytes {
			t.Errorf("%s: merged value is %d bytes, over the %d-byte contract", c.name, len(got), api.StageOutcomesMaxHeaderBytes)
		}
	}
}
