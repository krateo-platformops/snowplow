package handlers

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"github.com/krateo-platformops/snowplow/internal/testsupport/pdfcheck"
	"go.opentelemetry.io/otel/log/logtest"
	"golang.org/x/image/font"
	"golang.org/x/image/font/sfnt"
	"golang.org/x/image/math/fixed"
)

// #970 — GET /export?format=pdf and the generic header labels. One test
// per rule of the issue; every PDF is parsed back with pdfcheck (structure
// checked against the PDF spec, text extracted by an independent reader).

// pdfUser is the caller: non-ASCII, so the username line also proves the
// Unicode round trip, and distinctive, so a log line carrying it is found.
const pdfUser = "jürgen.ω-970@example.com"

// exportAs runs an export as pdfUser, with the request logger captured.
func exportAs(t *testing.T, inner http.Handler, target string) (*httptest.ResponseRecorder, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithLogger(logger),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: pdfUser, Groups: []string{"devs"}}),
	))
	rec := httptest.NewRecorder()
	Export(inner).ServeHTTP(rec, req)
	return rec, &logs
}

func parsePDF(t *testing.T, rec *httptest.ResponseRecorder) pdfcheck.Doc {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/pdf" {
		t.Fatalf("Content-Type = %q, want application/pdf", ct)
	}
	return pdfcheck.Parse(t, rec.Body.Bytes())
}

// recordingCall serves body and records every re-dispatched query.
func recordingCall(body string, queries *[]url.Values) http.Handler {
	return http.HandlerFunc(func(wri http.ResponseWriter, req *http.Request) {
		*queries = append(*queries, req.URL.Query())
		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		_, _ = wri.Write([]byte(body))
	})
}

// itemsEnvelope is a /call envelope of n rows: id (unique, zero-padded),
// a nested object, a Unicode string and a number.
func itemsEnvelope(n int) string {
	items := make([]string, n)
	for i := range items {
		items[i] = fmt.Sprintf(`{"id":"row-%05d","spec":{"owner":"Grüße Ελληνικά Кириллица %d"},"size":%d.5}`, i, i, i)
	}
	return `{"items":[` + strings.Join(items, ",") + `]}`
}

var rowID = regexp.MustCompile(`row-\d{5}`)

// rowCounts counts every row id drawn in the document.
func rowCounts(d pdfcheck.Doc) map[string]int {
	seen := map[string]int{}
	for _, p := range d.Pages {
		for _, id := range rowID.FindAllString(p.Text(), -1) {
			seen[id]++
		}
	}
	return seen
}

func hasLine(p pdfcheck.Page, want string) bool {
	for _, l := range p.Lines {
		if l == want {
			return true
		}
	}
	return false
}

// TestExport970_PDFContent — rule 5 (and the response shape of rule 6): the
// first page states the title, the generation time in UTC, the caller and
// the row count; every page is landscape A4 and starts its table with the
// header row; every row is drawn exactly once; Unicode text round-trips;
// the fonts are embedded; nothing active or external is in the file.
func TestExport970_PDFContent(t *testing.T) {
	const n = 230 // several pages
	before := time.Now().UTC().Truncate(time.Second)
	var queries []url.Values
	rec, _ := exportAs(t, recordingCall(itemsEnvelope(n), &queries),
		"/export?resource=restactions&name=demo&namespace=ns&format=pdf&fields=id,spec.owner,size")
	after := time.Now().UTC()
	d := parsePDF(t, rec)

	if len(d.Pages) < 3 {
		t.Fatalf("pages = %d, want a multi-page document for %d rows", len(d.Pages), n)
	}
	first := d.Pages[0].Text()
	for _, want := range []string{"demo", "User: " + pdfUser, fmt.Sprintf("Rows: %d", n)} {
		if !strings.Contains(first, want) {
			t.Errorf("first page lacks %q:\n%s", want, first)
		}
	}
	gen := regexp.MustCompile(`Generated: (\S+)`).FindStringSubmatch(first)
	if gen == nil {
		t.Fatalf("first page has no generated-at line:\n%s", first)
	}
	at, err := time.Parse(time.RFC3339, gen[1])
	if err != nil || !strings.HasSuffix(gen[1], "Z") || at.Before(before) || at.After(after) {
		t.Errorf("generated-at %q is not the UTC time of the export (%v..%v): %v", gen[1], before, after, err)
	}

	header := strings.Join([]string{"id", "spec.owner", "size"}, pdfcheck.Sep)
	for i, p := range d.Pages {
		if p.Width != 841.89 || p.Height != 595.28 {
			t.Errorf("page %d is %vx%v pt, want landscape A4 841.89x595.28", i+1, p.Width, p.Height)
		}
		if !hasLine(p, header) {
			t.Errorf("page %d does not repeat the header row %q:\n%s", i+1, header, p.Text())
		}
		if want := fmt.Sprintf("Page %d of %d", i+1, len(d.Pages)); !hasLine(p, want) {
			t.Errorf("page %d lacks %q", i+1, want)
		}
	}
	seen := rowCounts(d)
	if len(seen) != n {
		t.Errorf("distinct rows drawn = %d, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Errorf("%s drawn %d times, want once", id, c)
		}
	}
	// Unicode round trip: the exact cell text of a row, as extracted.
	if want := "row-00007 | Grüße Ελληνικά Кириллица 7 | 7.5"; !strings.Contains(d.String(), want) {
		t.Errorf("row text does not round-trip as %q:\n%s", want, d.Pages[0].Text())
	}

	fonts := bytes.Count(d.Dicts, []byte("/Subtype /Type0"))
	if fonts == 0 || bytes.Count(d.Dicts, []byte("/FontFile2 ")) != fonts {
		t.Errorf("fonts not all embedded: %d Type0 fonts, %d FontFile2", fonts, bytes.Count(d.Dicts, []byte("/FontFile2 ")))
	}
	if a := d.ActiveContent(); len(a) != 0 {
		t.Errorf("the PDF carries links, actions, scripts or external references: %v", a)
	}
}

// TestExport970_LongCellsWrapNeverClip — rule 5: a cell far wider than its
// column is wrapped onto several lines inside the column, and every
// character of it is drawn; no text runs into the next column or off the
// page.
func TestExport970_LongCellsWrapNeverClip(t *testing.T) {
	long := strings.Repeat("Ünïcødé words that keep going ", 40) + strings.Repeat("x", 400)
	body := fmt.Sprintf(`{"items":[{"a":%q,"b":"short","c":"row-00001"}]}`, long)
	var queries []url.Values
	rec, _ := exportAs(t, recordingCall(body, &queries), "/export?resource=restactions&name=demo&format=pdf&fields=a,b,c")
	d := parsePDF(t, rec)

	// Measured with the font program embedded in the PDF itself, not with
	// the writer's own metrics. Body text is set in the regular face; the
	// bold header and title are wider, so they are checked against the
	// regular width only where it is a lower bound (the page edge).
	regular := embeddedRegular(t, d)
	const rightMargin = 28.35
	for _, p := range d.Pages {
		for i, r := range p.Runs {
			w := textWidth(t, regular, r.S, r.Size)
			if r.X+w > p.Width-rightMargin+0.01 {
				t.Errorf("run %q ends at %.2f, past the page's right margin %.2f", trim(r.S), r.X+w, p.Width-rightMargin)
			}
			if i+1 < len(p.Runs) && p.Runs[i+1].Y == r.Y && r.X+w > p.Runs[i+1].X {
				t.Errorf("run %q (ends %.2f) overlaps the next column's %q (starts %.2f)", trim(r.S), r.X+w, trim(p.Runs[i+1].S), p.Runs[i+1].X)
			}
		}
	}
	// Reassemble column a: its lines are the runs that continue the cell
	// text, in reading order; a soft break drops the space it broke at.
	rest, lines := long, 0
	for _, p := range d.Pages {
		for _, r := range p.Runs {
			if r.S != "" && strings.HasPrefix(rest, r.S) {
				rest = strings.TrimPrefix(strings.TrimPrefix(rest, r.S), " ")
				lines++
			}
		}
	}
	if rest != "" {
		t.Errorf("cell text was not drawn in full; %d characters missing, starting %q", len([]rune(rest)), trim(rest))
	}
	if lines < 4 {
		t.Errorf("long cell drawn on %d lines, want it wrapped onto many", lines)
	}
}

func trim(s string) string {
	if r := []rune(s); len(r) > 24 {
		return string(r[:24]) + "…"
	}
	return s
}

// embeddedRegular parses the first embedded font program (the body font)
// out of the PDF itself, to measure drawn text independently of the writer.
func embeddedRegular(t *testing.T, d pdfcheck.Doc) *sfnt.Font {
	t.Helper()
	for _, s := range d.Streams {
		if s.IsFontProgram() {
			f, err := sfnt.Parse(s.Data)
			if err != nil {
				t.Fatalf("embedded font program does not parse: %v", err)
			}
			return f
		}
	}
	t.Fatalf("no embedded font program")
	return nil
}

func textWidth(t *testing.T, f *sfnt.Font, s string, size float64) float64 {
	t.Helper()
	var b sfnt.Buffer
	upem := f.UnitsPerEm()
	var units fixed.Int26_6
	for _, r := range s {
		g, err := f.GlyphIndex(&b, r)
		if err != nil {
			t.Fatal(err)
		}
		a, err := f.GlyphAdvance(&b, g, fixed.Int26_6(upem)<<6, font.HintingNone)
		if err != nil {
			t.Fatal(err)
		}
		units += a
	}
	return float64(units) / 64 * size / float64(upem)
}

// TestExport970_ResponseHeaders — rule 6: application/pdf and an attachment
// named with the existing sanitizer and a .pdf extension.
func TestExport970_ResponseHeaders(t *testing.T) {
	var queries []url.Values
	rec, _ := exportAs(t, recordingCall(itemsEnvelope(2), &queries),
		"/export?resource=restactions&name=demo&format=pdf&filename="+url.QueryEscape("../Q3 report"))
	parsePDF(t, rec)
	if cd, want := rec.Header().Get("Content-Disposition"), `attachment; filename="Q3-report.pdf"`; cd != want {
		t.Errorf("Content-Disposition = %q, want %q", cd, want)
	}
	if cl := rec.Header().Get("Content-Length"); cl != strconv.Itoa(rec.Body.Len()) {
		t.Errorf("Content-Length = %q, body is %d bytes", cl, rec.Body.Len())
	}

	rec, _ = exportAs(t, recordingCall(itemsEnvelope(2), &queries), "/export?resource=restactions&name=demo&format=PDF")
	parsePDF(t, rec)
	if cd := rec.Header().Get("Content-Disposition"); !regexp.MustCompile(`^attachment; filename="demo-\d{8}-\d{6}\.pdf"$`).MatchString(cd) {
		t.Errorf("default Content-Disposition = %q, want the sanitized resource name with a .pdf extension", cd)
	}
}

// TestExport970_TitleParam — rule 1: title= sets the report title (default:
// the resource name), and title and header are export parameters, never
// forwarded to the /call re-dispatch.
func TestExport970_TitleParam(t *testing.T) {
	var queries []url.Values
	title := "Quarterly, Ελληνικά & <report>"
	rec, _ := exportAs(t, recordingCall(itemsEnvelope(3), &queries),
		"/export?resource=restactions&name=demo&format=pdf&fields=id&header=Identifier&title="+url.QueryEscape(title))
	d := parsePDF(t, rec)
	if got := d.Pages[0].Lines[0]; got != title {
		t.Errorf("title line = %q, want %q", got, title)
	}
	if len(queries) == 0 {
		t.Fatal("no /call re-dispatch recorded")
	}
	for i, q := range queries {
		for _, p := range []string{"title", "header", "format", "fields"} {
			if _, ok := q[p]; ok {
				t.Errorf("re-dispatch %d forwards the export parameter %q: %v", i, p, q)
			}
		}
	}

	rec, _ = exportAs(t, recordingCall(itemsEnvelope(3), &queries), "/export?resource=restactions&name=inventory-ra&format=pdf")
	if got := parsePDF(t, rec).Pages[0].Lines[0]; got != "inventory-ra" {
		t.Errorf("default title = %q, want the resource name", got)
	}
}

// TestExport970_SameRowsAsCSV — rule 2: the pdf rows come from the same
// re-dispatch and the same row extraction as csv: an explicit jq path, the
// auto-detected shapes, and fields dot-path flattening give the same cells.
func TestExport970_SameRowsAsCSV(t *testing.T) {
	cases := []struct{ name, body, query string }{
		{"jq path + fields", `{"status":{"svc":[{"n":"a","m":{"x":1}},{"n":"b","m":{"x":2.5}}]}}`, "path=.status.svc&fields=n,m.x"},
		{"auto-detect widgetData", `{"status":{"widgetData":{"rows":[{"k":"v1","t":true},{"k":"v2","t":false}]}}}`, ""},
		{"auto-detect items, nested", `{"items":[{"a":{"b":{"c":"deep"}},"arr":[1,2],"nul":null}]}`, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var q1, q2 []url.Values
			csvRec, _ := exportAs(t, recordingCall(c.body, &q1), "/export?resource=restactions&name=demo&format=csv&"+c.query)
			pdfRec, _ := exportAs(t, recordingCall(c.body, &q2), "/export?resource=restactions&name=demo&format=pdf&"+c.query)
			if !reflect.DeepEqual(q1, q2) {
				t.Errorf("pdf re-dispatched %v, csv %v: the /call lane must be identical", q2, q1)
			}
			records, err := csv.NewReader(strings.NewReader(csvRec.Body.String())).ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			d := parsePDF(t, pdfRec)
			for _, r := range records { // the header row, then every data row
				var cells []string
				for _, cell := range r {
					if cell != "" {
						cells = append(cells, cell)
					}
				}
				if want := strings.Join(cells, pdfcheck.Sep); !hasLine(d.Pages[0], want) {
					t.Errorf("pdf lacks the csv row %q:\n%s", want, d.Pages[0].Text())
				}
			}
		})
	}
}

// TestExport970_BoundIsAnErrorNeverATruncatedPDF — rule 3: a full-list pdf
// export that reaches the documented bound is refused with an explicit
// status and a JSON Status body, never sent truncated. Below the bound it
// renders; csv and json keep sending the capped list flagged truncated.
func TestExport970_BoundIsAnErrorNeverATruncatedPDF(t *testing.T) {
	setExportPaging(t, 3, 2) // bound = 6 rows
	for _, total := range []int{6, 100} {
		var calls [][2]int
		rec, _ := exportAs(t, pagedCall(t, total, &calls), "/export?resource=restactions&name=demo&format=pdf")
		if rec.Code != exportBoundStatus {
			t.Fatalf("%d rows: status = %d, want %d", total, rec.Code, exportBoundStatus)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("%d rows: Content-Type = %q, want application/json", total, ct)
		}
		var st map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil || st["kind"] != "Status" || st["code"] != float64(exportBoundStatus) {
			t.Errorf("%d rows: body is not a Status with code %d: %s", total, exportBoundStatus, rec.Body.String())
		}
		if !strings.Contains(fmt.Sprint(st["message"]), "6 rows") {
			t.Errorf("%d rows: message does not name the bound: %v", total, st["message"])
		}
		if bytes.Contains(rec.Body.Bytes(), []byte("%PDF")) || rec.Header().Get("Content-Disposition") != "" {
			t.Errorf("%d rows: a pdf (or its attachment header) was sent", total)
		}
	}

	var calls [][2]int
	rec, _ := exportAs(t, pagedCall(t, 5, &calls), "/export?resource=restactions&name=demo&format=pdf")
	if !strings.Contains(parsePDF(t, rec).Pages[0].Text(), "Rows: 5") {
		t.Errorf("an export under the bound must render all 5 rows")
	}

	for _, f := range []string{"csv", "json"} {
		rec, _ := exportAs(t, pagedCall(t, 100, &calls), "/export?resource=restactions&name=demo&format="+f)
		if rec.Code != http.StatusOK || rec.Header().Get(exportTruncatedHeader) != "true" {
			t.Errorf("%s: status %d, %s=%q: csv/json must still send the capped list, flagged", f, rec.Code, exportTruncatedHeader, rec.Header().Get(exportTruncatedHeader))
		}
	}
}

// writeOrder records every status the handler writes, in order.
type writeOrder struct {
	*httptest.ResponseRecorder
	codes []int
}

func (w *writeOrder) WriteHeader(code int) {
	w.codes = append(w.codes, code)
	w.ResponseRecorder.WriteHeader(code)
}

// TestExport970_BuildThenSend — rule 4: a document that fails to render is
// answered with an error status and a JSON Status — the handler wrote no
// 200 and no PDF byte before the render failed — and the AuditEvent
// records the real outcome and code, in the csv/json shape, format=pdf.
func TestExport970_BuildThenSend(t *testing.T) {
	recorder := logtest.NewRecorder()
	audit.SetDefault(audit.New(recorder.Logger("test-970")))
	t.Cleanup(func() { audit.SetDefault(nil) })

	// A label so long that the header row (repeated on every page) cannot
	// fit: the render fails after the rows were collected.
	label := strings.Repeat("a very long column label ", 400)
	var queries []url.Values
	req := httptest.NewRequest(http.MethodGet, "/export?resource=restactions&name=demo&namespace=ns&format=pdf&fields=id&header="+url.QueryEscape(label), nil)
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithLogger(slog.New(slog.DiscardHandler)),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: pdfUser})))
	w := &writeOrder{ResponseRecorder: httptest.NewRecorder()}
	Export(recordingCall(itemsEnvelope(3), &queries)).ServeHTTP(w, req)

	if !reflect.DeepEqual(w.codes, []int{http.StatusBadRequest}) {
		t.Errorf("statuses written = %v, want exactly [400]: nothing may be sent before the document is built", w.codes)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" || bytes.Contains(w.Body.Bytes(), []byte("%PDF")) {
		t.Errorf("Content-Type %q, body %q: want a JSON Status and no PDF bytes", ct, trim(w.Body.String()))
	}
	if w.Header().Get("Content-Disposition") != "" {
		t.Errorf("an attachment header was sent for a failed render")
	}

	rec, _ := exportAs(t, recordingCall(itemsEnvelope(3), &queries), "/export?resource=restactions&name=demo&namespace=ns&format=pdf")
	parsePDF(t, rec)
	setExportPaging(t, 3, 2)
	var calls [][2]int
	exportAs(t, pagedCall(t, 100, &calls), "/export?resource=restactions&name=demo&namespace=ns&format=pdf")
	exportAs(t, pagedCall(t, 100, &calls), "/export?resource=restactions&name=demo&namespace=ns&format=csv")

	type ev struct {
		outcome string
		code    int64
		body    string
	}
	var got []ev
	for _, recs := range recorder.Result() {
		for _, r := range recs {
			e := ev{body: r.Body.AsString()}
			for _, kv := range r.Attributes {
				switch kv.Key {
				case "outcome":
					e.outcome = kv.Value.AsString()
				case "http.response.status_code":
					e.code = kv.Value.AsInt64()
				}
			}
			got = append(got, e)
		}
	}
	want := []ev{
		{"failure", 400, "format=pdf rows=3 pages=1 truncated=false"},
		{"success", 200, "format=pdf rows=3 pages=1 truncated=false"},
		{"failure", int64(exportBoundStatus), "format=pdf rows=6 pages=2 truncated=true"},
		{"success", 200, "format=csv rows=6 pages=2 truncated=true"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("audit events:\n got %+v\nwant %+v", got, want)
	}
}

// TestExport970_HeaderLabels — rule 7: one repeated header= per field, in
// fields order, labels the csv header row, the json keys and the pdf header
// row; commas and any other character are allowed; csv cells and the json
// values are otherwise unchanged. A label list that does not match the
// fields is a 400 before anything is dispatched.
func TestExport970_HeaderLabels(t *testing.T) {
	body := `{"items":[{"name":"alpha","meta":{"created":"2026-10-29","n":3}},{"name":"=beta","meta":{"n":4.5}}]}`
	q := "fields=name,meta.created,meta.n&header=Name&header=" + url.QueryEscape("Created, UTC") + "&header=" + url.QueryEscape("Größe \"n\"")
	labels := []string{"Name", "Created, UTC", `Größe "n"`}
	var queries []url.Values

	rec, _ := exportAs(t, recordingCall(body, &queries), "/export?resource=restactions&name=demo&format=csv&"+q)
	records, err := csv.NewReader(strings.NewReader(rec.Body.String())).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if want := [][]string{labels, {"alpha", "2026-10-29", "3"}, {"'=beta", "", "4.5"}}; !reflect.DeepEqual(records, want) {
		t.Errorf("csv = %q, want %q", records, want)
	}

	rec, _ = exportAs(t, recordingCall(body, &queries), "/export?resource=restactions&name=demo&format=json&"+q)
	wantJSON := `[
  {
    "Name": "alpha",
    "Created, UTC": "2026-10-29",
    "Größe \"n\"": 3
  },
  {
    "Name": "=beta",
    "Created, UTC": null,
    "Größe \"n\"": 4.5
  }
]
`
	if rec.Body.String() != wantJSON {
		t.Errorf("json =\n%s\nwant (keys in fields order, typed values)\n%s", rec.Body.String(), wantJSON)
	}

	rec, _ = exportAs(t, recordingCall(body, &queries), "/export?resource=restactions&name=demo&format=pdf&"+q)
	d := parsePDF(t, rec)
	if want := strings.Join(labels, pdfcheck.Sep); !hasLine(d.Pages[0], want) {
		t.Errorf("pdf header row is not the labels %q:\n%s", want, d.Pages[0].Text())
	}
	if !hasLine(d.Pages[0], strings.Join([]string{"=beta", "4.5"}, pdfcheck.Sep)) {
		t.Errorf("pdf cells changed by labels:\n%s", d.Pages[0].Text())
	}

	for _, bad := range []string{
		"header=Name",                          // no fields
		"fields=name,meta.n&header=Name",       // fewer labels than fields
		"fields=name&header=Name&header=More",  // more labels than fields
		"fields=name,meta.n&header=X&header=X", // duplicate label
	} {
		queries = nil
		rec, _ := exportAs(t, recordingCall(body, &queries), "/export?resource=restactions&name=demo&format=csv&"+bad)
		if rec.Code != http.StatusBadRequest || len(queries) != 0 {
			t.Errorf("%s: status %d after %d dispatches, want 400 before any dispatch", bad, rec.Code, len(queries))
		}
	}
}

// TestExport970_UnpaginatedWidgetExportsFullList — rule 8: a widget whose
// parent declares a smaller slice (perPage 50) serves 50 rows to a /call
// that carries no page/perPage. An unpaginated export still exports the
// whole list, because every re-dispatch carries an explicit page AND
// perPage that override the declared slice.
func TestExport970_UnpaginatedWidgetExportsFullList(t *testing.T) {
	const total = 1234
	var mu sync.Mutex
	var dispatched []url.Values
	widget := http.HandlerFunc(func(wri http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		mu.Lock()
		dispatched = append(dispatched, q)
		mu.Unlock()
		page, perPage := 1, 50 // the parent's declared slice
		if q.Get("page") != "" && q.Get("perPage") != "" {
			page, _ = strconv.Atoi(q.Get("page"))
			perPage, _ = strconv.Atoi(q.Get("perPage"))
		}
		var rows []string
		for i := (page - 1) * perPage; i < min(page*perPage, total); i++ {
			rows = append(rows, fmt.Sprintf(`{"id":"row-%05d"}`, i))
		}
		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		fmt.Fprintf(wri, `{"status":{"widgetData":{"data":[%s]}}}`, strings.Join(rows, ","))
	})
	rec, _ := exportAs(t, widget, "/export?apiVersion=widgets.templates.krateo.io/v1beta1&resource=tables&name=t&namespace=ns&format=pdf")
	d := parsePDF(t, rec)
	if got := len(rowCounts(d)); got != total {
		t.Errorf("rows exported = %d, want the full %d (the parent's 50-row slice must not apply)", got, total)
	}
	for i, q := range dispatched {
		if q.Get("page") != strconv.Itoa(i+1) || q.Get("perPage") != strconv.Itoa(exportPageSize) {
			t.Errorf("re-dispatch %d carries page=%q perPage=%q, want explicit %d/%d", i, q.Get("page"), q.Get("perPage"), i+1, exportPageSize)
		}
	}
}

// TestExport970_NeverLogsTheUsername — the PDF shows the caller's username
// on purpose; no log line may carry it, on success or on any failure.
func TestExport970_NeverLogsTheUsername(t *testing.T) {
	var queries []url.Values
	var all bytes.Buffer
	_, logs := exportAs(t, recordingCall(itemsEnvelope(3), &queries), "/export?resource=restactions&name=demo&format=pdf")
	all.Write(logs.Bytes())
	_, logs = exportAs(t, recordingCall(itemsEnvelope(3), &queries), "/export?resource=restactions&name=demo&format=pdf&fields=id&header="+url.QueryEscape(strings.Repeat("label ", 3000)))
	all.Write(logs.Bytes())
	setExportPaging(t, 3, 2)
	var calls [][2]int
	_, logs = exportAs(t, pagedCall(t, 100, &calls), "/export?resource=restactions&name=demo&format=pdf")
	all.Write(logs.Bytes())

	if !bytes.Contains(all.Bytes(), []byte("unable to serve pdf export")) {
		t.Fatalf("the failure paths logged nothing — the capture is not observing the handler:\n%s", all.String())
	}
	for _, needle := range []string{pdfUser, "jürgen", "User: "} {
		if bytes.Contains(all.Bytes(), []byte(needle)) {
			t.Errorf("a log line carries the caller identity (%q):\n%s", needle, all.String())
		}
	}
}

// TestExportPathStreamToday pins what howto/export.md states about a `path`
// that yields a jq STREAM (several outputs) rather than one array: the
// outputs are concatenated as JSON text with no separator and that text is
// exported as ONE row in a single `value` column (a stream of numbers merges
// into one number); zero outputs export one empty row. #970 does not change
// this (csv/json stay byte-identical); fixing it is its own issue, and that
// fix flips this test deliberately. Wrapping the stream in [...] exports its
// outputs as rows.
func TestExportPathStreamToday(t *testing.T) {
	body := `{"items":[{"a":1},{"a":2}],"n":[1,2]}`
	cases := []struct{ path, want string }{
		{".items[]", "value\n\"{\"\"a\"\":1}{\"\"a\"\":2}\"\n"},
		{".n[]", "value\n12\n"},
		{"empty", "value\n\n"},
		{"[.items[]]", "a\n1\n2\n"},
		{"null", "\n"},
	}
	for _, c := range cases {
		var queries []url.Values
		rec, _ := exportAs(t, recordingCall(body, &queries), "/export?resource=restactions&name=demo&format=csv&path="+url.QueryEscape(c.path))
		if rec.Code != http.StatusOK || rec.Body.String() != c.want {
			t.Errorf("path %s: %d %q, want %q", c.path, rec.Code, rec.Body.String(), c.want)
		}
	}
}
