package handlers

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
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
	"github.com/krateo-platformops/snowplow/internal/support/pdftable"
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
	if v := rec.Header().Get("X-Content-Type-Options"); v != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", v)
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
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("%d rows: status = %d, want 422", total, rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("%d rows: Content-Type = %q, want application/json", total, ct)
		}
		var st map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil || st["kind"] != "Status" || st["code"] != float64(http.StatusUnprocessableEntity) {
			t.Errorf("%d rows: body is not a Status with code 422: %s", total, rec.Body.String())
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

// failRender makes the pdf render fail for one test.
func failRender(t *testing.T) {
	t.Helper()
	orig := renderPDF
	renderPDF = func(pdftable.Document) (*pdftable.PDF, error) { return nil, errors.New("render failed") }
	t.Cleanup(func() { renderPDF = orig })
}

// TestExport970_BuildThenSend — rule 4: a document that fails to render is
// answered with an error status and a JSON Status — the handler wrote no
// 200 and no PDF byte before the render failed — and the AuditEvent
// records the real outcome and code, in the csv/json shape, format=pdf.
func TestExport970_BuildThenSend(t *testing.T) {
	recorder := logtest.NewRecorder()
	audit.SetDefault(audit.New(recorder.Logger("test-970")))
	t.Cleanup(func() { audit.SetDefault(nil) })

	// The render fails after the rows were collected and converted.
	var queries []url.Values
	req := httptest.NewRequest(http.MethodGet, "/export?resource=restactions&name=demo&namespace=ns&format=pdf&fields=id", nil)
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithLogger(slog.New(slog.DiscardHandler)),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: pdfUser})))
	w := &writeOrder{ResponseRecorder: httptest.NewRecorder()}
	func() {
		failRender(t)
		Export(recordingCall(itemsEnvelope(3), &queries)).ServeHTTP(w, req)
	}()
	renderPDF = pdftable.Render

	if !reflect.DeepEqual(w.codes, []int{http.StatusInternalServerError}) {
		t.Errorf("statuses written = %v, want exactly [500]: nothing may be sent before the document is built", w.codes)
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
		{"failure", 500, "format=pdf rows=3 pages=1 truncated=false"},
		{"success", 200, "format=pdf rows=3 pages=1 truncated=false"},
		{"failure", http.StatusUnprocessableEntity, "format=pdf rows=6 pages=2 truncated=true"},
		{"success", 200, "format=csv rows=6 pages=2 truncated=true"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("audit events:\n got %+v\nwant %+v", got, want)
	}
}

// TestExport970_HeaderLabels — rule 7: one repeated header= per field, in
// fields order. Labels are display text: any character (commas included),
// used verbatim, and they may repeat (two "Name" columns). They head the
// csv and pdf columns; csv cells are otherwise unchanged. The json body
// becomes {columns:[{field,label}] in fields order, rows keyed by field} —
// labels are never keys, so repeated labels cannot collide. A field may be
// selected twice (two views of one value): csv and pdf repeat the column,
// json lists both columns and carries the field once per row. A label list
// that does not pair with the fields (no fields, a count mismatch) is a 400
// before anything is dispatched.
func TestExport970_HeaderLabels(t *testing.T) {
	body := `{"items":[{"name":"alpha","meta":{"created":"2026-10-29","n":3}},{"name":"=beta","meta":{"n":4.5}}]}`
	q := "fields=name,meta.created,meta.n&header=Name&header=" + url.QueryEscape("Created, UTC") + "&header=Name"
	labels := []string{"Name", "Created, UTC", "Name"}
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
	wantJSON := `{
  "columns": [
    {
      "field": "name",
      "label": "Name"
    },
    {
      "field": "meta.created",
      "label": "Created, UTC"
    },
    {
      "field": "meta.n",
      "label": "Name"
    }
  ],
  "rows": [
    {
      "name": "alpha",
      "meta.created": "2026-10-29",
      "meta.n": 3
    },
    {
      "name": "=beta",
      "meta.created": null,
      "meta.n": 4.5
    }
  ]
}
`
	if rec.Body.String() != wantJSON {
		t.Errorf("json =\n%s\nwant (columns in fields order, rows keyed by field, typed values)\n%s", rec.Body.String(), wantJSON)
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
		"header=Name",                         // no fields
		"fields=name,meta.n&header=Name",      // fewer labels than fields
		"fields=name&header=Name&header=More", // more labels than fields
	} {
		queries = nil
		rec, _ := exportAs(t, recordingCall(body, &queries), "/export?resource=restactions&name=demo&format=csv&"+bad)
		if rec.Code != http.StatusBadRequest || len(queries) != 0 {
			t.Errorf("%s: status %d after %d dispatches, want 400 before any dispatch", bad, rec.Code, len(queries))
		}
	}

	// One field selected twice, under two labels: exported in all three
	// formats. csv and pdf repeat the column; json lists both columns and
	// carries the field once per row.
	dup := "fields=meta.n,name,meta.n&header=" + url.QueryEscape("Size") + "&header=Name&header=" + url.QueryEscape("Size, raw")
	rec, _ = exportAs(t, recordingCall(body, &queries), "/export?resource=restactions&name=demo&format=csv&"+dup)
	if want := "Size,Name,\"Size, raw\"\n3,alpha,3\n4.5,'=beta,4.5\n"; rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Errorf("csv with a repeated field: %d %q, want %q", rec.Code, rec.Body.String(), want)
	}
	rec, _ = exportAs(t, recordingCall(body, &queries), "/export?resource=restactions&name=demo&format=json&"+dup)
	compact := regexp.MustCompile(`\s+`).ReplaceAllString(rec.Body.String(), "")
	if want := `{"columns":[{"field":"meta.n","label":"Size"},{"field":"name","label":"Name"},{"field":"meta.n","label":"Size,raw"}],` +
		`"rows":[{"meta.n":3,"name":"alpha"},{"meta.n":4.5,"name":"=beta"}]}`; rec.Code != http.StatusOK || compact != want {
		t.Errorf("json with a repeated field: %d %s, want %s", rec.Code, compact, want)
	}
	rec, _ = exportAs(t, recordingCall(body, &queries), "/export?resource=restactions&name=demo&format=pdf&"+dup)
	d = parsePDF(t, rec)
	for _, want := range []string{"Size | Name | Size, raw", "3 | alpha | 3", "4.5 | =beta | 4.5"} {
		if !hasLine(d.Pages[0], want) {
			t.Errorf("pdf with a repeated field lacks the line %q:\n%s", want, d.Pages[0].Text())
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
	func() {
		failRender(t)
		_, logs = exportAs(t, recordingCall(itemsEnvelope(3), &queries), "/export?resource=restactions&name=demo&format=pdf&fields=id")
	}()
	renderPDF = pdftable.Render
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

// failingWriter accepts the status and headers and fails every body write,
// as a connection the client dropped mid-download does.
type failingWriter struct {
	header http.Header
	code   int
	writes int
}

func (w *failingWriter) Header() http.Header  { return w.header }
func (w *failingWriter) WriteHeader(code int) { w.code = code }
func (w *failingWriter) Write([]byte) (int, error) {
	w.writes++
	return 0, errors.New("connection reset by peer")
}

// TestExport970_SendFailureIsReported — rule 4: when the built document
// cannot be sent (the write fails mid-PDF), the error is not swallowed: it
// is logged and the AuditEvent records outcome=failure (with the 200 that
// was already sent).
func TestExport970_SendFailureIsReported(t *testing.T) {
	recorder := logtest.NewRecorder()
	audit.SetDefault(audit.New(recorder.Logger("test-970")))
	t.Cleanup(func() { audit.SetDefault(nil) })

	var logs bytes.Buffer
	var queries []url.Values
	req := httptest.NewRequest(http.MethodGet, "/export?resource=restactions&name=demo&namespace=ns&format=pdf", nil)
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: pdfUser})))
	w := &failingWriter{header: http.Header{}}
	Export(recordingCall(itemsEnvelope(3), &queries)).ServeHTTP(w, req)

	if w.code != http.StatusOK || w.writes == 0 {
		t.Fatalf("status %d after %d writes: the document should have been built and its send attempted", w.code, w.writes)
	}
	if !bytes.Contains(logs.Bytes(), []byte("unable to serve pdf export")) || !bytes.Contains(logs.Bytes(), []byte("send pdf: connection reset by peer")) {
		t.Errorf("the send failure was not logged:\n%s", logs.String())
	}
	var outcomes []string
	for _, recs := range recorder.Result() {
		for _, r := range recs {
			for _, kv := range r.Attributes {
				if kv.Key == "outcome" {
					outcomes = append(outcomes, kv.Value.AsString())
				}
			}
		}
	}
	if !reflect.DeepEqual(outcomes, []string{"failure"}) {
		t.Errorf("audit outcomes = %v, want [failure] for a document that never arrived", outcomes)
	}
}

// TestExport970_LongColumnNamesRender — the header never refuses a table:
// 32 columns whose auto-detected dot-paths are 120 characters long (no
// labels) render as a 200 PDF with every column's name in the header.
func TestExport970_LongColumnNamesRender(t *testing.T) {
	keys := make([]string, 32)
	row := map[string]any{}
	for c := range keys {
		keys[c] = fmt.Sprintf("k%02d_", c) + strings.Repeat("abcdefghij", 12)[:116]
		row[keys[c]] = "v"
	}
	body, _ := json.Marshal(map[string]any{"items": []any{row, row}})
	var queries []url.Values
	rec, _ := exportAs(t, recordingCall(string(body), &queries), "/export?resource=restactions&name=demo&format=pdf")
	d := parsePDF(t, rec)
	text := strings.ReplaceAll(strings.ReplaceAll(d.Pages[0].Text(), "\n", ""), pdfcheck.Sep, "")
	for _, k := range keys {
		if !strings.Contains(text, k[:4]) {
			t.Errorf("column %s is missing from the header", k[:4])
		}
	}
}

// TestExport970_TitleIgnoredByCSVAndJSON — the export button sends one
// request shape for every format, title included: a csv or json export
// ignores title (no 400, byte-identical to the same export without it),
// and title never reaches the /call re-dispatch. Labelled and unlabelled
// requests both.
func TestExport970_TitleIgnoredByCSVAndJSON(t *testing.T) {
	body := `{"items":[{"name":"alpha","n":1},{"name":"beta","n":2}]}`
	for _, format := range []string{"csv", "json"} {
		for _, extra := range []string{"", "&fields=name,n&header=Name&header=Name"} {
			base := "/export?resource=restactions&name=demo&filename=f&format=" + format + extra
			var q1, q2 []url.Values
			without, _ := exportAs(t, recordingCall(body, &q1), base)
			with, _ := exportAs(t, recordingCall(body, &q2), base+"&title="+url.QueryEscape("Report, Q3"))
			if with.Code != http.StatusOK || without.Code != http.StatusOK {
				t.Fatalf("%s%s: status %d with title, %d without", format, extra, with.Code, without.Code)
			}
			if !bytes.Equal(with.Body.Bytes(), without.Body.Bytes()) || !reflect.DeepEqual(with.Header(), without.Header()) {
				t.Errorf("%s%s: title changed the export:\n%s\nvs\n%s", format, extra, with.Body.String(), without.Body.String())
			}
			if !reflect.DeepEqual(q1, q2) {
				t.Errorf("%s%s: title reached the /call re-dispatch: %v", format, extra, q2)
			}
		}
	}
	// A field listed twice without labels is today's behaviour, unchanged:
	// two columns of the same dot-path (labels are what make it ambiguous).
	var q []url.Values
	rec, _ := exportAs(t, recordingCall(body, &q), "/export?resource=restactions&name=demo&format=csv&fields=name,name")
	if rec.Code != http.StatusOK || rec.Body.String() != "name,name\nalpha,alpha\nbeta,beta\n" {
		t.Errorf("unlabelled repeated field: %d %q, want the unchanged two-column csv", rec.Code, rec.Body.String())
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
