package pdftable

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/testsupport/pdfcheck"
)

// #970 — the table writer. Every document is parsed back with pdfcheck:
// its structure is checked against the PDF specification and its text is
// extracted by an independent reader.

func render(t *testing.T, doc Document) pdfcheck.Doc {
	t.Helper()
	b, err := Render(doc)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	return pdfcheck.Parse(t, b)
}

func rowsOf(n, cols int) [][]string {
	rows := make([][]string, n)
	for i := range rows {
		rows[i] = make([]string, cols)
		for c := range rows[i] {
			rows[i][c] = fmt.Sprintf("r%05d-c%d", i, c)
		}
	}
	return rows
}

func headerOf(cols int) []string {
	h := make([]string, cols)
	for i := range h {
		h[i] = fmt.Sprintf("H%d", i)
	}
	return h
}

var created = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

func hasLine(p pdfcheck.Page, want string) bool {
	for _, l := range p.Lines {
		if l == want {
			return true
		}
	}
	return false
}

// countCells counts every match of re over the whole document.
func countCells(d pdfcheck.Doc, re *regexp.Regexp) map[string]int {
	seen := map[string]int{}
	for _, p := range d.Pages {
		for _, m := range re.FindAllString(p.Text(), -1) {
			seen[m]++
		}
	}
	return seen
}

// TestRenderHeaderOnEveryPageEveryRowOnce: a table longer than a page
// starts its table on every page with the header row, draws every row
// exactly once and below the header, and numbers the pages.
func TestRenderHeaderOnEveryPageEveryRowOnce(t *testing.T) {
	const n = 250
	header := []string{"First column", "Second, with comma", "Third"}
	d := render(t, Document{Title: "Report", Created: created, Meta: []string{"meta line"}, Header: header, Rows: rowsOf(n, 3)})
	if len(d.Pages) < 3 {
		t.Fatalf("pages = %d, want a multi-page table", len(d.Pages))
	}
	wantHeader := strings.Join(header, pdfcheck.Sep)
	cell := regexp.MustCompile(`r\d{5}-c0`)
	for i, p := range d.Pages {
		hdr := -1
		for j, l := range p.Lines {
			if l == wantHeader {
				hdr = j
				break
			}
		}
		if hdr < 0 {
			t.Fatalf("page %d has no header row %q:\n%s", i+1, wantHeader, p.Text())
		}
		for j, l := range p.Lines {
			if j < hdr && cell.MatchString(l) {
				t.Errorf("page %d: a row is drawn above the header: %q", i+1, l)
			}
		}
		if want := fmt.Sprintf("Page %d of %d", i+1, len(d.Pages)); !hasLine(p, want) {
			t.Errorf("page %d lacks %q", i+1, want)
		}
	}
	seen := countCells(d, cell)
	if len(seen) != n {
		t.Errorf("distinct rows = %d, want %d", len(seen), n)
	}
	for r, c := range seen {
		if c != 1 {
			t.Errorf("row %s appears %d times, want once", r, c)
		}
	}
	if got := d.Pages[0].Lines[:2]; got[0] != "Report" || got[1] != "meta line" {
		t.Errorf("first page starts %q, want the title then the meta lines", got)
	}
	if a := d.ActiveContent(); len(a) != 0 {
		t.Errorf("document carries active or external content: %v", a)
	}
	if !bytes.Contains(d.Dicts, []byte("/Title <FEFF005200650070006F00720074>")) {
		t.Errorf("document info lacks the title")
	}
}

// TestRenderUnicodeRoundTrip: text the embedded font covers extracts as
// the same Unicode text; a character it has no glyph for extracts as
// U+FFFD, never as a different character.
func TestRenderUnicodeRoundTrip(t *testing.T) {
	text := "Grüße — Ελληνικά — Кириллица — Łódź — €½±"
	d := render(t, Document{Title: text, Created: created, Header: []string{"Ünïcødé"}, Rows: [][]string{{text}, {"東京 ok"}}})
	page := d.Pages[0]
	for _, want := range []string{text, "Ünïcødé", "\uFFFD\uFFFD ok"} {
		if !hasLine(page, want) {
			t.Errorf("no line %q in:\n%s", want, page.Text())
		}
	}
}

// TestRenderWrapsNeverClips: a long cell wraps inside its column — at
// spaces, at line breaks, and between characters for a word wider than the
// column — and every character is drawn.
func TestRenderWrapsNeverClips(t *testing.T) {
	long := strings.Repeat("lorem ipsum dolor ", 60) + "\nsecond paragraph\tafter a tab\r\nthird " + strings.Repeat("W", 600)
	d := render(t, Document{Title: "t", Created: created, Header: []string{"long", "next"}, Rows: [][]string{{long, "neighbour"}}})
	var lines []string
	for _, p := range d.Pages {
		for _, r := range p.Runs {
			if r.S != "neighbour" && r.S != "long" && r.S != "next" && r.S != "t" && !strings.HasPrefix(r.S, "Page ") {
				lines = append(lines, r.S)
			}
		}
	}
	if len(lines) < 6 {
		t.Fatalf("long cell drawn on %d lines, want it wrapped:\n%s", len(lines), d)
	}
	want := strings.Join(strings.Fields(strings.ReplaceAll(long, "\t", " ")), "")
	if got := strings.Join(strings.Fields(strings.Join(lines, " ")), ""); got != want {
		t.Errorf("wrapped text lost or changed characters:\n got %q\nwant %q", trim(got), trim(want))
	}
	for _, l := range lines {
		if strings.Contains(l, "\t") || strings.Contains(l, "\r") {
			t.Errorf("a control character was drawn: %q", l)
		}
	}
}

func trim(s string) string {
	if r := []rune(s); len(r) > 60 {
		return string(r[:60]) + "…"
	}
	return s
}

// TestWrapParagraph pins the line-break rule on the face directly.
func TestWrapParagraph(t *testing.T) {
	reg, _, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	f := newFace(reg, "F1")
	w := f.width("aaaa bbbb", 10) // room for exactly "aaaa bbbb"
	cases := []struct {
		in   string
		want []string
	}{
		{"aaaa bbbb cccc", []string{"aaaa bbbb", "cccc"}},
		{"aaaa bbbb", []string{"aaaa bbbb"}},
		{"aaaabbbbccccdddd", nil}, // hard breaks, checked below
		{"", []string{""}},
		{"a\n\nb", []string{"a", "", "b"}},
		{"x\ty", []string{"x y"}},
	}
	for _, c := range cases {
		got := f.wrap(c.in, 10, w)
		if c.want == nil {
			if strings.Join(got, "") != c.in || len(got) < 2 {
				t.Errorf("wrap(%q) = %q, want hard breaks keeping every rune", c.in, got)
			}
			for _, l := range got {
				if f.width(l, 10) > w+widthEpsilon {
					t.Errorf("wrap(%q): line %q wider than %v", c.in, l, w)
				}
			}
			continue
		}
		if fmt.Sprint(got) != fmt.Sprint(c.want) {
			t.Errorf("wrap(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	// A column narrower than one glyph still makes progress: one rune a line.
	if got := f.wrap("abc", 10, 0.1); fmt.Sprint(got) != "[a b c]" {
		t.Errorf("wrap into a sub-glyph width = %q, want one rune per line", got)
	}
}

// TestRenderRowTallerThanAPage: a row whose cell needs more lines than a
// page holds continues on the next page, under a repeated header, and no
// line is lost.
func TestRenderRowTallerThanAPage(t *testing.T) {
	var cell []string
	for i := range 150 {
		cell = append(cell, fmt.Sprintf("line-%03d", i))
	}
	d := render(t, Document{Title: "t", Created: created, Header: []string{"tall", "other"}, Rows: [][]string{{"before", "x"}, {strings.Join(cell, "\n"), "y"}, {"after", "z"}}})
	if len(d.Pages) < 2 {
		t.Fatalf("pages = %d, want the tall row split across pages", len(d.Pages))
	}
	for i, p := range d.Pages {
		if !hasLine(p, "tall | other") {
			t.Errorf("page %d lacks the header", i+1)
		}
	}
	seen := countCells(d, regexp.MustCompile(`line-\d{3}`))
	if len(seen) != 150 {
		t.Errorf("lines drawn = %d, want 150", len(seen))
	}
	for l, c := range seen {
		if c != 1 {
			t.Errorf("%s drawn %d times", l, c)
		}
	}
	text := d.String()
	if !(strings.Index(text, "before") < strings.Index(text, "line-000") && strings.Index(text, "line-149") < strings.Index(text, "after")) {
		t.Errorf("rows out of order:\n%s", text)
	}
}

// TestRenderWideTablesSplitIntoColumnBands: columns that do not fit across
// a page at the minimum width print as bands of adjacent columns, each
// band with all the rows and its own header on every page.
func TestRenderWideTablesSplitIntoColumnBands(t *testing.T) {
	const cols, n = 70, 60
	if bands := splitBands(cols); len(bands) < 2 {
		t.Fatalf("bands = %v, want the %d columns split", bands, cols)
	}
	// Cells short enough not to wrap at the minimum width: two letters for
	// the row, two digits for the column.
	rows := make([][]string, n)
	for r := range rows {
		rows[r] = make([]string, cols)
		for c := range rows[r] {
			rows[r][c] = fmt.Sprintf("%c%c%02d", 'a'+r/26, 'a'+r%26, c)
		}
	}
	d := render(t, Document{Title: "wide", Created: created, Header: headerOf(cols), Rows: rows})
	seen := countCells(d, regexp.MustCompile(`\b[a-z]{2}\d{2}\b`))
	if len(seen) != n*cols {
		t.Errorf("cells drawn = %d, want %d", len(seen), n*cols)
	}
	for c, k := range seen {
		if k != 1 {
			t.Errorf("cell %s drawn %d times", c, k)
		}
	}
	for i, p := range d.Pages {
		if !strings.Contains(p.Text(), fmt.Sprintf("of %d", cols)) || !strings.Contains(p.Text(), " | ") {
			t.Errorf("page %d lacks its band caption or header:\n%s", i+1, p.Text())
		}
	}
}

// TestRenderHeaderTooTall: a header that cannot repeat on every page is an
// error, never a clipped or endless document.
func TestRenderHeaderTooTall(t *testing.T) {
	_, err := Render(Document{Title: "t", Created: created, Header: []string{strings.Repeat("label ", 3000)}, Rows: rowsOf(1, 1)})
	if !errors.Is(err, ErrHeaderTooTall) {
		t.Fatalf("err = %v, want ErrHeaderTooTall", err)
	}
}

// TestRenderEmptyShapes: no rows, and no columns, still give a valid
// one-page document.
func TestRenderEmptyShapes(t *testing.T) {
	d := render(t, Document{Title: "empty", Created: created, Header: []string{"a", "b"}})
	if len(d.Pages) != 1 || !hasLine(d.Pages[0], "a | b") {
		t.Errorf("no-rows document:\n%s", d)
	}
	d = render(t, Document{Title: "nothing", Created: created})
	if len(d.Pages) != 1 || !hasLine(d.Pages[0], "nothing") {
		t.Errorf("no-columns document:\n%s", d)
	}
	if _, err := Render(Document{Header: []string{"a"}, Rows: [][]string{{"1", "2"}}}); err == nil {
		t.Errorf("a row with more cells than columns must be an error")
	}
}

// TestRenderConcurrent: documents render concurrently (the parsed fonts
// are shared, each document has its own glyph state); run under -race.
func TestRenderConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	errs := make([]error, 8)
	outs := make([][]byte, 8)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outs[i], errs[i] = Render(Document{Title: fmt.Sprint("doc ", i), Created: created, Header: headerOf(3), Rows: rowsOf(80, 3)})
		}()
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("render %d: %v", i, errs[i])
		}
		d := pdfcheck.Parse(t, outs[i])
		if !hasLine(d.Pages[0], fmt.Sprint("doc ", i)) {
			t.Errorf("render %d has the wrong title", i)
		}
	}
}

func BenchmarkRender50Kx10(b *testing.B) {
	rows := rowsOf(50000, 10)
	header := headerOf(10)
	b.ResetTimer()
	for range b.N {
		out, err := Render(Document{Title: "bench", Created: created, Header: header, Rows: rows})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(out))/(1<<20), "MiB-pdf")
	}
}
