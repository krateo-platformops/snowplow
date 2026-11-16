// Package pdfcheck parses a PDF for tests (#970): it checks the file's
// structure against the PDF specification and extracts each page's text
// through an independent reader (github.com/ledongthuc/pdf), so a test
// asserts on what a PDF reader sees, not on what the writer meant to write.
//
// It is imported by tests only; nothing in the snowplow binary depends on it.
package pdfcheck

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ledongthuc/pdf"
)

// Doc is a parsed PDF.
type Doc struct {
	Pages []Page
	// Streams holds every stream, decoded, in file order.
	Streams []Stream
	// Dicts is the file with every stream's data removed: all the
	// dictionaries and objects outside streams.
	Dicts []byte
}

// Stream is one stream object: its dictionary and its decoded data.
type Stream struct {
	Dict string
	Data []byte
}

// IsFontProgram reports an embedded font file (binary glyph data).
func (s Stream) IsFontProgram() bool { return strings.Contains(s.Dict, "/Length1 ") }

// Page is one page's text as a reader extracts it.
type Page struct {
	// Width and Height are the page's MediaBox size, in points.
	Width, Height float64
	// Lines are the page's text lines from top to bottom. Text drawn at the
	// same height forms one line; separate text runs on a line (one per
	// table cell) are joined with Sep.
	Lines []string
	// Runs are the page's text runs (one per text-showing operator), in
	// line order and left to right within a line.
	Runs []Run
}

// Run is one text run: its start position (PDF user space, origin at the
// bottom left), its font size and its text.
type Run struct {
	X, Y, Size float64
	S          string
}

// Sep joins the separate text runs of one line.
const Sep = " | "

// Text is the page's lines joined with newlines.
func (p Page) Text() string { return strings.Join(p.Lines, "\n") }

var (
	objHeader = regexp.MustCompile(`^(\d+) 0 obj\n`)
	streamRe  = regexp.MustCompile(`/Length (\d+)[^>]*>>\nstream\n`)
)

// Parse checks that b is a well-formed PDF and extracts its text. Any
// structural fault fails the test.
func Parse(t testing.TB, b []byte) Doc {
	t.Helper()
	var d Doc
	if !bytes.HasPrefix(b, []byte("%PDF-1.")) {
		t.Fatalf("pdf: missing %%PDF header")
	}
	if !bytes.HasSuffix(b, []byte("%%EOF\n")) {
		t.Fatalf("pdf: missing %%%%EOF trailer")
	}
	checkXref(t, b)
	d.Streams, d.Dicts = splitStreams(t, b)

	r, err := pdf.NewReader(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatalf("pdf: reader rejects the file: %v", err)
	}
	n := r.NumPage()
	if n == 0 {
		t.Fatalf("pdf: no pages")
	}
	for i := 1; i <= n; i++ {
		d.Pages = append(d.Pages, pageText(t, r.Page(i), i))
	}
	return d
}

// checkXref verifies startxref, the cross-reference table and that every
// in-use entry points at the object it numbers.
func checkXref(t testing.TB, b []byte) {
	t.Helper()
	i := bytes.LastIndex(b, []byte("startxref\n"))
	if i < 0 {
		t.Fatalf("pdf: no startxref")
	}
	rest := b[i+len("startxref\n"):]
	off, err := strconv.Atoi(string(rest[:bytes.IndexByte(rest, '\n')]))
	if err != nil || off <= 0 || off >= len(b) {
		t.Fatalf("pdf: bad startxref offset: %v", err)
	}
	if !bytes.HasPrefix(b[off:], []byte("xref\n0 ")) {
		t.Fatalf("pdf: startxref %d does not point at an xref table", off)
	}
	lines := strings.Split(string(b[off:]), "\n")
	count, err := strconv.Atoi(strings.Fields(lines[1])[1])
	if err != nil || count < 2 {
		t.Fatalf("pdf: bad xref subsection header %q", lines[1])
	}
	if lines[2] != "0000000000 65535 f " {
		t.Fatalf("pdf: bad free-list head %q", lines[2])
	}
	for n := 1; n < count; n++ {
		e := lines[2+n]
		if len(e)+1 != 20 || !strings.HasSuffix(e, " 00000 n ") {
			t.Fatalf("pdf: xref entry %d is not a 20-byte in-use entry: %q", n, e)
		}
		o, err := strconv.Atoi(e[:10])
		if err != nil || o <= 0 || o >= off {
			t.Fatalf("pdf: xref entry %d has a bad offset %q", n, e)
		}
		m := objHeader.FindSubmatch(b[o:])
		if m == nil || string(m[1]) != strconv.Itoa(n) {
			t.Fatalf("pdf: xref entry %d points at %q, not at object %d", n, b[o:min(o+16, len(b))], n)
		}
	}
	trailer := strings.Join(lines[2+count:], "\n")
	if !strings.HasPrefix(trailer, "trailer\n<< /Size "+strconv.Itoa(count)+" ") {
		t.Fatalf("pdf: trailer /Size does not match the xref table's %d entries: %q", count, trailer)
	}
}

// splitStreams checks every stream's /Length against its data and decodes
// the Flate ones.
func splitStreams(t testing.TB, b []byte) (streams []Stream, dicts []byte) {
	t.Helper()
	pos := 0
	for {
		loc := streamRe.FindSubmatchIndex(b[pos:])
		if loc == nil {
			dicts = append(dicts, b[pos:]...)
			return streams, dicts
		}
		start := pos + loc[1]
		dicts = append(dicts, b[pos:start]...)
		length, _ := strconv.Atoi(string(b[pos+loc[2] : pos+loc[3]]))
		end := start + length
		if end+len("\nendstream") > len(b) || !bytes.HasPrefix(b[end:], []byte("\nendstream")) {
			t.Fatalf("pdf: a stream's /Length %d does not end at endstream", length)
		}
		data := b[start:end]
		dict := string(b[pos+loc[0] : start])
		if strings.Contains(dict, "/FlateDecode") {
			zr, err := zlib.NewReader(bytes.NewReader(data))
			if err != nil {
				t.Fatalf("pdf: flate stream does not decode: %v", err)
			}
			dec, err := io.ReadAll(zr)
			if err != nil {
				t.Fatalf("pdf: flate stream does not decode: %v", err)
			}
			data = dec
		}
		streams = append(streams, Stream{Dict: dict, Data: data})
		pos = end
	}
}

// pageText extracts a page's text runs and groups them into lines.
func pageText(t testing.TB, p pdf.Page, num int) (page Page) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("pdf: page %d content does not parse: %v", num, r)
		}
	}()
	// The reader emits one Text per character, each carrying the position
	// of the text run (Tj) it belongs to; consecutive characters at the
	// same position are one run.
	mb := p.V.Key("MediaBox")
	if mb.IsNull() {
		mb = p.V.Key("Parent").Key("MediaBox")
	}
	if mb.Len() == 4 {
		page.Width = mb.Index(2).Float64() - mb.Index(0).Float64()
		page.Height = mb.Index(3).Float64() - mb.Index(1).Float64()
	}
	type run struct {
		x, y, size float64
		s          strings.Builder
	}
	var runs []*run
	for _, tx := range p.Content().Text {
		if n := len(runs); n > 0 && runs[n-1].x == tx.X && runs[n-1].y == tx.Y {
			runs[n-1].s.WriteString(tx.S)
			continue
		}
		r := &run{x: tx.X, y: tx.Y, size: tx.FontSize}
		r.s.WriteString(tx.S)
		runs = append(runs, r)
	}
	sort.SliceStable(runs, func(i, j int) bool {
		if runs[i].y != runs[j].y {
			return runs[i].y > runs[j].y
		}
		return runs[i].x < runs[j].x
	})
	var cur []string
	for i, r := range runs {
		if i > 0 && r.y != runs[i-1].y {
			page.Lines = append(page.Lines, strings.Join(cur, Sep))
			cur = nil
		}
		cur = append(cur, r.s.String())
		page.Runs = append(page.Runs, Run{X: r.x, Y: r.y, Size: r.size, S: r.s.String()})
	}
	if len(cur) > 0 {
		page.Lines = append(page.Lines, strings.Join(cur, Sep))
	}
	return page
}

// ForbiddenKeys are the PDF names that would make a document active or
// make it reach outside itself: links, actions, scripts, forms, embedded
// or external content.
var ForbiddenKeys = []string{
	"/URI", "/JavaScript", "/JS", "/Launch", "/OpenAction", "/AA", "/Annots",
	"/GoToR", "/GoToE", "/SubmitForm", "/ImportData", "/AcroForm",
	"/EmbeddedFile", "/EmbeddedFiles", "/XObject", "/RichMedia", "/Names",
}

// ActiveContent lists the forbidden names present in the document's
// dictionaries or its non-font streams (a font program is binary glyph
// data, never interpreted as PDF syntax).
func (d Doc) ActiveContent() []string {
	var found []string
	for _, k := range ForbiddenKeys {
		re := regexp.MustCompile(regexp.QuoteMeta(k) + `[^A-Za-z0-9]`)
		hit := re.Match(d.Dicts)
		for _, s := range d.Streams {
			hit = hit || !s.IsFontProgram() && re.Match(s.Data)
		}
		if hit {
			found = append(found, k)
		}
	}
	return found
}

// String renders the document's text for failure messages.
func (d Doc) String() string {
	var b strings.Builder
	for i, p := range d.Pages {
		fmt.Fprintf(&b, "--- page %d ---\n%s\n", i+1, p.Text())
	}
	return b.String()
}
