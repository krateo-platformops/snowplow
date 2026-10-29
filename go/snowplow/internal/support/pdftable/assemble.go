package pdftable

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"slices"
	"sync"
	"unicode/utf16"
)

// PDF is a finished document: the bytes of the file in order, as parts.
// The page content streams are parts of their own, moved in from the
// layout rather than copied, so a document is held once, not twice.
type PDF struct {
	parts [][]byte
	size  int
}

// Len is the size of the file in bytes.
func (p *PDF) Len() int { return p.size }

// WriteTo writes the file to w.
func (p *PDF) WriteTo(w io.Writer) (int64, error) {
	var n int64
	for _, part := range p.parts {
		m, err := w.Write(part)
		n += int64(m)
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// Object numbers. The fixed objects come first; page i (0-based) is
// firstPageObj+3i, followed by its body and footer content streams.
const (
	catalogObj   = 1
	pagesObj     = 2
	infoObj      = 3
	firstFontObj = 4 // five objects per face, see writeFace
	objsPerFace  = 5
	numFaces     = 2
	firstPageObj = firstFontObj + objsPerFace*numFaces
)

// assemble writes the finished pages, fonts and metadata as one PDF.
func (l *layout) assemble(doc Document) (*PDF, error) {
	w := &pdfWriter{}
	w.buf.WriteString("%PDF-1.7\n%\xe2\xe3\xcf\xd3\n")

	n := len(l.pages)
	w.object(catalogObj, fmt.Sprintf("<< /Type /Catalog /Pages %d 0 R >>", pagesObj))

	var kids bytes.Buffer
	for i := range n {
		fmt.Fprintf(&kids, "%d 0 R ", firstPageObj+3*i)
	}
	w.object(pagesObj, fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", bytes.TrimSpace(kids.Bytes()), n))

	w.object(infoObj, fmt.Sprintf("<< /Title %s /Producer (Krateo snowplow) /CreationDate (D:%s) >>",
		utf16Hex(doc.Title), doc.Created.UTC().Format("20060102150405Z")))

	// The footer needs the page count, so it is drawn now, after layout;
	// encoding it first also records its glyphs before the fonts' width
	// and ToUnicode tables are written.
	footers := make([][]byte, n)
	for i := range n {
		var c bytes.Buffer
		label := fmt.Sprintf("Page %d of %d", i+1, n)
		x := pageW - margin - l.reg.width(label, footSize)
		fmt.Fprintf(&c, "BT /%s %s Tf 1 0 0 1 %s %s Tm ", l.reg.res, num(footSize), num(x), num(margin))
		l.reg.encode(&c, label)
		c.WriteString(" Tj ET\n")
		footers[i] = c.Bytes()
	}

	for i, f := range []*face{l.reg, l.bold} {
		if err := w.face(firstFontObj+objsPerFace*i, f); err != nil {
			return nil, err
		}
	}

	resources := fmt.Sprintf("<< /Font << /%s %d 0 R /%s %d 0 R >> /ProcSet [/PDF /Text] >>",
		l.reg.res, firstFontObj, l.bold.res, firstFontObj+objsPerFace)
	for i := range n {
		page := firstPageObj + 3*i
		w.object(page, fmt.Sprintf("<< /Type /Page /Parent %d 0 R /MediaBox [0 0 %s %s] /Resources %s /Contents [%d 0 R %d 0 R] >>",
			pagesObj, num(pageW), num(pageH), resources, page+1, page+2))
		w.streamPart(page+1, "/Filter /FlateDecode", l.pages[i])
		l.pages[i] = nil // the PDF holds it now
		w.stream(page+2, "", footers[i])
	}
	return w.finish(infoObj)
}

// face writes the five objects of one embedded TrueType face: the Type0
// font, its CIDFontType2 descendant, the font descriptor, the font program
// and the ToUnicode map.
func (w *pdfWriter) face(first int, f *face) error {
	if f.err != nil {
		return f.err
	}
	type0, cid, desc, file, toUni := first, first+1, first+2, first+3, first+4
	w.object(type0, fmt.Sprintf("<< /Type /Font /Subtype /Type0 /BaseFont /%s /Encoding /Identity-H /DescendantFonts [%d 0 R] /ToUnicode %d 0 R >>",
		f.psName, cid, toUni))

	ids := make([]uint16, 0, len(f.used))
	for id := range f.used {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	var widths bytes.Buffer
	for _, id := range ids {
		fmt.Fprintf(&widths, "%d [%s] ", id, num(f.used[id].adv*1000/f.upem))
	}
	w.object(cid, fmt.Sprintf("<< /Type /Font /Subtype /CIDFontType2 /BaseFont /%s /CIDSystemInfo << /Registry (Adobe) /Ordering (Identity) /Supplement 0 >> /FontDescriptor %d 0 R /CIDToGIDMap /Identity /DW 1000 /W [%s] >>",
		f.psName, desc, bytes.TrimSpace(widths.Bytes())))

	scale := 1000 / f.upem
	// Flags 32: nonsymbolic. StemV is a required hint no viewer needs for
	// an embedded font; 80 is the conventional regular-weight value.
	w.object(desc, fmt.Sprintf("<< /Type /FontDescriptor /FontName /%s /Flags 32 /FontBBox [%s %s %s %s] /ItalicAngle 0 /Ascent %s /Descent %s /CapHeight %s /StemV 80 /FontFile2 %d 0 R >>",
		f.psName, num(f.bbox[0]*scale), num(f.bbox[1]*scale), num(f.bbox[2]*scale), num(f.bbox[3]*scale),
		num(f.ascent*scale), num(f.descent*scale), num(f.capHeight*scale), file))

	program, err := f.compressedProgram()
	if err != nil {
		return err
	}
	w.stream(file, fmt.Sprintf("/Filter /FlateDecode /Length1 %d", len(f.ttf)), program)

	cmap, err := deflate(toUnicodeCMap(ids, f.used))
	if err != nil {
		return err
	}
	w.stream(toUni, "/Filter /FlateDecode", cmap)
	return nil
}

// compressedProgram is the font file, deflated once per process.
func (pf *parsedFont) compressedProgram() ([]byte, error) {
	pf.programOnce.Do(func() { pf.program, pf.programErr = deflate(pf.ttf) })
	return pf.program, pf.programErr
}

// toUnicodeCMap maps every drawn glyph id back to its text.
func toUnicodeCMap(ids []uint16, used map[uint16]usedGlyph) []byte {
	var b bytes.Buffer
	b.WriteString("/CIDInit /ProcSet findresource begin\n12 dict begin\nbegincmap\n" +
		"/CIDSystemInfo << /Registry (Adobe) /Ordering (UCS) /Supplement 0 >> def\n" +
		"/CMapName /Adobe-Identity-UCS def\n/CMapType 2 def\n" +
		"1 begincodespacerange\n<0000> <FFFF>\nendcodespacerange\n")
	for chunk := range slices.Chunk(ids, 100) { // at most 100 entries per block
		fmt.Fprintf(&b, "%d beginbfchar\n", len(chunk))
		for _, id := range chunk {
			fmt.Fprintf(&b, "<%04X> <", id)
			for _, u := range utf16.Encode([]rune{used[id].text}) {
				fmt.Fprintf(&b, "%04X", u)
			}
			b.WriteString(">\n")
		}
		b.WriteString("endbfchar\n")
	}
	b.WriteString("endcmap\nCMapName currentdict /CMap defineresource pop\nend\nend\n")
	return b.Bytes()
}

// utf16Hex is a PDF text string: UTF-16BE with a byte-order mark, as hex.
func utf16Hex(s string) string {
	var b bytes.Buffer
	b.WriteString("<FEFF")
	for _, u := range utf16.Encode([]rune(s)) {
		fmt.Fprintf(&b, "%04X", u)
	}
	b.WriteByte('>')
	return b.String()
}

func deflate(p []byte) ([]byte, error) {
	var b bytes.Buffer
	zw := zlib.NewWriter(&b)
	if _, err := zw.Write(p); err != nil {
		return nil, fmt.Errorf("pdftable: compress: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("pdftable: compress: %w", err)
	}
	return b.Bytes(), nil
}

// pdfWriter writes numbered objects and the cross-reference table. Small
// objects are written into buf; a large stream given to streamPart becomes
// a part of its own, so its bytes are never copied.
type pdfWriter struct {
	parts   [][]byte
	done    int          // bytes in parts
	buf     bytes.Buffer // the part being written
	offsets map[int]int
}

func (w *pdfWriter) offset() int { return w.done + w.buf.Len() }

// cut closes the part being written.
func (w *pdfWriter) cut() {
	if w.buf.Len() > 0 {
		w.done += w.buf.Len()
		w.parts = append(w.parts, w.buf.Bytes())
		w.buf = bytes.Buffer{} // the part keeps the old buffer's bytes
	}
}

func (w *pdfWriter) begin(n int) {
	if w.offsets == nil {
		w.offsets = map[int]int{}
	}
	w.offsets[n] = w.offset()
	fmt.Fprintf(&w.buf, "%d 0 obj\n", n)
}

func (w *pdfWriter) object(n int, body string) {
	w.begin(n)
	w.buf.WriteString(body)
	w.buf.WriteString("\nendobj\n")
}

func (w *pdfWriter) stream(n int, dict string, data []byte) {
	w.begin(n)
	fmt.Fprintf(&w.buf, "<< /Length %d %s>>\nstream\n", len(data), dict+" ")
	w.buf.Write(data)
	w.buf.WriteString("\nendstream\nendobj\n")
}

// streamPart is stream with data kept as a part of its own, not copied.
func (w *pdfWriter) streamPart(n int, dict string, data []byte) {
	w.begin(n)
	fmt.Fprintf(&w.buf, "<< /Length %d %s>>\nstream\n", len(data), dict+" ")
	w.cut()
	w.parts = append(w.parts, data)
	w.done += len(data)
	w.buf.WriteString("\nendstream\nendobj\n")
}

// finish writes the cross-reference table and trailer. Every object number
// from 1 to the highest must have been written.
func (w *pdfWriter) finish(info int) (*PDF, error) {
	size := len(w.offsets) + 1
	xref := w.offset()
	fmt.Fprintf(&w.buf, "xref\n0 %d\n0000000000 65535 f \n", size)
	for n := 1; n < size; n++ {
		off, ok := w.offsets[n]
		if !ok {
			return nil, fmt.Errorf("pdftable: object %d was never written", n)
		}
		fmt.Fprintf(&w.buf, "%010d 00000 n \n", off)
	}
	fmt.Fprintf(&w.buf, "trailer\n<< /Size %d /Root %d 0 R /Info %d 0 R >>\nstartxref\n%d\n%%%%EOF\n", size, catalogObj, info, xref)
	w.cut()
	return &PDF{parts: w.parts, size: w.done}, nil
}

// programCache holds a parsedFont's deflated font file, computed once.
type programCache struct {
	programOnce sync.Once
	program     []byte
	programErr  error
}
