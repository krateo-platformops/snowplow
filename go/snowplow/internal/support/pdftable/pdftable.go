// Package pdftable renders a table as a self-contained PDF 1.7 document:
// landscape A4, the column header repeated at the top of every page, long
// cells wrapped (never clipped), and the Go fonts embedded so any text the
// fonts cover renders and extracts as Unicode.
//
// The output has no links, annotations, actions, scripts, forms or external
// references: it is text and lines only, with every font embedded.
//
// Render builds the whole document in memory and returns it, or an error.
// It never returns a partial document, so a caller can send nothing until
// Render has succeeded.
package pdftable

import (
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

// Document is one table report.
type Document struct {
	// Title is printed at the top of the first page and stored as the
	// document title.
	Title string
	// Created is stored as the document creation date.
	Created time.Time
	// Meta lines are printed under the title on the first page.
	Meta []string
	// Header holds one label per column, repeated on every page.
	Header []string
	// Rows holds the cell text of every row, one cell per column.
	Rows [][]string
}

// ErrHeaderTooTall reports column labels whose wrapped height leaves less
// than half of a page for rows. The header row is repeated on every page,
// so it cannot be split across pages, and it is never clipped.
var ErrHeaderTooTall = errors.New("pdftable: the column header row does not fit in half a page")

// Page geometry, in PDF points (1/72 in). A4 landscape is 297 x 210 mm.
const (
	pageW   = 841.89
	pageH   = 595.28
	margin  = 28.35 // 10 mm
	footerH = 14.0  // reserved under the table for the page number
	availW  = pageW - 2*margin
	bottomY = pageH - margin - footerH // rows end above this distance from the top

	titleSize = 14.0
	metaSize  = 8.0
	headSize  = 7.5
	bodySize  = 7.0
	footSize  = 7.0
	leading   = 1.25 // line height as a multiple of the font size
	padX      = 2.5  // cell padding, left and right
	padY      = 1.75 // cell padding, top and bottom

	// minColW is the narrowest a column may get: about four characters of
	// body text. When the columns do not all fit across a page at this
	// width, they are split into bands of adjacent columns, each band
	// printed as its own run of pages with all the rows.
	minColW = 24.0
)

// Render lays the document out and returns the complete PDF.
func Render(doc Document) ([]byte, error) {
	reg, bold, err := loadFonts()
	if err != nil {
		return nil, err
	}
	for i, row := range doc.Rows {
		if len(row) != len(doc.Header) {
			return nil, fmt.Errorf("pdftable: row %d has %d cells, want %d", i, len(row), len(doc.Header))
		}
	}
	l := &layout{reg: newFace(reg, "F1"), bold: newFace(bold, "F2")}
	if err := l.run(doc); err != nil {
		return nil, err
	}
	for _, f := range []*face{l.reg, l.bold} {
		if f.err != nil {
			return nil, f.err
		}
	}
	return l.assemble(doc)
}

// layout is the page writer: it holds the page being drawn and the
// compressed content stream of every finished page.
type layout struct {
	reg, bold *face

	pages [][]byte     // finished pages' compressed content streams
	cur   bytes.Buffer // content stream of the page being drawn
	open  bool         // a page is being drawn
	zbuf  bytes.Buffer
	zw    *zlib.Writer

	scratch []byte // number formatting buffer

	y        float64   // distance from the page top to the next free line
	colX     []float64 // column boundaries of the current band, len(cols)+1
	tableTop float64   // where the table starts on the current page
	rowsOn   int       // body rows (or row parts) drawn on the current page
}

func (l *layout) run(doc Document) error {
	ncols := len(doc.Header)
	natural := l.naturalWidths(doc)
	bands := splitBands(ncols)

	l.newPage()
	if err := l.titleBlock(doc); err != nil {
		return err
	}
	if ncols == 0 {
		return l.finishPage()
	}
	for bi, band := range bands {
		widths := allocate(natural[band[0]:band[1]], availW)
		l.colX = make([]float64, 0, len(widths)+1)
		x := margin
		l.colX = append(l.colX, x)
		for _, w := range widths {
			x += w
			l.colX = append(l.colX, x)
		}
		header := doc.Header[band[0]:band[1]]
		caption := ""
		if len(bands) > 1 {
			caption = fmt.Sprintf("Columns %d–%d of %d", band[0]+1, band[1], ncols)
		}
		if l.headerHeight(header) > (bottomY-margin)/2 {
			return ErrHeaderTooTall
		}
		// A band starts on a fresh page, and so does the first one when the
		// title block leaves no room for the header and one row under it.
		if bi > 0 || l.y+l.tableStartHeight(header, caption)+bodySize*leading+2*padY > bottomY {
			if err := l.finishPage(); err != nil {
				return err
			}
			l.newPage()
		}
		l.tableStart(header, caption)
		for _, row := range doc.Rows {
			if err := l.bodyRow(row[band[0]:band[1]], header, caption); err != nil {
				return err
			}
		}
	}
	return l.finishPage()
}

// naturalWidths is each column's unwrapped width: its widest label or
// cell line plus padding.
func (l *layout) naturalWidths(doc Document) []float64 {
	nat := make([]float64, len(doc.Header))
	for c, h := range doc.Header {
		nat[c] = l.bold.widestLine(h, headSize)
	}
	for _, row := range doc.Rows {
		for c, cell := range row {
			if w := l.reg.widestLine(cell, bodySize); w > nat[c] {
				nat[c] = w
			}
		}
	}
	for c := range nat {
		nat[c] += 2 * padX
	}
	return nat
}

// splitBands groups the columns into bands of adjacent columns that fit
// across a page at minColW each; [start, end) per band.
func splitBands(ncols int) [][2]int {
	per := int(math.Floor(availW / minColW))
	var bands [][2]int
	for start := 0; start < ncols; start += per {
		bands = append(bands, [2]int{start, min(start+per, ncols)})
	}
	return bands
}

// allocate shares total width among columns: when the natural widths fit
// they are widened in proportion to fill the page; otherwise every column
// that needs no more than an equal share keeps its natural width and the
// rest split what remains equally (and wrap). No column is narrower than
// minColW; splitBands guarantees len(nat)*minColW <= total.
func allocate(nat []float64, total float64) []float64 {
	want := make([]float64, len(nat))
	var sum float64
	for i, n := range nat {
		want[i] = max(n, minColW)
		sum += want[i]
	}
	w := make([]float64, len(nat))
	if sum <= total {
		for i := range want {
			w[i] = want[i] * total / sum
		}
		return w
	}
	open := make([]int, len(nat))
	for i := range open {
		open[i] = i
	}
	remaining := total
	for {
		share := remaining / float64(len(open))
		kept := open[:0:0]
		for _, i := range open {
			if want[i] <= share {
				w[i] = want[i]
				remaining -= want[i]
			} else {
				kept = append(kept, i)
			}
		}
		if len(kept) == len(open) {
			for _, i := range open {
				w[i] = share
			}
			return w
		}
		open = kept
	}
}

func (l *layout) newPage() {
	l.cur.Reset()
	l.open = true
	l.y = margin
	l.rowsOn = 0
	l.tableTop = -1
	l.cur.WriteString("0.4 w 0.55 G\n")
}

// finishPage closes the table's column lines and compresses the page.
func (l *layout) finishPage() error {
	if !l.open {
		return nil
	}
	if l.tableTop >= 0 {
		for _, x := range l.colX {
			l.line(x, l.tableTop, x, l.y)
		}
	}
	l.zbuf.Reset()
	if l.zw == nil {
		l.zw = zlib.NewWriter(&l.zbuf)
	} else {
		l.zw.Reset(&l.zbuf)
	}
	if _, err := l.zw.Write(l.cur.Bytes()); err != nil {
		return fmt.Errorf("pdftable: compress page: %w", err)
	}
	if err := l.zw.Close(); err != nil {
		return fmt.Errorf("pdftable: compress page: %w", err)
	}
	l.pages = append(l.pages, bytes.Clone(l.zbuf.Bytes()))
	l.open = false
	return nil
}

// titleBlock prints the title and meta lines at the start of the document,
// continuing on a new page should they ever outgrow one.
func (l *layout) titleBlock(doc Document) error {
	put := func(f *face, size float64, line string) error {
		if l.y+size*leading > bottomY {
			if err := l.finishPage(); err != nil {
				return err
			}
			l.newPage()
		}
		l.text(f, size, margin, l.y, line)
		l.y += size * leading
		return nil
	}
	for _, line := range l.bold.wrap(doc.Title, titleSize, availW) {
		if err := put(l.bold, titleSize, line); err != nil {
			return err
		}
	}
	l.y += 2
	for _, m := range doc.Meta {
		for _, line := range l.reg.wrap(m, metaSize, availW) {
			if err := put(l.reg, metaSize, line); err != nil {
				return err
			}
		}
	}
	l.y += 6
	return nil
}

// tableStartHeight is the height tableStart takes on a page.
func (l *layout) tableStartHeight(header []string, caption string) float64 {
	h := l.headerHeight(header)
	if caption != "" {
		h += metaSize*leading + 2
	}
	return h
}

func (l *layout) headerHeight(header []string) float64 {
	n := 1
	for c, h := range header {
		n = max(n, len(l.bold.wrap(h, headSize, l.colX[c+1]-l.colX[c]-2*padX)))
	}
	return float64(n)*headSize*leading + 2*padY
}

// tableStart prints the band caption and the header row at the current
// position of a page.
func (l *layout) tableStart(header []string, caption string) {
	if caption != "" {
		l.text(l.reg, metaSize, margin, l.y, caption)
		l.y += metaSize*leading + 2
	}
	l.tableTop = l.y
	h := l.headerHeight(header)
	fmt.Fprintf(&l.cur, "0.9 g %s %s %s %s re f 0 g\n",
		num(margin), num(pageH-l.y-h), num(l.colX[len(l.colX)-1]-margin), num(h))
	l.line(margin, l.y, l.colX[len(l.colX)-1], l.y)
	l.headerCells(header)
	l.y += h
	l.line(margin, l.y, l.colX[len(l.colX)-1], l.y)
}

// bodyRow prints one row. A row that does not fit under the rows already
// on the page moves to a new page; a row taller than a whole page is split
// across pages, line by line.
func (l *layout) bodyRow(row, header []string, caption string) error {
	lines := make([][]string, len(row))
	n := 1
	for c, cell := range row {
		lines[c] = l.reg.wrap(cell, bodySize, l.colX[c+1]-l.colX[c]-2*padX)
		n = max(n, len(lines[c]))
	}
	lh := bodySize * leading
	for done := 0; done < n; {
		fit := int((bottomY - l.y - 2*padY) / lh)
		if fit < n-done && l.rowsOn > 0 || fit < 1 {
			if err := l.finishPage(); err != nil {
				return err
			}
			l.newPage()
			l.tableStart(header, caption)
			continue
		}
		k := min(fit, n-done)
		y := l.y + padY
		l.cur.WriteString("BT\n")
		for c := range row {
			for i := done; i < done+k && i < len(lines[c]); i++ {
				l.textInBT(l.reg, bodySize, l.colX[c]+padX, y+float64(i-done)*lh, lines[c][i])
			}
		}
		l.cur.WriteString("ET\n")
		l.y += float64(k)*lh + 2*padY
		l.line(margin, l.y, l.colX[len(l.colX)-1], l.y)
		l.rowsOn++
		done += k
	}
	return nil
}

// headerCells prints every label's wrapped lines in its column.
func (l *layout) headerCells(header []string) {
	lh := headSize * leading
	l.cur.WriteString("BT\n")
	for c, label := range header {
		for i, line := range l.bold.wrap(label, headSize, l.colX[c+1]-l.colX[c]-2*padX) {
			l.textInBT(l.bold, headSize, l.colX[c]+padX, l.y+padY+float64(i)*lh, line)
		}
	}
	l.cur.WriteString("ET\n")
}

// text prints one line whose line box starts top points below the page top.
func (l *layout) text(f *face, size, x, top float64, s string) {
	l.cur.WriteString("BT\n")
	l.textInBT(f, size, x, top, s)
	l.cur.WriteString("ET\n")
}

func (l *layout) textInBT(f *face, size, x, top float64, s string) {
	if s == "" {
		return
	}
	// Centre the font's ascent..descent span in the line box.
	fontH := (f.ascent - f.descent) * size / f.upem
	baseline := top + (size*leading-fontH)/2 + f.ascent*size/f.upem
	// The text line and the rule below are the hot path (every cell line,
	// every row), so they are written without fmt.
	l.cur.WriteByte('/')
	l.cur.WriteString(f.res)
	l.cur.WriteByte(' ')
	l.num(size)
	l.cur.WriteString(" Tf 1 0 0 1 ")
	l.num(x)
	l.cur.WriteByte(' ')
	l.num(pageH - baseline)
	l.cur.WriteString(" Tm ")
	f.encode(&l.cur, s)
	l.cur.WriteString(" Tj\n")
}

func (l *layout) line(x1, top1, x2, top2 float64) {
	l.num(x1)
	l.cur.WriteByte(' ')
	l.num(pageH - top1)
	l.cur.WriteString(" m ")
	l.num(x2)
	l.cur.WriteByte(' ')
	l.num(pageH - top2)
	l.cur.WriteString(" l S\n")
}

// num writes a coordinate to the page being drawn.
func (l *layout) num(v float64) {
	l.scratch = appendNum(l.scratch[:0], v)
	l.cur.Write(l.scratch)
}

// num formats a coordinate with at most two decimals.
func num(v float64) string { return string(appendNum(nil, v)) }

func appendNum(b []byte, v float64) []byte {
	b = strconv.AppendFloat(b, v, 'f', 2, 64)
	for b[len(b)-1] == '0' {
		b = b[:len(b)-1]
	}
	if b[len(b)-1] == '.' {
		b = b[:len(b)-1]
	}
	if string(b) == "-0" {
		b = append(b[:0], '0')
	}
	return b
}
