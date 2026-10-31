package pdftable

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"golang.org/x/image/font"
	"golang.org/x/image/font/gofont/gobold"
	"golang.org/x/image/font/gofont/goregular"
	"golang.org/x/image/font/sfnt"
	"golang.org/x/image/math/fixed"
)

// The embedded faces are the Go fonts (Bigelow & Holmes, BSD-3-Clause, the
// licence of the Go project), shipped as TrueType bytes by
// golang.org/x/image/font/gofont. They cover WGL4 (Latin incl. Latin-1 and
// Latin Extended-A, Greek, Cyrillic, common symbols). A rune the font has
// no glyph for is drawn as the font's .notdef glyph and maps back to U+FFFD
// in the ToUnicode table, so extraction never yields a wrong character.

// parsedFont is a TrueType font parsed once per process. sfnt.Font is safe
// for concurrent use as long as every caller brings its own sfnt.Buffer.
type parsedFont struct {
	programCache // the deflated font file, see compressedProgram

	ttf    []byte
	font   *sfnt.Font
	psName string
	upem   float64
	// Vertical metrics and bounding box in font units.
	ascent, descent, capHeight float64
	bbox                       [4]float64
}

var (
	fontsOnce   sync.Once
	fontsErr    error
	regularFont *parsedFont
	boldFont    *parsedFont
)

func loadFonts() (*parsedFont, *parsedFont, error) {
	fontsOnce.Do(func() {
		regularFont, fontsErr = parseFont(goregular.TTF)
		if fontsErr != nil {
			return
		}
		boldFont, fontsErr = parseFont(gobold.TTF)
	})
	return regularFont, boldFont, fontsErr
}

func parseFont(ttf []byte) (*parsedFont, error) {
	f, err := sfnt.Parse(ttf)
	if err != nil {
		return nil, fmt.Errorf("parse embedded font: %w", err)
	}
	var buf sfnt.Buffer
	upem := f.UnitsPerEm()
	ppem := fixed.Int26_6(upem) << 6 // one pixel per font unit
	m, err := f.Metrics(&buf, ppem, font.HintingNone)
	if err != nil {
		return nil, fmt.Errorf("embedded font metrics: %w", err)
	}
	b, err := f.Bounds(&buf, ppem, font.HintingNone)
	if err != nil {
		return nil, fmt.Errorf("embedded font bounds: %w", err)
	}
	name, err := f.Name(&buf, sfnt.NameIDPostScript)
	if err != nil {
		return nil, fmt.Errorf("embedded font name: %w", err)
	}
	return &parsedFont{
		ttf:       ttf,
		font:      f,
		psName:    pdfName(name),
		upem:      float64(upem),
		ascent:    unitsOf(m.Ascent),
		descent:   -unitsOf(m.Descent), // sfnt reports descent as a positive distance
		capHeight: unitsOf(m.CapHeight),
		// sfnt's Y axis points down; PDF's points up.
		bbox: [4]float64{unitsOf(b.Min.X), -unitsOf(b.Max.Y), unitsOf(b.Max.X), -unitsOf(b.Min.Y)},
	}, nil
}

func unitsOf(v fixed.Int26_6) float64 { return float64(v) / 64 }

// pdfName keeps only the characters a PDF name may carry unescaped.
func pdfName(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r > ' ' && r < 0x7f && !strings.ContainsRune("()<>[]{}/%#", r) {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "Embedded"
	}
	return b.String()
}

// glyph is one rune's glyph id and advance width in font units.
type glyph struct {
	id  uint16
	adv float64
}

// face is a parsedFont as used by ONE document: its glyph cache and the set
// of glyphs the document draws (for the /W widths and the ToUnicode map).
type face struct {
	*parsedFont
	res    string // resource name, e.g. "F1"
	buf    sfnt.Buffer
	glyphs map[rune]glyph
	used   map[uint16]usedGlyph
	err    error // first glyph-lookup failure; Render returns it
}

// usedGlyph is a glyph the document drew: the text it maps back to and its
// advance width in font units.
type usedGlyph struct {
	text rune
	adv  float64
}

func newFace(pf *parsedFont, res string) *face {
	return &face{parsedFont: pf, res: res, glyphs: map[rune]glyph{}, used: map[uint16]usedGlyph{}}
}

// notdefRune is what a glyph-less rune maps back to on extraction.
const notdefRune = utf8.RuneError // U+FFFD

func (f *face) glyph(r rune) glyph {
	if g, ok := f.glyphs[r]; ok {
		return g
	}
	// GlyphIndex returns (0, nil) for a rune the font has no glyph for, so
	// an error here is a broken font, never missing coverage.
	var g glyph
	id, err := f.font.GlyphIndex(&f.buf, r)
	if err == nil {
		var adv fixed.Int26_6
		adv, err = f.font.GlyphAdvance(&f.buf, id, fixed.Int26_6(f.upem)<<6, font.HintingNone)
		g = glyph{id: uint16(id), adv: unitsOf(adv)}
	}
	if err != nil && f.err == nil {
		f.err = fmt.Errorf("embedded font %s: glyph lookup: %w", f.psName, err)
	}
	f.glyphs[r] = g
	return g
}

// width is the advance width of s at size points.
func (f *face) width(s string, size float64) float64 {
	var w float64
	for _, r := range s {
		w += f.glyph(r).adv
	}
	return w * size / f.upem
}

// runeWidth is the advance width of r at size points.
func (f *face) runeWidth(r rune, size float64) float64 {
	return f.glyph(r).adv * size / f.upem
}

const hexDigits = "0123456789ABCDEF"

// encode appends s as a hex string of 2-byte glyph ids (Identity-H) and
// records every glyph it draws.
func (f *face) encode(dst *bytes.Buffer, s string) {
	dst.WriteByte('<')
	for _, r := range s {
		g := f.glyph(r)
		if _, seen := f.used[g.id]; !seen {
			text := r
			if g.id == 0 {
				text = notdefRune
			}
			f.used[g.id] = usedGlyph{text: text, adv: g.adv}
		}
		dst.WriteByte(hexDigits[g.id>>12])
		dst.WriteByte(hexDigits[(g.id>>8)&0xf])
		dst.WriteByte(hexDigits[(g.id>>4)&0xf])
		dst.WriteByte(hexDigits[g.id&0xf])
	}
	dst.WriteByte('>')
}

// wrap breaks text into lines no wider than maxW points at size: at line
// breaks in the text, else at the last space that fits, else (a word wider
// than the line) between runes. Every rune of text lands on some line: a
// cell is wrapped, never clipped. Tabs render as spaces.
func (f *face) wrap(text string, size, maxW float64) []string {
	text = normalize(text)
	if !strings.Contains(text, "\n") {
		return f.wrapParagraph(nil, text, size, maxW)
	}
	var lines []string
	for _, para := range strings.Split(text, "\n") {
		lines = f.wrapParagraph(lines, para, size, maxW)
	}
	return lines
}

// widestLine is the width of text's widest line, unwrapped.
func (f *face) widestLine(text string, size float64) float64 {
	var widest, w float64
	for _, r := range normalize(text) {
		if r == '\n' {
			widest, w = max(widest, w), 0
			continue
		}
		w += f.glyph(r).adv
	}
	return max(widest, w) * size / f.upem
}

var lineBreaks = strings.NewReplacer("\r\n", "\n", "\r", "\n", "\t", " ")

// normalize turns CR and CRLF into LF and tabs into spaces.
func normalize(text string) string {
	if strings.ContainsAny(text, "\r\t") {
		return lineBreaks.Replace(text)
	}
	return text
}

// widthEpsilon absorbs float rounding when a line is exactly as wide as
// its column (a column allotted its natural width): such a line fits.
const widthEpsilon = 1e-6

func (f *face) wrapParagraph(lines []string, p string, size, maxW float64) []string {
	maxW += widthEpsilon
	start := 0      // byte offset where the current line starts
	w := 0.0        // width of p[start:i]
	lastSpace := -1 // byte offset of the last space in p[start:i], or -1
	for i, r := range p {
		rw := f.runeWidth(r, size)
		if w+rw > maxW && i > start {
			if lastSpace > start {
				lines = append(lines, strings.TrimRight(p[start:lastSpace], " "))
				start = lastSpace + 1
				w = f.width(p[start:i], size)
				lastSpace = -1
			}
			if w+rw > maxW && i > start {
				lines = append(lines, p[start:i])
				start, w, lastSpace = i, 0, -1
			}
		}
		w += rw
		if r == ' ' {
			lastSpace = i
		}
	}
	return append(lines, strings.TrimRight(p[start:], " "))
}
