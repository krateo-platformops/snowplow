package pathtemplate

import (
	"fmt"
	"testing"
)

// #1185 arms: a value that leaves a path segment empty is refused. The render
// only; every verb and both dials are pinned in restactions/api.

// The three ways a value renders nothing. A `+` operand and a segments() splice
// treat null and absent alike; an interpolation does not (see the arm below).
var emptyValues1185 = map[string]map[string]any{
	"empty":  {"x": ""},
	"null":   {"x": nil},
	"absent": {},
}

// Each position, in each way a value can be spliced with `+`, and each way the
// path can end after it.
func TestIssue1185_AValueThatLeavesASegmentEmptyIsRefused(t *testing.T) {
	for name, data := range emptyValues1185 {
		for _, tmpl := range []string{
			// First, middle, last.
			`${ "/" + .x + "/b/c" }`,
			`${ "/a/" + .x + "/c" }`,
			`${ "/a/b/" + .x }`,
			// The value opens the path: without it the path starts elsewhere.
			`${ .x + "/b/c" }`,
			// The path ends at the template's '?' or '#', which end the segment.
			`${ "/a/" + .x + "?q=1" }`,
			`${ "/a/" + .x + "#f" }`,
			`${ .x + "?q=1" }`,
			// The only segment of the path: the endpoint's root (#1128 finding B).
			`${ "/" + .x }`,
			// Declared (#1185 item B): segments() is no way to make a value optional.
			`${ "/" + segments(.x) }`,
			`${ "/a/" + segments(.x) }`,
			`${ "/a/" + segments(.x) + "/z" }`,
			`${ segments(.x) + "/z" }`,
			`${ "/a/" + segments(.x) + "?q=1" }`,
			// However the value got there.
			`${ .x as $v | "/a/" + $v }`,
			`${ ("/a/" + .x) as $p | $p + "/z" }`,
			`${ "/a/" + (if .c then "b" else .x end) }`,
			`${ "/a/" + (.x // "") }`,
			`${ "/a/" + .x + .x }`,
			`${ "/a/" + "" + .x }`,
			// The other segments are present: one empty one is enough.
			`${ "/a/b/c/" + .x + "/d/e/f" }`,
		} {
			t.Run(name+"/"+tmpl, func(t *testing.T) {
				wantRefused(t, tmpl, data, ReasonEmptySegment)
			})
		}
	}

	// An interpolation and an author-encoded value see "" the same way.
	for _, tmpl := range []string{
		`${ "/\(.x)/b" }`,
		`${ "/a/\(.x)/c" }`,
		`${ "/a/b/\(.x)" }`,
		`${ "/a/" + (.x | @uri) }`,
		`${ @uri "/a/\(.x)/c" }`,
		`${ .x }`,
	} {
		wantRefused(t, tmpl, map[string]any{"x": ""}, ReasonEmptySegment)
	}

	// The refusal is the value's, whichever one it is.
	wantRefused(t, `${ "/" + .a + "/" + .b }`, map[string]any{"a": "x"}, ReasonEmptySegment)
	wantRefused(t, `${ "/" + .a + "/" + .b }`, map[string]any{"b": "x"}, ReasonEmptySegment)
	if _, err := Render(t.Context(), `${ "/a/" + .x }`, nil); !IsEmptySegment(err) || !IsRefused(err) {
		t.Fatalf("IsEmptySegment/IsRefused do not read the refusal: %v", err)
	}
}

// What the rule is not about. It looks at whole segments of the path part, and
// only at segments a value was spliced into.
func TestIssue1185_WhatIsNotAnEmptySegment(t *testing.T) {
	for name, data := range emptyValues1185 {
		t.Run(name, func(t *testing.T) {
			// A value empty inside a segment that has other text.
			wantPath(t, `${ "/configmaps/" + .x + "-" + .x + "-jsonschema" }`, data, "/configmaps/--jsonschema")
			wantPath(t, `${ "/a/" + .x + "-b" }`, data, "/a/-b")
			wantPath(t, `${ "/a/b-" + .x }`, data, "/a/b-")
			wantPath(t, `${ "/a" + .x }`, data, "/a")
			wantPath(t, `${ "/a" + .x + "/b" }`, data, "/a/b")
			wantPath(t, `${ .x + "a/b" }`, data, "a/b")

			// A query value and a fragment are not segments.
			wantPath(t, `${ "/a?q=" + .x }`, data, "/a?q=")
			wantPath(t, `${ "/a?q=" + .x + "&r=/" + .x + "/" }`, data, "/a?q=&r=//")
			wantPath(t, `${ "/a/?q=" + .x }`, data, "/a/?q=")
			wantPath(t, `${ "/a#" + .x }`, data, "/a#")

			// The optional shape: the '/' is inside the part, and the part is the
			// author's "" when the value is empty. No empty value is spliced.
			opt := `${ "/a" + (if (.x // "") == "" then "" else "/" + .x end) }`
			wantPath(t, opt, data, "/a")
			wantPath(t, `${ "/a" + (if (.x // "") == "" then "" else "/" + .x end) + "/z" }`, data, "/a/z")
			wantPath(t, `${ (.x // "") as $v | "/a" + (if $v == "" then "" else "/" + $v end) }`, data, "/a")

			// A value with its own default.
			wantPath(t, `${ "/a/" + ((.x // "") | if . == "" then "d" else . end) }`, data, "/a/d")
		})
	}
	opt := `${ "/a" + (if (.x // "") == "" then "" else "/" + .x end) }`
	wantPath(t, opt, map[string]any{"x": "b"}, "/a/b")
	// `//` replaces null, not "": jq's rule, and the value still renders empty.
	wantPath(t, `${ "/a/" + (.x // "d") }`, map[string]any{}, "/a/d")
	wantRefused(t, `${ "/a/" + (.x // "d") }`, map[string]any{"x": ""}, ReasonEmptySegment)
}

// Decided from which pieces are values, not from the final string: `/a/` is
// refused when a value left it so, and left alone when the author wrote it. An
// author's `/` or `//` is neither refused nor excused here: the dial guards read
// it as they read any literal (TestIssue1185_AuthorLiteralsKeepTheirVerdict in
// restactions/api).
func TestIssue1185_AnAuthorsLiteralIsTheAuthors(t *testing.T) {
	for _, data := range []map[string]any{{"x": "v"}, {"x": ""}, {}} {
		wantPath(t, `${ "/a/" }`, data, "/a/")
		wantPath(t, `${ "/a//b" }`, data, "/a//b")
		wantPath(t, `${ "//a" }`, data, "//a")
		wantPath(t, `${ "/" }`, data, "/")
		wantPath(t, `${ "/a/" + "" }`, data, "/a/")
		wantPath(t, `${ "/a/" + (if .c then "b" else "" end) }`, data, "/a/")
		wantPath(t, `${ (if .c then "/b" else "/a/" end) }`, data, "/a/")
		wantPath(t, `/a//b/`, data, "/a//b/") // not a template
	}
	// The howto's table: the author's "" for an absent value keeps the '/' as
	// text. The branch has to catch null too: else null is the spliced value.
	for _, data := range []map[string]any{{"x": ""}, {"x": nil}, {}} {
		wantPath(t, `${ "/a/" + (if (.x // "") == "" then "" else .x end) }`, data, "/a/")
	}
	wantRefused(t, `${ "/a/" + (if .x == "" then "" else .x end) }`, map[string]any{}, ReasonEmptySegment)
	// Around a value that is there, the same text is still the author's.
	v := map[string]any{"x": "v"}
	wantPath(t, `${ "/a/" + .x + "/" }`, v, "/a/v/")
	wantPath(t, `${ "/a//" + .x }`, v, "/a//v")
	wantPath(t, `${ "//" + .x }`, v, "//v")
	wantPath(t, `${ .x + "//b" }`, v, "v//b")
	// An author's empty segment next to a value that left one empty: the value's
	// is the refusal, and the literal next to it does not excuse it.
	wantRefused(t, `${ "/a//" + .x }`, map[string]any{}, ReasonEmptySegment)
	wantRefused(t, `${ "/a/" + .x + "/" }`, map[string]any{}, ReasonEmptySegment)
	wantRefused(t, `${ "/a/" + .x + "//b" }`, map[string]any{}, ReasonEmptySegment)
}

// Whitespace is decided by what the template's check already does: it compares
// a segment exactly and trims nothing (hasDotSegment: ` . ` is not a dot
// segment). A space is one escaped character, so the segment holds `%20` and the
// path has as many segments as the template meant. It widens nothing.
func TestIssue1185_WhitespaceIsNotEmpty(t *testing.T) {
	for in, want := range map[string]string{" ": "%20", "   ": "%20%20%20", "\t": "%09", "\n": "%0A"} {
		wantPath(t, `${ "/" + .x + "/b/c" }`, map[string]any{"x": in}, "/"+want+"/b/c")
		wantPath(t, `${ "/a/" + .x + "/c" }`, map[string]any{"x": in}, "/a/"+want+"/c")
		wantPath(t, `${ "/a/b/" + .x }`, map[string]any{"x": in}, "/a/b/"+want)
		wantPath(t, `${ "/a/" + segments(.x) }`, map[string]any{"x": in}, "/a/"+want)
		wantPath(t, `${ "/a/\(.x)/c" }`, map[string]any{"x": in}, "/a/"+want+"/c")
	}
	wantPath(t, `${ "/" + .x }`, map[string]any{"x": " . "}, "/%20.%20")
}

// jq's interpolation of null is the text `null`, not an empty value: a segment
// with text in it, the template's own number of segments, and nothing wider.
// It is the render #1123 pinned for a whole program (`${ .n }`).
func TestIssue1185_AnInterpolatedNullIsTheTextNull(t *testing.T) {
	for _, data := range []map[string]any{{"x": nil}, {}} {
		wantPath(t, `${ "/a/\(.x)/c" }`, data, "/a/null/c")
		wantPath(t, `${ .x }`, data, "null")
	}
}

// A declared value: every piece is one segment, so an empty piece is an empty
// segment wherever it is. The one '/' that is not a boundary between pieces is
// the leading one of a value that opens the path: the root.
func TestIssue1185_DeclaredValuePieces(t *testing.T) {
	for _, c := range []struct{ tmpl, p string }{
		{`${ segments(.p) }`, "a//b"},
		{`${ "/x/" + segments(.p) }`, "a//b"},
		{`${ segments(.p) }`, "/a//b"},
		{`${ segments(.p) }`, "a/"},
		{`${ segments(.p) }`, "/a/"},
		{`${ "/x/" + segments(.p) }`, "a/"},
		{`${ "/x/" + segments(.p) + "/z" }`, "a/"},
		{`${ segments(.p) }`, "/"},
		{`${ segments(.p) }`, "//"},
		{`${ segments(.p) }`, "//a"},
		{`${ "/x/" + segments(.p) }`, "/"},
		// After other text the leading '/' is a boundary, so the piece before it
		// is empty: `/x//a`.
		{`${ "/x/" + segments(.p) }`, "/a"},
		{`${ "/x" + segments(.p) + "/y/" + segments(.p) }`, "/a"},
		// The root of a value is the root only when it opens the path.
		{`${ .e + segments(.p) }`, "/a"},
	} {
		t.Run(fmt.Sprintf("%s/%s", c.tmpl, c.p), func(t *testing.T) {
			wantRefused(t, c.tmpl, map[string]any{"p": c.p}, ReasonEmptySegment)
		})
	}

	wantPath(t, `${ segments(.p) }`, map[string]any{"p": "/apis/apps/v1"}, "/apis/apps/v1")
	wantPath(t, `${ segments(.p) }`, map[string]any{"p": "a/b"}, "a/b")
	wantPath(t, `${ "/x/" + segments(.p) }`, map[string]any{"p": "a/b"}, "/x/a/b")
	wantPath(t, `${ "/x" + segments(.p) }`, map[string]any{"p": "/a"}, "/x/a")
	wantPath(t, `${ "/x/" + segments(.p) + "/z" }`, map[string]any{"p": "a/ /b"}, "/x/a/%20/b/z")
	// A piece empty inside a segment that has other text.
	wantPath(t, `${ "/x-" + segments(.p) }`, map[string]any{"p": ""}, "/x-")
	wantPath(t, `${ segments(.p) + "-x" }`, map[string]any{"p": "a/"}, "a/-x")
	// The other refusals keep their reasons.
	wantRefused(t, `${ "/" + .a + "/.." }`, map[string]any{}, ReasonDotSegment)
	wantRefused(t, `${ "/a?q=" + segments(.x) }`, map[string]any{"x": ""}, ReasonSegmentsInQuery)
}

// The refusal never carries a value, like every RefusedError.
func TestIssue1185_TheRefusalNamesTheRule(t *testing.T) {
	const canary = "sup3rsecretvalue"
	_, err := Render(t.Context(), `${ "/`+canary+`/" + .x + "/" + .y }`, map[string]any{"y": "b"})
	if !IsEmptySegment(err) {
		t.Fatalf("got %v", err)
	}
	if got := err.Error(); got != (&RefusedError{Reason: ReasonEmptySegment}).Error() {
		t.Fatalf("the refusal depends on the render: %q", got)
	}
}
