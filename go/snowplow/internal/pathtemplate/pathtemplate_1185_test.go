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

// Decided from which pieces are expressions, not from the final string: `/a/` is
// refused when an expression left it so, and left alone when the author wrote it.
// An author's `/` or `//` in a plain literal is neither refused nor excused here:
// the dial guards read it as they read any literal
// (TestIssue1185_AuthorLiteralsKeepTheirVerdict in restactions/api).
func TestIssue1185_AnAuthorsLiteralIsTheAuthors(t *testing.T) {
	for _, data := range []map[string]any{{"x": "v"}, {"x": ""}, {}} {
		wantPath(t, `${ "/a/" }`, data, "/a/")
		wantPath(t, `${ "/a//b" }`, data, "/a//b")
		wantPath(t, `${ "//a" }`, data, "//a")
		wantPath(t, `${ "/" }`, data, "/")
		wantPath(t, `/a//b/`, data, "/a//b/") // not a template
	}
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

// The extended ruling: any expression that leaves a segment empty is refused,
// wherever the empty comes from. Each spelling here names the collection, or the
// root, without an empty VALUE ever being spliced.
func TestIssue1185_AnExpressionThatLeavesASegmentEmptyIsRefused(t *testing.T) {
	for _, tmpl := range []string{
		// The ruling's spelling, at each position and however the path goes on.
		`${ "/v2/tickets/" + (if (.id // "") == "" then "" else .id end) }`,
		`${ "/" + (if (.id // "") == "" then "" else .id end) + "/tickets" }`,
		`${ "/v2/tickets/" + (if (.id // "") == "" then "" else .id end) + "/comments" }`,
		`${ "/v2/tickets/" + (if (.id // "") == "" then "" else .id end) + "?q=1" }`,
		`${ "/v2/tickets/" + (if (.id // "") == "" then "" else .id end) + "#f" }`,
		`${ "/v2/tickets/" + (if (.id // "") == "x" then "y" elif (.id // "") == "" then "" else .id end) }`,
		// The empty moved behind a variable.
		`${ (if (.id // "") == "" then "" else .id end) as $v | "/v2/tickets/" + $v }`,
		`${ (.id // "") as $i | "/v2/tickets/" + (if $i == "" then "" else $i end) }`,
		`${ "" as $e | "/v2/tickets/" + $e }`,
		// The empty moved behind a pipe, or written plainly.
		`${ "/v2/tickets/" + (.id | "") }`,
		`${ "/v2/tickets/" + ("") }`,
		`${ "/v2/tickets/" + "" }`,
		// The branch holds the '/': the segment after it is still empty.
		`${ (if (.id // "") == "" then "/v2/tickets/" else "/v2/tickets/" + .id end) }`,
		`${ "/v2" + (if (.id // "") == "" then "/" else "/" + .id end) }`,
		`${ (if (.id // "") == "" then "/" else "/" + .id end) }`,
		`${ (if (.id // "") == "" then "/?q=1" else "/" + .id end) }`,
		// A branch's text is read piece by piece, wherever it lands.
		`${ "/v2/" + (if (.id // "") == "" then "/x" else .id end) }`,
		`${ (if (.id // "") == "" then "/v2//x" else "/v2/" + .id end) }`,
	} {
		for name, data := range map[string]map[string]any{"empty": {"id": ""}, "null": {"id": nil}, "absent": {}} {
			t.Run(name+"/"+tmpl, func(t *testing.T) {
				wantRefused(t, tmpl, data, ReasonEmptySegment)
			})
		}
	}
	// A condition that lets a null through refuses it as a value, as before.
	wantRefused(t, `${ "/a/" + (if .x == "" then "" else .x end) }`, map[string]any{}, ReasonEmptySegment)
	// The same stages with the value there.
	wantPath(t, `${ "/v2/tickets/" + (if (.id // "") == "" then "" else .id end) }`, map[string]any{"id": "42"}, "/v2/tickets/42")
	wantPath(t, `${ (if (.id // "") == "" then "/v2/tickets/" else "/v2/tickets/" + .id end) }`, map[string]any{"id": "42"}, "/v2/tickets/42")
}

// What the extended ruling leaves valid. The portal's shape holds its '/' inside
// the branch, so its "" lands in a segment that has text.
func TestIssue1185_TheBranchThatHoldsItsSlashLeavesNoSegmentEmpty(t *testing.T) {
	// portal.targetPath, helm/*/templates/_fetchable.tpl at tag 1.8.93.
	const portal = `${ (if .g == "" then "/api/" + .v else "/apis/" + .g + "/" + .v end) + (if .ns == "" then "" else "/namespaces/" + .ns end) + (if .pl == "" then "" else "/" + .pl end) + (if .nm == "" then "" else "/" + .nm end) }`
	for _, c := range []struct {
		g, v, ns, pl, nm, want string
	}{
		{"", "v1", "", "", "", "/api/v1"},
		{"apps", "v1", "", "", "", "/apis/apps/v1"},
		{"apps", "v1", "", "deployments", "", "/apis/apps/v1/deployments"},
		{"apps", "v1", "demo", "deployments", "", "/apis/apps/v1/namespaces/demo/deployments"},
		{"apps", "v1", "demo", "deployments", "web", "/apis/apps/v1/namespaces/demo/deployments/web"},
		{"", "v1", "demo", "configmaps", "cm", "/api/v1/namespaces/demo/configmaps/cm"},
	} {
		wantPath(t, portal, map[string]any{"g": c.g, "v": c.v, "ns": c.ns, "pl": c.pl, "nm": c.nm}, c.want)
	}
	// The same program with the one value it does not guard empty.
	wantRefused(t, portal, map[string]any{"g": "", "v": "", "ns": "", "pl": "", "nm": ""}, ReasonEmptySegment)

	for _, data := range []map[string]any{{"p": ""}, {"p": nil}, {}} {
		opt := `(if (.p // "") == "" then "" else "/" + .p end)`
		wantPath(t, `${ "/a" + `+opt+` }`, data, "/a")
		wantPath(t, `${ "/a" + `+opt+` + "/z" }`, data, "/a/z")
		wantPath(t, `${ "/a" + `+opt+` + "?q=1" }`, data, "/a?q=1")
		wantPath(t, `${ "/a" + `+opt+` + `+opt+` }`, data, "/a")
		wantPath(t, `${ "/a-" + (if (.p // "") == "" then "" else .p end) }`, data, "/a-")
		// Nothing precedes the root: an optional part that opens the path leaves
		// no segment, and a whole program of nothing is the dial plan's to refuse.
		wantPath(t, `${ `+opt+` + "/b" }`, data, "/b")
		wantPath(t, `${ "" + "/b" }`, data, "/b")
		wantPath(t, `${ (if (.p // "") == "" then "" else .p end) + "/b" }`, data, "/b")
		wantPath(t, `${ `+opt+` }`, data, "")
		wantPath(t, `${ "" }`, data, "")
		// In the query or the fragment an `if` writes what it likes.
		wantPath(t, `${ "/a?q=1" + (if (.p // "") == "" then "" else "&r=" + .p end) }`, data, "/a?q=1")
		wantPath(t, `${ "/a?q=" + (if (.p // "") == "" then "" else .p end) }`, data, "/a?q=")
		wantPath(t, `${ "/a#" + (if (.p // "") == "" then "" else .p end) }`, data, "/a#")
	}
	wantPath(t, `${ "/a" + (if (.p // "") == "" then "" else "/" + .p end) }`, map[string]any{"p": "b"}, "/a/b")

	// A branch's text: written as it stands, and its '?' ends the path.
	wantPath(t, `${ (if .c then "/b" else "/a" end) + "/z" }`, nil, "/a/z")
	wantPath(t, `${ (if .c then "/b/" else "/a/" end) + "z" }`, nil, "/a/z")
	wantPath(t, `${ "/" + (if .c then "b" else "a" end) }`, nil, "/a")
	wantPath(t, `${ "/a/" + (if .c then "x" else "b" end) + "/z" }`, nil, "/a/b/z")
	wantPath(t, `${ (if .c then "/x?r=" else "/a?q=" end) + .v }`, map[string]any{"v": "a&b"}, "/a?q=a%26b")
	wantPath(t, `${ "/a/" + ((.x // "") | if . == "" then "default" else . end) + "/z" }`, nil, "/a/default/z")
	// jq's own reading of the branch is unchanged where the text is not a path.
	wantPath(t, `${ (if .c then "1h" else "6h" end) as $r | "/s?w=" + $r + (if $r == "6h" then "&b=1" else "" end) }`, nil, "/s?w=6h&b=1")
}
