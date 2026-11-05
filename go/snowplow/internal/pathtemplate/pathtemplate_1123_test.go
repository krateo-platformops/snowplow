package pathtemplate

import (
	"context"
	"errors"
	"testing"
)

// #1123 arms: a value spliced into a stage path is one segment (or one query
// value), whatever it contains.

func render(t *testing.T, tmpl string, data any) (string, error) {
	t.Helper()
	return Render(context.Background(), tmpl, data)
}

func wantPath(t *testing.T, tmpl string, data any, want string) {
	t.Helper()
	got, err := render(t, tmpl, data)
	if err != nil {
		t.Fatalf("Render(%s) error: %v", tmpl, err)
	}
	if got != want {
		t.Fatalf("Render(%s)\n got  %q\n want %q", tmpl, got, want)
	}
}

func wantRefused(t *testing.T, tmpl string, data any, reason string) {
	t.Helper()
	got, err := render(t, tmpl, data)
	var r *RefusedError
	if !errors.As(err, &r) {
		t.Fatalf("Render(%s) = %q, %v; want refused %s", tmpl, got, err, reason)
	}
	if r.Reason != reason {
		t.Fatalf("Render(%s) refused for %s, want %s", tmpl, r.Reason, reason)
	}
	if got != "" {
		t.Fatalf("a refused render returned a path %q", got)
	}
}

const ticketPath = `${ "/api/v2/tickets/" + .id + "/comments" }`
const ticketInterp = `${ "/api/v2/tickets/\(.id)/comments" }`

// Each injection form, in both splice syntaxes: refused when it cannot be one
// segment, escaped into one segment when it can.
func TestIssue1123_InjectionFormsAreOneSegment(t *testing.T) {
	for _, tmpl := range []string{ticketPath, ticketInterp} {
		t.Run(tmpl, func(t *testing.T) {
			// The measured payloads.
			wantRefused(t, tmpl, map[string]any{"id": "42/../../users/me"}, ReasonValueSpansSegments)
			wantPath(t, tmpl, map[string]any{"id": "42?role=admin"}, "/api/v2/tickets/42%3Frole=admin/comments")
			// The rest of the class.
			wantRefused(t, tmpl, map[string]any{"id": ".."}, ReasonDotSegment)
			wantRefused(t, tmpl, map[string]any{"id": "."}, ReasonDotSegment)
			wantRefused(t, tmpl, map[string]any{"id": "42/comments"}, ReasonValueSpansSegments)
			wantPath(t, tmpl, map[string]any{"id": "42#frag"}, "/api/v2/tickets/42%23frag/comments")
			wantPath(t, tmpl, map[string]any{"id": "%2e%2e"}, "/api/v2/tickets/%252e%252e/comments")
			wantPath(t, tmpl, map[string]any{"id": "42%2F..%2F"}, "/api/v2/tickets/42%252F..%252F/comments")
			wantPath(t, tmpl, map[string]any{"id": "%3Frole=admin"}, "/api/v2/tickets/%253Frole=admin/comments")
			wantPath(t, tmpl, map[string]any{"id": "a b"}, "/api/v2/tickets/a%20b/comments")
			wantPath(t, tmpl, map[string]any{"id": `42\..\x`}, "/api/v2/tickets/42%5C..%5Cx/comments")
			// A clean value renders as itself.
			wantPath(t, tmpl, map[string]any{"id": "42"}, "/api/v2/tickets/42/comments")
			wantPath(t, tmpl, map[string]any{"id": "system:aggregate-to-admin"}, "/api/v2/tickets/system:aggregate-to-admin/comments")
		})
	}
}

// A partial segment: the value is escaped, the segment it lands in is checked.
func TestIssue1123_PartialSegment(t *testing.T) {
	tmpl := `${ "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/" + .resource + "." + .group }`
	wantPath(t, tmpl, map[string]any{"resource": "widgets", "group": "templates.krateo.io"},
		"/apis/apiextensions.k8s.io/v1/customresourcedefinitions/widgets.templates.krateo.io")
	wantRefused(t, tmpl, map[string]any{"resource": "", "group": ""}, ReasonDotSegment)
	wantRefused(t, tmpl, map[string]any{"resource": "x/../../../api/v1/secrets", "group": "g"}, ReasonValueSpansSegments)
}

// A value after the template's '?' is one query value: it can neither add a
// parameter nor end the query.
func TestIssue1123_QueryValueIsEncodedAsAValue(t *testing.T) {
	tmpl := `${ "/api/v2/search?type=ticket&query=" + .q }`
	wantPath(t, tmpl, map[string]any{"q": "status:open"}, "/api/v2/search?type=ticket&query=status:open")
	wantPath(t, tmpl, map[string]any{"q": "x&role=admin"}, "/api/v2/search?type=ticket&query=x%26role%3Dadmin")
	wantPath(t, tmpl, map[string]any{"q": "x#frag"}, "/api/v2/search?type=ticket&query=x%23frag")
	wantPath(t, tmpl, map[string]any{"q": "a+b c"}, "/api/v2/search?type=ticket&query=a%2Bb%20c")
	wantPath(t, tmpl, map[string]any{"q": "%26"}, "/api/v2/search?type=ticket&query=%2526")
	wantPath(t, tmpl, map[string]any{"q": "x;y"}, "/api/v2/search?type=ticket&query=x%3By")
	// '/' and '?' are query characters: a value carrying them stays one value.
	wantPath(t, tmpl, map[string]any{"q": "a/b?c"}, "/api/v2/search?type=ticket&query=a/b?c")
	// A query key spliced from a value is escaped the same way.
	wantPath(t, `${ "/get?" + .k + "=1" }`, map[string]any{"k": "a=b&c"}, "/get?a%3Db%26c=1")
}

// The fragment is never sent; a value in it still cannot end it.
func TestIssue1123_FragmentValue(t *testing.T) {
	wantPath(t, `${ "/a#" + .f }`, map[string]any{"f": "x?y&z"}, "/a#x?y%26z")
}

// A program that is not a template is one value: a whole path read from data
// is refused until it is declared.
func TestIssue1123_WholeProgramIsOneValue(t *testing.T) {
	wantRefused(t, `${ .path }`, map[string]any{"path": "/apis/apps/v1/deployments"}, ReasonValueSpansSegments)
	wantPath(t, `${ .path }`, map[string]any{"path": "health"}, "health")
	wantPath(t, `${ .path }`, map[string]any{"path": "x?y"}, "x%3Fy")
	// jq's tostring of a non-string, as the plain render's JSON output.
	wantPath(t, `${ .n }`, map[string]any{"n": 8080}, "8080")
	wantPath(t, `${ .n }`, map[string]any{}, "null")
}

// segments(): a declared multi-segment value keeps its '/', and still cannot
// walk up the tree or add a query.
func TestIssue1123_DeclaredSegments(t *testing.T) {
	wantPath(t, `${ segments(.path) }`, map[string]any{"path": "/apis/apps/v1/namespaces/demo/deployments/web"},
		"/apis/apps/v1/namespaces/demo/deployments/web")
	wantPath(t, `${ "/apis/" + segments(.gv) + "/" + .resource }`, map[string]any{"gv": "apps/v1", "resource": "deployments"},
		"/apis/apps/v1/deployments")
	wantRefused(t, `${ segments(.path) }`, map[string]any{"path": "/apis/apps/v1/../../api/v1/secrets"}, ReasonDotSegment)
	// An escaped dot segment in a declared value is escaped again: one piece.
	wantPath(t, `${ segments(.path) }`, map[string]any{"path": "/apis/apps/v1/%2e%2e/x"}, "/apis/apps/v1/%252e%252e/x")
	wantPath(t, `${ segments(.path) }`, map[string]any{"path": "/apis/apps/v1/deployments?x=/p1s"},
		"/apis/apps/v1/deployments%3Fx=/p1s")
	wantPath(t, `${ segments(.path) }`, map[string]any{"path": "/a/b#c"}, "/a/b%23c")
	wantRefused(t, `${ "/a?q=" + segments(.path) }`, map[string]any{"path": "b/c"}, ReasonSegmentsInQuery)
	wantPath(t, `${ "/a/" + segments(.p) + "/z" }`, map[string]any{}, "/a//z")
}

// The edges of segments(E), the contract the portal writes against
// (howto/restactions.md). The render only: the dial plan's verdict on a path
// that renders empty is pinned in restactions/api.
func TestIssue1123_SegmentsEdges(t *testing.T) {
	// null and "" contribute nothing; the template's own text stays.
	for name, data := range map[string]map[string]any{
		"missing": {},
		"null":    {"p": nil},
		"empty":   {"p": ""},
	} {
		t.Run(name, func(t *testing.T) {
			wantPath(t, `${ segments(.p) }`, data, "")
			wantPath(t, `${ "/a/" + segments(.p) }`, data, "/a/")
			wantPath(t, `${ segments(.p) + "/z" }`, data, "/z")
			wantPath(t, `${ "/a/" + segments(.p) + "/z" }`, data, "/a//z")
			// The query refusal reads the template, so no value escapes it.
			wantRefused(t, `${ "/a?q=" + segments(.p) }`, data, ReasonSegmentsInQuery)
			wantRefused(t, `${ "/a#f?q=" + segments(.p) }`, data, ReasonSegmentsInQuery)
		})
	}

	// An inner empty piece is kept, and so are the leading and trailing ones.
	wantPath(t, `${ segments(.p) }`, map[string]any{"p": "a//b"}, "a//b")
	wantPath(t, `${ "/x/" + segments(.p) }`, map[string]any{"p": "a//b"}, "/x/a//b")
	wantPath(t, `${ segments(.p) }`, map[string]any{"p": "/a/"}, "/a/")
	wantPath(t, `${ segments(.p) }`, map[string]any{"p": "//a"}, "//a")
	wantPath(t, `${ segments(.p) }`, map[string]any{"p": "/"}, "/")
	// Each piece is still one segment: dots are refused, escapes stay escaped.
	wantRefused(t, `${ segments(.p) }`, map[string]any{"p": "a//.."}, ReasonDotSegment)
	wantPath(t, `${ segments(.p) }`, map[string]any{"p": "a//b?c"}, "a//b%3Fc")

	// A non-string is a render error, never its tostring. The error carries no
	// part of the value.
	for _, v := range []any{42, 1.5, true, false, []any{"a", "b"}, map[string]any{"a": "b"}} {
		for _, tmpl := range []string{`${ segments(.p) }`, `${ "/a/" + segments(.p) }`} {
			got, err := render(t, tmpl, map[string]any{"p": v})
			if !errors.Is(err, errNotString) || IsRefused(err) || got != "" {
				t.Fatalf("Render(%s) on %v = %q, %v; want the not-a-string error", tmpl, v, got, err)
			}
		}
	}
}

// A value the program encoded with @uri is not encoded twice: `%2F` stays
// `%2F`. It is one query value as it stands; in the path it is still one
// segment, so an encoded '/' is refused like a '/'.
func TestIssue1123_AnAuthorEncodedValueIsNotEncodedTwice(t *testing.T) {
	// The query: as jq wrote it.
	q := `${ "/s?labelSelector=" + ("a.io/t=" + .t | @uri) + "&x=1" }`
	wantPath(t, q, map[string]any{"t": "b&c=d"}, "/s?labelSelector=a.io%2Ft%3Db%26c%3Dd&x=1")
	wantPath(t, `${ "/s?q=" + (.q | @uri) }`, map[string]any{"q": "a/b#c"}, "/s?q=a%2Fb%23c")
	wantPath(t, `${ "/s?q=" + (.q | @uri) }`, map[string]any{"q": "%26"}, "/s?q=%2526")
	wantPath(t, `${ (.q | @uri) as $e | "/s?q=" + $e }`, map[string]any{"q": "a&b"}, "/s?q=a%26b")
	wantPath(t, `${ @uri "/s?q=\(.q)" }`, map[string]any{"q": "a&b/c"}, "/s?q=a%26b%2Fc")
	wantPath(t, `${ @uri "/s" }`, nil, "/s")

	// The path: one segment, whichever way the value was encoded.
	seg := `${ "/a/" + (.x | @uri) + "/z" }`
	wantPath(t, seg, map[string]any{"x": "b c?d#e"}, "/a/b%20c%3Fd%23e/z")
	wantPath(t, seg, map[string]any{"x": "%2F"}, "/a/%252F/z")
	wantRefused(t, seg, map[string]any{"x": "b/c"}, ReasonValueSpansSegments)
	wantRefused(t, seg, map[string]any{"x": ".."}, ReasonDotSegment)
	wantRefused(t, `${ @uri "/a/\(.x)/z" }`, map[string]any{"x": "b/c"}, ReasonValueSpansSegments)
	wantRefused(t, `${ .x | @uri }`, map[string]any{"x": "/a/b"}, ReasonValueSpansSegments)

	// Only @uri: any other pipe or format is a value, and is escaped.
	wantPath(t, `${ "/s?q=" + (.q | @base64) }`, map[string]any{"q": "a?"}, "/s?q=YT8%3D")
	wantPath(t, `${ "/s?q=" + (.q | ascii_downcase) }`, map[string]any{"q": "A&B"}, "/s?q=a%26b")
	wantPath(t, `${ "/s?q=" + (.q | @urid) }`, map[string]any{"q": "a%26b"}, "/s?q=a%26b")
}

// The blueprint-detail case measured in #1116: apiVersion
// `apps/v1/deployments?x=` and resource `p1s` made the ServiceAccount dial
// `/apis/apps/v1/deployments?x=/p1s` — deployments, while the refilter checked
// p1s. As written today the value cannot be one segment: refused, no dial.
// The recommended declared form splits a group/version into two single
// values, so the extra input is dropped and the resource stays p1s.
func TestIssue1123_BlueprintDetailPathSplit(t *testing.T) {
	asWritten := `${ "/apis/" + .apiVersion + "/" + .resource }`
	attack := map[string]any{"apiVersion": "apps/v1/deployments?x=", "resource": "p1s"}
	wantRefused(t, asWritten, attack, ReasonValueSpansSegments)

	declared := `${ (.apiVersion | split("/")) as [$g, $v] | "/apis/" + $g + "/" + $v + "/" + .resource }`
	wantPath(t, declared, attack, "/apis/apps/v1/p1s")
	wantPath(t, declared, map[string]any{"apiVersion": "composition.krateo.io/v1-2-0", "resource": "fireworksapps"},
		"/apis/composition.krateo.io/v1-2-0/fireworksapps")
	wantPath(t, declared, map[string]any{"apiVersion": "apps/v1?x=", "resource": "p1s"}, "/apis/apps/v1%3Fx=/p1s")
}

// Variables: one bound to the author's text stays the author's text; one bound
// to a value is a value.
func TestIssue1123_Variables(t *testing.T) {
	iv := `${ (.range // "24h") as $r | (if $r == "1h" then "1%20HOUR" elif $r == "7d" then "7%20DAY" else "24%20HOUR" end) as $iv | "/?query=SELECT%201%20WHERE%20t%20%3E%20now%28%29%20-%20INTERVAL%20" + $iv + "%20FORMAT%20JSON" }`
	wantPath(t, iv, map[string]any{"range": "7d"}, "/?query=SELECT%201%20WHERE%20t%20%3E%20now%28%29%20-%20INTERVAL%207%20DAY%20FORMAT%20JSON")
	wantPath(t, iv, map[string]any{}, "/?query=SELECT%201%20WHERE%20t%20%3E%20now%28%29%20-%20INTERVAL%2024%20HOUR%20FORMAT%20JSON")

	val := `${ .n as $n | "/x?q=%27" + $n + "%27" }`
	wantPath(t, val, map[string]any{"n": "demo"}, "/x?q=%27demo%27")
	wantPath(t, val, map[string]any{"n": "x&readonly=0"}, "/x?q=%27x%26readonly%3D0%27")

	// A variable bound to a template with values in it: the values are still
	// escaped where they land, the text is the author's.
	cond := `${ (.svc // "") as $svc | (if $svc == "" then "" else "%20AND%20s%3D%27" + $svc + "%27" end) as $c | "/?query=SELECT" + $c + "%20FORMAT" }`
	wantPath(t, cond, map[string]any{"svc": "snowplow"}, "/?query=SELECT%20AND%20s%3D%27snowplow%27%20FORMAT")
	wantPath(t, cond, map[string]any{"svc": "a&b"}, "/?query=SELECT%20AND%20s%3D%27a%26b%27%20FORMAT")
	wantPath(t, cond, map[string]any{}, "/?query=SELECT%20FORMAT")

	// A variable bound to a template keeps its own value everywhere else.
	same := `${ ("/a/" + .x) as $p | if ($p | length) > 3 then $p + "/long" else $p end }`
	wantPath(t, same, map[string]any{"x": "bc"}, "/a/bc/long")
	wantRefused(t, same, map[string]any{"x": "b/c"}, ReasonValueSpansSegments)

	// An object of templates, indexed by a literal key.
	obj := `${ (if .r == "1h" then {iv: "1%20HOUR", bk: "2%20MINUTE"} else {iv: "24%20HOUR", bk: "1%20HOUR"} end) as $w | "/?q=" + $w.bk + "%20" + $w.iv }`
	wantPath(t, obj, map[string]any{"r": "1h"}, "/?q=2%20MINUTE%201%20HOUR")
	wantPath(t, obj, map[string]any{}, "/?q=1%20HOUR%2024%20HOUR")

	// A value-bound variable shadows a template-bound one.
	shadow := `${ "lit/x" as $a | .v as $a | "/p/" + $a }`
	wantRefused(t, shadow, map[string]any{"v": "q/r"}, ReasonValueSpansSegments)
}

// jq semantics the rewrite must keep.
func TestIssue1123_JQSemanticsPreserved(t *testing.T) {
	wantPath(t, `${ "/ns/" + (.namespace // "default") + "/x" }`, map[string]any{}, "/ns/default/x")
	wantPath(t, `${ "/ns/" + .namespace + "/x" }`, map[string]any{}, "/ns//x") // + null
	wantPath(t, `${ "/api/v1/namespaces/demo-system/pods" }`, nil, "/api/v1/namespaces/demo-system/pods")
	wantPath(t, `/api/v1/namespaces?limit=10`, nil, "/api/v1/namespaces?limit=10") // not a template
	wantPath(t, `${ "/api/v1/namespaces/" + (.) + "/pods" }`, "kube-system", "/api/v1/namespaces/kube-system/pods")
	wantPath(t, `${ "/todos?userId=" + (.id|tostring) }`, map[string]any{"id": 7}, "/todos?userId=7")
	wantPath(t, `${ .ns as $ns | "/a/" + $ns }`, map[string]any{"ns": "b"}, "/a/b")
	// A definition in the path program is kept; a CALL is a value (its output
	// is not text the template shows), so a function that returns a path is
	// refused like any other value with a '/'.
	wantPath(t, `${ def p: "/x/"; "/y/" + .a }`, map[string]any{"a": "b"}, "/y/b")
	wantRefused(t, `${ def p: "/x/"; p + .a }`, map[string]any{"a": "b"}, ReasonValueSpansSegments)
	wantPath(t, `${ if .a then "/a/" + .a else "/none" end }`, map[string]any{"a": "q"}, "/a/q")
	wantPath(t, `${ if .a then "/a/" + .a else "/none" end }`, map[string]any{}, "/none")
	wantPath(t, `${ .spec | "/apis/" + .group }`, map[string]any{"spec": map[string]any{"group": "g"}}, "/apis/g")
	wantPath(t, `${ empty }`, nil, "")

	// A number spliced with '+' is jq's error, not a path.
	if _, err := render(t, `${ "/a/" + .n }`, map[string]any{"n": 1}); err == nil || IsRefused(err) {
		t.Fatalf("string + number: want a jq-class error, got %v", err)
	}
	// More than one value is not a path.
	if _, err := render(t, `${ "/a/" + (.x[]) }`, map[string]any{"x": []any{"b", "c"}}); err == nil {
		t.Fatal("a generator path rendered")
	}
	// A parse error is the plain render's error class.
	if _, err := render(t, `${ "/a/" + }`, nil); err == nil || IsRefused(err) {
		t.Fatalf("parse error: got %v", err)
	}
	// The rewrite's own names are reserved.
	if _, err := render(t, `${ "/a/" + $__snowplow_path_t1 }`, nil); err == nil {
		t.Fatal("reserved name accepted")
	}
	// segments() means nothing outside the path: a compile error there.
	if _, err := render(t, `${ segments(.a) | ascii_downcase }`, map[string]any{"a": "b"}); err == nil {
		t.Fatal("segments() outside a splice compiled")
	}
}

// The rewrite yields one path per output of the original, in order, so a
// generator a later stage relies on is still an error rather than a merge.
func TestIssue1123_OneOutputPerOriginalOutput(t *testing.T) {
	if got, err := render(t, `${ "/a/" + (.x | first) }`, map[string]any{"x": []any{"b", "c"}}); err != nil || got != "/a/b" {
		t.Fatalf("got %q, %v", got, err)
	}
}

// A compiled Template renders exactly what Render does, from many goroutines.
func TestIssue1123_CompiledTemplateIsSharedSafely(t *testing.T) {
	pt := Compile(ticketPath)
	done := make(chan error, 16)
	for g := 0; g < 16; g++ {
		go func(g int) {
			for i := 0; i < 50; i++ {
				id := map[string]any{"id": "x?" + string(rune('a'+g))}
				got, err := pt.Render(context.Background(), id)
				want, werr := Render(context.Background(), ticketPath, id)
				if err != nil || werr != nil || got != want {
					done <- errors.New("compiled render diverged: " + got + " vs " + want)
					return
				}
			}
			done <- nil
		}(g)
	}
	for g := 0; g < 16; g++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	// A template that does not compile fails every render the same way.
	if _, err := Compile(`${ "/a/" + }`).Render(context.Background(), nil); err == nil {
		t.Fatal("a template that does not parse rendered")
	}
}

func TestIssue1123_EscapeQueryValue(t *testing.T) {
	for in, want := range map[string]string{
		"":                  "",
		"abcXYZ019-._~":     "abcXYZ019-._~",
		"!$'()*,:@/?":       "!$'()*,:@/?",
		"&=+;#% ":           "%26%3D%2B%3B%23%25%20",
		"é":                 "%C3%A9",
		"\"<>[\\]^`{|}\x00": "%22%3C%3E%5B%5C%5D%5E%60%7B%7C%7D%00",
	} {
		if got := EscapeQueryValue(in); got != want {
			t.Errorf("EscapeQueryValue(%q) = %q, want %q", in, got, want)
		}
	}
}
