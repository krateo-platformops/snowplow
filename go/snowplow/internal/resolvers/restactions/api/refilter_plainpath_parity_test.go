// refilter_plainpath_parity_test.go — the plain-field-path fast path of the
// userAccessFilter per-item expressions (refilter_plainpath.go) must give the
// value, the error and the RBAC verdict that the gojq path gives, for every
// item.
//
// Each arm compares the fast path (uafExpr.eval) against gojq run on the same
// compiled code (evalJQStringCompiled) — the instrument is gojq itself, not
// anything the fast path computes.

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"strings"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// portalUAFExprs are the per-item expressions the portal charts use
// (namespaceFrom .metadata.namespace / .metadata.name, nameFrom defaulting to
// .metadata.name) plus "." from the namespaces stage.
var portalUAFExprs = []string{".metadata.namespace", ".metadata.name", "."}

// plainUAFExprs must be recognised as plain field paths.
var plainUAFExprs = append([]string{
	".a", ".a.b", ".a.b.c", ".spec.x", "._x", ".A9.b_2", ".metadata.labels.app",
	".if", ".and", ".or", ".not", ".null", ".true", ".end", ".reduce",
}, portalUAFExprs...)

// nonPlainUAFExprs must NOT be recognised: each one either is not a field
// path or has a different meaning from the plain read.
var nonPlainUAFExprs = []string{
	"", " ", ".. ", "..", "...", ".a.", ".a..b", " .a", ".a ", ".a .b", ". a",
	".a?", ".a.b?", ".a?.b", `.["a"]`, `."a"`, `.a."b"`, ".a[0]", ".[0]", ".a[]",
	".a | .b", ".a // \"\"", ".a, .b", ".a+.b", "-.a", ".a.b|tostring",
	"tostring", "$__loc__", "if . then . else . end", ".a.1", ".1", ".a-b",
	".é", ".a\n.b", ".a\t", "(.a)", ".a as $x | $x", "def f: .; f", `.a["b"]`,
	".a[1:2]", "..a", ".a..", ".$x", "@base64", `"x"`, "1", "null", "empty",
	".a.b.c?", "try .a", ".a // empty", "import \"x\" as y; .a",
}

// uafParityItems covers every value type the fast path walks or ends on.
func uafParityItems() []any {
	var typedNilMap map[string]any
	values := []any{
		"ns-1", "", "ünïcødé", "with space", "1", "null",
		nil, true, false,
		float64(0), float64(1), float64(-1), 1.5, 1e20, 1e-7, int(3), int64(4),
		json.Number("5"), json.Number("1.50"), big.NewInt(7),
		[]any{}, []any{"a"}, []any{nil},
		map[string]any{}, map[string]any{"x": "y"}, typedNilMap,
	}
	var items []any
	for _, v := range values {
		items = append(items,
			v, // the item itself (top-level scalar / array / map)
			map[string]any{"metadata": v},
			map[string]any{"metadata": map[string]any{"namespace": v, "name": v}},
			map[string]any{"a": map[string]any{"b": map[string]any{"c": v}}, "spec": map[string]any{"x": v}},
			map[string]any{"a": v, "if": v, "and": v, "_x": v},
		)
	}
	items = append(items,
		map[string]any{},
		map[string]any{"metadata": map[string]any{}},
		map[string]any{"metadata": map[string]any{"namespace": "team-a"}},
		map[string]any{"metadata": map[string]any{"name": "alpha"}},
		map[string]any{"metadata": map[string]any{"namespace": "team-a", "name": "alpha", "labels": map[string]any{"app": "x"}}},
		map[string]any{"a": typedNilMap},
	)
	return items
}

// gojqOnly is e with the fast path switched off: the reference.
func gojqOnly(e *uafExpr) *uafExpr { return &uafExpr{code: e.code} }

func mustUAFExpr(t testing.TB, expr string) *uafExpr {
	t.Helper()
	e := compileUAFExpr(expr, jqsupport.ModuleLoader())
	if e == nil {
		t.Fatalf("compileUAFExpr(%q) failed", expr)
	}
	return e
}

// uafEvalOutcome is one evaluation's full result. gojq panics on a Go value
// that is not a jq value (an int64, say); the outcome records that too, so a
// panic on one path and an answer on the other is a parity failure.
type uafEvalOutcome struct {
	value string
	err   string
	panic string
}

func catchUAFEval(f func() (string, error)) (o uafEvalOutcome) {
	defer func() {
		if r := recover(); r != nil {
			o.panic = fmt.Sprint(r)
		}
	}()
	v, err := f()
	o.value = v
	if err != nil {
		o.err = err.Error()
	}
	return o
}

// assertUAFEvalParity compares the fast path with gojq on one item.
func assertUAFEvalParity(t testing.TB, ctx context.Context, expr string, e *uafExpr, item any) {
	t.Helper()
	got := catchUAFEval(func() (string, error) { return e.eval(ctx, item) })
	want := catchUAFEval(func() (string, error) { return evalJQStringCompiled(ctx, e.code, item) })
	if got != want {
		t.Fatalf("expr %q on item %#v: fast path %+v != gojq %+v", expr, item, got, want)
	}
}

func TestUAFPlainPath_Recogniser(t *testing.T) {
	for _, expr := range plainUAFExprs {
		path, ok := plainFieldPath(expr)
		if !ok {
			t.Errorf("plainFieldPath(%q) rejected a plain field path", expr)
			continue
		}
		if want := strings.Split(strings.TrimPrefix(expr, "."), "."); expr != "." && strings.Join(path, ".") != strings.Join(want, ".") {
			t.Errorf("plainFieldPath(%q) = %q, want %q", expr, path, want)
		}
		if expr == "." && len(path) != 0 {
			t.Errorf(`plainFieldPath(".") = %q, want no keys`, path)
		}
	}
	for _, expr := range nonPlainUAFExprs {
		if path, ok := plainFieldPath(expr); ok {
			t.Errorf("plainFieldPath(%q) accepted a non-plain expression as %q", expr, path)
		}
	}
	// The portal's expressions take the fast path once compiled.
	for _, expr := range portalUAFExprs {
		if !mustUAFExpr(t, expr).plain {
			t.Errorf("compileUAFExpr(%q) is not on the fast path", expr)
		}
	}
}

// TestUAFPlainPath_ValueParity: every recognised expression on every corpus
// item, and every non-plain expression that compiles (which must run gojq
// unchanged).
func TestUAFPlainPath_ValueParity(t *testing.T) {
	ctx := context.Background()
	items := uafParityItems()
	fast := 0
	for _, expr := range append(append([]string{}, plainUAFExprs...), nonPlainUAFExprs...) {
		e := compileUAFExpr(expr, jqsupport.ModuleLoader())
		if e == nil {
			continue // compile failure: the callers deny, no eval happens
		}
		for _, item := range items {
			assertUAFEvalParity(t, ctx, expr, e, item)
			if e.plain {
				if _, ok := plainPathString(e.path, item); ok {
					fast++
				}
			}
		}
	}
	// The parity above must have exercised the direct read, not only the
	// fallback.
	if fast < 100 {
		t.Fatalf("only %d (expr, item) pairs took the direct read; the corpus no longer exercises the fast path", fast)
	}
}

// TestUAFPlainPath_FallbackOnNonStringOrNonObject: the shapes the direct read
// must hand to gojq. Each one has a gojq answer the direct read cannot give
// (an error, or a rendered number / object), so a skipped fallback shows here
// as a wrong value or a missing error.
func TestUAFPlainPath_FallbackOnNonStringOrNonObject(t *testing.T) {
	ctx := context.Background()
	e := mustUAFExpr(t, ".metadata.namespace")
	cases := []struct {
		item    any
		want    string
		wantErr bool
	}{
		{map[string]any{"metadata": "a-string"}, "", true}, // .namespace on a string
		{map[string]any{"metadata": []any{"x"}}, "", true}, // .namespace on an array
		{map[string]any{"metadata": float64(1)}, "", true}, // .namespace on a number
		{"ns-1", "", true}, // .metadata on a string item
		{map[string]any{"metadata": map[string]any{"namespace": float64(1)}}, "1", false}, // number rendered by gojq
		{map[string]any{"metadata": map[string]any{"namespace": true}}, "true", false},
		{map[string]any{"metadata": map[string]any{"namespace": map[string]any{"a": "b"}}}, `{"a":"b"}`, false},
		{map[string]any{"metadata": map[string]any{"namespace": nil}}, "", false}, // null → ""
		{map[string]any{"metadata": nil}, "", false},                              // null.namespace → null → ""
		{map[string]any{}, "", false}, // missing → null → ""
	}
	for _, c := range cases {
		assertUAFEvalParity(t, ctx, ".metadata.namespace", e, c.item)
		got, err := e.eval(ctx, c.item)
		if (err != nil) != c.wantErr || got != c.want {
			t.Errorf("eval(%#v) = (%q, %v), want (%q, err=%v)", c.item, got, err, c.want, c.wantErr)
		}
	}
}

// TestUAFPlainPath_CancelledContext: gojq answers a cancelled context with
// its error (the callers deny the item); the direct read must not answer it
// with a namespace.
func TestUAFPlainPath_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := mustUAFExpr(t, ".metadata.namespace")
	item := map[string]any{"metadata": map[string]any{"namespace": "team-a"}}
	assertUAFEvalParity(t, ctx, ".metadata.namespace", e, item)
	if _, err := e.eval(ctx, item); err == nil {
		t.Fatalf("eval on a cancelled context returned no error; gojq fails it")
	}
}

// TestUAFPlainPath_GeneratedParity: random trees under the keys the
// expressions read, against random plain paths over the same key alphabet.
func TestUAFPlainPath_GeneratedParity(t *testing.T) {
	rng := rand.New(rand.NewSource(20261022))
	keys := []string{"metadata", "namespace", "name", "a", "b"}
	ctx := context.Background()
	var gen func(depth int) any
	gen = func(depth int) any {
		switch n := rng.Intn(10); {
		case depth > 3 || n == 0:
			return nil
		case n <= 3 || depth == 3:
			leaves := []any{"s", "", float64(rng.Intn(3)), 2.5, true, nil, []any{"x"}, json.Number("9")}
			return leaves[rng.Intn(len(leaves))]
		default:
			m := map[string]any{}
			for _, k := range keys {
				if rng.Intn(2) == 0 {
					m[k] = gen(depth + 1)
				}
			}
			return m
		}
	}
	exprs := []*uafExpr{mustUAFExpr(t, ".")}
	names := []string{"."}
	for i := 0; i < 40; i++ {
		var p []string
		for j := 0; j <= rng.Intn(4); j++ {
			p = append(p, keys[rng.Intn(len(keys))])
		}
		expr := "." + strings.Join(p, ".")
		e := mustUAFExpr(t, expr)
		if !e.plain {
			t.Fatalf("generated plain path %q not recognised", expr)
		}
		exprs = append(exprs, e)
		names = append(names, expr)
	}
	for i := 0; i < 20000; i++ {
		item := gen(0)
		for j, e := range exprs {
			assertUAFEvalParity(t, ctx, names[j], e, item)
		}
	}
}

// TestUAFPlainPath_VerdictParity: the RBAC verdict per item, through
// evalSingle, with the fast path vs gojq only — for the list verb and a
// name-specific verb, over the parity corpus (errors deny) and the 50K
// benchmark fixture.
func TestUAFPlainPath_VerdictParity(t *testing.T) {
	newMemoTestWatcher(t)
	ctx := ctxWithUser(narrowUser, "devs")
	groups := []string{"devs"}
	items := append(uafParityItems(), bench50KCompositions(2000)...)
	for _, it := range []map[string]any{
		{"metadata": map[string]any{"namespace": "team-a", "name": "x"}},
		{"metadata": map[string]any{"namespace": "ns-named", "name": "alpha"}},
		{"metadata": map[string]any{"namespace": "ns-named", "name": "beta"}},
	} {
		items = append(items, it)
	}
	uafs := []*templates.UserAccessFilterSpec{
		{Verb: "list", Group: memoGroup, Resource: "widgets"},
		{Verb: "list", Group: memoGroup, Resource: "widgets", NamespaceFrom: ".metadata.namespace"},
		{Verb: "get", Group: memoGroup, Resource: "widgets"},
		{Verb: "get", Group: memoGroup, Resource: "widgets", NamespaceFrom: ".metadata.namespace", NameFrom: ".metadata.name"},
		{Verb: "list", Group: memoGroup, Resource: "widgets", NamespaceFrom: "."},
	}
	kept := 0
	for _, uaf := range uafs {
		nsExpr, nsCode := compileNamespaceFrom(uaf)
		nameExpr, nameCode := compileNameFrom(uaf)
		if !nsCode.plain || !nameCode.plain {
			t.Fatalf("uaf %+v: expressions not on the fast path", uaf)
		}
		for _, item := range items {
			verdict := func(ns, name *uafExpr) uafEvalOutcome {
				return catchUAFEval(func() (string, error) {
					ok := evalSingle(ctx, discardLogger(), narrowUser, groups, uaf, []string{"widgets"}, item, nsExpr, ns, nameExpr, name)
					return fmt.Sprint(ok), nil
				})
			}
			got := verdict(nsCode, nameCode)
			want := verdict(gojqOnly(nsCode), gojqOnly(nameCode))
			if got != want {
				t.Fatalf("uaf %+v item %#v: fast-path verdict %+v != gojq verdict %+v", uaf, item, got, want)
			}
			if got.value == "true" {
				kept++
			}
		}
	}
	// Both verdicts occur, so the parity is not "all deny".
	if kept == 0 {
		t.Fatalf("no item was permitted; the fixture no longer exercises an allow")
	}
}

// FuzzUAFPlainPathParity: any expression and any JSON item (decoded both as
// float64 and json.Number) give the same value and error on both paths.
func FuzzUAFPlainPathParity(f *testing.F) {
	for _, expr := range append(append([]string{}, plainUAFExprs...), nonPlainUAFExprs...) {
		f.Add(expr, `{"metadata":{"namespace":"team-a","name":"alpha"}}`)
		f.Add(expr, `{"metadata":{"namespace":1,"name":null}}`)
		f.Add(expr, `{"metadata":"x"}`)
		f.Add(expr, `"ns-1"`)
	}
	f.Fuzz(func(t *testing.T, expr, doc string) {
		e := compileUAFExpr(expr, jqsupport.ModuleLoader())
		if e == nil {
			return
		}
		var item any
		if json.Unmarshal([]byte(doc), &item) != nil {
			return
		}
		assertUAFEvalParity(t, context.Background(), expr, e, item)
		dec := json.NewDecoder(strings.NewReader(doc))
		dec.UseNumber()
		var numItem any
		if dec.Decode(&numItem) == nil {
			assertUAFEvalParity(t, context.Background(), expr, e, numItem)
		}
	})
}
