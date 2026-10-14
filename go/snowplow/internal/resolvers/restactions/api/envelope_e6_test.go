package api

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/krateo-platformops/snowplow/internal/listenvelope"
)

// E6 (#603 design §1, #674): every Kubernetes LIST envelope served to a
// RESTAction stage carries EXACTLY {apiVersion, kind, items} — no list-level
// metadata — whatever built it: an informer, the content cache, the
// cluster-list collapse, or branch C (informer-served or live). The
// jqinterest analysis relies on it (internal/cache/jqinterest/interest.go
// toItemReads), and so does its emulator (emulate_test.go builds its
// envelope with listenvelope.New). Three guards:
//
//   - TestE6_EveryEnvelopeConstructorServesExactlyTheKeySet: every
//     constructor in internal/resolvers/restactions/api and internal/cache,
//     including the rebuilds of a STORED envelope that carries
//     metadata.resourceVersion and continue (they must drop it), an empty
//     selector result, and branch C's live LIST;
//   - the behavioural chokepoint (envelope_e6_chokepoint_test.go): at
//     jsonHandlerCore, where every LIST envelope reaches a stage, every
//     dispatch test in this package asserts the key set;
//   - TestE6_NoUnregisteredEnvelopeBuilder: an AST check over both
//     packages' production files. A function or package-level value that
//     builds a map with an "items" key, assigns m["items"], decodes into a
//     json:"items" struct, writes `"items":` bytes, serialises an
//     UnstructuredList (UnstructuredContent), or calls listenvelope.New/For
//     must be registered in e6Registry, and every registered builder or
//     decoder must be covered by the table.

// e6Class is what a registered function does with a list envelope.
type e6Class string

const (
	// e6Builder builds a served envelope; the table checks its key set.
	e6Builder e6Class = "builder"
	// e6Decoder decodes a stored envelope into a struct that keeps only
	// apiVersion, kind and items; the table checks the rebuild drops the rest.
	e6Decoder e6Class = "decoder"
	// e6ItemsRewrite replaces the items of an envelope it was handed, in
	// place (the userAccessFilter refilter). It adds no key, so the envelope
	// keeps the key set its builder gave it; the chokepoint sees that set.
	e6ItemsRewrite e6Class = "items-rewrite"
	// e6FailClosed replaces a stage's value with {"items": []} on a
	// fail-closed path. It is not a function of the list's content (no
	// identity, an unpublished RBAC snapshot, an SA endpoint failure), so a
	// list event never moves a cell between it and an E6 envelope, and its
	// keys are a subset of E6.
	e6FailClosed e6Class = "fail-closed"
)

// e6Registry: every function (package-prefixed) in the scanned packages'
// production files that builds, decodes or rewrites a list envelope. Keep it
// exact: a new entry needs a class, and a builder or decoder needs a row in
// e6Table.
var e6Registry = map[string]e6Class{
	"api.listEnvelopeValue":               e6Builder,
	"api.serveParsedListEnvelope":         e6Builder,
	"api.marshalAsList":                   e6Builder,
	"api.marshalAsListRaw":                e6Builder,
	"api.dispatchViaInternalRESTConfig":   e6Builder, // branch C live LIST, normalised (#676)
	"api.normaliseListEnvelope":           e6Builder, // jsonHandlerCore: every GET LIST incl. the fall-through (#676 C2(a))
	"cache.ListServableEnvelopeJSONWhere": e6Builder, // branch C informer serve (#676)
	"api.parseListEnvelope":               e6Decoder,
	"api.validateClusterListShape":        e6Decoder,
	"api.applyUserAccessFilter":           e6ItemsRewrite,
	"api.applyUserAccessFilterOnPig":      e6ItemsRewrite,
	"api.setRefilteredEmpty":              e6FailClosed,
	"api.resolveStageEndpoint":            e6FailClosed,
}

// e6StoredEnvelope: a stored raw envelope that DOES carry list-level
// metadata. Every rebuild must drop it.
func e6StoredEnvelope(t *testing.T, items []*unstructured.Unstructured) []byte {
	t.Helper()
	list := make([]any, 0, len(items))
	for _, it := range items {
		list = append(list, it.Object)
	}
	raw, err := json.Marshal(map[string]any{
		"apiVersion": apiVersionForGVR(f1WidgetsGVR),
		"kind":       listKindForResource(f1WidgetsGVR.Resource),
		"metadata":   map[string]any{"resourceVersion": "4242", "continue": "tok"},
		"items":      list,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type e6Row struct {
	name   string
	covers []string // registered functions this row exercises
	build  func(t *testing.T) any
}

func e6Table(t *testing.T) []e6Row {
	rw := newF1Watcher(t)
	items, servable := rw.ListObjectsServable(f1WidgetsGVR, "")
	if !servable || len(items) == 0 {
		t.Fatalf("setup: widgets GVR not servable after sync")
	}
	av, lk := apiVersionForGVR(f1WidgetsGVR), listKindForResource(f1WidgetsGVR.Resource)
	stored := e6StoredEnvelope(t, items)
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: f1NarrowUser}))
	decode := func(t *testing.T, b []byte, err error) any {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		var v any
		if err := json.Unmarshal(b, &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	parsed := func(t *testing.T) parsedListEnvelope {
		t.Helper()
		p, ok := parseListEnvelope(f1WidgetsGVR, stored)
		if !ok {
			t.Fatal("parseListEnvelope rejected the stored envelope")
		}
		return p
	}
	cluster := func(t *testing.T) parsedListEnvelope {
		t.Helper()
		shape, ok, why := validateClusterListShape(f1WidgetsGVR, stored)
		if !ok {
			t.Fatalf("validateClusterListShape rejected the stored envelope: %s", why)
		}
		p, why := decodeClusterListItems(shape)
		if why != "" {
			t.Fatalf("decodeClusterListItems: %s", why)
		}
		return p
	}
	return []e6Row{
		{"api.listEnvelopeValue", []string{"api.listEnvelopeValue"}, func(t *testing.T) any {
			return listEnvelopeValue(av, lk, items)
		}},
		{"marshalAsList (decoded bytes)", []string{"api.marshalAsList"}, func(t *testing.T) any {
			b, err := marshalAsList(av, lk, items)
			return decode(t, b, err)
		}},
		{"marshalAsListRaw (decoded bytes)", []string{"api.marshalAsListRaw"}, func(t *testing.T) any {
			raws := make([][]byte, 0, len(items))
			for _, it := range items {
				b, err := json.Marshal(it.Object)
				if err != nil {
					t.Fatal(err)
				}
				raws = append(raws, b)
			}
			b, err := marshalAsListRaw(av, lk, raws)
			return decode(t, b, err)
		}},
		{"marshalAsListRaw, empty list", []string{"api.marshalAsListRaw"}, func(t *testing.T) any {
			b, err := marshalAsListRaw(av, lk, nil)
			return decode(t, b, err)
		}},
		{"content hit: serveParsedListEnvelope(parseListEnvelope(stored with metadata))",
			[]string{"api.parseListEnvelope", "api.serveParsedListEnvelope"}, func(t *testing.T) any {
				v, ok := serveParsedListEnvelope(parsed(t))
				if !ok {
					t.Fatal("not served")
				}
				return v
			}},
		{"content hit, gated: gateListItems(parseListEnvelope(stored with metadata))",
			[]string{"api.parseListEnvelope", "api.listEnvelopeValue"}, func(t *testing.T) any {
				v, ok := gateListItems(ctx, f1WidgetsGVR, parsed(t))
				if !ok {
					t.Fatal("not served")
				}
				return v
			}},
		{"rebuild: gateListEnvelope(stored with metadata)",
			[]string{"api.parseListEnvelope", "api.listEnvelopeValue"}, func(t *testing.T) any {
				v, ok := gateListEnvelope(ctx, f1WidgetsGVR, stored)
				if !ok {
					t.Fatal("not served")
				}
				return v
			}},
		{"cluster-list collapse: validateClusterListShape + decode, served (stored with metadata)",
			[]string{"api.validateClusterListShape", "api.serveParsedListEnvelope"}, func(t *testing.T) any {
				v, ok := serveParsedListEnvelope(cluster(t))
				if !ok {
					t.Fatal("not served")
				}
				return v
			}},
		{"cluster-list collapse, gated (stored with metadata)",
			[]string{"api.validateClusterListShape", "api.listEnvelopeValue"}, func(t *testing.T) any {
				v, ok := gateListItems(ctx, f1WidgetsGVR, cluster(t))
				if !ok {
					t.Fatal("not served")
				}
				return v
			}},
		{"jsonHandlerCore normalisation of a live apiserver List (the httpcall.Do fall-through)",
			[]string{"api.normaliseListEnvelope"}, func(t *testing.T) any {
				var live map[string]any
				if err := json.Unmarshal(stored, &live); err != nil {
					t.Fatal(err)
				}
				return normaliseListEnvelope(f1WidgetsGVR, live)
			}},
		// Branch C's informer serve lives in internal/cache (reviewer-603,
		// #676). Its empty result used to drop apiVersion/kind.
		{"cache: ListServableEnvelopeJSONWhere, every item",
			[]string{"cache.ListServableEnvelopeJSONWhere"}, func(t *testing.T) any {
				b, ok := rw.ListServableEnvelopeJSONWhere(f1WidgetsGVR, "", nil)
				if !ok {
					t.Fatal("not servable")
				}
				return decode(t, b, nil)
			}},
		{"cache: ListServableEnvelopeJSONWhere, a selector that matches nothing",
			[]string{"cache.ListServableEnvelopeJSONWhere"}, func(t *testing.T) any {
				b, ok := rw.ListServableEnvelopeJSONWhere(f1WidgetsGVR, "", func(*unstructured.Unstructured) bool { return false })
				if !ok {
					t.Fatal("not servable")
				}
				return decode(t, b, nil)
			}},
		{"branch C: live paged LIST (dispatchViaInternalRESTConfig, no serve-watcher)",
			[]string{"api.dispatchViaInternalRESTConfig"}, func(t *testing.T) any {
				live, _ := e6BranchC(t)
				return decode(t, live, nil)
			}},
		{"branch C: informer serve (dispatchViaInternalRESTConfig + serve-watcher)",
			[]string{"api.dispatchViaInternalRESTConfig", "cache.ListServableEnvelopeJSONWhere"}, func(t *testing.T) any {
				_, inf := e6BranchC(t)
				return decode(t, inf, nil)
			}},
	}
}

// e6BranchC runs branch C both ways over the 1a fixture: the live paged
// LIST (its apiserver returns list metadata) and the informer serve.
func e6BranchC(t *testing.T) (live, inf []byte) {
	t.Helper()
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	const n = 3
	items := make([]*unstructured.Unstructured, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, serve1aItem(i, "composition-"+itoa(i), "bench-ns-"+itoa(i)))
	}
	return serve1aLiveAndInformerRaw(t, n, items)
}

// TestE6_BranchCLiveListMatchesTheRequestPath (reviewer-603, #676): branch
// C — the internal-driver dispatch of the refresher, prewarm re-resolve and
// Phase-1 walk — serves the SAME envelope as the request path. The informer
// serve is byte-identical to marshalAsList over the same items; the live
// LIST differs from it only inside items (the informer's strip set), never
// in the envelope: no metadata.resourceVersion, same apiVersion and kind.
func TestE6_BranchCLiveListMatchesTheRequestPath(t *testing.T) {
	live, inf := e6BranchC(t)
	gvr := serve1aGVR
	var infEnv map[string]any
	if err := json.Unmarshal(inf, &infEnv); err != nil {
		t.Fatal(err)
	}
	// The request path over the informer's own items.
	var items []*unstructured.Unstructured
	for _, it := range infEnv["items"].([]any) {
		items = append(items, &unstructured.Unstructured{Object: it.(map[string]any)})
	}
	req, err := marshalAsList(apiVersionForGVR(gvr), listKindForResource(gvr.Resource), items)
	if err != nil {
		t.Fatal(err)
	}
	if string(inf) != string(req) {
		t.Errorf("branch C informer serve is not byte-identical to the request path:\n branch C: %.300s\n request:  %.300s", inf, req)
	}
	strip := func(raw []byte) string {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		m["items"] = []any{}
		b, _ := json.Marshal(m)
		return string(b)
	}
	if strip(live) != strip(req) {
		t.Errorf("branch C live LIST envelope differs from the request path's (items aside):\n branch C: %s\n request:  %s", strip(live), strip(req))
	}
	var liveEnv any
	if err := json.Unmarshal(live, &liveEnv); err != nil {
		t.Fatal(err)
	}
	if got := e6Keys(t, liveEnv); !reflect.DeepEqual(got, listenvelope.Keys()) {
		t.Errorf("branch C live LIST keys %v, want %v", got, listenvelope.Keys())
	}
}

func e6Keys(t *testing.T, v any) []string {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("envelope is %T, want a JSON object", v)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TestE6_EveryEnvelopeConstructorServesExactlyTheKeySet is the E6 table
// (#674): every constructor yields exactly listenvelope.Keys(), and the
// rebuilds of a stored envelope that carries metadata.resourceVersion and
// continue drop them.
func TestE6_EveryEnvelopeConstructorServesExactlyTheKeySet(t *testing.T) {
	want := listenvelope.Keys()
	if !reflect.DeepEqual(want, []string{"apiVersion", "items", "kind"}) {
		t.Fatalf("listenvelope.Keys() = %v; E6 is {apiVersion, kind, items}", want)
	}
	covered := map[string]bool{}
	for _, row := range e6Table(t) {
		got := e6Keys(t, row.build(t))
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: envelope keys %v, want exactly %v (E6, #674)", row.name, got, want)
		}
		for _, c := range row.covers {
			covered[c] = true
		}
	}
	for fn, class := range e6Registry {
		if (class == e6Builder || class == e6Decoder) && !covered[fn] {
			t.Errorf("registered %s %s has no row in the E6 table", class, fn)
		}
	}
}

// e6ScanDirs: every package whose production code builds a list envelope
// a stage can receive (reviewer-603, #676: branch C serves from
// internal/cache's ListServableEnvelopeJSONWhere). Keyed in e6Registry by the
// prefix.
var e6ScanDirs = map[string]string{
	"api":   ".",
	"cache": "../../../cache",
}

// TestE6_NoUnregisteredEnvelopeBuilder is the anti-bypass AST guard (#674),
// secondary to the behavioural chokepoint (envelope_e6_chokepoint_test.go).
// It closes B2 (an index assignment m["items"] = x) and B5 (a package-level
// func var). B1 (a key in a constant), B3 (a key split across literals) and
// B4 (a converter or list type from another package) are left to the
// chokepoint, which sees the envelope itself.
func TestE6_NoUnregisteredEnvelopeBuilder(t *testing.T) {
	found := map[string][]string{}
	for prefix, dir := range e6ScanDirs {
		f, err := e6ScanPackage(dir)
		if err != nil {
			t.Fatal(err)
		}
		for fn, why := range f {
			found[prefix+"."+fn] = why
		}
	}
	names := make([]string, 0, len(found))
	for fn := range found {
		names = append(names, fn)
	}
	sort.Strings(names)
	for _, fn := range names {
		t.Logf("%-42s %-13s %s", fn, e6Registry[fn], strings.Join(found[fn], "; "))
	}
	for _, fn := range names {
		if _, ok := e6Registry[fn]; !ok {
			t.Errorf("%s (%s) builds, decodes or rewrites a list envelope but is not in e6Registry: "+
				"register it with its class, and add a builder or decoder to the E6 table (envelope_e6_test.go, #674)",
				fn, strings.Join(found[fn], "; "))
		}
	}
	for fn := range e6Registry {
		if _, ok := found[fn]; !ok {
			t.Errorf("e6Registry lists %s, which no longer builds, decodes or rewrites a list envelope: remove it", fn)
		}
	}
}

// e6ScanPackage returns, per enclosing function (or "var <name>" for a
// package-level value, "type <name>" for a decoder struct type), why the AST
// check flags it. Production files only.
func e6ScanPackage(dir string) (map[string][]string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	found := map[string][]string{}
	add := func(name, file string, whys []string) {
		for _, why := range whys {
			found[name] = append(found[name], filepath.Base(file)+": "+why)
		}
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		file, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			return nil, err
		}
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Body != nil {
					add(d.Name.Name, f, e6Hits(d.Body))
				}
			case *ast.GenDecl:
				for _, sp := range d.Specs {
					switch x := sp.(type) {
					case *ast.TypeSpec:
						// A package-level struct type with a json:"items" field
						// decodes an envelope wherever it is used.
						if e6HasItemsTag(x.Type) {
							add("type "+x.Name.Name, f, []string{`struct with a json:"items" field`})
						}
					case *ast.ValueSpec:
						// B5: a package-level var whose value builds one
						// (var f = func(...) {...}).
						for i, v := range x.Values {
							name := "var"
							if i < len(x.Names) {
								name = "var " + x.Names[i].Name
							}
							add(name, f, e6Hits(v))
						}
					}
				}
			}
		}
	}
	return found, nil
}

func e6Hits(body ast.Node) []string {
	var hits []string
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.KeyValueExpr:
			if e6IsItemsKey(x.Key) {
				hits = append(hits, `a composite literal with an "items" key`)
			}
		case *ast.AssignStmt:
			// B2: m["items"] = x.
			for _, l := range x.Lhs {
				if ix, ok := l.(*ast.IndexExpr); ok && e6IsItemsKey(ix.Index) {
					hits = append(hits, `an index assignment to ["items"]`)
				}
			}
		case *ast.BasicLit:
			if x.Kind == token.STRING && strings.Contains(x.Value, `"items":`) {
				hits = append(hits, "a string literal writing `\"items\":`")
			}
		case *ast.StructType:
			if e6HasItemsTag(x) {
				hits = append(hits, `a struct with a json:"items" field`)
			}
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
				if sel.Sel.Name == "UnstructuredContent" {
					hits = append(hits, "UnstructuredContent() of a list")
				}
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "listenvelope" && (sel.Sel.Name == "New" || sel.Sel.Name == "For") {
					hits = append(hits, "listenvelope."+sel.Sel.Name)
				}
			}
		}
		return true
	})
	return hits
}

func e6IsItemsKey(e ast.Expr) bool {
	switch k := e.(type) {
	case *ast.BasicLit:
		return k.Kind == token.STRING && k.Value == `"items"`
	case *ast.SelectorExpr:
		id, ok := k.X.(*ast.Ident)
		return ok && id.Name == "listenvelope" && k.Sel.Name == "Items"
	}
	return false
}

func e6HasItemsTag(e ast.Expr) bool {
	st, ok := e.(*ast.StructType)
	if !ok || st.Fields == nil {
		return false
	}
	for _, f := range st.Fields.List {
		if f.Tag != nil && (strings.Contains(f.Tag.Value, `json:"items"`) || strings.Contains(f.Tag.Value, `json:"items,`)) {
			return true
		}
	}
	return false
}
