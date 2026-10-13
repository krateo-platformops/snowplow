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

// E6 (#603 design §1, #674): every LIST envelope this package serves to a
// RESTAction stage from an informer or from the content cache carries
// EXACTLY {apiVersion, kind, items} — no list-level metadata. The jqinterest
// analysis relies on it (internal/cache/jqinterest/interest.go toItemReads),
// and so does its emulator (emulate_test.go builds its envelope with
// listenvelope.New). Two guards:
//
//   - TestE6_EveryEnvelopeConstructorServesExactlyTheKeySet: every
//     constructor, including the rebuilds of a STORED envelope that carries
//     metadata.resourceVersion and continue (they must drop it);
//   - TestE6_NoUnregisteredEnvelopeBuilder: an AST check over this package's
//     production files. A function that builds a map with an "items" key,
//     decodes into a struct with a json:"items" field, writes `"items":`
//     bytes, serialises an UnstructuredList (UnstructuredContent), or calls
//     listenvelope.New must be registered in e6Registry, and every registered
//     builder or decoder must be covered by the table.

// e6Class is what a registered function does with a list envelope.
type e6Class string

const (
	// e6Builder builds a served envelope; the table checks its key set.
	e6Builder e6Class = "builder"
	// e6Decoder decodes a stored envelope into a struct that keeps only
	// apiVersion, kind and items; the table checks the rebuild drops the rest.
	e6Decoder e6Class = "decoder"
	// e6FailClosed replaces a stage's value with {"items": []} on a
	// fail-closed path. It is not a function of the list's content (no
	// identity, an unpublished RBAC snapshot, an SA endpoint failure), so a
	// list event never moves a cell between it and an E6 envelope.
	e6FailClosed e6Class = "fail-closed"
	// e6Apiserver serialises a live apiserver LIST (UnstructuredContent:
	// apiVersion, kind, metadata, items). E6 does NOT hold for it: it is
	// the apiserver fall-through class, which the #603 §2.4 runtime fallback
	// must read as "everything". This is branch C, the internal-driver
	// dispatch (refresher, prewarm re-resolve, Phase-1 walk).
	e6Apiserver e6Class = "apiserver-shaped"
)

// e6Registry: every function in this package's production files that
// builds or decodes a list envelope. Keep it exact: a new entry needs a
// class, and a builder or decoder needs a row in e6Table.
var e6Registry = map[string]e6Class{
	"listEnvelopeValue":             e6Builder,
	"serveParsedListEnvelope":       e6Builder,
	"marshalAsList":                 e6Builder,
	"marshalAsListRaw":              e6Builder,
	"parseListEnvelope":             e6Decoder,
	"validateClusterListShape":      e6Decoder,
	"setRefilteredEmpty":            e6FailClosed,
	"resolveStageEndpoint":          e6FailClosed,
	"dispatchViaInternalRESTConfig": e6Apiserver,
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
		{"listEnvelopeValue", []string{"listEnvelopeValue"}, func(t *testing.T) any {
			return listEnvelopeValue(av, lk, items)
		}},
		{"marshalAsList (decoded bytes)", []string{"marshalAsList"}, func(t *testing.T) any {
			b, err := marshalAsList(av, lk, items)
			return decode(t, b, err)
		}},
		{"marshalAsListRaw (decoded bytes)", []string{"marshalAsListRaw"}, func(t *testing.T) any {
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
		{"marshalAsListRaw, empty list", []string{"marshalAsListRaw"}, func(t *testing.T) any {
			b, err := marshalAsListRaw(av, lk, nil)
			return decode(t, b, err)
		}},
		{"content hit: serveParsedListEnvelope(parseListEnvelope(stored with metadata))",
			[]string{"parseListEnvelope", "serveParsedListEnvelope"}, func(t *testing.T) any {
				v, ok := serveParsedListEnvelope(parsed(t))
				if !ok {
					t.Fatal("not served")
				}
				return v
			}},
		{"content hit, gated: gateListItems(parseListEnvelope(stored with metadata))",
			[]string{"parseListEnvelope", "listEnvelopeValue"}, func(t *testing.T) any {
				v, ok := gateListItems(ctx, f1WidgetsGVR, parsed(t))
				if !ok {
					t.Fatal("not served")
				}
				return v
			}},
		{"rebuild: gateListEnvelope(stored with metadata)",
			[]string{"parseListEnvelope", "listEnvelopeValue"}, func(t *testing.T) any {
				v, ok := gateListEnvelope(ctx, f1WidgetsGVR, stored)
				if !ok {
					t.Fatal("not served")
				}
				return v
			}},
		{"cluster-list collapse: validateClusterListShape + decode, served (stored with metadata)",
			[]string{"validateClusterListShape", "serveParsedListEnvelope"}, func(t *testing.T) any {
				v, ok := serveParsedListEnvelope(cluster(t))
				if !ok {
					t.Fatal("not served")
				}
				return v
			}},
		{"cluster-list collapse, gated (stored with metadata)",
			[]string{"validateClusterListShape", "listEnvelopeValue"}, func(t *testing.T) any {
				v, ok := gateListItems(ctx, f1WidgetsGVR, cluster(t))
				if !ok {
					t.Fatal("not served")
				}
				return v
			}},
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

// TestE6_NoUnregisteredEnvelopeBuilder is the anti-bypass guard (#674).
func TestE6_NoUnregisteredEnvelopeBuilder(t *testing.T) {
	found, err := e6ScanPackage(".")
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(found))
	for fn := range found {
		names = append(names, fn)
	}
	sort.Strings(names)
	for _, fn := range names {
		t.Logf("%-30s %-17s %s", fn, e6Registry[fn], strings.Join(found[fn], "; "))
	}
	for fn, where := range found {
		if _, ok := e6Registry[fn]; !ok {
			t.Errorf("%s (%s) builds or decodes a list envelope but is not in e6Registry: "+
				"register it with its class, and add a builder or decoder to the E6 table (envelope_e6_test.go, #674)",
				fn, strings.Join(where, "; "))
		}
	}
	for fn := range e6Registry {
		if _, ok := found[fn]; !ok {
			t.Errorf("e6Registry lists %s, which no longer builds or decodes a list envelope: remove it", fn)
		}
	}
}

// e6ScanPackage returns, per enclosing function, why the AST check flags
// it. Production files only.
func e6ScanPackage(dir string) (map[string][]string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	found := map[string][]string{}
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
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			for _, why := range e6Hits(fd.Body) {
				found[fd.Name.Name] = append(found[fd.Name.Name], filepath.Base(f)+": "+why)
			}
		}
		// A package-level struct type with a json:"items" field decodes an
		// envelope wherever it is used: flag it by its type name.
		for _, decl := range file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, sp := range gd.Specs {
				ts, ok := sp.(*ast.TypeSpec)
				if ok && e6HasItemsTag(ts.Type) {
					found["type "+ts.Name.Name] = append(found["type "+ts.Name.Name], filepath.Base(f)+": struct with a json:\"items\" field")
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
				switch {
				case sel.Sel.Name == "UnstructuredContent":
					hits = append(hits, "UnstructuredContent() of a list")
				case sel.Sel.Name == "New":
					if id, ok := sel.X.(*ast.Ident); ok && id.Name == "listenvelope" {
						hits = append(hits, "listenvelope.New")
					}
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
		if f.Tag != nil && strings.Contains(f.Tag.Value, `json:"items`) {
			return true
		}
	}
	return false
}
