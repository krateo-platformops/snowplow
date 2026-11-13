package main

// The value each field label selects on, derived from kube-apiserver's own source in the table's
// notation, so the check covers the paths the fake reads and not only the labels it accepts:
//
//   - the label's mapping: what the conversion function returns for it (a label that returns
//     another of the same kind's labels, such as Pod's spec.host -> spec.nodeName, is an alias;
//     events.k8s.io's labels map to core's);
//   - the strategy's field set for the internal type (the registry function that takes *<pkg>.<Kind>
//     and returns fields.Set): which object field the mapped label reads, and how it prints (string;
//     strconv.FormatBool, or fmt.Sprint of a bool -> "bool:"; strconv.Itoa -> "int:"; the first
//     element of a list -> "first:"; a fallback from one field to another -> "either:"; no value ->
//     "absent:");
//   - that internal field, walked to the served v1 object's JSON paths through the group's
//     conversion functions (pkg/apis/<group>/v1 conversion.go and zz_generated.conversion.go:
//     `out.X = <in.chain>` and `Convert_..(&in.<chain>, &out.X, s)`), then the v1 type's JSON tags
//     (client-go's scheme). Every v1 field a conversion fills from that internal field carries the
//     same value (Pod's spec.serviceAccount and spec.serviceAccountName; status.podIP and
//     status.podIPs[0].ip), so each is an equally upstream reading, and the table's must be one.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
)

// parsedDir is every non-test file of one package directory.
func parsedDir(dir string) []*ast.File {
	fset := token.NewFileSet()
	pkgs, _ := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, 0)
	var out []*ast.File
	for _, p := range pkgs {
		for _, f := range p.Files {
			out = append(out, f)
		}
	}
	return out
}

// structFields is each struct type of a package: field name -> its type expression.
func structFields(files []*ast.File) map[string]map[string]ast.Expr {
	out := map[string]map[string]ast.Expr{}
	for _, f := range files {
		for _, d := range f.Decls {
			g, ok := d.(*ast.GenDecl)
			if !ok || g.Tok != token.TYPE {
				continue
			}
			for _, s := range g.Specs {
				ts := s.(*ast.TypeSpec)
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				m := map[string]ast.Expr{}
				for _, fl := range st.Fields.List {
					for _, n := range fl.Names {
						m[n.Name] = fl.Type
					}
				}
				out[ts.Name.Name] = m
			}
		}
	}
	return out
}

// typeName is the named type an expression denotes, through pointers and slices.
func typeName(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.StarExpr:
		return typeName(x.X)
	case *ast.ArrayType:
		return typeName(x.Elt)
	case *ast.SelectorExpr:
		return x.Sel.Name
	}
	return ""
}

// chainOf is the field chain of a selector rooted at the identifier root (in.A.B -> [A B]), and
// whether it indexes a list element on the way (in.A[0].B).
func chainOf(e ast.Expr, root string) (chain []string, indexed bool, ok bool) {
	for {
		switch x := e.(type) {
		case *ast.SelectorExpr:
			chain = append([]string{x.Sel.Name}, chain...)
			e = x.X
		case *ast.IndexExpr:
			indexed = true
			e = x.X
		case *ast.Ident:
			return chain, indexed, x.Name == root && len(chain) > 0
		default:
			return nil, false, false
		}
	}
}

// firstChain finds the first `<root>.<chain>` selector inside e.
func firstChain(e ast.Expr, root string) (chain []string, indexed bool) {
	ast.Inspect(e, func(n ast.Node) bool {
		if chain != nil {
			return false
		}
		if x, ok := n.(ast.Expr); ok {
			if c, ix, ok := chainOf(x, root); ok {
				chain, indexed = c, ix
				return false
			}
		}
		return true
	})
	return chain, indexed
}

// mapping is one v1 field a conversion function fills from an internal field chain; indexed when
// the chain takes a list element (out.PodIP = in.PodIPs[0].IP).
type mapping struct {
	in      []string
	indexed bool
	out     string
}

var convName = regexp.MustCompile(`^(?:auto)?Convert_([a-z]+)_([A-Za-z0-9]+)_To_v1_([A-Za-z0-9]+)$`)

// conversions is, per internal type, how the internal-to-v1 conversion functions in dir fill the v1
// type's fields, and the internal package each converts from.
func conversions(dir string) (map[string][]mapping, map[string]string) {
	out := map[string][]mapping{}
	ipkg := map[string]string{}
	for _, f := range parsedDir(dir) {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			m := convName.FindStringSubmatch(fn.Name.Name)
			if m == nil || m[2] != m[3] {
				continue
			}
			typ := m[2]
			ipkg[typ] = m[1]
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					if len(x.Lhs) != 1 || len(x.Rhs) != 1 {
						return true
					}
					if lc, _, ok := chainOf(x.Lhs[0], "out"); ok && len(lc) == 1 {
						if in, ix := firstChain(x.Rhs[0], "in"); in != nil {
							out[typ] = append(out[typ], mapping{in: in, indexed: ix, out: lc[0]})
						}
					}
				case *ast.CallExpr:
					// Convert_x_T_To_v1_T(&in.A, &out.B, s)
					if len(x.Args) == 3 {
						ia, ok1 := x.Args[0].(*ast.UnaryExpr)
						oa, ok2 := x.Args[1].(*ast.UnaryExpr)
						if ok1 && ok2 {
							in, ix, okIn := chainOf(ia.X, "in")
							oc, _, okOut := chainOf(oa.X, "out")
							if okIn && okOut && len(oc) == 1 {
								out[typ] = append(out[typ], mapping{in: in, indexed: ix, out: oc[0]})
							}
						}
					}
				}
				return true
			})
		}
	}
	return out, ipkg
}

func jsonName(sf reflect.StructField) string {
	name := strings.Split(sf.Tag.Get("json"), ",")[0]
	if name == "" {
		return sf.Name
	}
	return name
}

// reading is one v1 JSON path an internal field reaches; indexConsumed when a conversion took the
// list element itself (the path is then a scalar).
type reading struct {
	path          []string
	leaf          reflect.Type
	indexConsumed bool
}

// walk turns an internal field chain from type typ into every v1 JSON path that carries it: the
// conversions of the chain's head from the first conversion set that converts it (the group's own
// before the internal package's), or, with none, the v1 field of the same name.
func walk(structs map[string]map[string]ast.Expr, convs []map[string][]mapping, typ string, chain []string, rt reflect.Type) ([]reading, error) {
	for rt.Kind() == reflect.Pointer || rt.Kind() == reflect.Slice {
		rt = rt.Elem()
	}
	if len(chain) == 0 {
		return []reading{{leaf: rt}}, nil
	}
	var cands []mapping
	for _, cv := range convs {
		for _, m := range cv[typ] {
			if len(m.in) <= len(chain) && strings.Join(m.in, ".") == strings.Join(chain[:len(m.in)], ".") {
				cands = append(cands, m)
			}
		}
		if len(cands) > 0 {
			break
		}
	}
	if len(cands) == 0 {
		cands = []mapping{{in: chain[:1], out: chain[0]}}
	}
	var out []reading
	var lastErr error
	for _, m := range cands {
		sf, ok := rt.FieldByName(m.out)
		if !ok {
			lastErr = fmt.Errorf("%s has no field %s (internal %s.%s)", rt, m.out, typ, strings.Join(m.in, "."))
			continue
		}
		next := typ
		for _, el := range m.in {
			next = typeName(structs[next][el])
		}
		rest, err := walk(structs, convs, next, chain[len(m.in):], sf.Type)
		if err != nil {
			lastErr = err
			continue
		}
		for _, r := range rest {
			out = append(out, reading{path: append([]string{jsonName(sf)}, r.path...), leaf: r.leaf, indexConsumed: r.indexConsumed || m.indexed})
		}
	}
	if len(out) == 0 {
		return nil, lastErr
	}
	return out, nil
}

// fieldsFunc finds the registry function that takes *<ipkg>.<kind> and returns fields.Set.
func fieldsFunc(src, ipkg, kind string) (*ast.FuncDecl, error) {
	var found []*ast.FuncDecl
	_ = filepath.Walk(filepath.Join(src, "pkg/registry"), func(p string, fi os.FileInfo, err error) error {
		if err != nil || !fi.IsDir() {
			return nil
		}
		for _, f := range parsedDir(p) {
			alias := ""
			for _, im := range f.Imports {
				path, _ := strconv.Unquote(im.Path.Value)
				if path == "k8s.io/kubernetes/pkg/apis/"+ipkg {
					alias = ipkg
					if im.Name != nil {
						alias = im.Name.Name
					}
				}
			}
			if alias == "" {
				continue
			}
			for _, d := range f.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Body == nil || fn.Recv != nil || fn.Type.Results == nil || len(fn.Type.Results.List) != 1 || len(fn.Type.Params.List) != 1 || len(fn.Type.Params.List[0].Names) != 1 {
					continue
				}
				if r, ok := fn.Type.Results.List[0].Type.(*ast.SelectorExpr); !ok || r.Sel.Name != "Set" || fmt.Sprint(r.X) != "fields" {
					continue
				}
				st, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
				if !ok {
					continue
				}
				if se, ok := st.X.(*ast.SelectorExpr); ok && fmt.Sprint(se.X) == alias && se.Sel.Name == kind {
					found = append(found, fn)
				}
			}
		}
		return nil
	})
	if len(found) != 1 {
		return nil, fmt.Errorf("%d registry functions take *%s.%s and return fields.Set, want 1", len(found), ipkg, kind)
	}
	return found[0], nil
}

// fieldSet is label -> the expressions the function assigns it (several when branches differ), the
// expressions assigned to its locals, and whether ObjectMeta's name and namespace are added.
func fieldSet(fn *ast.FuncDecl, consts map[string]string) (set, locals map[string][]ast.Expr, name, ns bool) {
	set, locals = map[string][]ast.Expr{}, map[string][]ast.Expr{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CompositeLit:
			if se, ok := x.Type.(*ast.SelectorExpr); ok && se.Sel.Name == "Set" {
				for _, el := range x.Elts {
					if kv, ok := el.(*ast.KeyValueExpr); ok {
						if k, ok := str(kv.Key, consts); ok {
							set[k] = append(set[k], kv.Value)
						}
					}
				}
			}
		case *ast.AssignStmt:
			for i, l := range x.Lhs {
				if i >= len(x.Rhs) {
					break
				}
				switch lx := l.(type) {
				case *ast.IndexExpr:
					if k, ok := str(lx.Index, consts); ok {
						set[k] = append(set[k], x.Rhs[i])
					}
				case *ast.Ident:
					locals[lx.Name] = append(locals[lx.Name], x.Rhs[i])
				}
			}
		case *ast.CallExpr:
			if se, ok := x.Fun.(*ast.SelectorExpr); ok && (se.Sel.Name == "ObjectMetaFieldsSet" || se.Sel.Name == "AddObjectMetaFieldsSet") {
				name = true
				if last, ok := x.Args[len(x.Args)-1].(*ast.Ident); ok && last.Name == "true" {
					ns = true
				}
			}
		}
		return true
	})
	return set, locals, name, ns
}

type valueCtx struct {
	param   string
	structs map[string]map[string]ast.Expr
	convs   []map[string][]mapping
	typ     string
	rt      reflect.Type
	locals  map[string][]ast.Expr
}

// readings walks a field expression of the strategy's parameter.
func (v valueCtx) readings(e ast.Expr) ([]reading, bool, error) {
	for {
		switch x := e.(type) {
		case *ast.ParenExpr:
			e = x.X
			continue
		case *ast.StarExpr:
			e = x.X
			continue
		case *ast.CallExpr:
			if len(x.Args) == 1 {
				e = x.Args[0]
				continue
			}
		}
		break
	}
	chain, indexed, ok := chainOf(e, v.param)
	if !ok {
		return nil, false, fmt.Errorf("not a field of %s", v.param)
	}
	rs, err := v.walkChain(chain)
	return rs, indexed, err
}

func (v valueCtx) walkChain(chain []string) ([]reading, error) {
	return walk(v.structs, v.convs, v.typ, chain, v.rt)
}

// values renders one assigned expression in the table's notation: every equally upstream rendering,
// or none for a constant (a branch that assigns a literal when the field is unset).
func (v valueCtx) values(e ast.Expr) ([]string, error) {
	switch x := e.(type) {
	case *ast.ParenExpr:
		return v.values(x.X)
	case *ast.StarExpr:
		return v.values(x.X)
	case *ast.BasicLit, *ast.CompositeLit:
		return nil, nil
	case *ast.Ident:
		if x.Name == "true" || x.Name == "false" {
			return nil, nil
		}
		// A local: the fields it is assigned, in order; several are a fallback (either:).
		var parts [][]string
		for _, r := range v.locals[x.Name] {
			s, err := v.values(r)
			if err != nil {
				return nil, err
			}
			if len(s) > 0 {
				parts = append(parts, s)
			}
		}
		switch len(parts) {
		case 0:
			return nil, fmt.Errorf("local %s has no field value", x.Name)
		case 1:
			return parts[0], nil
		}
		combos := []string{""}
		for _, p := range parts {
			var next []string
			for _, c := range combos {
				for _, alt := range p {
					if c == "" {
						next = append(next, alt)
					} else {
						next = append(next, c+","+alt)
					}
				}
			}
			combos = next
		}
		for i := range combos {
			combos[i] = "either:" + combos[i]
		}
		return combos, nil
	case *ast.CallExpr:
		fn := ""
		switch f := x.Fun.(type) {
		case *ast.Ident:
			fn = f.Name
		case *ast.SelectorExpr:
			fn = fmt.Sprint(f.X) + "." + f.Sel.Name
		}
		if len(x.Args) != 1 {
			return nil, fmt.Errorf("call %s with %d arguments", fn, len(x.Args))
		}
		inner, err := v.values(x.Args[0])
		if err != nil || len(inner) == 0 {
			return inner, err
		}
		prefix := ""
		switch fn {
		case "string", "int":
		case "strconv.FormatBool":
			prefix = "bool:"
		case "strconv.Itoa":
			prefix = "int:"
		case "fmt.Sprint":
			rs, _, err := v.readings(x.Args[0])
			if err == nil && len(rs) > 0 && rs[0].leaf.Kind() == reflect.Bool {
				prefix = "bool:"
			}
		default:
			return nil, fmt.Errorf("call %s is not a known rendering", fn)
		}
		out := make([]string, len(inner))
		for i, s := range inner {
			out[i] = prefix + s
		}
		return out, nil
	case *ast.SelectorExpr, *ast.IndexExpr:
		rs, indexed, err := v.readings(x)
		if err != nil {
			return nil, err
		}
		var out []string
		for _, r := range rs {
			p := strings.Join(r.path, ".")
			if indexed && !r.indexConsumed {
				p = "first:" + p
			}
			out = append(out, p)
		}
		return out, nil
	}
	return nil, fmt.Errorf("expression %T is not a field value", e)
}

// upstreamValues derives, for one table kind, the upstream readings of every label it accepts
// (several when v1 carries the field twice), and the alias target of every label that maps to
// another of the kind's labels. mapped is each label -> the label its conversion function returns.
func upstreamValues(src, group, kind string, mapped map[string]string) (values map[string][]string, aliases map[string]string, err error) {
	grp := ""
	for g, gr := range groupOf {
		if gr == group {
			grp = g
		}
	}
	if grp == "" {
		grp = strings.TrimSuffix(group, ".k8s.io")
	}
	convDir := filepath.Join(src, "pkg/apis", grp, "v1")
	conv, ipkgs := conversions(convDir)
	ipkg := ipkgs[kind]
	if ipkg == "" {
		return nil, nil, fmt.Errorf("no internal-to-v1 conversion for %s in %s", kind, convDir)
	}
	convs := []map[string][]mapping{conv}
	if ipkg != grp {
		c, _ := conversions(filepath.Join(src, "pkg/apis", ipkg, "v1"))
		convs = append(convs, c)
	}
	obj, err := scheme.Scheme.New(schema.GroupVersionKind{Group: group, Version: "v1", Kind: kind})
	if err != nil {
		return nil, nil, err
	}
	fn, err := fieldsFunc(src, ipkg, kind)
	if err != nil {
		return nil, nil, err
	}
	structs := structFields(parsedDir(filepath.Join(src, "pkg/apis", ipkg)))
	set, locals, hasName, hasNS := fieldSet(fn, constants(filepath.Join(src, "pkg/apis", ipkg)))
	v := valueCtx{param: fn.Type.Params.List[0].Names[0].Name, structs: structs, convs: convs, typ: kind, rt: reflect.TypeOf(obj).Elem(), locals: locals}
	values, aliases = map[string][]string{}, map[string]string{}
	for label, to := range mapped {
		if to != label && mapped[to] == to {
			aliases[label] = to
			continue
		}
		if to == "metadata.name" && hasName || to == "metadata.namespace" && hasNS {
			values[label] = []string{to}
			continue
		}
		// The branches that assign a field must agree (the others assign a constant when it is unset).
		var got []string
		for _, e := range set[to] {
			s, err := v.values(e)
			if err != nil {
				return nil, nil, fmt.Errorf("%s (%s): %w", label, to, err)
			}
			if len(s) == 0 {
				continue
			}
			sort.Strings(s)
			if got != nil && strings.Join(got, "|") != strings.Join(s, "|") {
				return nil, nil, fmt.Errorf("%s (%s): the field set assigns it %v in one branch and %v in another", label, to, got, s)
			}
			got = s
		}
		if got == nil {
			got = []string{"absent:"}
		}
		values[label] = got
	}
	return values, aliases, nil
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}
