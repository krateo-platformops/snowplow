// Command checkfieldlabels compares the sandbox fake apiserver's field-selector table
// (internal/sandbox/fakeapiserver/fieldlabels.json) with the field labels kube-apiserver's own
// source registers: every AddFieldLabelConversionFunc in k8s.io/kubernetes pkg/apis/*/v1/
// conversion.go, read with go/ast. Each upstream v1 kind must be in the table with exactly its
// labels, and the table may hold no kind upstream does not register.
//
//	go run ./scripts/checkfieldlabels <k8s.io/kubernetes source dir> internal/sandbox/fakeapiserver/fieldlabels.json
//
// check_js_wasm.sh downloads the source (go mod download k8s.io/kubernetes@<the table's version>).
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

var groupOf = map[string]string{"core": "", "batch": "batch", "certificates": "certificates.k8s.io", "events": "events.k8s.io", "resource": "resource.k8s.io", "apps": "apps"}

type table struct {
	Kinds []struct {
		Group   string            `json:"group"`
		Version string            `json:"version"`
		Kind    string            `json:"kind"`
		Labels  map[string]string `json:"labels"`
		Aliases map[string]string `json:"aliases"`
	} `json:"kinds"`
}

// constants is every string constant declared in the package directory dir (for labels named by
// a constant, such as resource.ResourceSliceSelectorNodeName).
func constants(dir string) map[string]string {
	out := map[string]string{}
	fset := token.NewFileSet()
	pkgs, _ := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, 0)
	for _, p := range pkgs {
		for _, f := range p.Files {
			for _, d := range f.Decls {
				g, ok := d.(*ast.GenDecl)
				if !ok || g.Tok != token.CONST {
					continue
				}
				for _, s := range g.Specs {
					vs := s.(*ast.ValueSpec)
					for i, n := range vs.Names {
						if i < len(vs.Values) {
							if bl, ok := vs.Values[i].(*ast.BasicLit); ok && bl.Kind == token.STRING {
								out[n.Name], _ = strconv.Unquote(bl.Value)
							}
						}
					}
				}
			}
		}
	}
	return out
}

func str(e ast.Expr, consts map[string]string) (string, bool) {
	switch x := e.(type) {
	case *ast.BasicLit:
		if x.Kind == token.STRING {
			s, err := strconv.Unquote(x.Value)
			return s, err == nil
		}
	case *ast.SelectorExpr:
		v, ok := consts[x.Sel.Name]
		return v, ok
	case *ast.Ident:
		v, ok := consts[x.Name]
		return v, ok
	}
	return "", false
}

// upstream is "group/version/Kind" -> its accepted labels, from one conversion.go.
func upstream(src string) (map[string][]string, error) {
	out := map[string][]string{}
	files, _ := filepath.Glob(filepath.Join(src, "pkg/apis/*/v1/conversion.go"))
	if len(files) == 0 {
		return nil, fmt.Errorf("no pkg/apis/*/v1/conversion.go under %s", src)
	}
	for _, file := range files {
		grp := filepath.Base(filepath.Dir(filepath.Dir(file)))
		group, known := groupOf[grp]
		if !known {
			group = grp + ".k8s.io"
		}
		consts := constants(filepath.Join(src, "pkg/apis", grp))
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			return nil, err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "AddFieldLabelConversionFunc" || len(call.Args) != 2 {
				return true
			}
			wk, ok := call.Args[0].(*ast.CallExpr)
			if !ok || len(wk.Args) != 1 {
				return true
			}
			kind, _ := str(wk.Args[0], consts)
			key := group + "/v1/" + kind
			var labels []string
			// The labels are the case clauses of the func's switch, or the keys of a map the func
			// looks the label up in (events.k8s.io's mapping).
			ast.Inspect(call.Args[1], func(m ast.Node) bool {
				if cc, ok := m.(*ast.CaseClause); ok {
					for _, e := range cc.List {
						if s, ok := str(e, consts); ok {
							labels = append(labels, s)
						}
					}
				}
				return true
			})
			if len(labels) == 0 {
				ast.Inspect(f, func(m ast.Node) bool {
					if cl, ok := m.(*ast.CompositeLit); ok {
						if mt, ok := cl.Type.(*ast.MapType); ok && fmt.Sprint(mt.Key) == "string" {
							for _, el := range cl.Elts {
								if kv, ok := el.(*ast.KeyValueExpr); ok {
									if s, ok := str(kv.Key, consts); ok {
										labels = append(labels, s)
									}
								}
							}
						}
					}
					return true
				})
			}
			out[key] = append(out[key], labels...)
			return true
		})
	}
	return out, nil
}

func set(xs []string) []string {
	m := map[string]bool{}
	for _, x := range xs {
		m[x] = true
	}
	var out []string
	for x := range m {
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: checkfieldlabels <k8s.io/kubernetes source dir> <fieldlabels.json>")
		os.Exit(2)
	}
	up, err := upstream(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	b, err := os.ReadFile(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var t table
	if err := json.Unmarshal(b, &t); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	bad := 0
	ours := map[string]bool{}
	for _, k := range t.Kinds {
		key := k.Group + "/" + k.Version + "/" + k.Kind
		ours[key] = true
		var mine []string
		for l := range k.Labels {
			mine = append(mine, l)
		}
		for l := range k.Aliases {
			mine = append(mine, l)
		}
		want, have := set(up[key]), set(mine)
		if len(want) == 0 {
			fmt.Printf("%s: in the table, but upstream registers no field labels for it\n", key)
			bad++
			continue
		}
		if strings.Join(want, ",") != strings.Join(have, ",") {
			fmt.Printf("%s: upstream %v, table %v\n", key, want, have)
			bad++
		}
	}
	for key := range up {
		if !ours[key] {
			fmt.Printf("%s: upstream registers field labels %v, the table has none\n", key, set(up[key]))
			bad++
		}
	}
	if bad > 0 {
		os.Exit(1)
	}
	fmt.Printf("checkfieldlabels: OK (%d kinds equal to upstream)\n", len(t.Kinds))
}
