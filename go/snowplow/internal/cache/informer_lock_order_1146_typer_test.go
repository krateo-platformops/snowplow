//go:build unit || integration

// informer_lock_order_1146_typer_test.go — the call resolution the #1146 guard
// walks with: a syntactic, package-local type inference, so a call reaches the
// declarations of its receiver's type rather than every method of that name.
//
// A receiver whose type it can name inside the package resolves to that type's
// methods; a receiver of a type from another package (time.Timer, sync.Once,
// client-go) resolves to nothing in this package; a receiver it cannot type —
// an interface, a struct whose method is promoted from an embedded field, an
// expression it does not model — resolves to EVERY method of that name. So an
// unknown is always an over-approximation, never a missed path. A bare call
// f() resolves to package functions only (never to a method, never to a
// builtin such as close).

package cache

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

type typer1146 struct {
	typeDecls     map[string]ast.Expr                   // package type name -> its type expression
	pkgVars       map[string]ast.Expr                   // package var -> its type
	funcs         map[string][]*ast.FuncDecl            // package functions, by name
	methods       map[string]map[string][]*ast.FuncDecl // receiver type -> method -> decls
	methodsByName map[string][]*ast.FuncDecl            // every method, by name
	imports       map[string]bool                       // names imported packages are referred to by
	scopes        map[ast.Node]map[string]ast.Expr      // function -> variable types
	pendingVars   []*ast.ValueSpec                      // package vars typed once every decl is known
}

// extType1146 marks a type declared in another package.
var extType1146 = &ast.Ident{Name: "<external>"}

func newTyper1146() *typer1146 {
	return &typer1146{
		typeDecls:     map[string]ast.Expr{},
		pkgVars:       map[string]ast.Expr{},
		funcs:         map[string][]*ast.FuncDecl{},
		methods:       map[string]map[string][]*ast.FuncDecl{},
		methodsByName: map[string][]*ast.FuncDecl{},
		imports:       map[string]bool{},
		scopes:        map[ast.Node]map[string]ast.Expr{},
	}
}

func (ty *typer1146) addFile(f *ast.File) {
	for _, imp := range f.Imports {
		if imp.Name != nil {
			ty.imports[imp.Name.Name] = true
			continue
		}
		p, _ := strconv.Unquote(imp.Path.Value)
		last := p[strings.LastIndex(p, "/")+1:]
		if i := strings.Index(last, "."); i > 0 { // gopkg.in/yaml.v3
			last = last[:i]
		}
		ty.imports[last] = true
	}
	for _, d := range f.Decls {
		switch x := d.(type) {
		case *ast.FuncDecl:
			if x.Body == nil {
				continue
			}
			if x.Recv == nil || len(x.Recv.List) == 0 {
				ty.funcs[x.Name.Name] = append(ty.funcs[x.Name.Name], x)
				continue
			}
			rt := recvTypeName1146(x.Recv.List[0].Type)
			if ty.methods[rt] == nil {
				ty.methods[rt] = map[string][]*ast.FuncDecl{}
			}
			ty.methods[rt][x.Name.Name] = append(ty.methods[rt][x.Name.Name], x)
			ty.methodsByName[x.Name.Name] = append(ty.methodsByName[x.Name.Name], x)
		case *ast.GenDecl:
			for _, sp := range x.Specs {
				switch s := sp.(type) {
				case *ast.TypeSpec:
					ty.typeDecls[s.Name.Name] = s.Type
				case *ast.ValueSpec:
					if x.Tok == token.VAR {
						ty.pendingVars = append(ty.pendingVars, s)
					}
				}
			}
		}
	}
}

func recvTypeName1146(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.StarExpr:
		return recvTypeName1146(x.X)
	case *ast.Ident:
		return x.Name
	case *ast.IndexExpr: // generic receiver
		return recvTypeName1146(x.X)
	case *ast.IndexListExpr:
		return recvTypeName1146(x.X)
	}
	return ""
}

func (ty *typer1146) resolveVars() {
	for _, s := range ty.pendingVars {
		for i, n := range s.Names {
			switch {
			case s.Type != nil:
				ty.pkgVars[n.Name] = s.Type
			case i < len(s.Values):
				if t := ty.typeOf(s.Values[i], nil); t != nil {
					ty.pkgVars[n.Name] = t
				}
			}
		}
	}
	ty.pendingVars = nil
}

// scopeOf builds (once) the variable types of fn: receiver, parameters, and
// every variable its body defines (func literals' included). The first typed
// binding of a name wins — shadowing is not modelled, which can only make an
// unknown of a known.
func (ty *typer1146) scopeOf(fn ast.Node) map[string]ast.Expr {
	if ty.pendingVars != nil {
		ty.resolveVars()
	}
	if sc, ok := ty.scopes[fn]; ok {
		return sc
	}
	sc := map[string]ast.Expr{}
	ty.scopes[fn] = sc
	bind := func(name string, t ast.Expr) {
		if name == "_" || t == nil {
			return
		}
		if _, ok := sc[name]; !ok {
			sc[name] = t
		}
	}
	fields := func(fl *ast.FieldList) {
		if fl == nil {
			return
		}
		for _, f := range fl.List {
			for _, n := range f.Names {
				bind(n.Name, f.Type)
			}
		}
	}
	var body *ast.BlockStmt
	switch x := fn.(type) {
	case *ast.FuncDecl:
		fields(x.Recv)
		fields(x.Type.Params)
		body = x.Body
	case *ast.FuncLit:
		fields(x.Type.Params)
		body = x.Body
	}
	if body == nil {
		return sc
	}
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncLit:
			fields(x.Type.Params)
		case *ast.AssignStmt:
			if x.Tok != token.DEFINE {
				return true
			}
			if len(x.Lhs) == len(x.Rhs) {
				for i, l := range x.Lhs {
					if id, ok := l.(*ast.Ident); ok {
						bind(id.Name, ty.typeOf(x.Rhs[i], sc))
					}
				}
			} else if len(x.Rhs) == 1 && len(x.Lhs) > 0 {
				// v, ok := m[k] / x.(T) / f()
				if id, ok := x.Lhs[0].(*ast.Ident); ok {
					bind(id.Name, ty.typeOf(x.Rhs[0], sc))
				}
			}
		case *ast.DeclStmt:
			if gd, ok := x.Decl.(*ast.GenDecl); ok && gd.Tok == token.VAR {
				for _, sp := range gd.Specs {
					vs := sp.(*ast.ValueSpec)
					for i, nm := range vs.Names {
						if vs.Type != nil {
							bind(nm.Name, vs.Type)
						} else if i < len(vs.Values) {
							bind(nm.Name, ty.typeOf(vs.Values[i], sc))
						}
					}
				}
			}
		case *ast.RangeStmt:
			if x.Tok != token.DEFINE {
				return true
			}
			ct := ty.underlying(ty.typeOf(x.X, sc))
			var kt, vt ast.Expr
			switch c := ct.(type) {
			case *ast.MapType:
				kt, vt = c.Key, c.Value
			case *ast.ArrayType:
				vt = c.Elt
			}
			if id, ok := x.Key.(*ast.Ident); ok {
				bind(id.Name, kt)
			}
			if id, ok := x.Value.(*ast.Ident); ok {
				bind(id.Name, vt)
			}
		}
		return true
	})
	return sc
}

// underlying strips pointers and resolves a package type name to its
// declaration.
func (ty *typer1146) underlying(t ast.Expr) ast.Expr {
	for i := 0; i < 8 && t != nil; i++ {
		switch x := t.(type) {
		case *ast.StarExpr:
			t = x.X
		case *ast.ParenExpr:
			t = x.X
		case *ast.Ident:
			d, ok := ty.typeDecls[x.Name]
			if !ok {
				return t
			}
			t = d
		default:
			return t
		}
	}
	return t
}

// named is the package type a type expression names ("" when it names none),
// or ext when it names a type of another package.
func (ty *typer1146) named(t ast.Expr) (name string, ext bool) {
	for t != nil {
		switch x := t.(type) {
		case *ast.StarExpr:
			t = x.X
		case *ast.ParenExpr:
			t = x.X
		case *ast.Ident:
			if x == extType1146 {
				return "", true
			}
			if _, ok := ty.typeDecls[x.Name]; ok {
				return x.Name, false
			}
			return "", true // a predeclared type
		case *ast.SelectorExpr:
			return "", true // pkg.T
		case *ast.IndexExpr: // a generic instantiation
			t = x.X
		default:
			return "", false
		}
	}
	return "", false
}

// typeOf is the type of expression e, or nil when it cannot tell.
func (ty *typer1146) typeOf(e ast.Expr, sc map[string]ast.Expr) ast.Expr {
	switch x := e.(type) {
	case *ast.ParenExpr:
		return ty.typeOf(x.X, sc)
	case *ast.Ident:
		if t, ok := sc[x.Name]; ok {
			return t
		}
		if t, ok := ty.pkgVars[x.Name]; ok {
			return t
		}
		return nil
	case *ast.CompositeLit:
		return x.Type
	case *ast.UnaryExpr:
		if x.Op == token.AND {
			return ty.typeOf(x.X, sc)
		}
		return nil
	case *ast.StarExpr:
		return ty.typeOf(x.X, sc)
	case *ast.TypeAssertExpr:
		return x.Type
	case *ast.SelectorExpr:
		if id, ok := x.X.(*ast.Ident); ok {
			if _, local := sc[id.Name]; !local {
				if _, v := ty.pkgVars[id.Name]; !v && ty.imports[id.Name] {
					return extType1146 // pkg.Name
				}
			}
		}
		base := ty.typeOf(x.X, sc)
		if _, ext := ty.named(base); ext {
			return extType1146
		}
		if st, ok := ty.underlying(base).(*ast.StructType); ok {
			for _, f := range st.Fields.List {
				for _, n := range f.Names {
					if n.Name == x.Sel.Name {
						return f.Type
					}
				}
			}
		}
		if m := ty.methodFor(base, x.Sel.Name); len(m) == 1 {
			return m[0].Type // a method value: its func type
		}
		return nil
	case *ast.IndexExpr:
		switch c := ty.underlying(ty.typeOf(x.X, sc)).(type) {
		case *ast.MapType:
			return c.Value
		case *ast.ArrayType:
			return c.Elt
		}
		return nil
	case *ast.CallExpr:
		if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "new" && len(x.Args) == 1 {
			return x.Args[0]
		}
		var decls []*ast.FuncDecl
		switch fn := x.Fun.(type) {
		case *ast.Ident:
			decls = ty.funcs[fn.Name]
		case *ast.SelectorExpr:
			if t := ty.typeOf(fn.X, sc); t != nil {
				decls = ty.methodFor(t, fn.Sel.Name)
			}
		}
		if len(decls) == 1 && decls[0].Type.Results != nil && len(decls[0].Type.Results.List) > 0 {
			return decls[0].Type.Results.List[0].Type
		}
		return nil
	}
	return nil
}

// methodFor is the methods named m of the package type t names (nil when t
// names none, or names one without such a method).
func (ty *typer1146) methodFor(t ast.Expr, m string) []*ast.FuncDecl {
	name, ext := ty.named(t)
	if ext || name == "" {
		return nil
	}
	return ty.methods[name][m]
}

// targets is every package declaration a call can run.
func (ty *typer1146) targets(call *ast.CallExpr, sc map[string]ast.Expr) []*ast.FuncDecl {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		if _, local := sc[fn.Name]; local {
			return nil // a func-typed variable: a value, walked where it is written
		}
		return ty.funcs[fn.Name]
	case *ast.SelectorExpr:
		if id, ok := fn.X.(*ast.Ident); ok {
			if _, local := sc[id.Name]; !local {
				if _, v := ty.pkgVars[id.Name]; !v && ty.imports[id.Name] {
					return nil // pkg.Func: another package
				}
			}
		}
		t := ty.typeOf(fn.X, sc)
		if t == nil {
			return ty.methodsByName[fn.Sel.Name]
		}
		name, ext := ty.named(t)
		if ext {
			return nil
		}
		if name == "" {
			return ty.methodsByName[fn.Sel.Name]
		}
		if m := ty.methods[name][fn.Sel.Name]; len(m) > 0 {
			return m
		}
		// An interface, or a method promoted from an embedded field.
		return ty.methodsByName[fn.Sel.Name]
	}
	return nil
}
