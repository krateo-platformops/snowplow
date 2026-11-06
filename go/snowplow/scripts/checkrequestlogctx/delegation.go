package main

// delegation.go — L3 and L4, the rules one level above a log site (#1184 review).
//
// L1 and L2 read a site. A context can still be lost on the way to one: the site
// passes the context it was given, and a caller gave it context.Background(). The
// review of #1201 found exactly that: EnsureResourceType(gvr) delegates with
// Background, and a request reached it through a function value.
//
// So the checker builds the static call graph of the module's own functions and
// asks which of them can reach a WARN or ERROR that passes a context, following
// only calls to a function that can take one. Over that graph:
//
//   - L3: a literal context.Background() or context.TODO() handed to such a
//     function, from a place with a context in scope.
//   - L4: a call to a WRAPPER from a place with a context in scope. A wrapper is
//     a function with no context in scope that hands such a function a literal
//     Background, or that calls another wrapper.
//   - L4 also fires on a wrapper taken as a function value, wherever: the call
//     through it can land anywhere, so nothing at the reference can vouch for it.
//
// A named detached context (`bg := context.Background()`, or a derived
// context.WithTimeout) is not literal and is the author's statement. A deliberate
// cut says so with `// log-ctx-waiver: detached: <reason>`, the one class these
// rules accept and L2 does not. On the literal inside a function with no context
// it means no caller needs one, so the function is no wrapper.
//
// The graph sees static calls and function references. An interface call has no
// body to follow, so a wrapper reached only through an interface is not seen.

import (
	"fmt"
	"go/ast"
	"go/types"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/packages"
)

const detachedClass = "detached"

type fnNode struct {
	name    string
	logs    bool            // a WARN/ERROR site in the body passes a context
	carries bool            // the function can take a context
	callees map[string]bool // module functions it calls, and those it references
}

// edge is one reference to a module function.
type edge struct {
	file    string
	line    int
	caller  string // key of the enclosing function, "" at package level
	callee  string
	name    string // the callee as written
	ctx     string // a context in scope at the reference, "" if none
	literal bool   // handed a literal Background or TODO as a context argument
	isCall  bool
}

type graph struct {
	nodes map[string]*fnNode
	edges []edge
}

func newGraph() *graph { return &graph{nodes: map[string]*fnNode{}} }

func (g *graph) node(fn *types.Func) *fnNode {
	k := fnKey(fn)
	n := g.nodes[k]
	if n == nil {
		n = &fnNode{name: fn.Name(), carries: takesContext(fn), callees: map[string]bool{}}
		g.nodes[k] = n
	}
	return n
}

func fnKey(fn *types.Func) string { return fn.Origin().FullName() }

// takesContext reports a function that can be handed a context: as a context or
// *http.Request parameter, or in a receiver or parameter struct that holds one.
func takesContext(fn *types.Func) bool {
	sig, ok := fn.Type().(*types.Signature)
	if !ok {
		return false
	}
	carrier := func(v *types.Var) bool { return v != nil && contextOf(v) != "" }
	if carrier(sig.Recv()) {
		return true
	}
	for i := 0; i < sig.Params().Len(); i++ {
		if carrier(sig.Params().At(i)) {
			return true
		}
	}
	return false
}

// moduleFunc resolves id to a function declared in this module.
func moduleFunc(p *packages.Package, id *ast.Ident, module string) *types.Func {
	fn, ok := p.TypesInfo.Uses[id].(*types.Func)
	if !ok || fn.Pkg() == nil {
		return nil
	}
	if path := fn.Pkg().Path(); path != module && !strings.HasPrefix(path, module+"/") {
		return nil
	}
	return fn
}

// literalContextArg reports a call that hands fn a literal Background or TODO in
// a context parameter.
func literalContextArg(fn *types.Func, call *ast.CallExpr) bool {
	sig, ok := fn.Type().(*types.Signature)
	if !ok || call == nil {
		return false
	}
	for i, arg := range call.Args {
		if i < sig.Params().Len() && isContext(sig.Params().At(i).Type()) && isDetached(arg) {
			return true
		}
	}
	return false
}

// walkFile adds file f's functions and references to g.
func (g *graph) walkFile(p *packages.Package, f *ast.File, rel, module string) {
	for _, d := range f.Decls {
		fd, isFunc := d.(*ast.FuncDecl)
		caller := ""
		var node *fnNode
		if isFunc {
			if fn, ok := p.TypesInfo.Defs[fd.Name].(*types.Func); ok {
				node = g.node(fn)
				caller = fnKey(fn)
			}
		}
		var stack []ast.Node
		fun := map[ast.Node]*ast.CallExpr{}
		ast.Inspect(d, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			stack = append(stack, n)
			if c, ok := n.(*ast.CallExpr); ok {
				fun[ast.Unparen(c.Fun)] = c
				if s, ok := ast.Unparen(c.Fun).(*ast.SelectorExpr); ok {
					fun[s.Sel] = c
				}
			}
			if s, ok := n.(*ast.SelectorExpr); ok {
				if node != nil && slogFunc(p, s) != nil && passesContext(p, s, fun[s]) {
					node.logs = true
				}
			}
			id, ok := n.(*ast.Ident)
			if !ok {
				return true
			}
			fn := moduleFunc(p, id, module)
			if fn == nil {
				return true
			}
			call := fun[id]
			e := edge{file: rel, line: p.Fset.Position(id.Pos()).Line, caller: caller, callee: fnKey(fn), name: fn.Name(), isCall: call != nil}
			if isFunc {
				e.ctx = inScope(p, fd, stack, id.Pos())
			}
			e.literal = literalContextArg(fn, call)
			g.node(fn)
			if node != nil {
				node.callees[e.callee] = true
			}
			g.edges = append(g.edges, e)
			return true
		})
	}
}

// reachLogs is the functions from which a context can reach a logged record:
// those that log with one, and those that call one through a function that can
// take one.
func (g *graph) reachLogs() map[string]bool {
	reach := map[string]bool{}
	for k, n := range g.nodes {
		if n.logs {
			reach[k] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for k, n := range g.nodes {
			if reach[k] || !n.carries {
				continue
			}
			for c := range n.callees {
				if reach[c] {
					reach[k] = true
					changed = true
					break
				}
			}
		}
	}
	return reach
}

// wrappers is the functions that drop to a literal Background with no context
// of their own, directly or through another wrapper.
func (g *graph) wrappers(reach map[string]bool, waivers map[string][]*waiver) map[string]bool {
	w := map[string]bool{}
	for _, e := range g.edges {
		if !e.literal || e.ctx != "" || e.caller == "" || !reach[e.callee] {
			continue
		}
		// A waived cut is deliberate for every caller: no wrapper.
		if c := waiverFor(waivers[e.file], e.line); c != nil && c.class == detachedClass {
			c.used = true
			continue
		}
		w[e.caller] = true
	}
	for changed := true; changed; {
		changed = false
		for _, e := range g.edges {
			if e.isCall && e.ctx == "" && e.caller != "" && w[e.callee] && !w[e.caller] {
				w[e.caller] = true
				changed = true
			}
		}
	}
	return w
}

// delegationViolations applies L3 and L4; waivers is keyed by file.
func (g *graph) delegationViolations(waivers map[string][]*waiver) []violation {
	reach := g.reachLogs()
	wrap := g.wrappers(reach, waivers)
	var vs []violation
	seen := map[string]bool{}
	report := func(e edge, why string) {
		if w := waiverFor(waivers[e.file], e.line); w != nil && w.class == detachedClass {
			w.used = true
			return
		}
		k := fmt.Sprintf("%s:%d:%s", e.file, e.line, why)
		if !seen[k] {
			seen[k] = true
			vs = append(vs, violation{e.file, e.line, why})
		}
	}
	for _, e := range g.edges {
		switch {
		case e.literal && e.ctx != "" && reach[e.callee]:
			report(e, fmt.Sprintf("%s is handed a literal background context, with %s in scope, and it logs with what it gets: pass the context in scope", e.name, e.ctx))
		case wrap[e.callee] && !e.isCall:
			report(e, fmt.Sprintf("%s delegates context.Background() and is taken as a function value: use its context-taking form", e.name))
		case wrap[e.callee] && e.ctx != "":
			report(e, fmt.Sprintf("%s delegates context.Background(), which drops the context in scope (%s): use its context-taking form", e.name, e.ctx))
		}
	}
	return vs
}

func relTo(abs, name string) (string, bool) {
	rel, err := filepath.Rel(abs, name)
	return rel, err == nil && !skipped(rel)
}
