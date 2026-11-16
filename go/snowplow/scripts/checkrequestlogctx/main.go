// Command checkrequestlogctx is the #1184 CI guard: a WARN or ERROR record keeps
// the context of the work that emitted it.
//
// THE DEFECT CLASS (#1184, installer#392 G11). traceCorrelationHandler stamps
// trace_id/span_id from the record's context, and a plain log.Warn / log.Error
// reaches it with context.Background(). So a request's WARN carried no trace_id:
// 0 of 96,315 live rows on krateo-057, with the stamping itself working and
// tested. Nothing but the call's spelling decides it, so nothing but a guard
// keeps it fixed.
//
// THE RULE. A slog WARN or ERROR is a VIOLATION unless it passes a context:
//
//   - L1: a context is in scope at the site (a variable or parameter of type
//     context.Context, an *http.Request, or a receiver or parameter whose struct
//     has a context.Context field) and the call does not pass one. A blank or
//     unnamed context parameter counts: name it.
//   - L2: no context is in scope, and the site carries no waiver.
//   - L3 and L4 (delegation.go): a function that logs with the context it is given
//     must be given one. A literal Background handed to it with a context in scope
//     fails, and so does calling a wrapper that delegates one.
//
// The scope test has no idea whether a context is a request's. It does not need
// one: on background work the context has no span, the record is byte-identical
// to a plain call's, and the same function very often serves both (the resolver
// runs for a request and for the refresher). A site is never classified by what
// its context is, only by whether it has one.
//
// A call counts as passing no context when it is Warn / Error (package function
// or *slog.Logger method, called or taken as a method value), or when
// WarnContext / ErrorContext / Log / LogAttrs is handed a literal
// context.Background() or context.TODO(). Log and LogAttrs count only at a level
// that can be WARN or above (a constant below it is skipped; a computed level is
// not).
//
// THE WAIVER. A site with no context in scope is a decision, not a
// default, so it says which:
//
//	// log-ctx-waiver: informer-event: a watch error on the reflector goroutine
//	slog.Warn("cache.watch.error", ...)
//
// on the site's line or the line above. The class must be one of
// waiverClasses and the reason is mandatory. A waiver on a site with a context
// in scope, or on a line with no site at all, is itself a violation, so a waiver
// cannot outlive the code it excuses. L3 and L4 take only the class "detached",
// and L2 never does.
//
// Types are read with go/packages, so a logger behind any receiver is seen and
// http.Error or an error's Error() is not. Test files, testdata/ and the scripts,
// e2e and hack trees are skipped.
//
// USAGE:
//
//	checkrequestlogctx [-report] <module-dir>
//
// Exits 1 and prints file:line for each violation. -report prints the whole
// enumeration (one line per site) instead, for a PR body.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// waiverClasses are the reasons a WARN or ERROR has no context to pass.
var waiverClasses = map[string]string{
	"informer-event": "an informer, reflector or watch callback: a cluster event, no request exists",
	"worker":         "a background worker, ticker or debounce goroutine that owns no context",
	"boot":           "process start: runs before the pod is ready, so no request exists",
	"process-once":   "a process-wide lazy init: it reports the process's configuration, and the request that happens to run it first is not its cause",
	"no-prod-caller": "no production caller (a test seam or retained helper)",
	detachedClass:    "a deliberate cut of the context in scope (L3 and L4 only)",
}

const waiverMark = "log-ctx-waiver:"

var waiverRe = regexp.MustCompile(`^\s*//\s*log-ctx-waiver:\s*([a-z-]*)\s*:?\s*(.*)$`)

// skippedTrees are not production code, whatever they log.
var skippedTrees = []string{"scripts", "e2e", "hack"}

type site struct {
	file  string
	line  int
	fn    string // enclosing function
	call  string // the logger expression, as written
	ctx   string // a context in scope, "" if none
	state string // "context" | "plain" | "waived:<class>"
}

type waiver struct {
	line   int
	class  string
	reason string
	used   bool
}

type violation struct {
	file string
	line int
	why  string
}

func main() {
	report := flag.Bool("report", false, "print the whole enumeration instead of the verdict")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: checkrequestlogctx [-report] <module-dir>")
		os.Exit(2)
	}
	sites, vs, err := check(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
	if *report {
		for _, s := range sites {
			fmt.Printf("%s:%d\t%s\t%s\t%s\t%s\n", s.file, s.line, s.fn, s.call, s.state, s.ctx)
		}
		return
	}
	if len(vs) > 0 {
		sort.Slice(vs, func(i, j int) bool {
			if vs[i].file != vs[j].file {
				return vs[i].file < vs[j].file
			}
			return vs[i].line < vs[j].line
		})
		for _, v := range vs {
			fmt.Fprintf(os.Stderr, "ERROR: %s:%d  %s\n", v.file, v.line, v.why)
		}
		fmt.Fprintf(os.Stderr, "\nERROR: %d WARN/ERROR log site(s) drop their context (#1184).\n"+
			"       A record reaches trace correlation with the context the call passes. Pass the\n"+
			"       request's context (WarnContext / ErrorContext); thread it through the function if\n"+
			"       it has none. A site that really has none says why, on the line above it:\n"+
			"       // %s <class>: <reason>\n", len(vs), waiverMark)
		os.Exit(1)
	}
	context, waived := 0, 0
	for _, s := range sites {
		if s.state == "context" {
			context++
		} else {
			waived++
		}
	}
	fmt.Printf("OK: %d WARN/ERROR log sites pass a context; %d have none to pass and carry a classified waiver (#1184).\n", context, waived)
}

func check(root string) ([]site, []violation, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, nil, err
	}
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports |
			packages.NeedDeps | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedModule,
		Dir: abs,
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		return nil, nil, fmt.Errorf("load: %w", err)
	}
	if len(pkgs) == 0 {
		return nil, nil, fmt.Errorf("no packages under %s", abs)
	}
	var sites []site
	var vs []violation
	g := newGraph()
	waivers := map[string][]*waiver{}
	for _, p := range pkgs {
		if len(p.Errors) > 0 {
			return nil, nil, fmt.Errorf("%s: %v", p.PkgPath, p.Errors[0])
		}
		if p.Module == nil {
			return nil, nil, fmt.Errorf("%s: no module", p.PkgPath)
		}
		for i, f := range p.Syntax {
			rel, ok := relTo(abs, p.CompiledGoFiles[i])
			if !ok {
				continue
			}
			s, v, ws := checkFile(p, f, rel)
			sites = append(sites, s...)
			vs = append(vs, v...)
			waivers[rel] = ws
			g.walkFile(p, f, rel, p.Module.Path)
		}
	}
	vs = append(vs, g.delegationViolations(waivers)...)
	for rel, ws := range waivers {
		for _, w := range ws {
			if !w.used {
				vs = append(vs, violation{rel, w.line, "waiver on no WARN/ERROR site"})
			}
		}
	}
	return sites, vs, nil
}

func skipped(rel string) bool {
	if strings.HasSuffix(rel, "_test.go") {
		return true
	}
	first, _, _ := strings.Cut(filepath.ToSlash(rel), "/")
	for _, t := range skippedTrees {
		if first == t {
			return true
		}
	}
	return false
}

func checkFile(p *packages.Package, f *ast.File, rel string) ([]site, []violation, []*waiver) {
	var sites []site
	var vs []violation
	waivers := fileWaivers(p, f)
	for _, w := range waivers {
		if _, ok := waiverClasses[w.class]; !ok {
			vs = append(vs, violation{rel, w.line, fmt.Sprintf("waiver names an unknown class %q", w.class)})
		}
		if strings.TrimSpace(w.reason) == "" {
			vs = append(vs, violation{rel, w.line, "waiver has no reason"})
		}
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		var stack []ast.Node
		calls := map[*ast.SelectorExpr]*ast.CallExpr{}
		ast.Inspect(fd, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			stack = append(stack, n)
			if c, ok := n.(*ast.CallExpr); ok {
				if s, ok := c.Fun.(*ast.SelectorExpr); ok {
					calls[s] = c
				}
			}
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || !dropsContext(p, sel, calls[sel]) {
				if ok && passesContext(p, sel, calls[sel]) {
					pos := p.Fset.Position(sel.Pos())
					sites = append(sites, site{rel, pos.Line, funcName(fd), types.ExprString(sel), inScope(p, fd, stack, sel.Pos()), "context"})
				}
				return true
			}
			pos := p.Fset.Position(sel.Pos())
			ctx := inScope(p, fd, stack, sel.Pos())
			s := site{rel, pos.Line, funcName(fd), types.ExprString(sel), ctx, "plain"}
			w := waiverFor(waivers, pos.Line)
			switch {
			case ctx != "":
				vs = append(vs, violation{rel, pos.Line, fmt.Sprintf("%s drops the context in scope (%s): pass it", s.call, ctx)})
				if w != nil {
					w.used = true
					vs = append(vs, violation{rel, w.line, "waiver on a site with a context in scope"})
				}
			case w != nil && w.class == detachedClass:
				w.used = true
				vs = append(vs, violation{rel, w.line, "waiver class detached excuses only a cut context, not a site with none"})
			case w != nil:
				w.used = true
				s.state = "waived:" + w.class
			default:
				vs = append(vs, violation{rel, pos.Line, fmt.Sprintf("%s in %s has no context in scope and no waiver: thread the context, or classify it", s.call, s.fn)})
			}
			sites = append(sites, s)
			return true
		})
	}
	return sites, vs, waivers
}

func funcName(fd *ast.FuncDecl) string {
	if fd.Recv != nil && len(fd.Recv.List) == 1 {
		return types.ExprString(fd.Recv.List[0].Type) + "." + fd.Name.Name
	}
	return fd.Name.Name
}

func fileWaivers(p *packages.Package, f *ast.File) []*waiver {
	var out []*waiver
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if !strings.Contains(c.Text, waiverMark) {
				continue
			}
			m := waiverRe.FindStringSubmatch(c.Text)
			if m == nil {
				continue // prose that mentions the mark
			}
			out = append(out, &waiver{line: p.Fset.Position(c.Pos()).Line, class: m[1], reason: m[2]})
		}
	}
	return out
}

// waiverFor is the waiver on line, or the one that ends on the line above it.
func waiverFor(ws []*waiver, line int) *waiver {
	for _, w := range ws {
		if w.line == line || w.line == line-1 {
			return w
		}
	}
	return nil
}

func slogFunc(p *packages.Package, sel *ast.SelectorExpr) *types.Func {
	fn, ok := p.TypesInfo.Uses[sel.Sel].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != "log/slog" {
		return nil
	}
	return fn
}

// dropsContext reports a WARN/ERROR-capable call that hands the handler no
// usable context. call is nil for a method value.
func dropsContext(p *packages.Package, sel *ast.SelectorExpr, call *ast.CallExpr) bool {
	fn := slogFunc(p, sel)
	if fn == nil {
		return false
	}
	switch fn.Name() {
	case "Warn", "Error":
		return true
	case "WarnContext", "ErrorContext":
		return call != nil && len(call.Args) > 0 && isDetached(call.Args[0])
	case "Log", "LogAttrs":
		return call != nil && len(call.Args) > 1 && isDetached(call.Args[0]) && canReachWarn(p, call.Args[1])
	}
	return false
}

// passesContext is the enumeration's other half: a WARN/ERROR-capable call with
// a real context argument.
func passesContext(p *packages.Package, sel *ast.SelectorExpr, call *ast.CallExpr) bool {
	fn := slogFunc(p, sel)
	if fn == nil || call == nil {
		return false
	}
	switch fn.Name() {
	case "WarnContext", "ErrorContext":
		return true
	case "Log", "LogAttrs":
		return len(call.Args) > 1 && canReachWarn(p, call.Args[1])
	}
	return false
}

// isDetached reports a literal context.Background() or context.TODO(). A named
// variable is the author's statement of which context it is.
func isDetached(e ast.Expr) bool {
	c, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	s := types.ExprString(c.Fun)
	return s == "context.Background" || s == "context.TODO"
}

func canReachWarn(p *packages.Package, level ast.Expr) bool {
	tv, ok := p.TypesInfo.Types[level]
	if !ok || tv.Value == nil {
		return true // computed: it can
	}
	v, ok := constant.Int64Val(constant.ToInt(tv.Value))
	return !ok || v >= 4 // slog.LevelWarn
}

// inScope names a context reachable at pos, innermost first, or "".
func inScope(p *packages.Package, fd *ast.FuncDecl, stack []ast.Node, pos token.Pos) string {
	for s := p.Types.Scope().Innermost(pos); s != nil && s != p.Types.Scope(); s = s.Parent() {
		names := s.Names()
		sort.Strings(names)
		for _, n := range names {
			v, ok := s.Lookup(n).(*types.Var)
			if !ok || v.Pos() >= pos {
				continue
			}
			if c := contextOf(v); c != "" {
				return c
			}
		}
	}
	// Scopes hold no blank identifier, so a `_ context.Context` parameter is
	// read off the enclosing signatures.
	for i := len(stack) - 1; i >= 0; i-- {
		var ft *ast.FuncType
		switch n := stack[i].(type) {
		case *ast.FuncLit:
			ft = n.Type
		case *ast.FuncDecl:
			ft = n.Type
		}
		if ft == nil {
			continue
		}
		for _, fld := range ft.Params.List {
			if tv, ok := p.TypesInfo.Types[fld.Type]; ok && isContext(tv.Type) {
				return "the unnamed context parameter"
			}
		}
	}
	return ""
}

func contextOf(v *types.Var) string {
	t := v.Type()
	if isContext(t) {
		return v.Name()
	}
	ptr, isPtr := types.Unalias(t).(*types.Pointer)
	if isPtr {
		if isNamed(ptr.Elem(), "net/http", "Request") {
			return v.Name() + ".Context()"
		}
		t = ptr.Elem()
	}
	if _, ok := types.Unalias(t).(*types.Named); !ok {
		return ""
	}
	st, ok := t.Underlying().(*types.Struct)
	if !ok {
		return ""
	}
	for i := 0; i < st.NumFields(); i++ {
		if isContext(st.Field(i).Type()) {
			return v.Name() + "." + st.Field(i).Name()
		}
	}
	return ""
}

func isContext(t types.Type) bool { return isNamed(t, "context", "Context") }

func isNamed(t types.Type, pkg, name string) bool {
	n, ok := types.Unalias(t).(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == pkg && n.Obj().Name() == name
}
