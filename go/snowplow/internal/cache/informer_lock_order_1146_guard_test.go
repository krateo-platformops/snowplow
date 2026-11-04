//go:build unit || integration

// informer_lock_order_1146_guard_test.go — #1146 structural guards.
//
// THE CYCLE (#1146, the #875 class). A listener of an informer — the dep
// handler (syncChState) — takes rw.mu.RLock. client-go's sharedProcessor.run,
// once the informer's stop channel closes, holds listenersLock and waits for
// every listener to return (shared_informer.go:1143-1159, v0.36.5).
// RemoveEventHandler takes startedLock, then blockDeltas, then listenersLock
// (:1004-1014), so with rw.mu held it waits on a listener that waits on rw.mu.
// And while it waits it HOLDS startedLock, which LastSyncResourceVersion,
// AddEventHandler, SetTransform, SetWatchErrorHandler, AddIndexers, HasStarted
// and IsStopped all take (:819, :877, :703, :691, :837, :785, :998): any of
// them under rw.mu, on that informer, closes the same cycle one hop longer.
// OnAdd / OnUpdate / OnDelete on the informer distribute under
// listenersLock.RLock (:1094), which queues behind the stopping processor.
//
// Not listed, because they take no informer lock: HasSynced (a channel read
// since v0.36.5, :791), GetIndexer / GetStore (field reads), and the store's
// own reads — LastStoreSyncResourceVersion, List, GetByKey — whose lock is held
// only for a map operation and never across a handler.
//
// GUARD 1 — no listed call is reachable with rw.mu held, except the pre-Run
// wiring (preRunWiring1146), whose callers are pinned.
//
// GUARD 2 — a critical section that can retire a delivery log
// (retireDeliveryLogLocked) releases rw.mu through unlockAndDetach, which
// removes the retired logs' handlers after the unlock. A plain Unlock there
// would leave the handler attached until some other section drains the queue.
//
// HOW REACHABILITY IS COMPUTED. Every non-test file of the package is parsed.
// A function is walked statement by statement with a "held" state: rw.mu.Lock
// / RLock sets it, rw.mu.Unlock / RUnlock / rw.unlockAndDetach clears it, a
// branch that changes it does not leak the change past the branch (a branch
// that unlocks returns), and a function named *Locked starts held (the
// package's "callers hold rw.mu" convention). A call made while held reaches,
// by NAME, every function and method of that name (an over-approximation: a
// false positive is visible and fixable, a missed path is not), every func
// literal stored in a struct field of that name (perGVRFields' del / install),
// and a method value of a listed call stored in a field makes a call of that
// field a listed call. A func literal runs where it is written only when it is
// passed as an argument (sort.Slice, Range) or called on the spot; one stored
// in a composite literal, assigned, returned or started with `go` is a value,
// and is walked from an unheld state. Calls under a `go` statement are never
// held.

package cache

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// informerLockCalls1146 are the informer methods that take the informer's own
// locks (client-go v0.36.5 tools/cache/shared_informer.go).
var informerLockCalls1146 = map[string]bool{
	"RemoveEventHandler":              true,
	"AddEventHandler":                 true,
	"AddEventHandlerWithResyncPeriod": true,
	"AddEventHandlerWithOptions":      true,
	"LastSyncResourceVersion":         true,
	"SetTransform":                    true,
	"SetWatchErrorHandler":            true,
	"SetWatchErrorHandlerWithContext": true,
	"AddIndexers":                     true,
	"HasStarted":                      true,
	"IsStopped":                       true,
	"Run":                             true,
	"RunWithContext":                  true,
	"OnAdd":                           true,
	"OnUpdate":                        true,
	"OnDelete":                        true,
}

// preRunWiring1146 — the listed calls that DO run under rw.mu, and why that
// cannot close the cycle: each wires an informer BEFORE this watcher runs it,
// in the critical section that registers it. An informer built in that section
// (the streaming, standalone dynamic and standalone metadata branches, and
// every build candidate) is reachable from no other goroutine: it has no
// listener, no processor and no handler removal in flight, so its locks are
// free. A shared-factory informer (factory.ForResource) is registered only for
// a GVR R6 never tears down on its own (removable GVRs take a standalone
// informer, watcher.go addResourceTypeLocked), and after this PR the only
// holder of an informer lock that waits on a listener is a handler removal
// (deliveryLog.detach), which runs on a torn-down GVR's informer with no lock
// held.
//
// Key: the function that makes the call → the call. Value: the only functions
// allowed to call that function (pinned: a new caller is a new path to review).
var preRunWiring1146 = map[string]struct {
	calls   []string
	callers []string
}{
	"installDeliveryLogLocked": {
		calls:   []string{"SetTransform", "AddEventHandler"},
		callers: []string{"addResourceTypeLocked", "addResourceTypeMetadataOnlyLocked", "constructCandidateLocked"},
	},
	"wireInstanceLocked": {
		calls:   []string{"AddEventHandler"},
		callers: []string{"addResourceTypeLocked", "addResourceTypeMetadataOnlyLocked", "constructCandidateLocked"},
	},
	"attachMatchingHandlerExtensions": { // its attacher's AddEventHandler, on the instance being wired
		calls:   []string{"AddEventHandler"},
		callers: []string{"wireInstanceLocked"},
	},
	"setWatchErrorHandlerLocked": {
		calls:   []string{"SetWatchErrorHandler"},
		callers: []string{"installWatchErrorHandler", "installWatchErrorHandlerFor"},
	},
	"installWatchErrorHandlerFor": {callers: []string{"wireInstanceLocked"}},
	"installWatchErrorHandler":    {callers: nil},
}

const retireCall1146 = "retireDeliveryLogLocked"

type lockGuard1146 struct {
	fset *token.FileSet
	// byName: every function and method declared in the package, by name.
	byName map[string][]*ast.FuncDecl
	// fieldLits: func literals stored in a struct field, by field name.
	fieldLits map[string][]*ast.FuncLit
	// listed: the informer calls, plus the fields holding one as a method value.
	listed map[string]bool
	// retires: the *Locked functions and func fields whose call can retire a
	// delivery log (fixpoint from retireDeliveryLogLocked).
	retires map[string]bool
	// seenHeld: functions already walked from a held state.
	seenHeld map[ast.Node]bool
	// callersOf: name → names of the functions whose bodies call it.
	callersOf map[string]map[string]bool
	// ty resolves a call to the declarations it can reach.
	ty *typer1146
	// scope: the variable types of the function being walked.
	scope map[string]ast.Expr

	violations   []string
	unlockFaults []string

	// The root walk in progress: its name, and whether it deferred a plain
	// rw.mu.Unlock.
	root          string
	deferredPlain bool
}

// walkState is the state threaded through one function's statements.
type walkState struct {
	held    bool // rw.mu is held
	retired bool // a delivery log was retired since the lock was taken
}

func newLockGuard1146(t *testing.T, dir string) *lockGuard1146 {
	t.Helper()
	g := &lockGuard1146{
		fset:      token.NewFileSet(),
		byName:    map[string][]*ast.FuncDecl{},
		fieldLits: map[string][]*ast.FuncLit{},
		listed:    map[string]bool{},
		retires:   map[string]bool{retireCall1146: true},
		seenHeld:  map[ast.Node]bool{},
		callersOf: map[string]map[string]bool{},
		ty:        newTyper1146(),
	}
	for k := range informerLockCalls1146 {
		g.listed[k] = true
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(g.fset, filepath.Join(dir, n), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
		g.ty.addFile(f)
	}
	if len(files) == 0 {
		t.Fatal("PRECONDITION: no package source parsed")
	}
	structs := map[string][]string{} // struct type -> field names in order
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			if ts, ok := n.(*ast.TypeSpec); ok {
				if st, ok := ts.Type.(*ast.StructType); ok {
					var names []string
					for _, fl := range st.Fields.List {
						if len(fl.Names) == 0 {
							names = append(names, "")
						}
						for _, nm := range fl.Names {
							names = append(names, nm.Name)
						}
					}
					structs[ts.Name.Name] = names
				}
			}
			return true
		})
	}
	for _, f := range files {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				g.byName[fd.Name.Name] = append(g.byName[fd.Name.Name], fd)
				for _, c := range callNames1146(fd.Body) {
					if g.callersOf[c] == nil {
						g.callersOf[c] = map[string]bool{}
					}
					g.callersOf[c][fd.Name.Name] = true
				}
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CompositeLit:
				typeName := compositeTypeName1146(x.Type)
				for i, el := range x.Elts {
					if kv, ok := el.(*ast.KeyValueExpr); ok {
						if k, ok := kv.Key.(*ast.Ident); ok {
							g.noteField(k.Name, kv.Value)
						}
						continue
					}
					if fields := structs[typeName]; i < len(fields) {
						g.noteField(fields[i], el)
					}
				}
				// A slice/array/map of structs written positionally:
				// []perGVRField{{"x", f1, f2, f3}, ...}.
				if elem := elemTypeName1146(x.Type); elem != "" {
					for _, el := range x.Elts {
						inner, ok := el.(*ast.CompositeLit)
						if !ok || inner.Type != nil {
							continue
						}
						for i, ie := range inner.Elts {
							if _, kv := ie.(*ast.KeyValueExpr); kv {
								continue
							}
							if fields := structs[elem]; i < len(fields) {
								g.noteField(fields[i], ie)
							}
						}
					}
				}
			case *ast.AssignStmt:
				for i, lhs := range x.Lhs {
					if i >= len(x.Rhs) {
						break
					}
					if sel, ok := lhs.(*ast.SelectorExpr); ok {
						g.noteField(sel.Sel.Name, x.Rhs[i])
					}
				}
			}
			return true
		})
	}
	// The retiring set: a *Locked function or a func field whose body calls a
	// member. Fixpoint.
	for changed := true; changed; {
		changed = false
		for name, fds := range g.byName {
			if g.retires[name] || !strings.HasSuffix(name, "Locked") {
				continue
			}
			for _, fd := range fds {
				if g.anyIn(callNames1146(fd.Body)) {
					g.retires[name], changed = true, true
				}
			}
		}
		for field, lits := range g.fieldLits {
			if g.retires[field] {
				continue
			}
			for _, lit := range lits {
				if g.anyIn(callNames1146(lit.Body)) {
					g.retires[field], changed = true, true
				}
			}
		}
	}
	return g
}

func (g *lockGuard1146) anyIn(names []string) bool {
	for _, n := range names {
		if g.retires[n] {
			return true
		}
	}
	return false
}

// callNames1146 lists the names called anywhere in n.
func callNames1146(n ast.Node) []string {
	var out []string
	ast.Inspect(n, func(x ast.Node) bool {
		if call, ok := x.(*ast.CallExpr); ok {
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				out = append(out, fn.Name)
			case *ast.SelectorExpr:
				out = append(out, fn.Sel.Name)
			}
		}
		return true
	})
	return out
}

func compositeTypeName1146(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return x.Sel.Name
	}
	return ""
}

func elemTypeName1146(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.ArrayType:
		return compositeTypeName1146(x.Elt)
	case *ast.MapType:
		return compositeTypeName1146(x.Value)
	}
	return ""
}

// noteField records what a struct field is set to: a func literal (a call of
// the field runs it) or a method value of a listed call (a call of the field
// IS that call).
func (g *lockGuard1146) noteField(field string, v ast.Expr) {
	switch x := v.(type) {
	case *ast.FuncLit:
		g.fieldLits[field] = append(g.fieldLits[field], x)
	case *ast.SelectorExpr:
		if informerLockCalls1146[x.Sel.Name] {
			g.listed[field] = true
		}
	}
}

// rwMuOp1146 classifies a call that takes or releases rw.mu: "lock",
// "unlock" (a plain rw.mu.Unlock / RUnlock) or "detach" (rw.unlockAndDetach).
func rwMuOp1146(call *ast.CallExpr) string {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	if id, ok := sel.X.(*ast.Ident); ok && id.Name == "rw" && sel.Sel.Name == "unlockAndDetach" {
		return "detach"
	}
	mu, ok := sel.X.(*ast.SelectorExpr)
	if !ok || mu.Sel.Name != "mu" {
		return ""
	}
	if id, ok := mu.X.(*ast.Ident); !ok || id.Name != "rw" {
		return ""
	}
	switch sel.Sel.Name {
	case "Lock", "RLock":
		return "lock"
	case "Unlock", "RUnlock":
		return "unlock"
	}
	return ""
}

func (g *lockGuard1146) pos(n ast.Node) string {
	p := g.fset.Position(n.Pos())
	return fmt.Sprintf("%s:%d", filepath.Base(p.Filename), p.Line)
}

// enter walks a callee's body from a held state, once. A callee's own unlock
// discipline is its root walk's business, not its caller's.
func (g *lockGuard1146) enter(fn ast.Node, body *ast.BlockStmt, from string) {
	if g.seenHeld[fn] {
		return
	}
	g.seenHeld[fn] = true
	root, deferred, scope := g.root, g.deferredPlain, g.scope
	g.root, g.scope = "", g.ty.scopeOf(fn)
	g.block(body.List, walkState{held: true}, from)
	g.root, g.deferredPlain, g.scope = root, deferred, scope
}

// block walks a statement list; it returns the state at its end.
func (g *lockGuard1146) block(stmts []ast.Stmt, st walkState, ctx string) walkState {
	for _, s := range stmts {
		st = g.stmt(s, st, ctx)
	}
	return st
}

// branch walks a branch: held does not leak out of it, a retire does.
func (g *lockGuard1146) branch(stmts []ast.Stmt, st walkState, ctx string) walkState {
	end := g.block(stmts, st, ctx)
	st.retired = st.retired || (end.held && end.retired)
	return st
}

func (g *lockGuard1146) stmt(s ast.Stmt, st walkState, ctx string) walkState {
	switch x := s.(type) {
	case *ast.ExprStmt:
		if call, ok := x.X.(*ast.CallExpr); ok {
			switch rwMuOp1146(call) {
			case "lock":
				return walkState{held: true}
			case "unlock":
				if st.retired && g.root != "" {
					g.unlockFaults = append(g.unlockFaults, fmt.Sprintf(
						"%s: %s releases rw.mu with a plain Unlock after retiring a delivery log", g.pos(call), g.root))
				}
				return walkState{}
			case "detach":
				return walkState{}
			}
		}
		return g.expr(x.X, st, ctx)
	case *ast.DeferStmt:
		switch rwMuOp1146(x.Call) {
		case "unlock":
			g.deferredPlain = true
			return st
		case "detach", "lock":
			return st
		}
		return g.expr(x.Call, st, ctx)
	case *ast.GoStmt:
		g.expr(x.Call, walkState{}, ctx)
	case *ast.BlockStmt:
		return g.block(x.List, st, ctx)
	case *ast.IfStmt:
		if x.Init != nil {
			st = g.stmt(x.Init, st, ctx)
		}
		st = g.expr(x.Cond, st, ctx)
		st = g.branch(x.Body.List, st, ctx)
		if x.Else != nil {
			st = g.branch([]ast.Stmt{x.Else}, st, ctx)
		}
	case *ast.ForStmt:
		if x.Init != nil {
			st = g.stmt(x.Init, st, ctx)
		}
		if x.Cond != nil {
			st = g.expr(x.Cond, st, ctx)
		}
		if x.Post != nil {
			st = g.branch([]ast.Stmt{x.Post}, st, ctx)
		}
		st = g.branch(x.Body.List, st, ctx)
	case *ast.RangeStmt:
		st = g.expr(x.X, st, ctx)
		st = g.branch(x.Body.List, st, ctx)
	case *ast.SwitchStmt:
		if x.Init != nil {
			st = g.stmt(x.Init, st, ctx)
		}
		if x.Tag != nil {
			st = g.expr(x.Tag, st, ctx)
		}
		for _, c := range x.Body.List {
			cc := c.(*ast.CaseClause)
			for _, e := range cc.List {
				st = g.expr(e, st, ctx)
			}
			st = g.branch(cc.Body, st, ctx)
		}
	case *ast.TypeSwitchStmt:
		if x.Init != nil {
			st = g.stmt(x.Init, st, ctx)
		}
		st = g.stmt(x.Assign, st, ctx)
		for _, c := range x.Body.List {
			st = g.branch(c.(*ast.CaseClause).Body, st, ctx)
		}
	case *ast.SelectStmt:
		for _, c := range x.Body.List {
			cc := c.(*ast.CommClause)
			body := cc.Body
			if cc.Comm != nil {
				body = append([]ast.Stmt{cc.Comm}, body...)
			}
			st = g.branch(body, st, ctx)
		}
	case *ast.LabeledStmt:
		return g.stmt(x.Stmt, st, ctx)
	default:
		// Assign, Return, Send, IncDec, Decl, ...: walk their expressions.
		ast.Inspect(s, func(n ast.Node) bool {
			if e, ok := n.(ast.Expr); ok {
				st = g.expr(e, st, ctx)
				return false
			}
			return true
		})
	}
	return st
}

// expr walks an expression: its calls, and its func literals (see the file
// comment for when one runs where it is written).
func (g *lockGuard1146) expr(e ast.Expr, st walkState, ctx string) walkState {
	if e == nil {
		return st
	}
	ast.Inspect(e, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncLit:
			// A value: walked from an unheld state, as its own section.
			g.literalAsValue(x, ctx)
			return false
		case *ast.CallExpr:
			st = g.call(x, st, ctx)
			if lit, ok := x.Fun.(*ast.FuncLit); ok {
				st = g.branch(lit.Body.List, st, ctx)
			} else {
				st = g.expr(x.Fun, st, ctx)
			}
			for _, a := range x.Args {
				if lit, ok := a.(*ast.FuncLit); ok {
					st = g.branch(lit.Body.List, st, ctx)
					continue
				}
				st = g.expr(a, st, ctx)
			}
			return false
		}
		return true
	})
	return st
}

// literalAsValue walks a func literal that runs later (a handler, a closure
// returned or stored): its own rw.mu sections, its own unlock discipline.
func (g *lockGuard1146) literalAsValue(lit *ast.FuncLit, ctx string) {
	root, deferred := g.root, g.deferredPlain
	lctx := ctx + " → func literal@" + g.pos(lit)
	g.root, g.deferredPlain = lctx, false
	end := g.block(lit.Body.List, walkState{}, lctx)
	g.closeRoot(end, lit)
	g.root, g.deferredPlain = root, deferred
}

func (g *lockGuard1146) closeRoot(end walkState, at ast.Node) {
	if g.root != "" && end.retired && g.deferredPlain {
		g.unlockFaults = append(g.unlockFaults, fmt.Sprintf(
			"%s: %s defers a plain rw.mu.Unlock after retiring a delivery log", g.pos(at), g.root))
	}
}

func (g *lockGuard1146) call(call *ast.CallExpr, st walkState, ctx string) walkState {
	if !st.held {
		return st
	}
	var name string
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		name = fn.Name
	case *ast.SelectorExpr:
		name = fn.Sel.Name
	default:
		return st
	}
	if g.retires[name] {
		st.retired = true
	}
	here := g.pos(call)
	if g.listed[name] {
		if !g.allowedPreRun(ctx, name) {
			g.violations = append(g.violations, fmt.Sprintf("%s: %s() with rw.mu held (reached via %s)", here, name, ctx))
		}
		return st
	}
	next := ctx + " → " + name + "@" + here
	for _, fd := range g.ty.targets(call, g.scope) {
		g.enter(fd, fd.Body, next)
	}
	if _, isSel := call.Fun.(*ast.SelectorExpr); isSel {
		for _, lit := range g.fieldLits[name] {
			g.enter(lit, lit.Body, next)
		}
	}
	return st
}

// allowedPreRun: ctx's innermost function is a pre-Run wiring function that
// makes this call.
func (g *lockGuard1146) allowedPreRun(ctx, call string) bool {
	fn := ctx
	if i := strings.LastIndex(ctx, " → "); i >= 0 {
		fn = ctx[i+len(" → "):]
	}
	if i := strings.Index(fn, "@"); i >= 0 {
		fn = fn[:i]
	}
	fn = strings.TrimSuffix(fn, " (named *Locked)")
	for _, c := range preRunWiring1146[fn].calls {
		if c == call {
			return true
		}
	}
	return false
}

// run walks every function: from an unheld state (its own rw.mu regions), and
// from a held one when its name says its callers hold rw.mu.
func (g *lockGuard1146) run() {
	var names []string
	for n := range g.byName {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		for _, fd := range g.byName[n] {
			g.root, g.deferredPlain, g.scope = n, false, g.ty.scopeOf(fd)
			end := g.block(fd.Body.List, walkState{}, n)
			g.closeRoot(end, fd)
			g.root = ""
		}
	}
	for _, n := range names {
		if strings.HasSuffix(n, "Locked") {
			for _, fd := range g.byName[n] {
				g.enter(fd, fd.Body, n+" (named *Locked)")
			}
		}
	}
	sort.Strings(g.violations)
	sort.Strings(g.unlockFaults)
}

// TestIssue1146_NoInformerLockCallUnderRWMu — GUARD 1.
func TestIssue1146_NoInformerLockCallUnderRWMu(t *testing.T) {
	g := newLockGuard1146(t, ".")
	g.run()
	for _, v := range g.violations {
		t.Errorf("#1146 RED: %s — an informer lock taken under rw.mu can wait on a listener that waits on rw.mu", v)
	}
	// The pre-Run wiring stays reachable only from the registrations that
	// build the informer it wires.
	for fn, w := range preRunWiring1146 {
		if len(g.byName[fn]) == 0 {
			t.Errorf("#1146 guard stale: pre-Run wiring function %s no longer exists", fn)
			continue
		}
		allowed := map[string]bool{}
		for _, c := range w.callers {
			allowed[c] = true
		}
		for caller := range g.callersOf[fn] {
			if !allowed[caller] {
				t.Errorf("#1146 RED: %s calls %s, which makes informer calls under rw.mu that are safe only on an "+
					"informer not yet run; review the new path and add it to preRunWiring1146 only if that holds", caller, fn)
			}
		}
	}
}

// TestIssue1146_RetiringSectionsUnlockThroughUnlockAndDetach — GUARD 2.
func TestIssue1146_RetiringSectionsUnlockThroughUnlockAndDetach(t *testing.T) {
	g := newLockGuard1146(t, ".")
	for _, want := range []string{"abortBuildLocked", "deletePerGVRStateLocked", "teardownSwapStateLocked", "del", "install"} {
		if !g.retires[want] {
			t.Errorf("PRECONDITION: %s is not seen as retiring a delivery log — the guard's reachability is broken", want)
		}
	}
	g.run()
	for _, f := range g.unlockFaults {
		t.Errorf("#1146 RED: %s — use rw.unlockAndDetach so the retired log's handler is removed", f)
	}
}

// TestIssue1146_GuardSeesTheBug — the guards' own falsifier: the pre-fix
// shapes, written as source, are reported.
func TestIssue1146_GuardSeesTheBug(t *testing.T) {
	dir := t.TempDir()
	src := `package cache

import "sync"

type ResourceWatcher struct {
	mu sync.RWMutex
	dl *deliveryLog
	gi interface{ Informer() interface{ LastSyncResourceVersion() string } }
}

type deliveryLog struct{ inf interface{ RemoveEventHandler(any) error }; reg any }

type perGVRField struct {
	name string
	del  func(rw *ResourceWatcher)
}

var perGVRFields = []perGVRField{
	{"deliveries", func(rw *ResourceWatcher) { rw.dl.close() }},
}

func (l *deliveryLog) close() { _ = l.inf.RemoveEventHandler(l.reg) }

func (rw *ResourceWatcher) retireDeliveryLogLocked(dl *deliveryLog) {}

func (rw *ResourceWatcher) deletePerGVRStateLocked() {
	for _, f := range perGVRFields {
		f.del(rw)
	}
}

func (rw *ResourceWatcher) removeResourceTypeWithReason() {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	rw.deletePerGVRStateLocked()
}

func (rw *ResourceWatcher) confirm() {
	rw.mu.Lock()
	_ = rw.gi.Informer().LastSyncResourceVersion()
	rw.mu.Unlock()
}

func (rw *ResourceWatcher) abort() {
	rw.mu.Lock()
	rw.retireDeliveryLogLocked(nil)
	rw.mu.Unlock()
}
`
	if err := os.WriteFile(filepath.Join(dir, "bug.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	g := newLockGuard1146(t, dir)
	g.run()
	joined := strings.Join(g.violations, "\n")
	for _, want := range []string{"RemoveEventHandler() with rw.mu held", "LastSyncResourceVersion() with rw.mu held"} {
		if !strings.Contains(joined, want) {
			t.Errorf("guard 1 is blind: %q not reported in:\n%s", want, joined)
		}
	}
	faults := strings.Join(g.unlockFaults, "\n")
	if !strings.Contains(faults, "abort releases rw.mu with a plain Unlock") {
		t.Errorf("guard 2 is blind to a plain Unlock after a retire:\n%s", faults)
	}
}
