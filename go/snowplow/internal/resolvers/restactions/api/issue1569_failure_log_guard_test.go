package api

// issue1569_failure_log_guard_test.go — #1569 B1: every failure-level line a
// cell's resolve writes goes through the cell's failure-log round
// (FailureLogLevel / FailureLevelOf), so an unchanged failure of a resident
// cell repeats at DEBUG at every site, and a changed class logs at its level.
//
// THE GUARD. It parses every non-test Go file of the resolve path — every
// package under internal/resolvers, and internal/objects (a widget's apiRef
// read and a `resolve: true` stage run through objects.Get) — and fails on a
// log call at a Warn or Error level the round does not compute:
// logger.Warn/Error/WarnContext/ErrorContext and slog.Warn/Error/WarnContext/
// ErrorContext, whatever their message (a literal, a concatenation, a
// variable); and Log/LogAttrs unless the level is a call of FailureLevelOf /
// FailureLogLevel / expectedDenyLevel / apiRefFailureLevel, slog.LevelDebug /
// slog.LevelInfo, or a local variable every assignment of which (in the
// enclosing function) is one of those. A level the guard cannot see the value
// of — a parameter, a field, another function's result — is flagged: route it,
// or allow-list the line. A variable is matched by its parser-resolved object,
// so a shadowing declaration of the same name is another variable; its address
// taken, an increment, an op-assignment or a range assignment makes it unseen.
//
// THE RESIDUAL. roundLevelFuncs is matched by the called function's NAME,
// whatever its receiver or package: a different function named, say,
// FailureLevelOf would be trusted. The guard reads syntax, not types; the names
// are this package's and widgets', and a look-alike would have to be written on
// purpose.
//
// THE ALLOW-LIST is the lines that are not a cell's failure, each with its
// reason. A new fixed-level line on the resolve path either goes through the
// round or is added here with a reason a reviewer can judge.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// failureLogGuardAllowed: "<path relative to internal/>|<message>" -> reason.
var failureLogGuardAllowed = map[string]string{
	"resolvers/restactions/api/apistage_stale_debt.go|apistage.stale_replace_overdue":                                          "the stale-debt ledger's overdue alarm: a background ledger condition, not a failure of the resolve that logs it",
	"resolvers/restactions/api/apistage_stale_debt.go|apistage.stale_debt_overdue":                                             "the stale-debt ledger's overdue alarm: a background ledger condition, not a failure of the resolve that logs it",
	"resolvers/restactions/api/cluster_list.go|cluster_list.shape_check.slow":                                                  "a latency observation (*.slow), not a failure",
	"resolvers/restactions/api/resolve.go|cache.lazy_register.slow":                                                            "a latency observation (*.slow), not a failure",
	"resolvers/restactions/api/cluster_list.go|cluster_list.cell.async_populate_panic":                                         "a recovered panic: a defect, loud every time",
	"resolvers/restactions/api/external_fetch_flight.go|external read: the shared request panicked after its last waiter left": "a recovered panic: a defect, loud every time",
	"resolvers/restactions/api/cluster_list_prewarm.go|cluster_list.prewarm.skipped":                                           "a prewarm pass's operation, not a cell's resolve",
	"resolvers/restactions/api/cluster_list_prewarm.go|cluster_list.prewarm.timeout":                                           "a prewarm pass's operation, not a cell's resolve",
	"resolvers/restactions/api/endpoints_tls_233.go|endpoints_tls.unparseable_ca_delegated":                                    "the endpoint's TLS-CA configuration line, logged where the transport is built, not a cell's failure",
	"resolvers/widgets/resourcesrefs/failed.go|resource ref failed; it is a failure entry in status.resourcesRefs":             "origin-gated (#563): WARN only on the serve origin, Debug for every background driver",
	"resolvers/crds/schema/schema_fill.go|schema: SA discovery singleton unavailable; falling back to per-call client":         "the ServiceAccount discovery client's construction falling back: a transport line, not a cell's failure (the read itself still runs, and fails through the cell's own lines)",
	"resolvers/widgets/resourcesrefs/resolve.go|resource ref action not allowed":                                               "origin-gated (#563): WARN only on the serve origin, Debug for every background driver",
}

// failureLogFinding is one fixed-level log call.
type failureLogFinding struct {
	file string // relative to internal/
	line int
	msg  string
}

func (f failureLogFinding) key() string { return f.file + "|" + f.msg }

var fixedLevelMethods = map[string]int{ // method -> index of the message argument
	"Warn": 0, "Error": 0, "WarnContext": 1, "ErrorContext": 1,
}

// roundLevelFuncs are the functions that compute a failure line's level through
// the cell's round (failure_log.go; widgets.apiRefFailureLevel wraps it).
var roundLevelFuncs = map[string]bool{
	"FailureLogLevel": true, "FailureLevelOf": true, "expectedDenyLevel": true, "apiRefFailureLevel": true,
}

// levelThroughRound reports whether e, the level argument of a Log/LogAttrs
// call in fn, is computed by the round or is below Warn: a call of a
// roundLevelFuncs function, slog.LevelDebug / slog.LevelInfo, or a local
// variable EVERY assignment of which in fn is one of those. Anything else — a
// fixed slog.LevelWarn / slog.LevelError, a variable holding one, a parameter
// or field the guard cannot see the value of — is not, and the call is flagged
// (#1569 review G3: a fixed level passed through a variable).
func levelThroughRound(e ast.Expr, fn *ast.FuncDecl, depth int) bool {
	if depth > 8 {
		return false
	}
	switch v := e.(type) {
	case *ast.ParenExpr:
		return levelThroughRound(v.X, fn, depth+1)
	case *ast.CallExpr:
		switch f := v.Fun.(type) {
		case *ast.Ident:
			return roundLevelFuncs[f.Name]
		case *ast.SelectorExpr:
			return roundLevelFuncs[f.Sel.Name]
		}
		return false
	case *ast.SelectorExpr:
		id, ok := v.X.(*ast.Ident)
		return ok && id.Name == "slog" && (v.Sel.Name == "LevelDebug" || v.Sel.Name == "LevelInfo")
	case *ast.Ident:
		return localLevelThroughRound(v, fn, depth)
	}
	return false
}

// localLevelThroughRound is levelThroughRound for a variable (#1569 review B3).
// The variable is matched by its OBJECT (the parser's resolution, mode 0), never
// by its name, so a shadowing declaration of the same name is another variable.
// It is seen only when it is a local of fn: no object, a parameter or receiver
// (an *ast.Field declaration) or a declaration outside fn's body (a package
// variable) is unseen. Every write of it in fn must be a plain `=` / `:=` /
// `var` assignment whose value is through the round; any other write makes it
// unseen: its address taken (`&v`, so a write through a pointer can follow),
// `v++` / `v--`, an op-assignment (`v += …`), a range key or value.
func localLevelThroughRound(v *ast.Ident, fn *ast.FuncDecl, depth int) bool {
	if fn == nil || fn.Body == nil || v.Obj == nil || v.Obj.Kind != ast.Var {
		return false
	}
	switch d := v.Obj.Decl.(type) {
	case *ast.Field:
		return false // a parameter or receiver
	case ast.Node:
		if d.Pos() < fn.Body.Pos() || d.Pos() > fn.Body.End() {
			return false // declared outside fn: a package variable
		}
	default:
		return false
	}
	same := func(e ast.Expr) bool {
		id, ok := e.(*ast.Ident)
		return ok && id.Obj == v.Obj
	}
	var rhs []ast.Expr
	unseen := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.AssignStmt:
			for i, l := range s.Lhs {
				if !same(l) {
					continue
				}
				if (s.Tok != token.ASSIGN && s.Tok != token.DEFINE) || len(s.Lhs) != len(s.Rhs) {
					unseen = true // an op-assignment, or a multi-value call's result
					continue
				}
				rhs = append(rhs, s.Rhs[i])
			}
		case *ast.ValueSpec:
			for i, id := range s.Names {
				if id.Obj != v.Obj {
					continue
				}
				switch {
				case len(s.Values) == 0:
					// declared without a value: slog.Level's zero value is
					// LevelInfo, below Warn
					rhs = append(rhs, &ast.SelectorExpr{X: ast.NewIdent("slog"), Sel: ast.NewIdent("LevelInfo")})
				case len(s.Values) == len(s.Names):
					rhs = append(rhs, s.Values[i])
				default:
					unseen = true
				}
			}
		case *ast.IncDecStmt:
			if same(s.X) {
				unseen = true
			}
		case *ast.RangeStmt:
			if (s.Key != nil && same(s.Key)) || (s.Value != nil && same(s.Value)) {
				unseen = true
			}
		case *ast.UnaryExpr:
			if s.Op == token.AND && same(s.X) {
				unseen = true // its address is taken: a write through the pointer is not seen
			}
		}
		return true
	})
	if unseen || len(rhs) == 0 {
		return false
	}
	for _, r := range rhs {
		if !levelThroughRound(r, fn, depth+1) {
			return false
		}
	}
	return true
}

// literalMessage is the message a call writes when it is a string literal, or a
// concatenation of literals; anything else is "<non-literal>".
func literalMessage(e ast.Expr) string {
	if s, ok := literalString(e); ok {
		return s
	}
	return "<non-literal>"
}

func literalString(e ast.Expr) (string, bool) {
	switch v := e.(type) {
	case *ast.BasicLit:
		if v.Kind == token.STRING {
			s, err := strconv.Unquote(v.Value)
			return s, err == nil
		}
	case *ast.BinaryExpr:
		if v.Op == token.ADD {
			l, lok := literalString(v.X)
			r, rok := literalString(v.Y)
			return l + r, lok && rok
		}
	case *ast.ParenExpr:
		return literalString(v.X)
	}
	return "", false
}

// scanFixedLevelLogCalls returns the log calls in file whose level is Warn or
// Error and not computed by the cell's round: Warn/Error/WarnContext/
// ErrorContext with any message (a literal, a concatenation, a variable), and
// Log/LogAttrs whose level is not levelThroughRound.
func scanFixedLevelLogCalls(fset *token.FileSet, file *ast.File, rel string) []failureLogFinding {
	var out []failureLogFinding
	visit := func(fn *ast.FuncDecl, root ast.Node) {
		ast.Inspect(root, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			msgAt := -1
			if i, ok := fixedLevelMethods[sel.Sel.Name]; ok {
				// a logger call carries its message; err.Error() carries none
				if len(call.Args) > i {
					msgAt = i
				}
			} else if (sel.Sel.Name == "Log" || sel.Sel.Name == "LogAttrs") && len(call.Args) >= 3 && !levelThroughRound(call.Args[1], fn, 0) {
				msgAt = 2
			}
			if msgAt < 0 {
				return true
			}
			out = append(out, failureLogFinding{file: rel, line: fset.Position(call.Pos()).Line, msg: literalMessage(call.Args[msgAt])})
			return true
		})
	}
	for _, d := range file.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok {
			visit(fn, fn)
		} else {
			visit(nil, d)
		}
	}
	return out
}

// resolvePathFiles lists the guard's files, relative to internal/.
func resolvePathFiles(t *testing.T, internal string) []string {
	t.Helper()
	var files []string
	for _, root := range []string{"resolvers", "objects"} {
		err := filepath.Walk(filepath.Join(internal, root), func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() && (info.Name() == "testdata" || strings.HasPrefix(info.Name(), ".")) {
				return filepath.SkipDir
			}
			if info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(internal, p)
			files = append(files, filepath.ToSlash(rel))
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	sort.Strings(files)
	return files
}

func TestIssue1569_EveryFailureLineOnTheResolvePathGoesThroughTheRound(t *testing.T) {
	internal := filepath.Join("..", "..", "..")
	files := resolvePathFiles(t, internal)
	if len(files) < 50 {
		t.Fatalf("NON-VACUITY: the guard found only %d files under %s; it is not reading the resolve path", len(files), internal)
	}
	fset := token.NewFileSet()
	seen := map[string]bool{}
	var bad []string
	for _, rel := range files {
		f, err := parser.ParseFile(fset, filepath.Join(internal, rel), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", rel, err)
		}
		for _, fd := range scanFixedLevelLogCalls(fset, f, rel) {
			if _, ok := failureLogGuardAllowed[fd.key()]; ok {
				seen[fd.key()] = true
				continue
			}
			bad = append(bad, rel+":"+strconv.Itoa(fd.line)+" "+strconv.Quote(fd.msg))
		}
	}
	if len(bad) > 0 {
		t.Fatalf("#1569: %d fixed-level Warn/Error log call(s) on a cell's resolve path bypass the failure-log round. "+
			"Log through FailureLevelOf / FailureLogLevel, or allow-list the line here with its reason:\n  %s",
			len(bad), strings.Join(bad, "\n  "))
	}
	// An allow-list entry nothing matches is stale: it would excuse a future line.
	for k := range failureLogGuardAllowed {
		if !seen[k] {
			t.Errorf("#1569: stale allow-list entry %q matches no log call; remove it", k)
		}
	}
}

// The guard's own falsifier: a raw fixed-level line on the resolve path is
// found, in every form the guard claims to catch, and a computed level is not.
func TestIssue1569_Guard_FindsARawFailureLine(t *testing.T) {
	src := `package x
import ("context"; "log/slog")
func f(ctx context.Context, log *slog.Logger, err error) {
	slog.Error("raw-1")
	slog.ErrorContext(ctx, "raw-2")
	log.WarnContext(ctx, "raw-3")
	log.Warn("raw-4", slog.Any("err", err))
	log.Log(ctx, slog.LevelError, "raw-5")
	log.LogAttrs(ctx, slog.LevelWarn, "raw-6")
	slog.WarnContext(ctx, "raw-"+
		"7")
	x := "y"
	log.Warn("raw-8 "+x)
	msg := "raw-9"
	slog.Error(msg)
	lvl := slog.LevelError
	log.Log(ctx, lvl, "raw-10")
	var mixed slog.Level
	if err != nil {
		mixed = slog.LevelWarn
	} else {
		mixed = FailureLevelOf(ctx, slog.LevelError, "t", err)
	}
	log.Log(ctx, mixed, "raw-11")
	log.Log(ctx, f2(), "raw-12")
	log.Log(ctx, FailureLevelOf(ctx, slog.LevelError, "t", err), "routed")
	routedLvl := FailureLogLevel(ctx, slog.LevelWarn, "t", "c")
	log.Log(ctx, routedLvl, "routed-2")
	var zero slog.Level
	if err != nil {
		zero = expectedDenyLevel(ctx, "t")
	}
	log.Log(ctx, zero, "routed-3")
	log.Log(ctx, slog.LevelInfo, "info")
	log.Debug("debug")
	_ = err.Error()
}
var pkgLvl = FailureLevelOf(nil, slog.LevelError, "t", nil)
func g(ctx context.Context, log *slog.Logger, lvl slog.Level, c bool, err error, levels []slog.Level) {
	if c {
		lvl := FailureLevelOf(ctx, slog.LevelError, "t", err)
		_ = lvl
	}
	log.Log(ctx, lvl, "raw-13")
	addr := FailureLevelOf(ctx, slog.LevelError, "t", err)
	p := &addr
	*p = slog.LevelError
	log.Log(ctx, addr, "raw-14")
	inc := FailureLevelOf(ctx, slog.LevelWarn, "t", err)
	inc++
	log.Log(ctx, inc, "raw-15")
	rng := FailureLevelOf(ctx, slog.LevelWarn, "t", err)
	for _, rng = range levels {
	}
	log.Log(ctx, rng, "raw-16")
	// raw-17: a package variable, even written through the round inside g, is unseen (its other writers are
	// outside the function body). The routed write pins that rule: without it "no write => unseen" alone
	// would flag the call, and a mutant counting a package variable as a local would survive (#1574 B4, S4).
	pkgLvl = FailureLevelOf(ctx, slog.LevelError, "t", err)
	log.Log(ctx, pkgLvl, "raw-17")
	opa := FailureLevelOf(ctx, slog.LevelWarn, "t", err)
	// raw-18: an op-assignment makes the variable unseen even when its right side goes through the round
	// (a literal right side would fail the round check on its own and leave the op-assign rule unpinned,
	// #1574 B4, S5).
	opa += FailureLevelOf(ctx, slog.LevelWarn, "t", err)
	log.Log(ctx, opa, "raw-18")
	shadowed := FailureLevelOf(ctx, slog.LevelError, "t", err)
	if c {
		shadowed := slog.LevelError
		_ = shadowed
	}
	log.Log(ctx, shadowed, "routed-4")
}`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "x.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	findings := scanFixedLevelLogCalls(fset, f, "x.go")
	for _, fd := range findings {
		got[fd.msg] = true
	}
	for _, want := range []string{"raw-1", "raw-2", "raw-3", "raw-4", "raw-5", "raw-6", "raw-7", "raw-10", "raw-11", "raw-12",
		// #1569 review B3: matched by object, and every other write is unseen
		"raw-13", "raw-14", "raw-15", "raw-16", "raw-17", "raw-18"} {
		if !got[want] {
			t.Errorf("#1569 guard RED: the raw line %q was not found (found %v)", want, got)
		}
	}
	// a non-literal message is reported as <non-literal>: raw-8 and raw-9 both
	if n := countMsg(findings, "<non-literal>"); n != 2 {
		t.Errorf("#1569 guard RED (B2): %d non-literal-message raw lines found, want 2 (raw-8 \"raw-8 \"+x, raw-9 slog.Error(msg)): %v", n, got)
	}
	for _, not := range []string{"routed", "routed-2", "routed-3", "routed-4", "info", "debug"} {
		if got[not] {
			t.Errorf("#1569 guard: %q is not a fixed-level line but was reported", not)
		}
	}
}

func countMsg(fs []failureLogFinding, msg string) int {
	n := 0
	for _, f := range fs {
		if f.msg == msg {
			n++
		}
	}
	return n
}
