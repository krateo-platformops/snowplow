package api

// issue1569_failure_log_guard_test.go — #1569 B1: every failure-level line a
// cell's resolve writes goes through the cell's failure-log round
// (FailureLogLevel / FailureLevelOf), so an unchanged failure of a resident
// cell repeats at DEBUG at every site, and a changed class logs at its level.
//
// THE GUARD. It parses every non-test Go file of the resolve path — every
// package under internal/resolvers, and internal/objects (a widget's apiRef
// read and a `resolve: true` stage run through objects.Get) — and fails on a
// log call at a FIXED Warn or Error level: logger.Warn/Error/WarnContext/
// ErrorContext, slog.Warn/Error/WarnContext/ErrorContext, and Log/LogAttrs
// whose level argument is slog.LevelWarn or slog.LevelError written out. A
// call whose level is computed (FailureLevelOf, FailureLogLevel,
// expectedDenyLevel, a level variable) is not a fixed-level call.
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

// isFixedFailureLevel reports whether e is slog.LevelWarn or slog.LevelError
// written out.
func isFixedFailureLevel(e ast.Expr) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == "slog" && (sel.Sel.Name == "LevelWarn" || sel.Sel.Name == "LevelError")
}

func literalMessage(e ast.Expr) string {
	if bl, ok := e.(*ast.BasicLit); ok && bl.Kind == token.STRING {
		if s, err := strconv.Unquote(bl.Value); err == nil {
			return s
		}
	}
	return "<non-literal>"
}

// scanFixedLevelLogCalls returns the fixed-level log calls in file.
func scanFixedLevelLogCalls(fset *token.FileSet, file *ast.File, rel string) []failureLogFinding {
	var out []failureLogFinding
	ast.Inspect(file, func(n ast.Node) bool {
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
				if _, isLit := call.Args[i].(*ast.BasicLit); isLit || i == 1 {
					msgAt = i
				}
			}
		} else if (sel.Sel.Name == "Log" || sel.Sel.Name == "LogAttrs") && len(call.Args) >= 3 && isFixedFailureLevel(call.Args[1]) {
			msgAt = 2
		}
		if msgAt < 0 {
			return true
		}
		out = append(out, failureLogFinding{file: rel, line: fset.Position(call.Pos()).Line, msg: literalMessage(call.Args[msgAt])})
		return true
	})
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
	log.Log(ctx, FailureLevelOf(ctx, slog.LevelError, "t", err), "routed")
	log.Debug("debug")
	_ = err.Error()
}`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "x.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, fd := range scanFixedLevelLogCalls(fset, f, "x.go") {
		got[fd.msg] = true
	}
	for _, want := range []string{"raw-1", "raw-2", "raw-3", "raw-4", "raw-5", "raw-6"} {
		if !got[want] {
			t.Errorf("#1569 guard RED: the raw line %q was not found (found %v)", want, got)
		}
	}
	for _, not := range []string{"routed", "debug"} {
		if got[not] {
			t.Errorf("#1569 guard: %q is not a fixed-level line but was reported", not)
		}
	}
}
