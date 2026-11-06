// Command checkrbacpublish is the CI guard for #1206: the RBAC snapshot is only
// ever published through the writer slot.
//
// WHY. EvaluateRBAC, the identity class in every cache key and the #424 re-check
// after a resolve all read one global snapshot, and they rely on it only moving
// forward. The slot gives that: one rebuild at a time, each reading the indexers
// after the last. A Store outside it can land after a rebuild that read LATER, so
// a snapshot with older content goes live: a revoke is undone, and the #424
// re-check can pass on stale state. #687 fixed this in production, and #1206 found
// it surviving in a test helper. Nothing else stops a second writer being added.
//
// THE CHAIN (each link may be reached only from the one before it):
//
//	scheduleRBACRebuild / publishRBACSnapshotUnderSlot   take the slot (CAS)
//	  → runRBACRebuildLoop                                holds it, releases it
//	    → drainRBACRebuild
//	      → rebuildRBACSnapshot                           the one production Store
//
// The check is on any REFERENCE, not on calls, so a function value or an alias
// cannot slip through. rbacSnap may be read with Load anywhere. It is parsed, not
// built, so every build tag is covered. Test files and testdata/ are skipped.
//
// TEST SEAMS. PublishRBACSnapshotForTest and DisableRBACSnapshotForTest Store
// outside the slot by design. They may only be used from a function whose name
// ends in ForTest; production code reaching one is a finding.
//
// A CLEAN RUN MUST SEE THE CHAIN. Every link is also required to be present, so a
// rename, a wrong root or a broken scan fails instead of reading as zero findings.
// --self-test runs the same check over a known-good chain and over each defect it
// exists to catch, and fails unless every one is told apart.
//
// Usage:
//
//	checkrbacpublish <root> [<root>...]
//	checkrbacpublish --self-test
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	snapVar  = "rbacSnap"
	slotVar  = "rbacRebuildLock"
	rebuild  = "rebuildRBACSnapshot"
	drain    = "drainRBACRebuild"
	loop     = "runRBACRebuildLoop"
	schedule = "scheduleRBACRebuild"
	publish  = "publishRBACSnapshotUnderSlot"
)

// onlyFrom is each chain link and the one function that may reference it.
var onlyFrom = map[string][]string{
	rebuild: {drain},
	drain:   {loop},
	loop:    {schedule, publish},
}

// storeSeams may Store outside the slot, and may be used only by test-only code.
var storeSeams = map[string]bool{
	"PublishRBACSnapshotForTest": true,
	"DisableRBACSnapshotForTest": true,
}

// source is one parsed file.
type source struct {
	fset *token.FileSet
	file *ast.File
}

// walker tracks what a run saw, so a clean result can be told from an empty one.
type walker struct {
	findings []string
	// stores in rebuild, references to each link by its caller, and the first
	// slot CAS position per function.
	prodStores int
	refs       map[string]map[string]token.Pos
	cas        map[string]token.Pos
	releases   map[string]int
}

func newWalker() *walker {
	return &walker{refs: map[string]map[string]token.Pos{}, cas: map[string]token.Pos{}, releases: map[string]int{}}
}

func (w *walker) reportf(fset *token.FileSet, pos token.Pos, format string, args ...any) {
	w.findings = append(w.findings, fmt.Sprintf("%s: %s", fset.Position(pos), fmt.Sprintf(format, args...)))
}

func testOnly(fn string) bool { return strings.HasSuffix(fn, "ForTest") }

// useOfLink records and judges one reference to a chain function.
func (w *walker) useOfLink(s source, name, fn string, pos token.Pos) {
	if w.refs[name] == nil {
		w.refs[name] = map[string]token.Pos{}
	}
	if _, seen := w.refs[name][fn]; !seen {
		w.refs[name][fn] = pos
	}
	for _, ok := range onlyFrom[name] {
		if fn == ok {
			return
		}
	}
	w.reportf(s.fset, pos, "%s referenced from %s: it may only be reached from %s, so the writer slot is held (#1206)",
		name, describe(fn), strings.Join(onlyFrom[name], " or "))
}

func describe(fn string) string {
	if fn == "" {
		return "package scope"
	}
	return fn
}

// visit walks one declaration body. fn is the enclosing function ("" at package
// scope). Only package cache owns the unexported names; a seam can be reached from
// any package.
func (w *walker) visit(s source, n ast.Node, fn string) {
	inCache := s.file.Name.Name == "cache"
	ast.Inspect(n, func(node ast.Node) bool {
		switch x := node.(type) {
		case *ast.SelectorExpr:
			if storeSeams[x.Sel.Name] && !testOnly(fn) {
				w.reportf(s.fset, x.Pos(), "%s used from %s: it stores the RBAC snapshot outside the writer slot, and only test-only code may use it (#1206)", x.Sel.Name, describe(fn))
			}
			id, ok := x.X.(*ast.Ident)
			if !ok || !inCache {
				return true
			}
			switch id.Name {
			case snapVar:
				w.snapMethod(s, x, fn)
				return false
			case slotVar:
				w.slotMethod(s, x, fn)
				return false
			}
		case *ast.Ident:
			if storeSeams[x.Name] && !testOnly(fn) {
				w.reportf(s.fset, x.Pos(), "%s used from %s: it stores the RBAC snapshot outside the writer slot, and only test-only code may use it (#1206)", x.Name, describe(fn))
			}
			if !inCache {
				return true
			}
			if _, link := onlyFrom[x.Name]; link {
				w.useOfLink(s, x.Name, fn, x.Pos())
			}
			if x.Name == snapVar {
				w.reportf(s.fset, x.Pos(), "%s used as a value in %s: only its Load and the one Store in %s are allowed (#1206)", snapVar, describe(fn), rebuild)
			}
		}
		return true
	})
}

func (w *walker) snapMethod(s source, x *ast.SelectorExpr, fn string) {
	switch x.Sel.Name {
	case "Load":
	case "Store":
		switch {
		case fn == rebuild:
			w.prodStores++
		case storeSeams[fn]:
		default:
			w.reportf(s.fset, x.Pos(), "%s.Store in %s: the RBAC snapshot may only be stored by %s, under the writer slot (#1206)", snapVar, describe(fn), rebuild)
		}
	default:
		w.reportf(s.fset, x.Pos(), "%s.%s in %s: only Load and the one Store in %s are allowed (#1206)", snapVar, x.Sel.Name, describe(fn), rebuild)
	}
}

func (w *walker) slotMethod(s source, x *ast.SelectorExpr, fn string) {
	switch x.Sel.Name {
	case "Load":
	case "CompareAndSwap":
		if _, seen := w.cas[fn]; !seen {
			w.cas[fn] = x.Pos()
		}
	case "Store":
		if fn != loop {
			w.reportf(s.fset, x.Pos(), "%s.Store in %s: only %s may release the writer slot (#1206)", slotVar, describe(fn), loop)
		}
		w.releases[fn]++
	default:
		w.reportf(s.fset, x.Pos(), "%s.%s in %s: the writer slot is taken by CompareAndSwap and released by %s (#1206)", slotVar, x.Sel.Name, describe(fn), loop)
	}
}

// check runs every rule over srcs and returns the findings, sorted. An empty
// result means the chain was seen whole and nothing bypassed it.
func check(srcs []source) []string {
	w := newWalker()
	for _, s := range srcs {
		for _, d := range s.file.Decls {
			switch decl := d.(type) {
			case *ast.FuncDecl:
				if decl.Body != nil {
					w.visit(s, decl.Body, decl.Name.Name)
				}
			case *ast.GenDecl:
				for _, sp := range decl.Specs {
					vs, ok := sp.(*ast.ValueSpec)
					if !ok {
						continue
					}
					// The names themselves are the declaration, not a use.
					for _, v := range vs.Values {
						w.visit(s, v, "")
					}
				}
			}
		}
	}
	w.requireChain()
	sort.Strings(w.findings)
	return w.findings
}

// requireChain fails a run that did not see the writer chain, which is what a
// rename, a wrong root or a broken scan looks like: otherwise zero findings.
func (w *walker) requireChain() {
	missing := func(format string, args ...any) {
		w.findings = append(w.findings, "the writer chain was not seen whole: "+fmt.Sprintf(format, args...))
	}
	if w.prodStores != 1 {
		missing("%d %s.Store in %s, want exactly 1", w.prodStores, snapVar, rebuild)
	}
	for name, callers := range onlyFrom {
		for _, c := range callers {
			if _, ok := w.refs[name][c]; !ok {
				missing("%s does not reach %s", c, name)
			}
		}
	}
	for _, fn := range []string{schedule, publish} {
		cas, ok := w.cas[fn]
		if !ok {
			missing("%s does not take the slot with %s.CompareAndSwap", fn, slotVar)
			continue
		}
		if use, ok := w.refs[loop][fn]; ok && use < cas {
			missing("%s enters %s before it takes the slot", fn, loop)
		}
	}
	if w.releases[loop] == 0 {
		missing("%s never releases the slot", loop)
	}
}

// parseDir collects every production file under root.
func parseDir(root string) ([]source, error) {
	var out []source
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "testdata", "vendor", ".git", ".claude", "scripts":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, p, nil, 0)
		if perr != nil {
			return perr
		}
		out = append(out, source{fset, f})
		return nil
	})
	return out, err
}

func parseString(name, src string) source {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		panic(fmt.Sprintf("self-test fixture %s does not parse: %v", name, err))
	}
	return source{fset, f}
}

// goodChain is the writer chain as production has it.
const goodChain = `package cache

func scheduleRBACRebuild(rw int) {
	if !rbacRebuildLock.CompareAndSwap(false, true) {
		return
	}
	go runRBACRebuildLoop(rw)
}

func publishRBACSnapshotUnderSlot(rw int) {
	for !rbacRebuildLock.CompareAndSwap(false, true) {
	}
	runRBACRebuildLoop(rw)
}

func runRBACRebuildLoop(rw int) {
	drainRBACRebuild(rw)
	rbacRebuildLock.Store(false)
}

func drainRBACRebuild(rw int) { rebuildRBACSnapshot(rw) }

func rebuildRBACSnapshot(rw int) { rbacSnap.Store(nil) }

func PublishRBACSnapshotForTest() { rbacSnap.Store(nil) }

func ResetForTest() { PublishRBACSnapshotForTest() }

func read() { _ = rbacSnap.Load() }
`

// defect is one thing the guard exists to catch. Exactly one edit of goodChain, or
// an added file, and the finding it must produce.
type defect struct {
	name, want string
	files      []string
}

func defects() []defect {
	add := func(src string) []string { return []string{goodChain, src} }
	swap := func(old, repl string) []string {
		if !strings.Contains(goodChain, old) {
			panic("self-test fixture out of date: " + old)
		}
		return []string{strings.Replace(goodChain, old, repl, 1)}
	}
	return []defect{
		{"a second Store", "rbacSnap.Store in other", add("package cache\nfunc other() { rbacSnap.Store(nil) }\n")},
		{"a Store at package scope", "rbacSnap.Store in package scope", add("package cache\nvar x = func() int { rbacSnap.Store(nil); return 0 }()\n")},
		{"a Swap", "rbacSnap.Swap", add("package cache\nfunc other() { rbacSnap.Swap(nil) }\n")},
		{"an alias of the snapshot", "used as a value", add("package cache\nfunc other() { p := &rbacSnap; p.Store(nil) }\n")},
		{"a direct rebuild", "rebuildRBACSnapshot referenced from other", add("package cache\nfunc other() { rebuildRBACSnapshot(1) }\n")},
		{"the rebuild as a value", "rebuildRBACSnapshot referenced from package scope", add("package cache\nvar f = rebuildRBACSnapshot\n")},
		{"a direct drain", "drainRBACRebuild referenced from other", add("package cache\nfunc other() { drainRBACRebuild(1) }\n")},
		{"a third way into the loop", "runRBACRebuildLoop referenced from other", add("package cache\nfunc other() { runRBACRebuildLoop(1) }\n")},
		{"the slot released elsewhere", "only runRBACRebuildLoop may release", add("package cache\nfunc other() { rbacRebuildLock.Store(false) }\n")},
		{"a production use of a seam", "PublishRBACSnapshotForTest used from other", add("package other\nfunc other() { cache.PublishRBACSnapshotForTest() }\n")},
		{"a seam used unqualified", "PublishRBACSnapshotForTest used from other", add("package cache\nfunc other() { PublishRBACSnapshotForTest() }\n")},
		{"entering the loop with no slot", "does not take the slot", swap("\tif !rbacRebuildLock.CompareAndSwap(false, true) {\n\t\treturn\n\t}\n", "")},
		{"entering the loop before the slot", "before it takes the slot", swap("\tfor !rbacRebuildLock.CompareAndSwap(false, true) {\n\t}\n\trunRBACRebuildLoop(rw)\n", "\trunRBACRebuildLoop(rw)\n\tfor !rbacRebuildLock.CompareAndSwap(false, true) {\n\t}\n")},
		{"a loop that never releases", "never releases the slot", swap("\trbacRebuildLock.Store(false)\n", "")},
		{"the chain renamed away", "was not seen whole", swap("func rebuildRBACSnapshot(rw int) { rbacSnap.Store(nil) }", "func renamedRebuild(rw int) { rbacSnap.Load() }")},
		{"nothing scanned", "was not seen whole", nil},
	}
}

// selfTest is the positive control: the good chain passes, and every defect is
// reported with its own finding. A guard that has stopped seeing anything fails
// here, whatever the tree looks like.
func selfTest() []string {
	var bad []string
	if f := check([]source{parseString("good.go", goodChain)}); len(f) != 0 {
		bad = append(bad, fmt.Sprintf("the known-good chain was flagged: %q", f))
	}
	for _, d := range defects() {
		srcs := make([]source, 0, len(d.files))
		for i, src := range d.files {
			srcs = append(srcs, parseString(fmt.Sprintf("f%d.go", i), src))
		}
		found := false
		findings := check(srcs)
		for _, f := range findings {
			found = found || strings.Contains(f, d.want)
		}
		if !found {
			bad = append(bad, fmt.Sprintf("defect %q not caught (want %q, got %q)", d.name, d.want, findings))
		}
	}
	return bad
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--self-test" {
		if bad := selfTest(); len(bad) != 0 {
			for _, b := range bad {
				fmt.Fprintln(os.Stderr, "SELF-TEST RED:", b)
			}
			os.Exit(1)
		}
		fmt.Printf("OK: self-test — the good chain passes and all %d defects are caught.\n", len(defects()))
		return
	}
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: checkrbacpublish <root> [<root>...] | --self-test")
		os.Exit(2)
	}
	var srcs []source
	for _, root := range os.Args[1:] {
		got, err := parseDir(root)
		if err != nil {
			fmt.Fprintln(os.Stderr, "checkrbacpublish:", err)
			os.Exit(2)
		}
		srcs = append(srcs, got...)
	}
	findings := check(srcs)
	if len(findings) != 0 {
		for _, f := range findings {
			fmt.Fprintln(os.Stderr, "ERROR:", f)
		}
		os.Exit(1)
	}
	fmt.Printf("OK: the RBAC snapshot is published only through the writer slot; %d files scanned.\n", len(srcs))
}
