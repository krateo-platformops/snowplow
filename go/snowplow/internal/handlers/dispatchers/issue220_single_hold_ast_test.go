// issue220_single_hold_ast_test.go — a build-time AST invariant over the #220
// coverage promote: `coveragePassSnapshotAndPromote` MUST take the harvester
// mutex exactly ONCE, and the three promotions MUST happen inside that hold.
//
// WHY THIS IS AN AST GUARD AND NOT A BEHAVIOURAL ARM. The failure it pins is a
// LOST UPDATE, not a data race: if the snapshot is read under one acquisition
// and the sets are cleared under a second, a forgetCoordinate landing in the
// gap is dropped, and it resurfaces as an unexplained loss at the FOLLOWING
// evaluation — a false alarm on an ordinary delete, which is the direction this
// whole rebuild exists to avoid. Both holds would be correctly locked, so
// -race sees nothing wrong, and landing a forget inside a split hold
// deterministically would need a production test seam. A seam is a floor, not
// a falsifier: an arm green against a seam is a simulation. The property is
// structural, so it is asserted structurally.
//
// WHAT IS AND IS NOT ALREADY COVERED, so nobody deletes this as redundant:
// the ALIASING half of a careless split IS armed — TestIssue220_CoverageSets_
// ConcurrentAccessIsRaceFree went red under -race when an early draft promoted
// the caller's own returned map instead of a second copy. The LOST-UPDATE half
// is armed only here.
//
// RED arm: TestIssue220_SingleHoldAST_RED_SplitHoldIsFlagged feeds the SAME
// auditor a synthetic split-hold function and asserts it is flagged, proving
// the scanner discriminates rather than rubber-stamps.

package dispatchers

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"
)

// covPromoteFunc is the function under audit and covPromoteAssignments are the
// three state promotions that must live inside its single hold.
const covPromoteFunc = "coveragePassSnapshotAndPromote"

var covPromoteAssignments = []string{"prevReached", "forgottenPrev", "forgottenPass"}

func covFindFuncDecl(f *ast.File, name string) *ast.FuncDecl {
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name != nil && fd.Name.Name == name && fd.Body != nil {
			return fd
		}
	}
	return nil
}

// covAuditSingleHold returns the reasons fd violates the single-acquisition
// contract. Empty means it holds.
func covAuditSingleHold(fd *ast.FuncDecl) []string {
	var locks, bareUnlocks, deferredUnlocks int
	assigned := map[string]bool{}

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.DeferStmt:
			if sel, ok := v.Call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Unlock" {
				deferredUnlocks++
			}
			// Do not descend: a deferred Unlock must not count as a bare one.
			return false
		case *ast.CallExpr:
			if sel, ok := v.Fun.(*ast.SelectorExpr); ok {
				switch sel.Sel.Name {
				case "Lock":
					locks++
				case "Unlock":
					bareUnlocks++
				}
			}
		case *ast.AssignStmt:
			for _, lhs := range v.Lhs {
				if sel, ok := lhs.(*ast.SelectorExpr); ok {
					assigned[sel.Sel.Name] = true
				}
			}
		}
		return true
	})

	var problems []string
	if locks != 1 {
		problems = append(problems, "takes the mutex "+itoaCov(locks)+" times, want exactly 1 — a second acquisition is the split hold that drops a forget landing in the gap")
	}
	if bareUnlocks != 0 {
		problems = append(problems, "has "+itoaCov(bareUnlocks)+" non-deferred Unlock call(s) — releasing mid-function reopens the gap even with a single Lock")
	}
	if deferredUnlocks != 1 {
		problems = append(problems, "has "+itoaCov(deferredUnlocks)+" deferred Unlock call(s), want exactly 1")
	}
	var missing []string
	for _, name := range covPromoteAssignments {
		if !assigned[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		problems = append(problems, "does not assign "+strings.Join(missing, ", ")+" — the promotion has moved OUT of the hold, which is the same defect by another route")
	}
	return problems
}

func itoaCov(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestIssue220_SingleHoldAST_PromoteTakesTheMutexExactlyOnce is the production
// assertion: parse the real source and audit the real function.
func TestIssue220_SingleHoldAST_PromoteTakesTheMutexExactlyOnce(t *testing.T) {
	const src = "phase1_pip_seed.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, src, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", src, err)
	}
	fd := covFindFuncDecl(f, covPromoteFunc)
	if fd == nil {
		t.Fatalf("premise: %s not found in %s — if it was renamed, RETARGET this guard rather than deleting it; "+
			"the single-acquisition property it pins has no behavioural arm", covPromoteFunc, src)
	}
	if problems := covAuditSingleHold(fd); len(problems) > 0 {
		t.Fatalf("RED (#220): %s violates the single-acquisition contract:\n  - %s\n\n"+
			"A forget landing between a released read and a second acquisition is DROPPED and resurfaces "+
			"as an unexplained loss at the FOLLOWING evaluation — a false alarm on an ordinary delete. "+
			"-race cannot see this: both holds are correctly locked, so it is a lost update, not a race.",
			covPromoteFunc, strings.Join(problems, "\n  - "))
	}
}

// TestIssue220_SingleHoldAST_RED_SplitHoldIsFlagged proves the auditor
// discriminates: the same function fed a split-hold body must flag it.
func TestIssue220_SingleHoldAST_RED_SplitHoldIsFlagged(t *testing.T) {
	const synthetic = `package p
func (h *navWidgetHarvester) coveragePassSnapshotAndPromote() (a, b, c map[string]struct{}) {
	h.mu.Lock()
	for k := range h.passReached {
		a[k] = struct{}{}
	}
	h.mu.Unlock()
	// ... a forgetCoordinate landing HERE is dropped ...
	h.mu.Lock()
	defer h.mu.Unlock()
	h.prevReached = a
	h.forgottenPrev = h.forgottenPass
	h.forgottenPass = map[string]struct{}{}
	return a, b, c
}`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "synthetic.go", synthetic, 0)
	if err != nil {
		t.Fatalf("parsing the synthetic split-hold source: %v", err)
	}
	fd := covFindFuncDecl(f, covPromoteFunc)
	if fd == nil {
		t.Fatalf("premise: the synthetic source does not declare %s", covPromoteFunc)
	}
	problems := covAuditSingleHold(fd)
	if len(problems) == 0 {
		t.Fatalf("RED: the auditor passed a SPLIT-HOLD function — it rubber-stamps instead of discriminating, " +
			"so the production assertion proves nothing")
	}
	joined := strings.Join(problems, " | ")
	if !strings.Contains(joined, "takes the mutex 2 times") {
		t.Fatalf("the auditor flagged the synthetic split hold, but not for the right reason: %s", joined)
	}
}
