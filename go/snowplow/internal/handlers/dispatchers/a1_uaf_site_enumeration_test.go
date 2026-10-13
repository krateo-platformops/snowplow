// a1_uaf_site_enumeration_test.go — the STRUCTURAL anti-shadow-drift guard for
// the 1.12.3 A-1 / R-1 UAF Put gate.
//
// WHY THIS REPLACED A HAND-LIST. The first version of this guard named three
// files and checked each one mentioned the gate. It passed — over an OPEN HOT
// CARRIER. The widgets class was never in the list, because the list was written
// from the same wrong belief as the comment it was guarding ("UAF output only
// ever lands in a restactions-class cell"). A hand-maintained enumeration can
// only ever confirm what its author already believed; it cannot find the site
// they did not think of. That is exactly how R-1 got through.
//
// So this guard enumerates the sites the SAME WAY scripts/checkresolvedentrysites
// does — by walking the prod tree's AST for every `ResolvedEntry{` /
// `cache.ResolvedEntry{` composite literal — and requires each one to EITHER be
// covered by the UAF gate in its enclosing function OR carry an explicit
// `// uaf-scope-waiver: <reason>` annotation, mirroring the established
// `scope-waiver:TTLOverride:` convention next to it.
//
// The property that matters: a SIXTH carrier added later cannot be missed the
// way the fifth was. A new ResolvedEntry Put site fails this test on the day it
// is written, and the author must either gate it or say in writing why the cell
// cannot hold a per-requester-narrowed body.
//
// A waiver is a legitimate answer for the identity-free, pre-refilter substrate
// classes — but it must state WHY, and an empty reason is rejected.
//
// #561 CHANGED WHAT COUNTS AS COVERAGE, and this guard changed with it.
//
// Before #561 there was exactly one acceptable disposition for a Put site that
// could hold a narrowed body: DECLINE it. That is no longer true. For an
// identity-BOUND class the post-#423 key separates two requesters whose
// narrowed bodies differ, so the body may be cached — but it is still narrowed
// per requester, so the cell must not outlive the requester's RBAC: it has to
// carry the short #118 (d) cap, which means stamping inputs.HasUAF AND passing
// those inputs to uafTTLOverrideForEntry.
//
// So a site is covered when it does ONE of:
//
//   DECLINE  — consults the gate (declineUAFPut / declineWidgetUAFPut /
//              uafDeclineReason, or reads the sink off ctx) and skips the Put;
//   STAMP+CAP — stamps HasUAF from the UAF signal AND sets TTLOverride from
//              uafTTLOverrideForEntry. BOTH are required and that conjunction
//              is the point: widgets.go called uafTTLOverrideForEntry's sibling
//              helper for months while never stamping HasUAF on this path, so
//              the cap derived 0 every time and the cell silently inherited the
//              full store TTL. A guard that accepted either half alone would
//              have called that site covered.

package dispatchers

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// uafGateTokens are the identifiers that constitute "this function consults the
// UAF gate". Any one of them appearing anywhere in the enclosing function is
// accepted: the gate is always an early return or an if/else arm in the same
// function as the Put, so function-level granularity is the right unit and it
// keeps the guard robust to how the branch is spelled.
var uafGateTokens = []string{
	"declineUAFPut(",                 // restactions-class gate
	"declineWidgetUAFPut(",           // widgets/widgetContent-class gate
	"uafDeclineReason(",              // the shared rule, when a site needs the reason
	"UAFTouchedSinkFromContext(",     // the observed limb read directly off ctx
	"BumpWidgetsUAFPutDeclined(",     // a site that counts its own decline
	"BumpRestactionsUAFPutDeclined(", // ditto
}

// uafStampTokens and uafCapTokens are the two halves of the STAMP+CAP
// disposition (#561). Both must appear in the enclosing function: a stamp
// without a cap derives nothing, and a cap without a stamp always derives 0.
var uafStampTokens = []string{
	"HasUAF = true",             // stamped from the observed sink
	"HasUAF = restactionHasUAF", // stamped from the declaration
}

var uafCapTokens = []string{
	"uafTTLOverrideForEntry(",
	// #592 moved THE derivation into package cache (cache.UAFTTLOverrideForEntry)
	// because the raFullList store Puts are a third caller in a package that
	// cannot import this one; the dispatchers-side name above is now a delegate.
	// Both spellings are the same rule, so both count as a cap.
	//
	// This does NOT by itself dispose of those two raFullList sites: they cap
	// here but are stamped by apiref.raFullListServe, a frame away, so they carry
	// an explicit `uaf-scope-waiver:` naming the caller and the arms that hold
	// it. The token is listed so a FUTURE in-package site using the cache-side
	// name is recognised as capped rather than silently reading as ungated.
	"UAFTTLOverrideForEntry(",
}

// uafWaiverPrefix mirrors scripts/checkresolvedentrysites' `scope-waiver:<field>:`
// convention. The trailing colon separates the marker from the MANDATORY reason.
const uafWaiverPrefix = "uaf-scope-waiver:"

// uafScanRoots are the prod trees to enumerate, relative to this package's
// directory (internal/handlers/dispatchers) — the same roots the CI invocation
// of scripts/checkresolvedentrysites passes.
var uafScanRoots = []string{"../../../internal", "../../../apis"}

type uafPutSite struct {
	file    string
	line    int
	fn      string
	gated   bool
	capped  bool   // stamps HasUAF AND sets TTLOverride (the #561 disposition)
	waiver  string // "" when absent
	waived  bool
	waivedE bool // waiver present but reason empty
}

// TestA1_EveryResolvedEntryPutSiteIsUAFGatedOrWaived is the structural guard.
func TestA1_EveryResolvedEntryPutSiteIsUAFGatedOrWaived(t *testing.T) {
	sites := collectUAFPutSites(t)

	// The enumeration must actually have found the sites — a walk that silently
	// matched nothing would pass vacuously, which is the failure mode this whole
	// test exists to prevent (feedback_falsifier_must_actually_run_under_gate_tag_env).
	const minExpectedSites = 10
	if len(sites) < minExpectedSites {
		t.Fatalf("VACUOUS GUARD: the AST walk found only %d ResolvedEntry Put site(s) under %v; expected at least %d. "+
			"Either the scan roots are wrong (this test runs from the package dir) or the literal shape changed — "+
			"fix the walk, do NOT lower the bound, or this guard silently stops guarding.",
			len(sites), uafScanRoots, minExpectedSites)
	}

	var ungated, emptyReason []string
	for _, s := range sites {
		switch {
		case s.gated:
			// covered by the gate in its enclosing function
		case s.capped:
			// covered by the #561 disposition: the narrowed body is cached
			// under a key that separates requesters, and capped so it cannot
			// outlive their RBAC.
		case s.waivedE:
			emptyReason = append(emptyReason, s.file+":"+itoaLine(s.line)+" ("+s.fn+")")
		case s.waived:
			// explicitly waived with a reason
		default:
			ungated = append(ungated, s.file+":"+itoaLine(s.line)+" ("+s.fn+")")
		}
	}

	if len(emptyReason) > 0 {
		t.Errorf("A-1 uaf-scope-waiver with an EMPTY reason at %d site(s):\n  %s\n\n"+
			"A bare marker is not a waiver. State WHY this cell cannot hold a per-requester-narrowed body "+
			"(for example: pre-refilter identity-free substrate, or unreachable because the caller bypasses).",
			len(emptyReason), strings.Join(emptyReason, "\n  "))
	}

	if len(ungated) > 0 {
		t.Errorf("A-1/R-1/#561: %d ResolvedEntry Put site(s) do none of: consult the UAF gate, stamp HasUAF AND cap "+
			"via uafTTLOverrideForEntry, or carry a `// uaf-scope-waiver: <reason>` annotation:\n  %s\n\n"+
			"Every L1 cell that can hold a userAccessFilter-narrowed body must either DECLINE the Put (identity-free "+
			"class: one cell is shared by every requester, so a narrowed body in it is read by everyone) or CAP it "+
			"(identity-bound class: the key folds the requester's binding set, so the body is their own — but it is "+
			"still narrowed per requester and must not outlive their RBAC).\n"+
			"THE R-1 LESSON: this is exactly the check that a hand-written list of files failed to make. The widgets "+
			"class carried the refiltered apiRef output all along (widgets/resolve.go folds it into status.widgetData) "+
			"and was omitted from the list because the author believed the comment that said it could not.\n"+
			"THE #561 LESSON: a cap derived from an unstamped HasUAF is always 0. Half a disposition is not a "+
			"disposition — stamp and cap, or decline, or write the waiver.",
			len(ungated), strings.Join(ungated, "\n  "))
	}
}

// collectUAFPutSites walks uafScanRoots and returns one entry per ResolvedEntry
// composite literal, annotated with whether its enclosing function consults the
// gate and whether a waiver sits on/above the literal.
func collectUAFPutSites(t *testing.T) []uafPutSite {
	t.Helper()
	var out []uafPutSite

	for _, root := range uafScanRoots {
		if _, err := os.Stat(root); err != nil {
			t.Fatalf("scan root %q not readable from the package dir: %v", root, err)
		}
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fset := token.NewFileSet()
			src, perr := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
			if perr != nil {
				return perr
			}
			raw, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			lines := strings.Split(string(raw), "\n")

			// line -> comment text, for the waiver lookup (go/ast does not
			// reliably attach a trailing comment to a composite literal).
			commentByLine := map[int]string{}
			for _, cg := range src.Comments {
				for _, c := range cg.List {
					ln := fset.Position(c.Pos()).Line
					commentByLine[ln] = strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
				}
			}

			for _, decl := range src.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				start := fset.Position(fn.Body.Pos()).Line
				end := fset.Position(fn.Body.End()).Line
				body := bodyText(lines, start, end)
				gated := false
				for _, tok := range uafGateTokens {
					if strings.Contains(body, tok) {
						gated = true
						break
					}
				}
				stamped, capped := false, false
				for _, tok := range uafStampTokens {
					if strings.Contains(body, tok) {
						stamped = true
						break
					}
				}
				for _, tok := range uafCapTokens {
					if strings.Contains(body, tok) {
						capped = true
						break
					}
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					lit, ok := n.(*ast.CompositeLit)
					if !ok || !isResolvedEntryLit(lit.Type) {
						return true
					}
					pos := fset.Position(lit.Pos())
					reason, waived := uafWaiverAt(commentByLine, pos.Line)
					out = append(out, uafPutSite{
						file:    path,
						line:    pos.Line,
						fn:      fn.Name.Name,
						gated:   gated,
						capped:  stamped && capped,
						waiver:  reason,
						waived:  waived,
						waivedE: waived && strings.TrimSpace(reason) == "",
					})
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %q: %v", root, err)
		}
	}
	return out
}

// bodyText joins the source lines spanning a function body (1-based, inclusive).
func bodyText(lines []string, start, end int) string {
	if start < 1 {
		start = 1
	}
	if end > len(lines) {
		end = len(lines)
	}
	if start > end {
		return ""
	}
	return strings.Join(lines[start-1:end], "\n")
}

// uafWaiverAt looks for the waiver on the literal's own line or on any of the
// three lines above it (a multi-line waiver comment sits above the literal).
func uafWaiverAt(commentByLine map[int]string, line int) (string, bool) {
	for _, ln := range []int{line, line - 1, line - 2, line - 3} {
		txt, ok := commentByLine[ln]
		if !ok {
			continue
		}
		if i := strings.Index(txt, uafWaiverPrefix); i >= 0 {
			return strings.TrimSpace(txt[i+len(uafWaiverPrefix):]), true
		}
	}
	return "", false
}

// isResolvedEntryLit mirrors scripts/checkresolvedentrysites' isResolvedEntry:
// the unqualified `ResolvedEntry` (inside package cache) or the qualified
// `cache.ResolvedEntry` (every dispatcher/resolver site).
func isResolvedEntryLit(tp ast.Expr) bool {
	switch e := tp.(type) {
	case *ast.Ident:
		return e.Name == "ResolvedEntry"
	case *ast.SelectorExpr:
		if e.Sel.Name != "ResolvedEntry" {
			return false
		}
		pkg, ok := e.X.(*ast.Ident)
		return ok && pkg.Name == "cache"
	}
	return false
}

func itoaLine(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
