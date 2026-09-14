// Command checkresolvedentrysites is the L-SCOPE-COMPLETENESS CI guard
// (docs/test-blindspot-analysis-2026-07-24.md, class 3 — enumeration
// incompleteness). It machine-enforces the discipline that caused #118(d):
// "stamp ALL sites of a class, or none — never a named subset."
//
// THE DEFECT CLASS (#118d, memory project_118d_seed_put_uaf_ttl_gap): a fix
// that must apply at EVERY `ResolvedEntry{` Put site was applied at the fix's
// NAMED subset. #118(d) stamped the short UAF TTLOverride at the dispatch +
// refresher Put but MISSED the seed Put (phase1_pip_seed.go) — boot-seeded UAF
// cells stayed governed by the long standard TTL. The site map that documents
// which Put sites are in/out of scope (uaf_shortttl.go R-d-4 SITE MAP) is
// HAND-maintained: a NEW `ResolvedEntry{` literal added later is silently
// absent from it. This gate converts that silent enumeration gap into a hard
// CI failure.
//
// TWO SITE-CLASSES, two invariants (both AST-based, not regex):
//
//  1. ResolvedEntry PUT sites — every `ResolvedEntry{...}` / `cache.
//     ResolvedEntry{...}` composite literal in the scanned prod tree must
//     EITHER set the per-entry metadata field TTLOverride as a keyed element,
//     OR carry an inline `//scope-waiver:TTLOverride: <reason>` annotation
//     (on the literal's line or the line immediately above). A site that does
//     neither is FLAGGED — it is a Put that neither participates in the
//     bounded-TTL discipline nor documents WHY it is exempt. This is exactly
//     "stamp all sites of a class or carry an explicit inline waiver": the
//     out-of-scope sites (widgets / widgetContent / raFullList / seedOneWidget
//     — reasoned out in the R-d-4 SITE MAP) carry the waiver; the in-scope
//     restactions sites set the field. A future Put site fails the gate
//     until its author consciously sets the field or writes the waiver.
//
//  2. Boot-scope STAMP sites — the readiness-critical boot-prewarm context
//     builders each stamp `cache.WithFallthroughScope(ctx, cache.
//     ScopeBootPrewarm*)`. A dropped stamp silently changes serve-behaviour
//     on the boot path (the RAFullList 4a-pin gate keys on the scope). The
//     gate counts the ScopeBootPrewarm* stamp sites and fails if the count
//     falls below the committed floor (bootScopeStampFloor) — a removed stamp
//     becomes a hard failure that must be consciously re-baselined.
//
// WAIVER FORMAT (explicit, greppable, reason-required):
//
//	c.Put(key, &cache.ResolvedEntry{ // scope-waiver:TTLOverride: identity-free substrate, data-plane TTL only
//	    ...
//	})
//
//	or on the line above the literal. The reason text after the second colon
//	is mandatory (a bare `//scope-waiver:TTLOverride:` with no reason is
//	itself flagged) so a waiver is never a silent opt-out.
//
// USAGE:
//
//	checkresolvedentrysites <root> [<root>...]
//
// Exits 0 when every ResolvedEntry literal sets TTLOverride or carries a
// waiver AND the boot-scope stamp count is at/above the floor; exits 1 and
// prints file:line for each offending site otherwise.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// enforcedEntryField is the per-entry metadata field the ResolvedEntry
// completeness invariant is keyed on. TTLOverride is the only field on the
// ResolvedEntry struct itself that carries per-entry bounded-staleness
// metadata (HasUAF / RBACSubGen live on the *ResolvedKeyInputs the entry
// carries, folded at the Inputs-construction sites, not on the entry literal).
// The waiver annotation names this field explicitly so a future second
// enforced field gets its own waiver namespace.
const enforcedEntryField = "TTLOverride"

// entryTypeName is the composite-literal type the gate enumerates. Both the
// unqualified form (inside package cache) and the qualified `cache.ResolvedEntry`
// (every dispatcher/resolver site) are matched.
const entryTypeName = "ResolvedEntry"

// waiverPrefix is the inline annotation that exempts a ResolvedEntry literal
// from the TTLOverride requirement. The trailing ":" separates the field from
// the mandatory reason: `//scope-waiver:TTLOverride: <reason>`.
const waiverPrefix = "scope-waiver:" + enforcedEntryField + ":"

// bootScopeStampFloor is the committed minimum number of
// `WithFallthroughScope(..., cache.ScopeBootPrewarm*)` stamp sites in the
// scanned prod tree. The three readiness-critical boot-prewarm context
// builders each stamp exactly once:
//
//	phase1_walk.go        — the discovery walk (ScopeBootPrewarmWalk)
//	phase1_pip_seed.go    — the per-cohort seed (ScopeBootPrewarmSeed)
//	phase1_content_prewarm.go — the content prewarm (ScopeBootPrewarmWalk)
//
// A stamp removed from any of them drops the count below the floor and fails
// the gate — a conscious re-baseline (edit this constant) is required to
// remove a boot-scope stamp, exactly the "don't silently drop a
// readiness-critical scope" discipline this gate exists for.
const bootScopeStampFloor = 3

type entryFinding struct {
	file string
	line int
	kind string // "missing-field-or-waiver" | "empty-waiver-reason"
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: checkresolvedentrysites <root> [<root>...]")
		os.Exit(2)
	}

	fset := token.NewFileSet()
	var entryFindings []entryFinding
	var scanned, entrySites, bootScopeStamps int

	for _, root := range os.Args[1:] {
		// #nosec G304,G703 -- dev-time AST lint tool; the walked path is a filepath.WalkDir result under the CLI-arg target dir (checker's own source tree), not untrusted/network input. gosec's SSA taint analyzer flags the path→parser.ParseFile flow; the `#nosec` (not `//nosec`) form is the one gosec master honors.
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				base := d.Name()
				// Never descend into worktrees / vendored copies / testdata —
				// the RED-arm fixture lives under testdata and would otherwise
				// fail the real gate run. testdata is scanned only by the
				// self-test (a separate explicit root argument).
				if base == ".claude" || base == "vendor" || base == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			scanned++
			ef, es, bs, perr := checkFile(fset, path)
			if perr != nil {
				return perr
			}
			entryFindings = append(entryFindings, ef...)
			entrySites += es
			bootScopeStamps += bs
			return nil
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "checkresolvedentrysites: walk %s: %v\n", root, err)
			os.Exit(2)
		}
	}

	var failed bool

	if len(entryFindings) > 0 {
		failed = true
		sort.Slice(entryFindings, func(i, j int) bool {
			if entryFindings[i].file != entryFindings[j].file {
				return entryFindings[i].file < entryFindings[j].file
			}
			return entryFindings[i].line < entryFindings[j].line
		})
		fmt.Fprintf(os.Stderr, "ERROR: %d ResolvedEntry Put site(s) neither set %s nor carry a %s waiver.\n",
			len(entryFindings), enforcedEntryField, enforcedEntryField)
		fmt.Fprintf(os.Stderr, "       This is the #118(d) enumeration-incompleteness class: a per-entry metadata\n")
		fmt.Fprintf(os.Stderr, "       field applied at a NAMED SUBSET of the ResolvedEntry Put sites instead of\n")
		fmt.Fprintf(os.Stderr, "       ALL of them (the boot-seed Put was the missed site). Every Put site must\n")
		fmt.Fprintf(os.Stderr, "       either set %s or document WHY it is exempt.\n\n", enforcedEntryField)
		for _, f := range entryFindings {
			switch f.kind {
			case "empty-waiver-reason":
				fmt.Fprintf(os.Stderr, "  %s:%d  %s{...} has a %s waiver with NO reason — the reason after the second colon is mandatory\n",
					f.file, f.line, entryTypeName, enforcedEntryField)
			default:
				fmt.Fprintf(os.Stderr, "  %s:%d  %s{...} does not set %s and has no `//%s <reason>` annotation\n",
					f.file, f.line, entryTypeName, enforcedEntryField, waiverPrefix)
			}
		}
		fmt.Fprintln(os.Stderr, "\n       Either stamp the field at the construction site (uafTTLOverrideForEntry(inputs)")
		fmt.Fprintln(os.Stderr, "       for a restactions cell), or add an inline waiver naming why this Put is exempt:")
		fmt.Fprintf(os.Stderr, "         &%s{ // %s <reason it carries no bounded TTL>\n", entryTypeName, waiverPrefix)
	}

	if bootScopeStamps < bootScopeStampFloor {
		failed = true
		fmt.Fprintf(os.Stderr, "\nERROR: only %d WithFallthroughScope(..., cache.ScopeBootPrewarm*) stamp site(s) found; floor is %d.\n",
			bootScopeStamps, bootScopeStampFloor)
		fmt.Fprintf(os.Stderr, "       A readiness-critical boot-prewarm scope stamp was removed. The RAFullList 4a-pin\n")
		fmt.Fprintf(os.Stderr, "       serve gate (apiref.shouldServeRAFullList) keys on this scope — a dropped stamp\n")
		fmt.Fprintf(os.Stderr, "       silently changes boot-path serve behaviour. Re-add the stamp, or if the removal\n")
		fmt.Fprintf(os.Stderr, "       is intentional, consciously lower bootScopeStampFloor in this checker with a note.\n")
	}

	if failed {
		os.Exit(1)
	}

	fmt.Printf("OK: %d ResolvedEntry Put site(s) all set %s or carry a waiver; %d boot-scope stamp site(s) (floor %d); %d files scanned.\n",
		entrySites, enforcedEntryField, bootScopeStamps, bootScopeStampFloor, scanned)
}

// checkFile parses one Go file and returns:
//   - findings for every ResolvedEntry literal missing both the field and a
//     valid waiver;
//   - the count of ResolvedEntry Put sites seen;
//   - the count of WithFallthroughScope(..., ScopeBootPrewarm*) stamp sites.
func checkFile(fset *token.FileSet, path string) ([]entryFinding, int, int, error) {
	src, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution) //nosec G304,G703 -- dev-time AST lint tool; path is a filepath.WalkDir result under the CLI-arg target dir, not untrusted input
	if err != nil {
		return nil, 0, 0, fmt.Errorf("parse %s: %w", path, err)
	}

	// Build a line -> comment-text index so a waiver on the literal's line or
	// the line immediately above is found without depending on go/ast's
	// comment association (which does not attach a trailing `// ...` to a
	// composite literal element reliably).
	commentByLine := map[int]string{}
	for _, cg := range src.Comments {
		for _, c := range cg.List {
			ln := fset.Position(c.Pos()).Line
			commentByLine[ln] = strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
		}
	}

	var findings []entryFinding
	var entrySites, bootScopeStamps int

	ast.Inspect(src, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CompositeLit:
			if !isResolvedEntry(node.Type) {
				return true
			}
			entrySites++
			pos := fset.Position(node.Pos())
			if hasKeyedField(node, enforcedEntryField) {
				return true
			}
			// No field set — look for a waiver on this line or the one above.
			reason, waived := waiverReason(commentByLine, pos.Line)
			if !waived {
				findings = append(findings, entryFinding{file: pos.Filename, line: pos.Line, kind: "missing-field-or-waiver"})
				return true
			}
			if strings.TrimSpace(reason) == "" {
				findings = append(findings, entryFinding{file: pos.Filename, line: pos.Line, kind: "empty-waiver-reason"})
			}
			return true

		case *ast.CallExpr:
			if isBootScopeStamp(node) {
				bootScopeStamps++
			}
			return true
		}
		return true
	})

	return findings, entrySites, bootScopeStamps, nil
}

// isResolvedEntry reports whether a composite-literal type is ResolvedEntry
// (unqualified, inside package cache) or cache.ResolvedEntry (qualified). The
// qualifier is matched by the identifier name "cache" — the only package in
// the module that exports ResolvedEntry, so no import-path resolution is
// needed for this single closed type.
func isResolvedEntry(t ast.Expr) bool {
	switch e := t.(type) {
	case *ast.Ident:
		return e.Name == entryTypeName
	case *ast.SelectorExpr:
		if e.Sel.Name != entryTypeName {
			return false
		}
		pkg, ok := e.X.(*ast.Ident)
		return ok && pkg.Name == "cache"
	}
	return false
}

// isBootScopeStamp reports whether a call is
// (cache.)WithFallthroughScope(ctx, (cache.)ScopeBootPrewarm*). It matches on
// the callee name WithFallthroughScope AND a ScopeBootPrewarm-prefixed
// argument, so an ordinary /call-class WithFallthroughScope (a request path,
// not boot) is NOT counted — only the readiness-critical boot stamps.
func isBootScopeStamp(call *ast.CallExpr) bool {
	if !calleeNameIs(call.Fun, "WithFallthroughScope") {
		return false
	}
	for _, arg := range call.Args {
		if sel, ok := arg.(*ast.SelectorExpr); ok && strings.HasPrefix(sel.Sel.Name, "ScopeBootPrewarm") {
			return true
		}
		if id, ok := arg.(*ast.Ident); ok && strings.HasPrefix(id.Name, "ScopeBootPrewarm") {
			return true
		}
	}
	return false
}

// calleeNameIs reports whether a call's function expression names fn, whether
// called bare (fn(...)) or package-qualified (pkg.fn(...)).
func calleeNameIs(fun ast.Expr, fn string) bool {
	switch e := fun.(type) {
	case *ast.Ident:
		return e.Name == fn
	case *ast.SelectorExpr:
		return e.Sel.Name == fn
	}
	return false
}

// hasKeyedField reports whether the composite literal has a keyed element
// `field: ...`. ResolvedEntry is always constructed with keyed fields in this
// repo; a positional literal is itself a red flag and is treated as "field
// absent".
func hasKeyedField(cl *ast.CompositeLit, field string) bool {
	for _, el := range cl.Elts {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if key, ok := kv.Key.(*ast.Ident); ok && key.Name == field {
			return true
		}
	}
	return false
}

// waiverReason returns (reason, true) if a `scope-waiver:<field>:` annotation
// is present on the literal's line or the line immediately above. The reason
// is the text after the second colon (may be empty — the caller flags an
// empty reason).
func waiverReason(commentByLine map[int]string, litLine int) (string, bool) {
	for _, ln := range []int{litLine, litLine - 1} {
		txt, ok := commentByLine[ln]
		if !ok {
			continue
		}
		// A trailing comment may sit after code on the literal's line; a
		// full-line comment sits on the line above. Both land in
		// commentByLine keyed by their own line, so a simple Contains is
		// sufficient and robust to the leading code text.
		if i := strings.Index(txt, waiverPrefix); i >= 0 {
			return strings.TrimSpace(txt[i+len(waiverPrefix):]), true
		}
	}
	return "", false
}
