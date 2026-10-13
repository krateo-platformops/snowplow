package cache

// issue643_stored_entry_inplace_guard_test.go — #643 PR-C (architect-ch C1):
// the resume rule "the stored entry is still the Put this pass made" is judged
// by pointer identity + putSeq. That is exact only while NO production code
// changes a stored *ResolvedEntry's BODY or DEPS in place. This guard pins the
// classification: across every production package under internal/, the body /
// deps fields of a ResolvedEntry are never assigned through a selector (only in
// a composite literal, i.e. a NEW entry), and their contents are never written
// through an index. Metadata fields written in place are allow-listed by name,
// each with its reason; a new in-place write of any other field fails here.
//
// Typed (go/packages), so it matches on the field's owning type, not a name.

import (
	"go/ast"
	"go/types"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// Fields that carry the cell's BODY or its DEPS (identity inputs): never
// written in place.
var resolvedEntryBodyFields = map[string]bool{"RawJSON": true, "Inputs": true}

// Metadata fields that ARE written on an entry pointer, with why each is not a
// body/deps change. All run on the INCOMING entry inside the Put path (before
// or during putCoreLocked, under c.mu where noted) or are lazily-derived views.
var resolvedEntryMetadataInPlace = map[string]string{
	"CreatedAt":       "putPreamble stamps the incoming entry's write time",
	"BornAt":          "putPreamble / ReplaceIfGenRefresh / putCoreLocked lifetime clock (incoming entry); SetBornAtForTest",
	"Pinned":          "putCoreLocked demotes a pin request on the incoming entry",
	"contentVersion":  "#406 raLayerCommitLocked versions the incoming RAFullList entry",
	"hitters":         "NoteHitter: the representative pool, metadata",
	"items":           "EnsureItems: lazily-derived parse of RawJSON, published once",
	"itemsAPIVersion": "EnsureItems: derived",
	"itemsKind":       "EnsureItems: derived",
	// Set on a NEW entry (built by a composite literal a few lines above) before
	// its first Put — apistage.go content fill and cluster_list.go populate:
	"TTLOverride":     "the R1 Layer-2 catalog-unservable TTL, stamped on the new entry before its PutIfGen",
	"Items":           "the parsed LIST view of the new entry's own RawJSON, attached before its PutIfGen (derived)",
	"ItemsAPIVersion": "derived with Items, before the Put",
	"ItemsKind":       "derived with Items, before the Put",
}

func TestIssue643_StoredEntryBodyIsNeverMutatedInPlace(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  "../..",
	}
	pkgs, err := packages.Load(cfg, "./internal/...")
	if err != nil {
		t.Fatalf("packages.Load: %v", err)
	}
	isEntry := func(tv types.Type) bool {
		if p, ok := tv.(*types.Pointer); ok {
			tv = p.Elem()
		}
		n, ok := tv.(*types.Named)
		return ok && n.Obj().Name() == "ResolvedEntry" && n.Obj().Pkg() != nil &&
			strings.HasSuffix(n.Obj().Pkg().Path(), "/internal/cache")
	}
	var bad, unknown []string
	seenMeta := map[string]bool{}
	for _, p := range pkgs {
		for _, f := range p.Syntax {
			fname := p.Fset.Position(f.Pos()).Filename
			if strings.HasSuffix(fname, "_test.go") {
				continue
			}
			check := func(lhs ast.Expr) {
				// Unwrap an index write (e.RawJSON[i] = …) to its field selector.
				for {
					if ix, ok := lhs.(*ast.IndexExpr); ok {
						lhs = ix.X
						continue
					}
					break
				}
				sel, ok := lhs.(*ast.SelectorExpr)
				if !ok {
					return
				}
				tv, ok := p.TypesInfo.Types[sel.X]
				if !ok || !isEntry(tv.Type) {
					return
				}
				pos := p.Fset.Position(sel.Pos())
				field := sel.Sel.Name
				switch {
				case resolvedEntryBodyFields[field]:
					bad = append(bad, pos.String()+" writes ResolvedEntry."+field+" in place")
				case resolvedEntryMetadataInPlace[field] != "":
					seenMeta[field] = true
				default:
					unknown = append(unknown, pos.String()+" writes ResolvedEntry."+field+" in place (unclassified)")
				}
			}
			ast.Inspect(f, func(n ast.Node) bool {
				switch s := n.(type) {
				case *ast.AssignStmt:
					for _, l := range s.Lhs {
						check(l)
					}
				case *ast.IncDecStmt:
					check(s.X)
				}
				return true
			})
		}
	}
	sort.Strings(bad)
	sort.Strings(unknown)
	for _, b := range bad {
		t.Errorf("BODY/DEPS MUTATED IN PLACE: %s — the #643 resume trusts a stored entry's pointer + putSeq as the Put it made; an in-place body/deps write breaks that. Build a new entry and Put it instead.", b)
	}
	for _, u := range unknown {
		t.Errorf("UNCLASSIFIED in-place write: %s — classify it in resolvedEntryMetadataInPlace (metadata, with a reason) or stop writing it in place.", u)
	}
	if len(seenMeta) == 0 {
		t.Fatal("the scan saw no in-place metadata write at all — the guard is not reaching the store code (vacuous)")
	}
}
