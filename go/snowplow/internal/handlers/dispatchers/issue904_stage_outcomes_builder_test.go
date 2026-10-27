package dispatchers

// issue904_stage_outcomes_builder_test.go — #911 re-review: the stage-outcomes
// header of a cached answer depends on ONE constructor, not on hand-placed
// stamps.
//
//   B1  newResolveEntry keeps a restactions-class cell's outcomes ("" when every
//       stage succeeded) and nothing for any other class.
//   B2  census: in this package, StageOutcomes is set ONLY inside
//       newResolveEntry, and every restactions-class Put function that
//       resolves (the /call dispatcher, the cohort seed, the refresher) builds
//       its entry with it.
//
// Mutations (logic-only): newResolveEntry drops StageOutcomes → B1 RED and the
// stored-outcomes arms (W2, P1, P2) RED; a Put site stamps StageOutcomes by
// hand → B2 RED.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func TestIssue904_B1_NewResolveEntryKeepsRestactionsOutcomesOnly(t *testing.T) {
	o := api.NewStageOutcomesForTest([]string{"a", "w"}, map[string]string{"a": api.StageReasonError})
	want := o.HeaderValue()
	if want == "" || want == stageOutcomesAllSucceeded {
		t.Fatalf("PRE: the collector must report a failed stage, got %q", want)
	}
	in := &cache.ResolvedKeyInputs{CacheEntryClass: "restactions"}
	if got := newResolveEntry("restactions", []byte(`{}`), in, o).StageOutcomes; got != want {
		t.Fatalf("#911 RED: a restactions-class entry kept StageOutcomes %q, want %q", got, want)
	}
	for _, class := range []string{"widgets", cache.CacheEntryClassWidgetContent, cache.CacheEntryClassRAFullList} {
		if got := newResolveEntry(class, []byte(`{}`), &cache.ResolvedKeyInputs{CacheEntryClass: class}, o).StageOutcomes; got != "" {
			t.Fatalf("a %s entry kept StageOutcomes %q — only a restactions cell answers a RESTAction /call", class, got)
		}
	}
	ok := api.NewStageOutcomesForTest([]string{"a"}, nil)
	if got := newResolveEntry("restactions", []byte(`{}`), in, ok).StageOutcomes; got != "" {
		t.Fatalf("an all-succeeded resolve must keep \"\" (the header's []), got %q", got)
	}
}

func TestIssue904_B2_StageOutcomesAreSetOnlyByTheBuilder(t *testing.T) {
	ents, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var handStamped []string
	callers := map[string]bool{}
	for _, de := range ents {
		n := de.Name()
		if de.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), n, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(node ast.Node) bool {
				switch x := node.(type) {
				case *ast.KeyValueExpr:
					if k, ok := x.Key.(*ast.Ident); ok && k.Name == "StageOutcomes" && fd.Name.Name != "newResolveEntry" {
						handStamped = append(handStamped, n+":"+fd.Name.Name)
					}
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "StageOutcomes" {
							handStamped = append(handStamped, n+":"+fd.Name.Name)
						}
					}
				case *ast.CallExpr:
					if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "newResolveEntry" {
						callers[n+":"+fd.Name.Name] = true
					}
				}
				return true
			})
		}
	}
	if len(handStamped) > 0 {
		t.Fatalf("#911 RED: StageOutcomes stamped outside newResolveEntry: %v", handStamped)
	}
	for _, want := range []string{
		"restactions.go:ServeHTTP",
		"phase1_pip_seed.go:seedRestactionResolveAndPutProd",
		"resolve_populate.go:resolveAndPopulateL1",
	} {
		if !callers[want] {
			t.Errorf("#911 RED: %s no longer builds its entry with newResolveEntry (callers: %v)", want, callers)
		}
	}
}
