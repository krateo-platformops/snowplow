// narrow_765_test.go — #603 PR-2 / #765 option B SHADOW, record-side arms.
//
// The REAL api.Resolve stage loop over a real synced informer (the #375
// harness) decides each list edge's interest at the end of the resolve:
//
//	R1  a clean, informer-served resolve narrows the edge to its read set;
//	R2  a JQ the analysis cannot prove (now) leaves it all;
//	R3  a chain that reads the whole item leaves it all;
//	R4  spec.filter is part of the chain: it can narrow a stage that is
//	    otherwise whole;
//	R5  a call that was not informer-served (an apiserver fall-through) or a
//	    stage error widens the edge for good, and a later clean resolve
//	    cannot narrow it back.
package api

import (
	"context"
	"go/ast"
	"go/types"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"golang.org/x/tools/go/packages"
	"k8s.io/client-go/rest"
)

func dk765() cache.DepKey { return cache.DepKey{GVR: f1WidgetsGVR, Namespace: "", Name: "*"} }

func resolve765(t *testing.T, key string, specFilter *string, stages ...*templates.API) {
	t.Helper()
	ctx := xcontext.BuildContext(cache.WithL1KeyContext(context.Background(), key),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: u375}))
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
	dict := Resolve(ctx, ResolveOptions{
		RC: &rest.Config{}, Items: stages, SpecFilter: specFilter,
		RESTActionNamespace: "default", RESTActionName: "c765",
	})
	if _, ok := dict["w"]; !ok {
		t.Fatalf("setup: stage w produced nothing: %v", dict)
	}
}

func stage765(filter string) *templates.API {
	st := stage375("w", f1WidgetsGVR)
	st.Filter = ptr.To(filter)
	return st
}

func TestNarrow765_R1_CleanInformerServedResolveNarrows(t *testing.T) {
	newWatcher375(t)
	const key = "n765-r1"
	resolve765(t, key, nil, stage765(`[.w.items[] | {name: .metadata.name}]`))
	got := cache.ListInterestStateForTest(key, dk765())
	if !strings.HasPrefix(got, "narrowed:") || strings.Contains(got, "spec") {
		t.Fatalf("R1 RED: a clean informer-served resolve whose chain reads only metadata.name must narrow "+
			"its list edge, got %q", got)
	}
}

func TestNarrow765_R2_UnprovableJQStaysAll(t *testing.T) {
	newWatcher375(t)
	const key = "n765-r2"
	resolve765(t, key, nil, stage765(`[.w.items[] | {name: .metadata.name, t: now}]`))
	if got := cache.ListInterestStateForTest(key, dk765()); got != "all" {
		t.Fatalf("R2 RED: an unprovable JQ (now) must leave the edge all, got %q", got)
	}
}

func TestNarrow765_R3_WholeItemStaysAll(t *testing.T) {
	newWatcher375(t)
	const key = "n765-r3"
	resolve765(t, key, nil, stage375("w", f1WidgetsGVR)) // filter ".w.items": the whole items
	if got := cache.ListInterestStateForTest(key, dk765()); got != "all" {
		t.Fatalf("R3 RED: a chain that reads the whole item must leave the edge all, got %q", got)
	}
}

func TestNarrow765_R4_SpecFilterIsPartOfTheChain(t *testing.T) {
	newWatcher375(t)
	const key = "n765-r4"
	resolve765(t, key, ptr.To(`[.w[] | .metadata.name]`), stage375("w", f1WidgetsGVR))
	got := cache.ListInterestStateForTest(key, dk765())
	if !strings.HasPrefix(got, "narrowed:") {
		t.Fatalf("R4 RED: spec.filter reads only metadata.name of a whole-items stage; the chain must narrow, got %q", got)
	}
}

// R7 — END TO END through #651 (architect-623): a UAF stage served from the
// informer whose resourcesFrom does not resolve is a served-but-FAILED stage
// (its feed returns nil, so the call reads as informer-served). The edge must
// stay all. The control (a resolving resourcesFrom) narrows, so the arm is not
// vacuous.
func TestNarrow765_R7_ServedButFailedUAFStageDoesNotNarrow(t *testing.T) {
	newWatcher375(t)
	orig := serviceAccountEndpointFn
	serviceAccountEndpointFn = func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: "http://test.invalid", Token: "sa-765"}, nil
	}
	t.Cleanup(func() { serviceAccountEndpointFn = orig })
	uafStage := func(resourcesFrom string) *templates.API {
		st := stage765(`[.w.items[] | {name: .metadata.name}]`)
		st.UserAccessFilter = &templates.UserAccessFilterSpec{
			Verb: "list", Group: f1WidgetsGVR.Group, ResourcesFrom: resourcesFrom, NamespaceFrom: ".metadata.namespace",
		}
		return st
	}
	run := func(key, resourcesFrom string) string {
		t.Helper()
		resolve765(t, key, nil, uafStage(resourcesFrom))
		return cache.ListInterestStateForTest(key, dk765())
	}
	if got := run("n765-r7-control", `["`+f1WidgetsGVR.Resource+`"]`); !strings.HasPrefix(got, "narrowed:") {
		t.Fatalf("R7 setup: a UAF stage with a resolving resourcesFrom must narrow (else the arm is vacuous), got %q", got)
	}
	if got := run("n765-r7-failed", "[ (.crds"); got != "all" {
		t.Fatalf("R7 RED: a served-but-failed UAF stage (#651: resourcesFrom does not resolve) narrowed its "+
			"edge (%q). The stage error must taint the resolve.", got)
	}
}

// R6 — STRUCTURAL: every stage-error sink bump in this package taints the
// resolve's narrowing. A stage error can put partial or fail-closed data into
// the output while its calls still read as informer-served (#651's
// served-but-failed stage), so an error site that bumps the sink without
// tainting would let that edge narrow. New error sites keep landing (#651,
// #760); a missing edge must read RED, not green (#533).
func TestNarrow765_R6_EveryStageErrorSiteTaintsTheNarrowing(t *testing.T) {
	// TYPE-BASED, not textual: every call of the method
	// (*cache.StageErrorSink).Bump in this package's production files,
	// whatever identifier, field, alias or closure it is reached through.
	// LIMIT (documented): a bump reached INDIRECTLY — the method passed as a
	// func value (sink.Bump as an argument) or a call from another package on
	// this resolve's sink — is not a call expression here and is not seen;
	// the guard also counts the sites (>= 4) so a refactor that moves the
	// bumps out of reach reads RED rather than vacuously green.
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
		packages.NeedTypes | packages.NeedTypesInfo, Tests: false}
	pkgs, err := packages.Load(cfg, ".")
	if err != nil || len(pkgs) != 1 || len(pkgs[0].Errors) > 0 {
		t.Fatalf("load package: %v %v", err, pkgs)
	}
	pkg := pkgs[0]
	isBump := func(e ast.Expr) bool {
		call, ok := e.(*ast.CallExpr)
		if !ok {
			return false
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Bump" {
			return false
		}
		fn, ok := pkg.TypesInfo.Uses[sel.Sel].(*types.Func)
		if !ok {
			return false
		}
		sig, ok := fn.Type().(*types.Signature)
		if !ok || sig.Recv() == nil {
			return false
		}
		return strings.HasSuffix(types.TypeString(sig.Recv().Type(), nil), "internal/cache.StageErrorSink")
	}
	isTaint := func(st ast.Stmt) bool {
		es, ok := st.(*ast.ExprStmt)
		if !ok {
			return false
		}
		call, ok := es.X.(*ast.CallExpr)
		if !ok {
			return false
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		return ok && sel.Sel.Name == "taintNarrow"
	}
	sites := 0
	for _, f := range pkg.Syntax {
		ast.Inspect(f, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && isBump(call) {
				sites++
			}
			blk, ok := n.(*ast.BlockStmt)
			if !ok {
				return true
			}
			for i, st := range blk.List {
				es, ok := st.(*ast.ExprStmt)
				if !ok || !isBump(es.X) {
					continue
				}
				if i == 0 || !isTaint(blk.List[i-1]) {
					t.Errorf("R6 RED: %s bumps the stage-error sink without r.taintNarrow() right before it: "+
						"an errored resolve could narrow its list edges", pkg.Fset.Position(es.Pos()))
				}
			}
			return true
		})
	}
	if sites < 4 {
		t.Fatalf("R6: found %d StageErrorSink.Bump calls, want >= 4 (the guard is not seeing the error sites)", sites)
	}
	// Every Bump call is a statement in a block (checked above); a Bump nested
	// in an expression would escape the adjacency rule, so forbid it.
	stmtBumps := 0
	for _, f := range pkg.Syntax {
		ast.Inspect(f, func(n ast.Node) bool {
			if blk, ok := n.(*ast.BlockStmt); ok {
				for _, st := range blk.List {
					if es, ok := st.(*ast.ExprStmt); ok && isBump(es.X) {
						stmtBumps++
					}
				}
			}
			return true
		})
	}
	if stmtBumps != sites {
		t.Fatalf("R6 RED: %d StageErrorSink.Bump calls, only %d are plain statements; a Bump inside an "+
			"expression escapes the taint rule", sites, stmtBumps)
	}
}

// R5 drives commitNarrow directly: what decides is how each call was served
// and whether the resolve was clean, not the dispatch machinery.
func TestNarrow765_R5_NotInformerServedOrStageErrorWidensForGood(t *testing.T) {
	newWatcher375(t)
	st := stage765(`[.w.items[] | {name: .metadata.name}]`)
	run := func(key string, served, dirty bool) string {
		t.Helper()
		r := &resolveRun{ctx: cache.WithL1KeyContext(context.Background(), key), opts: ResolveOptions{Items: []*templates.API{st}}}
		r.narrow = newNarrowRun(r)
		if r.narrow == nil {
			t.Fatalf("setup: no narrow run (cache off?)")
		}
		r.recordListEdge("w", key, f1WidgetsGVR, "")
		if served {
			r.noteNarrowServed("w", st.Path)
		}
		if dirty {
			r.taintNarrow()
		}
		r.commitNarrow()
		return cache.ListInterestStateForTest(key, dk765())
	}
	if got := run("n765-r5-ok", true, false); !strings.HasPrefix(got, "narrowed:") {
		t.Fatalf("R5 setup: a served clean resolve must narrow, got %q", got)
	}
	if got := run("n765-r5-fallthrough", false, false); got != "all" {
		t.Fatalf("R5 RED: a call that was not informer-served must leave the edge all, got %q", got)
	}
	if got := run("n765-r5-error", true, true); got != "all" {
		t.Fatalf("R5 RED: a resolve with a stage error must leave the edge all, got %q", got)
	}
	// Sticky: a later clean resolve of the same key never narrows it back.
	if got := run("n765-r5-fallthrough", true, false); got != "all" {
		t.Fatalf("R5 RED: a widened edge was narrowed again by a later resolve, got %q", got)
	}
	// A plain record (a replay) of a narrowed edge widens it.
	cache.Deps().ReplayEdges(context.Background(), "n765-r5-ok", []cache.DepKey{dk765()})
	if got := cache.ListInterestStateForTest("n765-r5-ok", dk765()); got != "all" {
		t.Fatalf("R5 RED: a replayed (plain) record must widen a narrowed edge, got %q", got)
	}
}
