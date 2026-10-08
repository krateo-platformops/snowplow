// inline443_enum_test.go — #443 part 2, the STRUCTURAL inertness guard.
//
// An inline dry-run resolve persists nothing because one ctx flag
// (cache.Inert) is honoured by every side-effecting cache entry point the
// resolve path can reach. A name deny-list cannot keep that true (reviewer-424
// bypassed one four ways: an unlisted persisting helper, a goroutine on
// context.TODO, a detached ctx assigned before the `go`, and an unlisted
// replay). So this guard is an ALLOW-LIST:
//
//  1. It type-checks the resolve-path code (internal/resolvers/...,
//     internal/objects/..., handlers/dispatchers/nested_call.go) and collects
//     EVERY internal/cache function or method it references. Each one must be
//     classified in cacheAllow443, or the test fails. A new cache call on the
//     resolve path therefore cannot land unclassified.
//  2. A symbol classed gatedInCache must itself contain a cache.Inert check in
//     its body (verified by parsing internal/cache), so the gate travels with
//     the function, not with a caller.
//  3. A symbol classed gatedAtCaller may only be used inside functions listed
//     for it, and each of those functions must contain a cache.Inert check.
//  4. EVERY `go` statement in scope must be listed in goAllow443 (a goroutine
//     can drop the flag however its ctx is built: context.TODO, Background, a
//     variable assigned before the `go`). Listed goroutines that reach the
//     cache must be gated in their enclosing function.
//
// Functions named *ForTest (test helpers compiled into production files) are
// out of scope.
package dispatchers

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const cachePkgPath443 = "github.com/krateo-platformops/snowplow/internal/cache"

type cacheClass443 int

const (
	// readOnly443: no state outlives the request (pure reads, ctx builders,
	// request-scoped sinks, client construction).
	readOnly443 cacheClass443 = iota
	// telemetry443: process counters or one-shot log tickers only; no cache,
	// informer, dep or refresher state.
	telemetry443
	// clusterFact443: identity-free caches keyed by cluster facts (the
	// permanent plurals/kind store), allowed by design #443 (m).
	clusterFact443
	// gatedInCache443: side-effecting; the cache function's own body checks
	// cache.Inert (verified below).
	gatedInCache443
	// gatedAtCaller443: side-effecting and ctx-less; every enclosing function
	// that uses it must be listed in callerAllow443 and contain a cache.Inert
	// check.
	gatedAtCaller443
	// unreachableInert443: reached only on a path the flag already closes
	// (documented per entry).
	unreachableInert443
)

var cacheAllow443 = map[string]cacheClass443{
	// --- side-effecting, gated inside the cache package ---------------------
	"DepTracker.Record":                      gatedInCache443,
	"DepTracker.RecordList":                  gatedInCache443,
	"DepTracker.ReplayEdges":                 gatedInCache443,
	"ResolvedCacheStore.PutIfGen":            gatedInCache443,
	"ResolvedCacheStore.PutRAFullListIfGen":  gatedInCache443,
	"ResolvedCacheStore.NoteRAFullListSlice": gatedInCache443,
	"ResourceWatcher.EnsureResourceTypeFor":  gatedInCache443,
	"RecordSliceabilityClassifiedCtx":        gatedInCache443,
	"SliceabilityLookupCtx":                  gatedInCache443,
	// #398 decline: always returns true on a sensitive read (inert or not), so
	// the decline is never hidden by the flag; only its counter is gated.
	"DeclineSensitivePut": gatedInCache443,
	// --- side-effecting, gated at the (listed) caller -----------------------
	"ResolvedCacheStore.Get":                      gatedAtCaller443, // warmth stamp → GetNoTouch under the flag
	"ResolvedEntry.NoteHitter":                    gatedAtCaller443, // representative pool
	"ResolvedCacheStore.ForgetRAFullListConsumer": gatedAtCaller443,
	"AddNavigationDiscoveredGroup":                gatedAtCaller443,
	"BumpExternalSkippedPut":                      gatedAtCaller443, // a Put-decline counter: no decline without a Put
	// --- unreachable under the flag -----------------------------------------
	"DepTracker.ReplayEdgesAsOf":          unreachableInert443, // seed-memo hit only (SeedResolveMemo is installed by seeds); delegates to the gated ReplayEdges
	"SeedResolveMemo.StoreLayered":        unreachableInert443, // memo installed only by seeds, never on a live/inline ctx
	"RegisterClusterListKey":              unreachableInert443, // after a PutIfGen that refuses under the flag
	"ResourceWatcher.ConfirmResourceType": unreachableInert443, // only when EnsureResourceTypeFor reported added=true (never under the flag)
	// via the package-level discoverGroupResourcesFn seam, invoked only inside
	// lazyRegisterInnerCallPaths (returns at its head under the flag, (c)).
	"DiscoverGroupResources": unreachableInert443,
	// --- cluster facts (design (m)) -----------------------------------------
	"GVRFor":     clusterFact443,
	"KindForGVR": clusterFact443,
	// --- telemetry ------------------------------------------------------------
	"BumpRAFullListUAFBypass":           telemetry443,
	"BumpUAFTouched":                    telemetry443, // request-scoped UAF-touched sink
	"RecordApiserverFallthrough":        telemetry443,
	"RecordClusterListCellColdFallback": telemetry443,
	"RecordClusterListCellWarm":         telemetry443,
	// #563 — RecordRefDenied replaces RecordPrewarmRefDenied at the single
	// resourceRef-denial call site: one counter keyed by the resolve's driver
	// (refresher / cohort-seed / prewarm-engine-boot / prewarm-path /
	// background-unattributed / serve) instead of a prewarm-only scalar. Process
	// counters only, no cache/informer/dep/refresher state — same class as the
	// scalar it replaces.
	"RecordRefDenied":                           telemetry443,
	"RecordRAFullListServe":                     telemetry443,
	"RecordResolverPluralsHit":                  telemetry443,
	"RecordResolverPluralsMiss":                 telemetry443,
	"PIPStageTimingSink.AccumulateContentServe": telemetry443,
	"PIPStageTimingSink.AccumulateDefensive":    telemetry443,
	"PIPStageTimingSink.BeginStage":             telemetry443,
	"PIPStageTimingSink.EndStage":               telemetry443,
	// --- read-only / request-scoped -------------------------------------------
	"AncestorsHeaderValue": readOnly443, "ApistageContentResolveFromContext": readOnly443,
	"ApistageL1Enabled": readOnly443, "ApistagePrewarmFromContext": readOnly443,
	"BackgroundResolveFromContext": readOnly443, "CatalogUnservableTTL": readOnly443,
	"ClientConfigFor": readOnly443, "ComputeKey": readOnly443, "DepGenEpochNow": readOnly443,
	"DepGenStartSeqFromContext": readOnly443, "DepTracker.BeginCapture": readOnly443,
	"DepTracker.EndCapture": readOnly443, "DepTracker.EdgesUnder": readOnly443, "Deps": readOnly443,
	"Disabled": readOnly443, "ExternalTouchedSink.Bump": readOnly443, "ExternalTouchedSink.Count": readOnly443,
	"ExternalTouchedSinkFromContext": readOnly443, "ExtractAPIServerGroupFromTemplatedPath": readOnly443,
	"FallthroughScope": readOnly443, "FullListIsEmpty": readOnly443, "Global": readOnly443,
	"GoSliceFullList": readOnly443, "HashExtras": readOnly443, "IdentityClass.String": readOnly443,
	"Inert": readOnly443, "InformerOnlyReadsFromContext": readOnly443, "InternalEndpointFromContext": readOnly443,
	"WithInformerOnlyReads":         readOnly443, // #403: ctx builder for the informer-only UAF pre-check read
	"InternalRESTConfigFromContext": readOnly443, "IsResolverGVRHit": readOnly443, "IsResolverPluralsHit": readOnly443,
	"IsStructurallyNonSliceable": readOnly443, "L1KeyFromContext": readOnly443, "LayeredSourcesMark": readOnly443,
	"LayeredSourcesSince": readOnly443, "LiveRBACSnapshot": readOnly443, "NestedCallDepthFromContext": readOnly443,
	"NestedCallMaxDepth": readOnly443, "NestedResolveAncestorPresent": readOnly443,
	"PIPStageTimingSinkFrom": readOnly443, "ParseAPIServerDiscoveryPath": readOnly443,
	"ParseAPIServerDiscoveryRoot": readOnly443, "ParseAPIServerListDepSkeleton": readOnly443,
	"ParseAPIServerPathToDep": readOnly443, "ParseAPIServerPathToGVR": readOnly443, "PrewarmEnabled": readOnly443,
	// #504 — the read-set skeleton sibling and its three-state name. Both are
	// PURE: a string in, a value out, no cache/informer/dep/refresher state and
	// nothing that outlives the request. Classified next to the sibling parsers
	// above (ParseAPIServerListDepSkeleton in particular) because they share the
	// skeletonizer and differ only in policy.
	"ReadSetSkeleton": readOnly443, "NameKind.IsSingleObject": readOnly443,
	"PrewarmIterSerialFromContext": readOnly443,
	// #563 — pure ctx classification (which driver is resolving); reads three
	// context values and returns a constant string. It SUBSUMES
	// PrewarmPathFromContext, which was listed here for the resourcesrefs denial
	// gate and has no resolve-path caller any more: the prewarm-vs-background
	// question now has one answer, inside the cache package, so the resolver
	// cannot drift out of half of it again (which is #563 itself). The stale-entry
	// half of this census is what required removing the row.
	"RefDenialOrigin":     readOnly443,
	"RAFullListKeyInputs": readOnly443, "RefreshTriggerGVRFromContext": readOnly443, "RefreshTriggerHas": readOnly443,
	"ReplayRAFullListSlices": readOnly443, "ResolvedCache": readOnly443, "ResolvedCacheEnabled": readOnly443,
	"ResolvedCacheStore.CaptureGen": readOnly443, "ResolvedCacheStore.GetNoTouch": readOnly443,
	"ResourceWatcher.GetObject": readOnly443, "ResourceWatcher.IsMetadataOnly": readOnly443,
	"ResourceWatcher.IsPassthrough": readOnly443, "ResourceWatcher.IsServable": readOnly443,
	"ResourceWatcher.IsSynced": readOnly443, "ResourceWatcher.ListObjectsServable": readOnly443,
	"ResourceWatcher.ListServableEnvelopeJSON": readOnly443, "ResourceWatcher.ServabilitySnapshotFor": readOnly443,
	"ResourceWatcher.Snapshot": readOnly443, "ResourceWatcher.WaitForGVRSync": readOnly443,
	"SecretsCacheNamespace": readOnly443, "SecretsCacheServable": readOnly443, "SecretsSnapshotLoad": readOnly443,
	"SeedResolveMemo.Key": readOnly443, "SeedResolveMemo.LoadLayered": readOnly443,
	"SeedResolveMemoFromContext": readOnly443, "ServeWatcherFromContext": readOnly443,
	"ServiceAccountDialFromContext": readOnly443, "SliceShapeHash": readOnly443,
	"SliceabilityShapeKnownNegative": readOnly443, "StageErrorSink.Bump": readOnly443,
	"StageErrorSinkFromContext": readOnly443, "WithApistageContentResolve": readOnly443,
	"WithContentDepGenSink": readOnly443, "WithInternalEndpoint": readOnly443,
	"WithInternalRESTConfig": readOnly443, "WithL1KeyContextFromEpoch": readOnly443,
	"WithNestedCallDepth": readOnly443, "WithNestedResolveAncestor": readOnly443, "WithServiceAccountDial": readOnly443,
	// #398 (#440): a pure GVR classifier and the request-scoped sensitive-touched sink.
	"IsSensitiveResource": readOnly443, "SensitiveTouchedSink.Bump": readOnly443,
	"SensitiveTouchedSink.Count": readOnly443, "SensitiveTouchedSinkFromContext": readOnly443,
	"WithSensitiveTouchedSink": readOnly443,
}

// callerAllow443: for each gatedAtCaller symbol, the "<file>:<func>" sites
// allowed to use it. Each listed function must contain a cache.Inert check.
var callerAllow443 = map[string][]string{
	"ResolvedCacheStore.Get":                      {"apistage.go:apistageContentServe", "ra_full_list.go:raFullListServe"},
	"ResolvedEntry.NoteHitter":                    {"ra_full_list.go:raFullListServe"},
	"ResolvedCacheStore.ForgetRAFullListConsumer": {"ra_full_list.go:raFullListServe"},
	"AddNavigationDiscoveredGroup":                {"resolve.go:lazyRegisterInnerCallPaths"},
	"BumpExternalSkippedPut":                      {"ra_full_list.go:raFullListServe"},
}

// goAllow443: every `go` statement on the resolve path, by "<file>:<func>",
// and whether its enclosing function must carry a cache.Inert check.
var goAllow443 = map[string]bool{
	"cluster_list.go:populateClusterListCellAsync":      true,  // (d): returns before the detached populate under the flag
	"nested_resolve_bound.go:waitForReleaseOrCtx":       false, // cond.Broadcast on ctx.Done: no cache access
	"informer_dispatch_metrics.go:startDispatchSummary": false, // once-per-process summary log ticker
	"informer_serve.go:startObjectsGetSummary":          false, // once-per-process summary log ticker
}

func symName443(obj *types.Func) string {
	name := obj.Name()
	if sig, _ := obj.Type().(*types.Signature); sig != nil && sig.Recv() != nil {
		rt := sig.Recv().Type()
		if pt, ok := rt.(*types.Pointer); ok {
			rt = pt.Elem()
		}
		if nt, ok := rt.(*types.Named); ok {
			name = nt.Obj().Name() + "." + name
		}
	}
	return name
}

func containsInert443(info *types.Info, body ast.Node) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			var id *ast.Ident
			switch f := call.Fun.(type) {
			case *ast.SelectorExpr:
				id = f.Sel
			case *ast.Ident:
				id = f
			}
			if id != nil && id.Name == "Inert" {
				if obj, ok := info.Uses[id].(*types.Func); ok && obj.Pkg() != nil && obj.Pkg().Path() == cachePkgPath443 {
					found = true
				}
			}
		}
		return !found
	})
	return found
}

func TestS443_InertEnumerationGuard(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
			packages.NeedTypes | packages.NeedTypesInfo,
		Dir: "../../..",
	}
	pkgs, err := packages.Load(cfg,
		"./internal/resolvers/...", "./internal/objects/...", "./internal/handlers/dispatchers", "./internal/cache")
	if err != nil {
		t.Fatalf("packages.Load: %v", err)
	}

	// (2) the cache package's own gated functions.
	gatedBody := map[string]bool{}
	for _, p := range pkgs {
		if p.PkgPath != cachePkgPath443 {
			continue
		}
		for _, f := range p.Syntax {
			for _, d := range f.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				obj, ok := p.TypesInfo.Defs[fn.Name].(*types.Func)
				if !ok {
					continue
				}
				gatedBody[symName443(obj)] = containsInert443(p.TypesInfo, fn.Body)
			}
		}
	}

	used := map[string][]string{} // symbol → "<file>:<func>" sites
	inertFuncs := map[string]bool{}
	var gos []string
	for _, p := range pkgs {
		for _, e := range p.Errors {
			t.Fatalf("load %s: %v", p.PkgPath, e)
		}
		if p.PkgPath == cachePkgPath443 {
			continue
		}
		for _, f := range p.Syntax {
			base := filepath.Base(p.Fset.Position(f.Pos()).Filename)
			if strings.HasSuffix(base, "_test.go") {
				continue
			}
			if strings.HasSuffix(p.PkgPath, "/handlers/dispatchers") && base != "nested_call.go" {
				continue
			}
			if base == "cluster_list_prewarm.go" { // prewarm-only, never on a resolve path
				continue
			}
			for _, d := range f.Decls {
				// Package-level var initializers count as uses too: a seam such
				// as `var x = cache.Foo` is a reference the guard must classify.
				if gd, ok := d.(*ast.GenDecl); ok {
					ast.Inspect(gd, func(n ast.Node) bool {
						if x, ok := n.(*ast.SelectorExpr); ok {
							if obj, ok := p.TypesInfo.Uses[x.Sel].(*types.Func); ok && obj.Pkg() != nil && obj.Pkg().Path() == cachePkgPath443 {
								s := symName443(obj)
								used[s] = append(used[s], base+":<package var>")
							}
						}
						return true
					})
					continue
				}
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Body == nil || strings.HasSuffix(fn.Name.Name, "ForTest") {
					continue
				}
				where := base + ":" + fn.Name.Name
				if containsInert443(p.TypesInfo, fn.Body) {
					inertFuncs[where] = true
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					switch x := n.(type) {
					case *ast.SelectorExpr:
						if obj, ok := p.TypesInfo.Uses[x.Sel].(*types.Func); ok && obj.Pkg() != nil && obj.Pkg().Path() == cachePkgPath443 {
							s := symName443(obj)
							used[s] = append(used[s], where)
						}
					case *ast.GoStmt:
						gos = append(gos, where+" @ "+p.Fset.Position(x.Pos()).String())
					}
					return true
				})
			}
		}
	}

	var syms []string
	for s := range used {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	if len(syms) < 50 {
		t.Fatalf("the guard saw only %d cache symbols — the type-check scope is wrong (the arm cannot fail)", len(syms))
	}
	for _, s := range syms {
		class, ok := cacheAllow443[s]
		if !ok {
			t.Errorf("cache.%s is used on the resolve path (%v) but is not classified in cacheAllow443: "+
				"classify it, and if it has a side effect gate it on cache.Inert", s, used[s])
			continue
		}
		switch class {
		case gatedInCache443:
			if !gatedBody[s] {
				t.Errorf("cache.%s is classed gatedInCache but its body has no cache.Inert check", s)
			}
		case gatedAtCaller443:
			allowed := map[string]bool{}
			for _, w := range callerAllow443[s] {
				allowed[w] = true
			}
			for _, w := range used[s] {
				if !allowed[w] {
					t.Errorf("cache.%s (ctx-less side effect) used in %s, which is not an allowed caller", s, w)
				} else if !inertFuncs[w] {
					t.Errorf("cache.%s is used in %s, which no longer contains a cache.Inert check", s, w)
				}
			}
		}
	}
	for s := range cacheAllow443 {
		if _, ok := used[s]; !ok {
			t.Errorf("stale cacheAllow443 entry %q: not used on the resolve path any more — remove it", s)
		}
	}

	seenGo := map[string]bool{}
	for _, g := range gos {
		key := g[:strings.Index(g, " @ ")]
		needsInert, ok := goAllow443[key]
		if !ok {
			t.Errorf("goroutine on the resolve path not in goAllow443: %s — a goroutine can drop the inert flag "+
				"(context.TODO/Background or a detached ctx); review it and list it", g)
			continue
		}
		seenGo[key] = true
		if needsInert && !inertFuncs[key] {
			t.Errorf("goroutine %s is listed as gated, but %s no longer contains a cache.Inert check", g, key)
		}
	}
	for k := range goAllow443 {
		if !seenGo[k] {
			t.Errorf("stale goAllow443 entry %q", k)
		}
	}
}
