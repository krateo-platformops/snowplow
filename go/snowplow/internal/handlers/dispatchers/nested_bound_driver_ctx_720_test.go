package dispatchers

// nested_bound_driver_ctx_720_test.go — #720 driver-level classification arms
// and the resolve-entry census.
//
// The nested-resolve admission gate (api/nested_resolve_bound.go) lets a
// CUSTOMER outermost tree bypass the memory ceiling and bounds only BACKGROUND
// trees. The classification is by ctx marker (cache.RefDenialOrigin != serve,
// or the content-prewarm marker). A background driver whose ctx carries no
// marker therefore bypasses the cap as if it were a customer — the defect
// reviewer-small found in the post-Ready apiRef pagination drain (a
// context.Background() root with no marker).
//
// N3 in the api package tests synthetic ctxs. These arms build each driver's
// REAL production ctx — through the production builder it uses, or by driving
// the production function and capturing the ctx at its resolve seam — and
// assert the gate classifies it BACKGROUND. Deleting the marker any driver
// depends on turns its arm RED.
//
// The census below closes the other half: every production call site of a
// resolver entry in the WHOLE MODULE is classified (customer, nested, seam,
// chain, or the background driver arm that proves it). A NEW resolve call site
// — a new background root, in any package — fails the census until it is
// classified, and a background classification must name a driver arm that
// exists in driverCtxArms720.
//
// #728 — the census used to scan only this package and to recognise a call by
// the package IDENTIFIER (restactions / widgets). A root added in another
// package, through apiref.Resolve or api.Resolve, or through an import alias,
// went unseen and classified CUSTOMER at run time, bypassing the nested cap.
// It now walks every non-test .go file of the module and recognises a call by
// the callee's IMPORT PATH (resolverEntries728). Widening the census was chosen
// over a required origin argument at the resolve entry: the latter changes
// every resolver signature and every nested in-resolve call for a coverage gap
// a test closes, and the census already proves each background driver's ctx
// through its driver arm.
//
// UNTAGGED on purpose (like the #563 / #268 censuses): it runs in ordinary CI.

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// driverRoot720 is an outermost ctx with no marker of its own — what a
// detached driver root (context.Background()) or an engine scope hands in.
func driverRoot720() context.Context {
	return cache.WithNestedCallDepth(context.Background(), 0)
}

// captureDrainCtx720 drives the REAL post-Ready pagination drain
// (drainApiRefPaginationJobs) from an UNMARKED root, exactly as the drain
// goroutine in phase1Warmup does (context.WithTimeout(context.Background())),
// and returns the ctx its page-N widget resolve receives.
func captureDrainCtx720(t *testing.T) context.Context {
	t.Helper()
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()

	oldCap := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = 2
	prevFetch, prevResolve := paginationFetchPageFn, paginationResolvePageFn
	defer func() {
		phase1MaxApiRefPagesForTest = oldCap
		paginationFetchPageFn, paginationResolvePageFn = prevFetch, prevResolve
	}()

	const ns, name = "krateo-system", "compositions-page-datagrid"
	paginationFetchPageFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
		return fetchOKResult(ns, name)
	}
	var (
		mu  sync.Mutex
		got context.Context
	)
	paginationResolvePageFn = func(ctx context.Context, _ widgets.ResolveOptions) (*unstructured.Unstructured, error) {
		mu.Lock()
		if got == nil {
			got = ctx
		}
		mu.Unlock()
		return nonRBACSensitivePageEnvelope(ns, name, 2, false), nil
	}

	root, cancel := context.WithTimeout(context.Background(), paginationDrainTimeout)
	defer cancel()
	drainApiRefPaginationJobs(root, []apiRefPaginationJob{{
		In:         newUnstructuredWidget(ns, name),
		GVR:        drainPageCellGVR(),
		Page1Res:   fakePage1Driven(),
		Depth:      1,
		PerPage:    5,
		KeyPerPage: -1,
		KeyPage:    -1,
		AuthnNS:    ns,
	}}, endpoints.Endpoint{}, nil)

	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("PRECONDITION: the drain never reached its page-N resolve seam")
	}
	return got
}

// driverCtxArms720 is every BACKGROUND driver of a nested-resolve tree, each
// producing its REAL production ctx. The census below requires every
// background resolve call site to name one of these keys.
var driverCtxArms720 = map[string]func(t *testing.T) []context.Context{
	// resolveOnceProd → refresherResolveCtx (the single refresher resolve entry).
	"refresher": func(t *testing.T) []context.Context {
		return []context.Context{refresherResolveCtx(driverRoot720())}
	},
	// The boot engine's cohort seed (prewarm_engine_boot.go) and the
	// keep-warm / rbac-shift reseed (reseed_engine.go) both build the seed ctx
	// through withCohortSeedContext.
	"cohort_seed": func(t *testing.T) []context.Context {
		target := seedTarget{Username: "user-720", Groups: []string{"g"}}
		seed := withCohortSeedContext(driverRoot720(), target, endpoints.Endpoint{}, nil)
		reseed, cancel := reseedCohortCtx(driverRoot720(), rePrewarmDeps{}, target)
		t.Cleanup(cancel)
		return []context.Context{seed, reseed}
	},
	// rePrewarmBootScoped → engineBootRewalkCtx: the engine ctx (seeds,
	// yields) and the SA re-walk ctx derived from it.
	"engine_boot": func(t *testing.T) []context.Context {
		ctx, rctx := engineBootRewalkCtx(driverRoot720(), rePrewarmDeps{})
		return []context.Context{ctx, rctx}
	},
	// resolveNavigationRoot (phase-1 discovery walk) builds its root through
	// withPhase1SAContext, then adds only the declined-external set (which the
	// I-6 guard pins to that function); the nav-root lister uses the same
	// builder. Its prewarm marker is the one withPhase1SAContext stamps.
	"boot_walk": func(t *testing.T) []context.Context {
		root := withPhase1SAContext(driverRoot720(), endpoints.Endpoint{}, nil)
		return []context.Context{
			root,
			cache.WithSeedDeclinedExternalSet(root, cache.EngineDeclinedExternalSet()),
		}
	},
	// The content prewarm pass (phase1_content_prewarm.go).
	"content_prewarm": func(t *testing.T) []context.Context {
		return []context.Context{withContentPrewarmSAContext(driverRoot720(), endpoints.Endpoint{}, nil)}
	},
	// The post-Ready apiRef pagination drain, driven for real from an unmarked
	// root (reviewer-small's #722 finding), and its re-collection pass, which
	// production runs under withPhase1SAContext(drainRoot).
	"pagination_drain": func(t *testing.T) []context.Context {
		return []context.Context{
			captureDrainCtx720(t),
			withPhase1SAContext(driverRoot720(), endpoints.Endpoint{}, nil),
		}
	},
}

// TestIssue720_EveryBackgroundDriverCtxIsBackground — each driver's REAL ctx
// is classified BACKGROUND by BOTH memory gates: the nested-resolve gate (#720)
// and the content-fill admission gate (#710 PR-2a). RED if a driver's marker
// is removed, or if the two gates' classifiers drift apart (a driver
// background at one gate and a bypassing "customer" at the other).
func TestIssue720_EveryBackgroundDriverCtxIsBackground(t *testing.T) {
	if restactionsapi.IsBackgroundNestedTreeForTest(driverRoot720()) ||
		restactionsapi.IsBackgroundContentFillForTest(driverRoot720()) {
		t.Fatal("PRECONDITION: an unmarked root must classify as CUSTOMER at both gates, else every arm below is vacuous")
	}
	names := make([]string, 0, len(driverCtxArms720))
	for n := range driverCtxArms720 {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			for i, ctx := range driverCtxArms720[name](t) {
				if !restactionsapi.IsBackgroundNestedTreeForTest(ctx) {
					t.Errorf("#720: driver %s ctx #%d classifies as CUSTOMER (RefDenialOrigin=%q) — its nested "+
						"trees would bypass the nested-resolve memory ceiling; mark it background",
						name, i, cache.RefDenialOrigin(ctx))
				}
				if !restactionsapi.IsBackgroundContentFillForTest(ctx) {
					t.Errorf("#720: driver %s ctx #%d is a CUSTOMER at the content-fill admission gate "+
						"(RefDenialOrigin=%q) — its content fills would bypass that memory cap; the two gates "+
						"must classify drivers identically", name, i, cache.RefDenialOrigin(ctx))
				}
			}
		})
	}
}

// resolveEntryClass720 classifies one production resolve call site. A site in
// this package is keyed "<file>:<owner>"; a site elsewhere in the module is keyed
// by its module-relative path, "internal/…/<file>:<owner>".
//   - "customer"     — a /call handler: bypasses the gate by owner ruling;
//   - "nested"       — inside a resolve already in progress: inherits its
//     caller's ctx and classification (the gate admits only at depth 0);
//   - "chain"        — a resolver package calling the next resolver of the
//     same in-progress resolve (widgets → apiref → restactions → api); legal
//     ONLY for a site under internal/resolvers/;
//   - "seam"         — the definition of a resolve seam (var fooFn = ...);
//   - "bg:<driver>[,<driver>]" — a background driver (or several sharing the
//     site); each <driver> must be a key of driverCtxArms720, whose arm proves
//     the ctx is classified background.
var resolveEntryClass720 = map[string]string{
	// --- the resolver chain: one in-progress resolve calling the next layer ---
	"internal/resolvers/widgets/resolve.go:resolveApiRef":   "chain",
	"internal/resolvers/widgets/apiref/resolve.go:Resolve":  "chain",
	"internal/resolvers/restactions/restactions.go:Resolve": "chain",
	// --- customer /call handlers: bypass the gate by owner ruling ---
	"restactions.go:restActionHandler.ServeHTTP": "customer",
	"widgets.go:widgetsHandler.ServeHTTP":        "customer",
	// --- inside a resolve in progress (inherits its caller's ctx) ---
	"nested_call.go:ResolveNestedCall": "nested",
	// --- seam definitions ---
	"dispatch_seams.go:var widgetsResolveFn":                "seam",
	"phase1_walk_pagination.go:var paginationResolvePageFn": "seam",
	// --- refresher: cache.RegisterRefreshFunc → resolveAndPopulateL1 →
	// resolveOnceFn (resolveOnceProd → refresherResolveCtx) → the *ForRefresh
	// resolves ---
	"resolve_populate.go:resolveAndPopulateL1":        "bg:refresher",
	"resolve_populate.go:resolveRAFullListForRefresh": "bg:refresher",
	"resolve_populate.go:resolveRestActionForRefresh": "bg:refresher",
	"resolve_populate.go:resolveWidgetForRefresh":     "bg:refresher",
	// --- cohort seed (boot engine + keep-warm reseed) ---
	"phase1_pip_seed.go:seedOneWidget":                   "bg:cohort_seed",
	"phase1_pip_seed.go:seedRestactionResolveAndPutProd": "bg:cohort_seed",
	// #728 — found by the import-path match (apiref.Resolve): the RAFullList
	// pin, called by seedOneWidget with its seed resolve ctx.
	"phase1_pip_seed.go:seedRAFullListForWidget": "bg:cohort_seed",
	// --- discovery walk: the phase-1 boot walk (resolveNavigationRoot) and the
	// engine's boot re-walk (engineBootRewalkCtx) ---
	"phase1_walk.go:phase1Walker.walk": "bg:boot_walk,engine_boot",
	// --- content prewarm ---
	"phase1_content_prewarm.go:prewarmOneRESTAction": "bg:content_prewarm",
	// --- post-Ready apiRef pagination drain + its re-collection ---
	"phase1_walk_pagination.go:iterateApiRefPages":                        "bg:pagination_drain",
	"phase1_walk_pagination_jobs.go:recollectPendingApiRefPaginationJobs": "bg:pagination_drain",
}

// resolveEntryIdents720 are this package's resolve seams, recognised by name
// (they are package-local identifiers).
var resolveEntryIdents720 = map[string]bool{
	"restactionsResolveFn":    true,
	"widgetsResolveFn":        true,
	"paginationResolvePageFn": true,
	"resolveOnceFn":           true,
}

// resolverEntries728 are the exported resolver entries, recognised by the
// callee's IMPORT PATH so an import alias cannot hide a call (#728).
var resolverEntries728 = map[string]map[string]bool{
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions":     {"Resolve": true},
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets":         {"Resolve": true},
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref":  {"Resolve": true},
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api": {"Resolve": true, "ResolveWithFailure": true},
}

// moduleRoot728 is the go/snowplow module root, relative to this package.
const moduleRoot728 = "../../.."

// dispatchersDir728 is this package, module-relative.
const dispatchersDir728 = "internal/handlers/dispatchers"

// scanResolveSites728 walks every non-test .go file under root and returns the
// resolve call sites, keyed as resolveEntryClass720 keys them.
func scanResolveSites728(t *testing.T, root string) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	found := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "testdata", "vendor", "node_modules", ".git":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly|parser.ParseComments)
		if perr != nil {
			return perr
		}
		// Map each local package name of a resolver import to its import path.
		aliases := map[string]string{}
		for _, im := range f.Imports {
			ip := strings.Trim(im.Path.Value, `"`)
			if _, ok := resolverEntries728[ip]; !ok {
				continue
			}
			name := ip[strings.LastIndex(ip, "/")+1:]
			if im.Name != nil {
				name = im.Name.Name
			}
			aliases[name] = ip
		}
		inDispatchers := filepath.ToSlash(filepath.Dir(rel)) == dispatchersDir728
		if len(aliases) == 0 && !inDispatchers {
			return nil
		}
		f, perr = parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		key := rel
		if inDispatchers {
			key = filepath.Base(rel)
		}
		isCall := func(n ast.Node) bool { return containsResolveCall728(n, aliases, inDispatchers) }
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				owner := d.Name.Name
				if d.Recv != nil && len(d.Recv.List) == 1 {
					owner = recvName720(d.Recv.List[0].Type) + "." + owner
				}
				if isCall(d) {
					found[key+":"+owner] = true
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					vs, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for i, n := range vs.Names {
						if i < len(vs.Values) && isCall(vs.Values[i]) {
							found[key+":var "+n.Name] = true
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return found
}

// TestIssue720_ResolveEntryCensus — every production call site of a resolver
// entry in the module is classified (#720, widened by #728). A new site (e.g. a
// new background root that resolves under an unmarked ctx, in ANY package)
// fails here until it is classified; a "bg:" class must name a driver arm that
// exists; "chain" is legal only inside the resolver packages.
func TestIssue720_ResolveEntryCensus(t *testing.T) {
	checkResolveCensus728(t, scanResolveSites728(t, moduleRoot728))
}

// checkResolveCensus728 checks found against resolveEntryClass720.
func checkResolveCensus728(t *testing.T, found map[string]bool) {
	t.Helper()
	if len(found) < 5 {
		t.Fatalf("census found only %d resolve call sites (%v) — the scan is broken and cannot fail", len(found), found)
	}
	outside := 0
	for s := range found {
		if strings.HasPrefix(s, "internal/") {
			outside++
		}
	}
	if outside == 0 {
		t.Fatalf("census found no resolve call site outside %s — the module-wide scan is broken (#728)", dispatchersDir728)
	}
	sites := make([]string, 0, len(found))
	for s := range found {
		sites = append(sites, s)
	}
	sort.Strings(sites)
	for _, s := range sites {
		class, ok := resolveEntryClass720[s]
		if !ok {
			t.Errorf("#720 census: resolve call site %s is not classified in resolveEntryClass720 — say whether it "+
				"is a customer, nested, chain, a seam, or a background driver (bg:<arm>) with a driver arm proving "+
				"its ctx is classified background by the nested-resolve gate", s)
			continue
		}
		if drvs, isBG := strings.CutPrefix(class, "bg:"); isBG {
			for _, drv := range strings.Split(drvs, ",") {
				if _, ok := driverCtxArms720[drv]; !ok {
					t.Errorf("#720 census: %s is classified %q but driverCtxArms720 has no %q arm", s, class, drv)
				}
			}
		} else if class == "chain" {
			if !strings.HasPrefix(s, "internal/resolvers/") {
				t.Errorf("#728 census: %s is classified \"chain\" but is not inside internal/resolvers/ — only a "+
					"resolver calling the next layer of the same resolve may be chain; a root elsewhere must be "+
					"customer or a proven background driver", s)
			}
		} else if class != "customer" && class != "nested" && class != "seam" {
			t.Errorf("#720 census: %s has unknown class %q", s, class)
		}
	}
	for s := range resolveEntryClass720 {
		if !found[s] {
			t.Errorf("#720 census: stale entry %s — no such resolve call site any more; remove it", s)
		}
	}
}

func recvName720(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.StarExpr:
		return recvName720(x.X)
	case *ast.Ident:
		return x.Name
	case *ast.IndexExpr:
		return recvName720(x.X)
	}
	return "?"
}

// containsResolveCall728 reports whether n calls a resolver entry: an exported
// entry through any local name of its import (aliases), or — in this package —
// one of the resolve seams.
func containsResolveCall728(n ast.Node, aliases map[string]string, seams bool) bool {
	hit := false
	ast.Inspect(n, func(n ast.Node) bool {
		if hit {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			if seams && resolveEntryIdents720[fun.Name] {
				hit = true
			}
		case *ast.SelectorExpr:
			if pkg, ok := fun.X.(*ast.Ident); ok {
				if ip, ok := aliases[pkg.Name]; ok && resolverEntries728[ip][fun.Sel.Name] {
					hit = true
				}
			}
		}
		return true
	})
	return hit
}
