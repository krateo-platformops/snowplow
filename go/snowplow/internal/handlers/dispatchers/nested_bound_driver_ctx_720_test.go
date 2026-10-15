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
// resolver entry in this package is classified (customer, nested, seam, or the
// background driver arm that proves it). A NEW resolve call site — a new
// background root — fails the census until it is classified, and a background
// classification must name a driver arm that exists in driverCtxArms720.
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

// resolveEntryClass720 classifies one production resolve call site.
//   - "customer"     — a /call handler: bypasses the gate by owner ruling;
//   - "nested"       — inside a resolve already in progress: inherits its
//     caller's ctx and classification (the gate admits only at depth 0);
//   - "seam"         — the definition of a resolve seam (var fooFn = ...);
//   - "bg:<driver>[,<driver>]" — a background driver (or several sharing the
//     site); each <driver> must be a key of driverCtxArms720, whose arm proves
//     the ctx is classified background.
var resolveEntryClass720 = map[string]string{
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
	// --- discovery walk: the phase-1 boot walk (resolveNavigationRoot) and the
	// engine's boot re-walk (engineBootRewalkCtx) ---
	"phase1_walk.go:phase1Walker.walk": "bg:boot_walk,engine_boot",
	// --- content prewarm ---
	"phase1_content_prewarm.go:prewarmOneRESTAction": "bg:content_prewarm",
	// --- post-Ready apiRef pagination drain + its re-collection ---
	"phase1_walk_pagination.go:iterateApiRefPages":                        "bg:pagination_drain",
	"phase1_walk_pagination_jobs.go:recollectPendingApiRefPaginationJobs": "bg:pagination_drain",
}

// resolveEntryIdents720 are the resolver entries a resolve call site is
// recognised by: the resolver packages' Resolve, and this package's seams.
var resolveEntryIdents720 = map[string]bool{
	"restactionsResolveFn":    true,
	"widgetsResolveFn":        true,
	"paginationResolvePageFn": true,
	"resolveOnceFn":           true,
}

// TestIssue720_ResolveEntryCensus — every production call site of a resolver
// entry in this package is classified. A new site (e.g. a new background root
// that resolves under an unmarked ctx) fails here until it is classified; a
// "bg:" class must name a driver arm that exists.
func TestIssue720_ResolveEntryCensus(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	found := map[string]bool{}
	for _, e := range entries {
		fn := e.Name()
		if e.IsDir() || !strings.HasSuffix(fn, ".go") || strings.HasSuffix(fn, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(".", fn), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", fn, perr)
		}
		for _, decl := range f.Decls {
			var owner string
			var body ast.Node
			switch d := decl.(type) {
			case *ast.FuncDecl:
				owner = d.Name.Name
				if d.Recv != nil && len(d.Recv.List) == 1 {
					owner = recvName720(d.Recv.List[0].Type) + "." + owner
				}
				body = d
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					vs, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for i, n := range vs.Names {
						if i < len(vs.Values) && containsResolveCall720(vs.Values[i]) {
							found[fn+":var "+n.Name] = true
						}
					}
				}
				continue
			default:
				continue
			}
			if containsResolveCall720(body) {
				found[fn+":"+owner] = true
			}
		}
	}
	if len(found) < 5 {
		t.Fatalf("census found only %d resolve call sites (%v) — the scan is broken and cannot fail", len(found), found)
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
				"is a customer, nested, a seam, or a background driver (bg:<arm>) with a driver arm proving its ctx "+
				"is classified background by the nested-resolve gate", s)
			continue
		}
		if drvs, isBG := strings.CutPrefix(class, "bg:"); isBG {
			for _, drv := range strings.Split(drvs, ",") {
				if _, ok := driverCtxArms720[drv]; !ok {
					t.Errorf("#720 census: %s is classified %q but driverCtxArms720 has no %q arm", s, class, drv)
				}
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

func containsResolveCall720(n ast.Node) bool {
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
			if resolveEntryIdents720[fun.Name] {
				hit = true
			}
		case *ast.SelectorExpr:
			if pkg, ok := fun.X.(*ast.Ident); ok && fun.Sel.Name == "Resolve" &&
				(pkg.Name == "restactions" || pkg.Name == "widgets") {
				hit = true
			}
		}
		return true
	})
	return hit
}
