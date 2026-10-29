package dispatchers

// prewarm_pagination_966_test.go — #966: every prewarm writer resolves a widget
// with EXACTLY the pagination its cell key encodes.
//
// Before #966 the Phase-1 walk and the per-cohort seed resolved a widget whose
// ref declares no slice at page=1, perPage=5 (PREWARM_PAGE_LIMIT) and stored it
// under the UNPAGINATED (-1,-1) key: a body no unpaginated request computes for
// any widget or RESTAction whose jq reads `.slice`, plus a
// status.resourcesRefs.slice the request never asked for.
//
// THE ARMS (each writer, on the shared #643 walk harness — real
// resolveNavigationRoot → phase1Walker.walk → widgets.Resolve, real
// seedOneWidget, informer-served CRs, the endpoint's discovery + CRD answers):
//
//   - root walk + content populate: the root's content cell, looked up at the
//     pagination paginationInfo derives for the root's URL, holds exactly the
//     body a live widgets.Resolve at that pagination produces.
//   - child walk + content populate: the same for a child reached by a ref that
//     declares no slice AND for one whose ref declares only perPage (the
//     canonical page=1 buildPath now emits), at the URL the root's resolved
//     resourcesRefs carries — the URL the browser requests.
//   - PIP cohort seed: for an apiRef widget whose RESTAction FILTER and widget
//     jq both read `.slice`, the per-cohort widgets cell at the request's
//     pagination holds exactly the live resolve's body under that cohort.
//
// Every fixture's jq reads `.slice`, and each arm first asserts the body shows
// the pagination it was resolved with (so an arm cannot pass vacuously on a
// body that does not depend on pagination).
//
// TestPrewarmPagination966_SiteCheck is the structural half: every function in
// this package that resolves a widget AND derives a widget cell key passes the
// SAME pagination expressions to both, and the prewarm writers pass the fields
// of one prewarmPagination value.
//
// Not t.Parallel(): the harness reassigns package vars.

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	s966Root     = "s966-root"
	s966Plain    = "s966-plain"    // reached by a ref with no slice
	s966PerPage  = "s966-perpage"  // reached by a ref declaring only perPage: 3
	s966RAPlain  = "s966-ra-plain" // apiRef widget, ref with no slice
	s966RAPaged  = "s966-ra-paged" // apiRef widget, ref declaring page 1 / perPage 3
	s966RA       = "s966-rows"     // the RESTAction whose filter reads .slice
	s966RowCount = 12
)

// s966TitleJQ is the widget jq that reads `.slice` (and the RESTAction's rows
// when there are any). widgetData.title is the one string the harness CRD
// declares, so the status validation the real resolve runs accepts it.
const s966TitleJQ = `${ ((.rows // []) | length | tostring) + "|" + ` +
	`(if .slice then ((.slice.page | tostring) + "x" + (.slice.perPage | tostring)) else "unpaginated" end) }`

// s966RAFilter is the documented `.slice` recipe: window the rows when the
// request is paginated, return them all otherwise.
const s966RAFilter = `{ rows: ([range(0; 12)] as $all | if .slice then $all[.slice.offset : (.slice.offset + .slice.perPage)] else $all end) }`

func s966Ref(name string, slice map[string]any) map[string]any {
	r := map[string]any{
		"id": "ref-" + name, "apiVersion": h643WidgetGVR.Group + "/" + h643WidgetGVR.Version,
		"resource": h643WidgetGVR.Resource, "name": name, "namespace": h643NS, "verb": "GET",
	}
	if slice != nil {
		r["slice"] = slice
	}
	return r
}

// s966Self is a non-GET ref (never walked) so every fixture resolves at least
// one resourcesRefs item — the wrapper is where status.resourcesRefs.slice
// appears for a paginated resolve.
func s966Self(name string) map[string]any {
	r := s966Ref(name, nil)
	r["id"], r["verb"] = "self-"+name, "DELETE"
	return r
}

func s966Panel(name, ra string, refs ...map[string]any) *unstructured.Unstructured {
	items := []any{s966Self(name)}
	for _, r := range refs {
		items = append(items, r)
	}
	spec := map[string]any{
		"widgetData":         map[string]any{"title": name},
		"widgetDataTemplate": []any{map[string]any{"forPath": "title", "expression": s966TitleJQ}},
		"resourcesRefs":      map[string]any{"items": items},
	}
	if ra != "" {
		spec["apiRef"] = map[string]any{"name": ra, "namespace": h643NS}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h643WidgetGVR.Group + "/" + h643WidgetGVR.Version,
		"kind":       "Panel",
		"metadata":   map[string]any{"name": name, "namespace": h643NS, "uid": "uid-" + name, "resourceVersion": "1"},
		"spec":       spec,
	}}
}

// s966RACR: one internal (informer-served) stage so the resolve dict carries
// `.slice` into the filter, and the filter that windows on it.
func s966RACR() *unstructured.Unstructured {
	ra := h643RA(s966RA, "", "/apis/"+h643ThingsGVR.Group+"/"+h643ThingsGVR.Version+"/namespaces/"+h643NS+"/"+h643ThingsGVR.Resource)
	ra.Object["spec"].(map[string]any)["filter"] = s966RAFilter
	return ra
}

type s966Fixture struct {
	env   *h643Env
	paths map[string]string // widget name → the /call URL the browser requests for it
}

// s966Driver is which REAL walk entry fills the cells before an arm reads them.
type s966Driver int

const (
	// s966Phase1Walk: the phase-1 boot walk (resolveNavigationRoot).
	s966Phase1Walk s966Driver = iota
	// s966EngineReWalk: the prewarm engine's boot re-walk (a scopeKindBoot
	// pass → rePrewarmBootScoped), the second root entry (#967 review F2/W4).
	s966EngineReWalk
	// s966NoWalk: nothing walks; the arm drives a writer itself.
	s966NoWalk
)

func (d s966Driver) String() string {
	return [...]string{"phase-1 walk", "engine boot re-walk", "no walk"}[d]
}

// s966Setup builds the tree, restores the REAL widgets.Resolve behind the seed
// seam (the #643 harness replaces it with an apiRef-only stand-in that runs no
// widget jq), and runs the REAL root walk `driver` names.
func s966Setup(t *testing.T, driver s966Driver) *s966Fixture {
	t.Helper()
	b2StubSeeds(t) // the walk arms observe the walk's own cells; the seed arm calls seedOneWidget directly
	tree := &b2Tree{objs: []*unstructured.Unstructured{
		s966RACR(),
		s966Panel(s966Root, "",
			s966Ref(s966Plain, nil),
			s966Ref(s966PerPage, map[string]any{"perPage": int64(3)}),
			s966Ref(s966RAPlain, nil),
			s966Ref(s966RAPaged, map[string]any{"page": int64(1), "perPage": int64(3)}),
		),
		s966Panel(s966Plain, ""),
		s966Panel(s966PerPage, ""),
		s966Panel(s966RAPlain, s966RA),
		s966Panel(s966RAPaged, s966RA),
	}}
	env := b2Setup(t, 1, s966Root, tree)
	prevW := widgetsResolveFn
	widgetsResolveFn = widgets.Resolve
	t.Cleanup(func() { widgetsResolveFn = prevW })

	switch driver {
	case s966Phase1Walk:
		roots, err := env.deps.lister(context.Background())
		if err != nil || len(roots) != 1 {
			t.Fatalf("lister: %v (%d roots)", err, len(roots))
		}
		if err := resolveNavigationRoot(context.Background(), roots[0].Root, roots[0].GVR, env.deps.saEP, env.deps.saRC,
			env.deps.authnNS, env.deps.harvester, env.deps.navHarv, nil); err != nil {
			t.Fatalf("phase1 root walk: %v", err)
		}
	case s966EngineReWalk:
		env.pass(prewarmScope{kind: scopeKindBoot})
		if b2WalkObs(s966Root) == 0 {
			t.Fatalf("SETUP: the engine boot pass did not walk the root")
		}
	}

	// The browser's URLs: the root's from the frontend config (no pagination),
	// each child's exactly as the root's resolved resourcesRefs emits it.
	f := &s966Fixture{env: env, paths: map[string]string{
		s966Root: "/call?resource=" + h643WidgetGVR.Resource + "&apiVersion=" + h643WidgetGVR.Group + "/" + h643WidgetGVR.Version +
			"&namespace=" + h643NS + "&name=" + s966Root,
	}}
	live := f.liveResolve(t, s966SACtx(env), s966Root, unpaginatedPrewarm)
	for _, c := range extractResourcesRefsItems(live.Object) {
		if c.Verb != "GET" {
			continue
		}
		if ref, ok := objects.ParseCallPathToObjectRef(c.Path); ok {
			f.paths[ref.Name] = c.Path
		}
	}
	for _, n := range []string{s966Plain, s966PerPage, s966RAPlain, s966RAPaged} {
		if f.paths[n] == "" {
			t.Fatalf("SETUP: the root's resolved resourcesRefs carry no path for %s (paths=%v)", n, f.paths)
		}
	}
	return f
}

// s966SACtx is the identity the walk resolves under (withPhase1SAContext).
func s966SACtx(env *h643Env) context.Context {
	return withPhase1SAContext(context.Background(), env.deps.saEP, env.deps.saRC)
}

// requestPagination is what the dispatcher derives for the browser's request of
// `name`'s URL — paginationInfo over a real request.
func (f *s966Fixture) requestPagination(t *testing.T, name string) (perPage, page int) {
	t.Helper()
	return paginationInfo(slog.Default(), httptest.NewRequest("GET", f.paths[name], nil))
}

// liveResolve is what a live request computes: widgets.Resolve of a FRESH copy
// of the CR, at the request's pagination, under ctx's identity.
func (f *s966Fixture) liveResolve(t *testing.T, ctx context.Context, name string, pg prewarmPagination) *unstructured.Unstructured {
	t.Helper()
	u, err := f.env.dyn.Resource(h643WidgetGVR).Namespace(h643NS).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	res, err := widgets.Resolve(ctx, widgets.ResolveOptions{
		In: u, RC: f.env.deps.saRC, AuthnNS: f.env.deps.authnNS, PerPage: pg.PerPage, Page: pg.Page,
	})
	if err != nil {
		t.Fatalf("live resolve of %s at %+v: %v", name, pg, err)
	}
	return res
}

func s966Title(t *testing.T, raw []byte) string {
	t.Helper()
	u := &unstructured.Unstructured{}
	if err := u.UnmarshalJSON(raw); err != nil {
		t.Fatalf("decode stored body: %v", err)
	}
	s, _, _ := unstructured.NestedString(u.Object, "status", "widgetData", "title")
	return s
}

// assertContentCell: `name`'s identity-free content cell, looked up at the
// pagination its browser request carries, exists, shows that pagination, and is
// byte-equal to a live resolve at that pagination under the walk identity.
func (f *s966Fixture) assertContentCell(t *testing.T, writer, name, wantTitle string) {
	t.Helper()
	tc := struct{ writer, name, wantTitle string }{writer, name, wantTitle}
	saCtx := s966SACtx(f.env)
	{
		{
			pp, p := f.requestPagination(t, tc.name)
			key, _ := widgetContentL1Key(h643WidgetGVR, h643NS, tc.name, pp, p)
			e, ok := cache.ResolvedCache().GetNoTouch(key)
			if !ok || e == nil {
				t.Fatalf("#966 RED (%s): no content cell for %s at the pagination its request carries (perPage=%d, page=%d; URL %s) — "+
					"the walk keyed it with a different pagination", tc.writer, tc.name, pp, p, f.paths[tc.name])
			}
			if got := s966Title(t, e.RawJSON); got != tc.wantTitle {
				t.Fatalf("#966 RED (%s): %s's cell at (perPage=%d, page=%d) was resolved as %q, want %q — the walk resolved it "+
					"with a pagination other than the one its key encodes", tc.writer, tc.name, pp, p, got, tc.wantTitle)
			}
			want, err := encodeResolvedJSON(f.liveResolve(t, saCtx, tc.name, newPrewarmPagination(pp, p)))
			if err != nil {
				t.Fatalf("encode live: %v", err)
			}
			if !bytes.Equal(e.RawJSON, want) {
				t.Fatalf("#966 RED (%s): %s's stored body differs from a live request's resolve at (perPage=%d, page=%d)\n stored: %s\n live:   %s",
					tc.writer, tc.name, pp, p, psTrunc(string(e.RawJSON), 600), psTrunc(string(want), 600))
			}
		}
	}
}

// TestPrewarmPagination966_WalkCellsEqualTheLiveRequest covers the root walk,
// the child walk and the walk's content populate, through BOTH root entries:
// the phase-1 walk and the engine's boot re-walk (#967 review F2/W4).
func TestPrewarmPagination966_WalkCellsEqualTheLiveRequest(t *testing.T) {
	for _, driver := range []s966Driver{s966Phase1Walk, s966EngineReWalk} {
		t.Run(driver.String(), func(t *testing.T) {
			f := s966Setup(t, driver)
			for _, tc := range []struct {
				writer, name, wantTitle string
			}{
				{"root walk", s966Root, "0|unpaginated"},
				{"child walk (ref declares no slice)", s966Plain, "0|unpaginated"},
				{"child walk (ref declares only perPage)", s966PerPage, "0|1x3"},
			} {
				t.Run(tc.writer, func(t *testing.T) { f.assertContentCell(t, driver.String()+": "+tc.writer, tc.name, tc.wantTitle) })
			}
		})
	}
}

// TestPrewarmPagination966_DrainChildCellsEqualTheLiveRequest covers the
// deep-page drain's children loop (#967 review F2/W5): children a deep page
// reveals are walked with the pagination of THEIR OWN /call path — the URL the
// browser requests — exactly as the page-1 children loop walks them.
func TestPrewarmPagination966_DrainChildCellsEqualTheLiveRequest(t *testing.T) {
	f := s966Setup(t, s966NoWalk)
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	oldCap := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = 2 // one deep page
	t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })
	prevFetch, prevResolve := paginationFetchPageFn, paginationResolvePageFn
	t.Cleanup(func() { paginationFetchPageFn, paginationResolvePageFn = prevFetch, prevResolve })

	root, err := f.env.dyn.Resource(h643WidgetGVR).Namespace(h643NS).Get(context.Background(), s966Root, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	paginationFetchPageFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
		return objects.Result{GVR: h643WidgetGVR, Unstructured: root.DeepCopy()}
	}
	// Deep page 2 of an apiRef+template list reveals two children, at the exact
	// paths the root's resourcesRefs emit (one undeclared, one perPage-only).
	paginationResolvePageFn = func(_ context.Context, _ widgets.ResolveOptions) (*unstructured.Unstructured, error) {
		page2 := fakePageEnvelope(2, false)
		page2.Object["status"].(map[string]any)["resourcesRefs"].(map[string]any)["items"] = []any{
			map[string]any{"id": "c1", "path": f.paths[s966Plain], "verb": "GET", "allowed": true},
			map[string]any{"id": "c2", "path": f.paths[s966PerPage], "verb": "GET", "allowed": true},
		}
		return page2, nil
	}

	iterateApiRefPages(s966SACtx(f.env), newPhase1Walker(f.env.deps.saRC, f.env.deps.authnNS), root.DeepCopy(), h643WidgetGVR,
		fakePage1Driven(), 1, newPrewarmPagination(5, 1), f.env.deps.authnNS)

	f.assertContentCell(t, "drain child walk (ref declares no slice)", s966Plain, "0|unpaginated")
	f.assertContentCell(t, "drain child walk (ref declares only perPage)", s966PerPage, "0|1x3")
}

// TestPrewarmPagination966_RecollectResolvesTheCandidatesPagination covers the
// drain's re-collection (#967 review F2/W1): it re-tries the FIRST page's
// continuation decision, so it must re-resolve exactly the pagination that page
// was resolved and keyed with — here a declared start page 2 of 3 rows, which
// no default (page 1, or a bounded perPage) reproduces.
func TestPrewarmPagination966_RecollectResolvesTheCandidatesPagination(t *testing.T) {
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	t.Cleanup(recollectDelayForTest(0))
	prevFetch, prevResolve := paginationFetchPageFn, paginationResolvePageFn
	t.Cleanup(func() { paginationFetchPageFn, paginationResolvePageFn = prevFetch, prevResolve })

	c := newApiRefPaginationCollector()
	job := fakeApiRefPaginationJob("krateo-system", "s966-recollect")
	job.Pagination = newPrewarmPagination(3, 2)
	job.Page1Res.Object["status"].(map[string]any)["resourcesRefs"].(map[string]any)["slice"].(map[string]any)["continue"] = false
	c.collect(job)
	if c.pendingRecollectCount() != 1 {
		t.Fatalf("SETUP: expected 1 pending re-collection candidate; got %d", c.pendingRecollectCount())
	}
	paginationFetchPageFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
		return fetchOKResult("krateo-system", "s966-recollect")
	}
	var got []prewarmPagination
	paginationResolvePageFn = func(_ context.Context, opts widgets.ResolveOptions) (*unstructured.Unstructured, error) {
		got = append(got, prewarmPagination{PerPage: opts.PerPage, Page: opts.Page})
		return fakeContinuingPage1(), nil
	}
	recollectPendingApiRefPaginationJobs(context.Background(), c, nil)
	if len(got) != 1 || got[0] != job.Pagination {
		t.Fatalf("#966 RED (re-collection): re-resolved the first page at %+v, want exactly the candidate's own pagination %+v",
			got, job.Pagination)
	}
	if c.count() != 1 {
		t.Fatalf("PRE: the continuing re-resolve must hand the job back (count=%d)", c.count())
	}
}

// TestPrewarmPagination966_SeedCellsEqualTheLiveRequest covers the PIP cohort
// seed for apiRef widgets whose RESTAction filter AND widget jq read `.slice`.
func TestPrewarmPagination966_SeedCellsEqualTheLiveRequest(t *testing.T) {
	f := s966Setup(t, s966Phase1Walk)
	cohort := h643CohortCtx(f.env)
	entries := map[string]navWidgetEntry{}
	for _, e := range f.env.deps.navHarv.snapshot() {
		entries[e.W.GetName()] = e
	}
	for _, tc := range []struct {
		name, wantTitle string
	}{
		{s966RAPlain, "12|unpaginated"},
		{s966RAPaged, "3|1x3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, ok := entries[tc.name]
			if !ok {
				t.Fatalf("SETUP: the walk did not harvest %s (harvested %d)", tc.name, len(entries))
			}
			if err := seedOneWidget(cohort, e, f.env.deps.authnNS, seedModeBoot); err != nil {
				t.Fatalf("seedOneWidget(%s): %v", tc.name, err)
			}
			pp, p := f.requestPagination(t, tc.name)
			key, _, _ := dispatchCacheLookupKey(cohort, "widgets", h643WidgetGVR.Group, h643WidgetGVR.Version, h643WidgetGVR.Resource,
				h643NS, tc.name, pp, p, effectiveKeyExtras(cohort, e.W.Object, nil))
			got, ok := cache.ResolvedCache().GetNoTouch(key)
			if !ok || got == nil {
				t.Fatalf("#966 RED (PIP seed): no widgets cell for %s at the pagination its request carries (perPage=%d, page=%d; URL %s)",
					tc.name, pp, p, f.paths[tc.name])
			}
			if title := s966Title(t, got.RawJSON); title != tc.wantTitle {
				t.Fatalf("#966 RED (PIP seed): %s's cell at (perPage=%d, page=%d) was resolved as %q, want %q — the seed resolved it "+
					"with a pagination other than the one its key encodes", tc.name, pp, p, title, tc.wantTitle)
			}
			want, err := encodeResolvedJSON(f.liveResolve(t, cohort, tc.name, newPrewarmPagination(pp, p)))
			if err != nil {
				t.Fatalf("encode live: %v", err)
			}
			if !bytes.Equal(got.RawJSON, want) {
				t.Fatalf("#966 RED (PIP seed): %s's stored body differs from a live request's resolve at (perPage=%d, page=%d)\n stored: %s\n live:   %s",
					tc.name, pp, p, psTrunc(string(got.RawJSON), 600), psTrunc(string(want), 600))
			}
		})
	}
}

// TestPrewarmPagination966_CanonicalPathAgreesWithTheRequest pins the two
// derivations of one URL to each other: prewarmPaginationFor (what a prewarm
// writer resolves and keys with) and paginationInfo (what the dispatcher keys
// the browser's request with), over every shape a declared ref slice can take.
func TestPrewarmPagination966_CanonicalPathAgreesWithTheRequest(t *testing.T) {
	base := "/call?resource=tables&apiVersion=widgets.templates.krateo.io/v1beta1&namespace=n&name=w"
	for _, q := range []string{
		// canonical: what buildPath emits
		"", "&page=1&perPage=50", "&page=3&perPage=5",
		// the six shapes #967 review F1 found disagreeing under the stricter parser
		"&perPage=50", "&page=0&perPage=50", "&page=-2&perPage=50", "&page=x&perPage=50", "&page=2", "&page=2&perPage=0",
		// and a few more a hand-written or portal#235 path could carry
		"&perPage=x", "&page=1", "&page=0&perPage=0", "&perPage=-5&page=3",
	} {
		path := base + q
		pp, p := paginationInfo(slog.Default(), httptest.NewRequest("GET", path, nil))
		if got := prewarmPaginationFor(path); got != (prewarmPagination{PerPage: pp, Page: p}) {
			t.Fatalf("#966: prewarmPaginationFor(%q) = %+v, the request's paginationInfo = (perPage=%d, page=%d)", path, got, pp, p)
		}
	}
}

// ── site check ─────────────────────────────────────────────────────────────

// s966KeyCalls are the widget cell key derivations, with the index of their
// perPage argument (page follows it).
var s966KeyCalls = map[string]int{
	"widgetContentL1Key":      3,
	"populateWidgetContentL1": 3,
	"dispatchCacheLookupKey":  7,
}

// s966PrewarmWriters are the prewarm writers (#966) — the check must find each
// one (so it cannot pass by matching nothing), and each must take its
// pagination from the fields of ONE value.
var s966PrewarmWriters = []string{"walk", "seedOneWidget", "iterateApiRefPages"}

func TestPrewarmPagination966_SiteCheck(t *testing.T) {
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	expr := func(e ast.Expr) string {
		var b strings.Builder
		_ = printer.Fprint(&b, fset, e)
		return b.String()
	}
	seen := map[string]bool{}
	for _, fn := range files {
		if strings.HasSuffix(fn, "_test.go") {
			continue
		}
		src, err := os.ReadFile(fn)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, fn, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", fn, err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			var resolves [][2]string // (PerPage, Page) of every widgets.ResolveOptions literal
			var keys [][3]string     // (call, perPage, page)
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CompositeLit:
					sel, ok := x.Type.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "ResolveOptions" || expr(sel.X) != "widgets" {
						return true
					}
					var pp, p string
					for _, el := range x.Elts {
						kv, ok := el.(*ast.KeyValueExpr)
						if !ok {
							continue
						}
						switch expr(kv.Key) {
						case "PerPage":
							pp = expr(kv.Value)
						case "Page":
							p = expr(kv.Value)
						}
					}
					resolves = append(resolves, [2]string{pp, p})
				case *ast.CallExpr:
					id, ok := x.Fun.(*ast.Ident)
					if !ok {
						return true
					}
					i, ok := s966KeyCalls[id.Name]
					if !ok || len(x.Args) <= i+1 {
						return true
					}
					if id.Name == "dispatchCacheLookupKey" && expr(x.Args[1]) != `"widgets"` {
						return true // a RESTAction cell: not resolved by widgets.Resolve
					}
					keys = append(keys, [3]string{id.Name, expr(x.Args[i]), expr(x.Args[i+1])})
				}
				return true
			})
			if len(resolves) == 0 || len(keys) == 0 {
				continue
			}
			name := fd.Name.Name
			seen[name] = true
			for _, r := range resolves {
				for _, k := range keys {
					if k[1] != r[0] || k[2] != r[1] {
						t.Errorf("#966 RED: %s (%s) resolves a widget with (PerPage: %q, Page: %q) but derives a cell key with %s(…, %s, %s) — "+
							"the key and the resolve must take their pagination from ONE value", name, fn, r[0], r[1], k[0], k[1], k[2])
					}
				}
			}
			for _, w := range s966PrewarmWriters {
				if name != w {
					continue
				}
				for _, r := range resolves {
					x := strings.TrimSuffix(r[0], ".PerPage")
					if x == r[0] || r[1] != x+".Page" {
						t.Errorf("#966 RED: prewarm writer %s resolves with (PerPage: %q, Page: %q) — it must pass the fields of one "+
							"prewarmPagination value", name, r[0], r[1])
					}
				}
			}
		}
	}
	var missing []string
	for _, w := range s966PrewarmWriters {
		if !seen[w] {
			missing = append(missing, w)
		}
	}
	checked := make([]string, 0, len(seen))
	for n := range seen {
		checked = append(checked, n)
	}
	sort.Strings(checked)
	t.Logf("resolve+key sites checked: %v", checked)
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("SETUP: the site check found no resolve+key pair in prewarm writer(s) %v — it would pass by matching nothing", missing)
	}
}
