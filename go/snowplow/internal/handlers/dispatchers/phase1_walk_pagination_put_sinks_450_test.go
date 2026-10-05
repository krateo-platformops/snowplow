// phase1_walk_pagination_put_sinks_450_test.go — #450: the pagination-drain
// widgetContent Put must decline on a userAccessFilter refilter and on an
// external-endpoint touch, exactly like the walker's page-1 Put.
//
// THE GAP. iterateApiRefPages (phase1_walk_pagination.go) resolves each deep
// page and Puts it into the IDENTITY-FREE widgetContent cell via
// populateWidgetContentL1. That function gates the Put on three sinks read
// from ctx: StageErrorSink, ExternalTouchedSink, UAFTouchedSink. The page-1
// walk (phase1_walk.go) installs all three on its resolve ctx; the drain
// installed only the StageErrorSink. With no UAF / external sink on ctx the
// resolver's bumps land on a nil receiver, the gates read Count()==0, and the
// Put proceeds.
//
// WHY THIS IS NOT A LIVE LEAK TODAY, AND HOW THE ARM UNMASKS IT. Every widget
// the drain reaches is isApiRefTemplateDriven (apiRef + resourcesRefsTemplate),
// and isRBACSensitiveApiRefWidget (the routing heuristic consulted FIRST inside
// populateWidgetContentL1) returns true for that same shape, so the Put is
// declined before any sink gate is read. The 1.12.3 A-1/R-1 sink gates exist
// precisely so the shared cell does NOT rest on that heuristic: it is
// declaration-shaped and it DE-CLASSIFIES ON ACCESSOR ERROR
// (`rrt, _ := widgets.GetResourcesRefsTemplate(obj)` discards the error and
// reads len()==0). The arm drives that documented de-classification: the
// resolved page envelope carries a spec.apiRef and a spec.resourcesRefsTemplate
// whose item is not an object, so GetResourcesRefsTemplate errors, the
// heuristic says "not sensitive", and the sink gates become the ONLY thing
// between the drained page and the shared cell. The CONTROL sub-arm (same
// envelope, nothing bumped) proves the bypass is effective: the Put lands, so
// the decline in the main arms is the sink gate's doing and not the heuristic's.
//
// THE RESOLVE SEAM. paginationResolvePageFn stands in for widgets.Resolve and
// marks the sink found on the ctx it was handed — the same thing the apiref
// chokepoint (cache.BumpUAFTouched) and the api resolver's external-endpoint
// branch (ExternalTouchedSinkFromContext(gctx).Bump()) do in production; those
// bump sites are pinned by their own arms (apiref/a1_uaf_sink_bump_test.go,
// restactions/api/resolve_inprocess_falsifier_test.go). What this arm pins is
// the WIRING at the drain: the sink the resolve sees is the sink the Put gate
// reads. Precedent: TestR1_SeedOneWidget_ResolveCtxCarriesTheGatesSink.
//
// RED on origin/main (c7533929): both refiltered and external sub-arms find the
// page cell Put. GREEN once the drain installs the shared widgetContent sink set.
//
// #450 review C1 (after #440 / #398 added the SensitiveTouchedSink): the
// sensitive sink is folded into withWidgetContentPutSinks, so the drain gets
// EVERY Put sink from one helper. Two more arms below:
//   - TestIssue450_WithWidgetContentPutSinks_InstallsEveryGateSink — the
//     completeness arm (non-nil, behavioural per gate, and an AST match of
//     populate's gates against the helper's installs);
//   - TestIssue450_DrainWidgetContentPut_DeclinesOnSecretRead — a drain whose
//     REAL resolve reads a core v1/secrets object must not Put.

package dispatchers

import (
	"context"
	"encoding/base64"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// heuristicDeclassifiedPageEnvelope is a drained page envelope that IS the
// apiRef+template shape in intent, but whose resourcesRefsTemplate item fails
// to decode — so isRBACSensitiveApiRefWidget de-classifies it (accessor error →
// len 0) and populateWidgetContentL1 falls through to its sink gates.
func heuristicDeclassifiedPageEnvelope(ns, name string, page int) *unstructured.Unstructured {
	e := nonRBACSensitivePageEnvelope(ns, name, page, false)
	spec := e.Object["spec"].(map[string]any)
	spec["resourcesRefsTemplate"] = []any{"not-an-object"}
	return e
}

func TestIssue450_DrainWidgetContentPut_DeclinesOnUAFAndExternal(t *testing.T) {
	const ns, name = "krateo-system", "compositions-page-datagrid"

	// Precondition for the whole arm: the envelope really does slip past the
	// heuristic. If a future change makes the heuristic fail closed on accessor
	// error, this arm would go vacuous — say so loudly instead.
	if isRBACSensitiveApiRefWidget(heuristicDeclassifiedPageEnvelope(ns, name, 2).Object) {
		t.Fatal("PRECONDITION: isRBACSensitiveApiRefWidget now classifies the accessor-error envelope as sensitive — " +
			"the heuristic no longer masks the drain's sink gates, so this arm cannot observe them; rework the bypass")
	}

	cases := []struct {
		name string
		// mark stands in for the resolver's production bump on the resolve
		// ctx. Returns whether a live sink was present to receive it.
		mark      func(ctx context.Context) bool
		wantPut   bool
		counterFn func() uint64
	}{
		{
			name: "uaf_refilter",
			mark: func(ctx context.Context) bool {
				present := cache.UAFTouchedSinkFromContext(ctx) != nil
				cache.BumpUAFTouched(ctx)
				return present
			},
			counterFn: func() uint64 { return uint64(cache.WidgetsUAFPutDeclined()) },
		},
		{
			name: "external_endpoint",
			mark: func(ctx context.Context) bool {
				s := cache.ExternalTouchedSinkFromContext(ctx)
				s.Bump() // nil-receiver-safe, as in production
				return s != nil
			},
			counterFn: cache.ExternalSkippedPut,
		},
		{
			name:    "control_nothing_marked",
			mark:    func(context.Context) bool { return true },
			wantPut: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CACHE_ENABLED", "true")
			t.Setenv("RESOLVED_CACHE_ENABLED", "true")
			t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
			cache.ResetResolvedCacheForTest()
			t.Cleanup(cache.ResetResolvedCacheForTest)
			cache.ResetDepsForTest()
			t.Cleanup(cache.ResetDepsForTest)
			cache.RegisterUAFPutDeclineMetricsForTest()
			cache.ResetUAFPutDeclineCountersForTest()
			t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

			paginationTestMu.Lock()
			defer paginationTestMu.Unlock()
			drainAllCustomerInFlight()

			oldCap := phase1MaxApiRefPagesForTest
			phase1MaxApiRefPagesForTest = 2 // drain page 2 only
			t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })

			prevFetch, prevResolve := paginationFetchPageFn, paginationResolvePageFn
			t.Cleanup(func() {
				paginationFetchPageFn = prevFetch
				paginationResolvePageFn = prevResolve
			})
			paginationFetchPageFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
				return fetchOKResult(ns, name)
			}
			var resolved, sinkPresent bool
			paginationResolvePageFn = func(ctx context.Context, _ widgets.ResolveOptions) (*unstructured.Unstructured, error) {
				resolved = true
				sinkPresent = tc.mark(ctx)
				return heuristicDeclassifiedPageEnvelope(ns, name, 2), nil
			}

			var before uint64
			if tc.counterFn != nil {
				before = tc.counterFn()
			}

			gvr := drainPageCellGVR()
			iterateApiRefPages(
				context.Background(),
				newPhase1Walker(nil, "krateo-system"),
				newUnstructuredWidget(ns, name),
				gvr,
				fakePage1Driven(), // apiRef+template, wants continue → loop entry
				1,                 // depth
				5,                 // perPage (resolution)
				-1,                // keyPerPage (root tuple)
				-1,                // keyPage (root tuple)
				"krateo-system",
			)

			if !resolved {
				t.Fatal("PRECONDITION: the drain never reached the page resolve — nothing was exercised")
			}
			key, _ := widgetContentL1Key(gvr, ns, name, -1, drainKeyPageFor(-1, 2))
			if key == "" {
				t.Fatal("PRECONDITION: widgetContent layer must be live (empty key)")
			}
			c := cache.ResolvedCache()
			if c == nil {
				t.Fatal("PRECONDITION: expected a live resolved cache")
			}
			entry, hit := c.Get(key)

			if tc.wantPut {
				if !hit {
					t.Fatalf("CONTROL BROKE: an unmarked drained page was not Put at %q — either the heuristic "+
						"bypass stopped working or the drain's Put is disabled wholesale; the decline arms would "+
						"then pass vacuously", key)
				}
				return
			}
			if hit {
				t.Fatalf("#450 RED (%s): the pagination drain Put a page envelope into the IDENTITY-FREE shared "+
					"widgetContent cell %q (%d bytes) although its resolve was marked. sinkOnResolveCtx=%v — the "+
					"drain's resolve ctx lacks the sink populateWidgetContentL1 gates on, so the gate is inert on "+
					"this path and the cell's isolation rests on isRBACSensitiveApiRefWidget alone",
					tc.name, key, len(entry.RawJSON), sinkPresent)
			}
			if !sinkPresent {
				t.Fatalf("%s: the Put was declined but the resolve ctx carried no sink — declined for the wrong reason", tc.name)
			}
			if got := tc.counterFn() - before; got != 1 {
				t.Fatalf("%s: the decline must route through its gate exactly once; counter delta=%d want 1", tc.name, got)
			}
		})
	}
}

// TestIssue450_WithWidgetContentPutSinks_InstallsEveryGateSink pins the helper's
// set against populateWidgetContentL1's gates. Both widgetContent Put paths
// (phase1Walker.walk, iterateApiRefPages) get their sinks ONLY from this helper,
// and the per-gate populate arms (e.g. TestR1_WidgetContentPopulate_*) install
// their own sink and call populateWidgetContentL1 directly, so they cannot see
// a sink dropped from the helper. Adding a gate to populateWidgetContentL1 means
// adding its sink to the helper AND a row in widgetContentPutGates.
//
// #450 review C1 (a) — COMPLETENESS. Three sub-arms, each failing on its own
// when a sink is dropped from the helper:
//   - installs: every gate's sink is non-nil on the helper's ctx.
//   - each_gate_declines_via_the_helper_sink: BEHAVIOURAL. For each gate, the
//     sink is fetched from the helper's ctx and bumped (as its production bump
//     site would), then the real populateWidgetContentL1 runs on that ctx over a
//     heuristic-bypass envelope. It must decline. A dropped sink → FromContext
//     returns nil → the bump is a nil-receiver no-op → the page is Put → RED.
//     A control (nothing bumped) must Put, so no sub-arm is vacuous.
//   - populate_gate_set_matches_helper: STRUCTURAL, over widget_content.go's
//     AST. Every sink-reading gate inside populateWidgetContentL1 must map to a
//     known installer, and the helper must call each of those installers. A gate
//     added to populate without a row here, or an installer deleted from the
//     helper, fails.

// widgetContentPutGate is one sink-gated decline inside populateWidgetContentL1.
type widgetContentPutGate struct {
	reader    string // the cache.* call populate gates on
	installer string // the cache.With* call the helper must make
	// mark bumps the sink found on ctx; reports whether one was present.
	mark func(ctx context.Context) bool
}

var widgetContentPutGates = []widgetContentPutGate{
	{reader: "StageErrorSinkFromContext", installer: "WithStageErrorSink", mark: func(ctx context.Context) bool {
		s := cache.StageErrorSinkFromContext(ctx)
		s.Bump("450-completeness", "synthetic") // nil-receiver-safe
		return s != nil
	}},
	{reader: "ExternalTouchedSinkFromContext", installer: "WithExternalTouchedSink", mark: func(ctx context.Context) bool {
		s := cache.ExternalTouchedSinkFromContext(ctx)
		s.Bump()
		return s != nil
	}},
	{reader: "UAFTouchedSinkFromContext", installer: "WithUAFTouchedSink", mark: func(ctx context.Context) bool {
		present := cache.UAFTouchedSinkFromContext(ctx) != nil
		cache.BumpUAFTouched(ctx)
		return present
	}},
	// DeclineSensitivePut reads SensitiveTouchedSinkFromContext (cache/sensitive_touched_sink.go).
	{reader: "DeclineSensitivePut", installer: "WithSensitiveTouchedSink", mark: func(ctx context.Context) bool {
		s := cache.SensitiveTouchedSinkFromContext(ctx)
		s.Bump()
		return s != nil
	}},
}

func TestIssue450_WithWidgetContentPutSinks_InstallsEveryGateSink(t *testing.T) {
	t.Run("installs", func(t *testing.T) {
		ctx := withWidgetContentPutSinks(context.Background())
		if cache.StageErrorSinkFromContext(ctx) == nil {
			t.Error("withWidgetContentPutSinks must install a StageErrorSink (#313 Cache-A gate)")
		}
		if cache.ExternalTouchedSinkFromContext(ctx) == nil {
			t.Error("withWidgetContentPutSinks must install an ExternalTouchedSink (external-no-cache gate)")
		}
		if cache.UAFTouchedSinkFromContext(ctx) == nil {
			t.Error("withWidgetContentPutSinks must install a UAFTouchedSink (1.12.3 A-1/R-1 gate)")
		}
		if cache.SensitiveTouchedSinkFromContext(ctx) == nil {
			t.Error("withWidgetContentPutSinks must install a SensitiveTouchedSink (#398 gate)")
		}
	})

	t.Run("each_gate_declines_via_the_helper_sink", func(t *testing.T) {
		const ns, name = "krateo-system", "completeness-450"
		gvr := drainPageCellGVR()
		if isRBACSensitiveApiRefWidget(heuristicDeclassifiedPageEnvelope(ns, name, 1).Object) {
			t.Fatal("PRECONDITION: the heuristic no longer de-classifies the bypass envelope; rework the bypass")
		}
		run := func(t *testing.T, mark func(context.Context) bool) (put, present bool) {
			t.Setenv("CACHE_ENABLED", "true")
			t.Setenv("RESOLVED_CACHE_ENABLED", "true")
			t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
			cache.ResetResolvedCacheForTest()
			t.Cleanup(cache.ResetResolvedCacheForTest)
			cache.ResetDepsForTest()
			t.Cleanup(cache.ResetDepsForTest)
			ctx := withWidgetContentPutSinks(context.Background())
			present = mark(ctx)
			populateWidgetContentL1(ctx, gvr, newUnstructuredWidget(ns, name), -1, -1,
				heuristicDeclassifiedPageEnvelope(ns, name, 1), 0)
			key, _ := widgetContentL1Key(gvr, ns, name, -1, -1)
			if key == "" {
				t.Fatal("PRECONDITION: widgetContent layer must be live (empty key)")
			}
			_, put = cache.ResolvedCache().Get(key)
			return put, present
		}
		t.Run("control_nothing_marked", func(t *testing.T) {
			if put, _ := run(t, func(context.Context) bool { return true }); !put {
				t.Fatal("CONTROL BROKE: an unmarked populate did not Put — every gate sub-arm would pass vacuously")
			}
		})
		for _, g := range widgetContentPutGates {
			g := g
			t.Run(g.installer, func(t *testing.T) {
				put, present := run(t, g.mark)
				if put {
					t.Fatalf("#450 C1 COMPLETENESS RED: the %s gate did not decline — withWidgetContentPutSinks "+
						"installed no sink for it (sinkOnHelperCtx=%v), so its production bump lands nowhere on "+
						"both widgetContent Put paths", g.reader, present)
				}
				if !present {
					t.Fatalf("%s: declined without a sink on the helper ctx — declined for the wrong reason", g.reader)
				}
			})
		}
	})

	t.Run("populate_gate_set_matches_helper", func(t *testing.T) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "widget_content.go", nil, 0)
		if err != nil {
			t.Fatalf("parse widget_content.go: %v", err)
		}
		cacheCalls := func(fn string, match func(string) bool) map[string]bool {
			out := map[string]bool{}
			found := false
			for _, d := range f.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok || fd.Recv != nil || fd.Name.Name != fn {
					continue
				}
				found = true
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					if x, ok := sel.X.(*ast.Ident); ok && x.Name == "cache" && match(sel.Sel.Name) {
						out[sel.Sel.Name] = true
					}
					return true
				})
			}
			if !found {
				t.Fatalf("func %s not found in widget_content.go", fn)
			}
			return out
		}
		readers := cacheCalls("populateWidgetContentL1", func(s string) bool {
			return strings.HasSuffix(s, "SinkFromContext") || (strings.HasPrefix(s, "Decline") && strings.HasSuffix(s, "Put"))
		})
		installers := cacheCalls("withWidgetContentPutSinks", func(s string) bool {
			return strings.HasPrefix(s, "With") && strings.HasSuffix(s, "Sink")
		})
		known := map[string]string{}
		for _, g := range widgetContentPutGates {
			known[g.reader] = g.installer
		}
		if len(readers) == 0 {
			t.Fatal("PRECONDITION: found no sink-reading gate in populateWidgetContentL1 — the matcher is broken")
		}
		for r := range readers {
			inst, ok := known[r]
			if !ok {
				t.Errorf("populateWidgetContentL1 gates on cache.%s, which has no row in widgetContentPutGates — add its "+
					"sink to withWidgetContentPutSinks and a row here (#450)", r)
				continue
			}
			if !installers[inst] {
				t.Errorf("populateWidgetContentL1 gates on cache.%s but withWidgetContentPutSinks never calls cache.%s — "+
					"that gate is inert on both widgetContent Put paths (#450 C1)", r, inst)
			}
		}
		for _, g := range widgetContentPutGates {
			if !readers[g.reader] {
				t.Errorf("widgetContentPutGates lists cache.%s but populateWidgetContentL1 no longer calls it — drop the row", g.reader)
			}
		}
	})
}

// TestIssue450_DrainWidgetContentPut_DeclinesOnSecretRead — #450 review C1 (b):
// a deep page whose resolve READS A SECRET must not be Put into the shared
// widgetContent cell.
//
// REAL BOUNDARY: the test bumps no sink. The drain runs under the production
// drain ctx (withPhase1SAContext over snowplow's SA endpoint), and its page
// resolve is the REAL widgets.Resolve → apiref.Resolve → restactions resolver →
// api.Resolve per-call dispatch. That dispatch bumps the SensitiveTouchedSink it
// finds on ITS ctx for a core v1/secrets path (restactions/api/resolve.go
// dispatchOneCall), and the read falls past the informer pivot to the apiserver
// (Gate 5b: secrets are never informed). The fake apiserver is the #398 one
// (psFakeAPIServer): it serves the Secret, sentinel in .data, to the SA token.
// The only seams are the page fetch (paginationFetchPageFn returns the widget
// CR) and a WRAPPER around the real widgets.Resolve that swallows its tail
// crdschema.ValidateObjectStatus error. That validation needs a CRD GET the fake
// apiserver does not serve, and widgetData is already rendered by then (the
// a3Serve / TestS398_SeedMemoHitStillDeclinesSecret pattern). If the drain's
// resolve ctx carries no sensitive sink, the production bump lands on a nil
// receiver, the gate reads Count()==0, and the Secret-derived page body is Put
// into the identity-free shared cell.
//
// The widget CR uses the same heuristic bypass as the UAF/external arm (an rrt
// item that is not an object), so the sink gate is the ONLY thing between the
// drained page and the cell. CONTROL: the identical drain over a ConfigMap step
// (non-sensitive, informer-served) MUST Put the page, sentinel included. That
// proves the bypass works on the real resolve and that the secret arm's decline
// is the sensitive gate's doing.
func TestIssue450_DrainWidgetContentPut_DeclinesOnSecretRead(t *testing.T) {
	for _, tc := range []struct {
		name    string
		arm     psArm
		wantPut bool
	}{
		{name: "secret_read", arm: s398Arm()},
		{name: "control_configmap_read", arm: psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}, wantPut: true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			k423Env(t)
			t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
			a := tc.arm

			ra := psRACR(a)
			// No widgetDataTemplate: any wdt makes the heuristic classify the
			// widget (len(wdt) > 0). The apiRef RA is still resolved by the real
			// widgets.Resolve, so its Secret step is still dispatched under the
			// drain's resolve ctx.
			wcr := h1WidgetUnstructured(map[string]any{
				"apiRef": map[string]any{"name": psRAName, "namespace": h1NS},
				// Heuristic bypass (see heuristicDeclassifiedPageEnvelope): the rrt
				// accessor errors, so isRBACSensitiveApiRefWidget de-classifies.
				"resourcesRefsTemplate": []any{"not-an-object"},
			})
			if isRBACSensitiveApiRefWidget(wcr.Object) {
				t.Fatal("PRECONDITION: the heuristic classifies the bypass widget as sensitive — the arm cannot observe the sink gate")
			}
			// snowplow's SA reads everything (the chart's */* get/list/watch), so
			// the drain's objects.Get of the RESTAction is admitted by the real
			// RBAC filter under the SA identity, as in production.
			saRead := []runtime.Object{
				&rbacv1.ClusterRole{
					ObjectMeta: metav1.ObjectMeta{Name: "s450-sa-read-all"},
					Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get", "list", "watch"}, APIGroups: []string{"*"}, Resources: []string{"*"}}},
				},
				&rbacv1.ClusterRoleBinding{
					ObjectMeta: metav1.ObjectMeta{Name: "s450-sa-read-all", UID: types.UID("uid-s450-sa")},
					Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: "krateo-system", Name: "snowplow"}},
					RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "s450-sa-read-all"},
				},
			}
			psBuildWatcher(t, a, append(append(k423WidgetExtras(a), saRead...), runtime.Object(ra))...)
			if _, ch := cache.Global().EnsureResourceType(h1RAGVR); ch != nil {
				select {
				case <-ch:
				case <-time.After(5 * time.Second):
					t.Fatal("RA informer did not sync")
				}
			}
			counters := k423Counters()
			counters[psSAUser] = &atomic.Int64{}
			srv := psFakeAPIServer(t, a, counters)
			psSeedClientconfigs(t, srv.URL)
			// The drain ctx derives the SA username from the JWT `sub` of the SA
			// token (phase1SAUsername), so the SA endpoint carries a JWT-shaped
			// token. A front proxy maps it to the fake apiserver's SA token.
			saJWT := "e30." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"`+psSAUser+`"}`)) + ".sig"
			upstream, _ := url.Parse(srv.URL)
			rp := httputil.NewSingleHostReverseProxy(upstream)
			var targetDials atomic.Int64 // apiserver reads of the step's target object
			targetPath := "/api/v1/namespaces/" + psTargetNS + "/" + a.target.Resource + "/" + psTargetObj
			front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == targetPath {
					targetDials.Add(1)
				}
				if r.Header.Get("Authorization") == "Bearer "+saJWT {
					r.Header.Set("Authorization", "Bearer tok-sa")
				}
				rp.ServeHTTP(w, r)
			}))
			t.Cleanup(front.Close)
			cache.RegisterUAFPutDeclineMetricsForTest()
			cache.ResetUAFPutDeclineCountersForTest()
			t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

			paginationTestMu.Lock()
			defer paginationTestMu.Unlock()
			drainAllCustomerInFlight()
			oldCap := phase1MaxApiRefPagesForTest
			phase1MaxApiRefPagesForTest = 2 // drain page 2 only
			t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })

			prevFetch, prevResolve := paginationFetchPageFn, paginationResolvePageFn
			t.Cleanup(func() {
				paginationFetchPageFn = prevFetch
				paginationResolvePageFn = prevResolve
			})
			paginationFetchPageFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
				return objects.Result{GVR: h1WidgetGVR, Unstructured: wcr.DeepCopy()}
			}
			var resolved bool
			var resolveErr error
			paginationResolvePageFn = func(ctx context.Context, opts widgets.ResolveOptions) (*unstructured.Unstructured, error) {
				resolved = true
				out, err := widgets.Resolve(ctx, opts) // REAL resolve
				resolveErr = err
				// Swallow ONLY the tail crdschema.ValidateObjectStatus failure
				// (Resolve wraps it as a 400 StatusError). Anything else, e.g.
				// an apiRef failure, propagates and the drain declines on it, as
				// in production. The control would then fail loudly.
				var se *apierrors.StatusError
				if err != nil && errors.As(err, &se) && se.ErrStatus.Code == http.StatusBadRequest {
					return out, nil
				}
				return out, err
			}

			saEP := endpoints.Endpoint{ServerURL: front.URL, Token: saJWT}
			saRC := &rest.Config{Host: front.URL, BearerToken: saJWT}
			if u, ok := phase1SAUsername(saJWT); !ok || u != psSAUser {
				t.Fatalf("PRECONDITION: the drain ctx must carry the SA identity (got %q ok=%v)", u, ok)
			}
			drainCtx := withPhase1SAContext(context.Background(), saEP, saRC)

			sensBefore, extBefore, uafBefore := sensitiveSkipped398(), cache.ExternalSkippedPut(), cache.WidgetsUAFPutDeclined()
			iterateApiRefPages(drainCtx, newPhase1Walker(saRC, psAuthnNS), wcr.DeepCopy(), h1WidgetGVR,
				fakePage1Driven(), 1, 5, -1, -1, psAuthnNS)
			if !resolved {
				t.Fatal("PRECONDITION: the drain never reached the page resolve")
			}

			key, _ := widgetContentL1Key(h1WidgetGVR, h1NS, h1WName, -1, drainKeyPageFor(-1, 2))
			if key == "" {
				t.Fatal("PRECONDITION: widgetContent layer must be live (empty key)")
			}
			entry, hit := cache.ResolvedCache().Get(key)
			sensDelta := sensitiveSkipped398() - sensBefore
			extDelta := cache.ExternalSkippedPut() - extBefore
			uafDelta := uint64(cache.WidgetsUAFPutDeclined() - uafBefore)
			t.Logf("%s: pagePut=%v sensitiveDeclines=%d externalDeclines=%d uafDeclines=%d saApiserverDials=%d targetApiserverDials=%d resolveErr=%v",
				tc.name, hit, sensDelta, extDelta, uafDelta, counters[psSAUser].Load(), targetDials.Load(), resolveErr)

			if tc.wantPut {
				if !hit {
					t.Fatalf("CONTROL BROKE: a non-sensitive drained page was not Put — the real-resolve bypass does "+
						"not reach the sink gates, so the secret arm would pass vacuously "+
						"(sensitiveDeclines=%d externalDeclines=%d uafDeclines=%d)", sensDelta, extDelta, uafDelta)
				}
				if sensDelta != 0 {
					t.Fatalf("CONTROL: a ConfigMap read must not tick the sensitive decline (delta=%d)", sensDelta)
				}
				return
			}
			if hit {
				t.Fatalf("#450 C1 RED (secret_read): the pagination drain Put a page whose REAL resolve read a core "+
					"v1/secrets object into the IDENTITY-FREE shared widgetContent cell (%d bytes, carriesSecretData=%v) — "+
					"the drain's resolve ctx lacks the SensitiveTouchedSink, so the resolver's bump landed nowhere",
					len(entry.RawJSON), psHasSentinel(entry.RawJSON))
			}
			if targetDials.Load() == 0 {
				t.Fatal("secret_read: the Secret step never reached the apiserver — the real dispatch (Gate 5b fall-through) did not run")
			}
			if extDelta != 0 || uafDelta != 0 {
				t.Fatalf("secret_read: an earlier gate declined first (external=%d uaf=%d) — the sensitive gate was masked",
					extDelta, uafDelta)
			}
			if sensDelta != 1 {
				t.Fatalf("secret_read: the decline must route through the sensitive gate exactly once; delta=%d want 1", sensDelta)
			}
			c := cache.ResolvedCache()
			for _, k := range c.KeysForTest() {
				if e, ok := c.GetNoTouch(k); ok && e != nil && psHasSentinel(e.RawJSON) {
					t.Fatal("#398/#450: an L1 cell holds the Secret's data after the drain")
				}
			}
		})
	}
}
