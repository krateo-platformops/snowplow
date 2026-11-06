// issue1134_lazy_subject_test.go — #1134 (the #1130 review's survivor): a
// FRESH armed hit derives no requester digest, at every hit site, through the
// real handlers (the #547 harness: real widgets and RESTAction handlers, real
// store, real refresher). #1062 times a stale serve per requester, and the
// requester's subject is a keyed digest (cache.StaleSubject, ~9 allocations);
// NoteStaleHit derives it only when a serve opens, so a fresh hit — nearly
// every hit — allocates nothing for it. That property was pinned only inside
// NoteStaleHit: a hit site that computed the digest eagerly and wrapped the
// result in a closure passed every arm.
//
// TWO PINS, because no counter reaches the whole class.
//
//   - The digest is counted at the digest, inside cache.StaleSubject
//     (SetStaleSubjectHookForTest), so an eager cache.StaleSubject is seen
//     whether or not it goes through staleSubjectOf. The handler allocates on
//     its own, so a whole-hit AllocsPerRun has no baseline that excludes the
//     digest, while a derivation that does not happen allocates nothing.
//   - Nothing below StaleSubject can be counted: it is redact.User, and a fresh
//     hit pays redact.User for its own log labels (beginPerCall's user field is
//     built before slog reads the level), so a count there is never 0. The
//     structural arm closes that gap: every production NoteStaleHit call hands
//     over staleSubjectOf itself, so no closure over an eager value of any
//     origin can reach it.
//
// Sites: the RESTAction L1 hit (restactions.go), the per-cohort widgets L1 hit
// and the identity-free widgetContent shell hit (widgets.go).
//
// Mutation → RED: at any one of the three sites, an eager staleSubjectOf(ctx)
// or cache.StaleSubject wrapped in a closure, or staleHitWritePoint doing the
// same (the count); an eager redact.User wrapped in a closure (the structural
// arm).
package dispatchers

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// k1134Derivations counts cache.StaleSubject calls while installed.
func k1134Derivations(t *testing.T) *atomic.Int64 {
	t.Helper()
	n := &atomic.Int64{}
	t.Cleanup(cache.SetStaleSubjectHookForTest(func() { n.Add(1) }))
	return n
}

// k1134Get is h.get / h.getRA returning the recorder (its headers).
func k1134Get(h *k547Harness, handler http.Handler, gvrGroupVersion, resource, name string, k, m int) *httptest.ResponseRecorder {
	h.t.Helper()
	ex, _ := json.Marshal(k547Extras(m))
	u := "/call?apiVersion=" + url.QueryEscape(gvrGroupVersion) + "&resource=" + resource +
		"&name=" + name + "&namespace=" + k547WNS + "&extras=" + url.QueryEscape(string(ex))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", u, nil).WithContext(k547ReqCtx(k)))
	return rec
}

func TestIssue1134_AFreshArmedHitDerivesNoRequesterDigest(t *testing.T) {
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	const hits = 5
	widgetGV := k547WidgetGVR.Group + "/" + k547WidgetGVR.Version
	raGV := k547RAGVR.Group + "/" + k547RAGVR.Version

	check := func(t *testing.T, h *k547Harness, site string, handler http.Handler, gv, resource, name, wantClass string) {
		t.Helper()
		c0 := h.custCalls.Load()
		n := k1134Derivations(t)
		for i := 0; i < hits; i++ {
			rec := k1134Get(h, handler, gv, resource, name, 0, k547Changed)
			if rec.Code != http.StatusOK || rec.Header().Get(refreshKeyHeader) == "" || rec.Header().Get(refreshClassHeader) != wantClass {
				t.Fatalf("PRECONDITION (%s): an armed %s hit (code=%d key=%q class=%q)", site, wantClass,
					rec.Code, rec.Header().Get(refreshKeyHeader), rec.Header().Get(refreshClassHeader))
			}
		}
		if d := h.custCalls.Load() - c0; d != 0 {
			t.Fatalf("PRECONDITION (%s): %d customer resolve(s) — the requests were not L1 hits", site, d)
		}
		if s := k1062Layer(cache.StaleLayerL1); s.Serves != 0 {
			t.Fatalf("PRECONDITION (%s): the cell is not fresh, %d l1 stale serve(s) opened", site, s.Serves)
		}
		if got := n.Load(); got != 0 {
			t.Fatalf("RED (%s): %d fresh armed hit(s) derived the requester digest %d time(s), want 0: the subject must be derived only when a stale serve opens",
				site, hits, got)
		}
	}

	t.Run("restactions_l1_hit", func(t *testing.T) {
		h := newK547Harness(t, k547DenyBackground)
		cache.ResetStalePushForTest()
		t.Cleanup(cache.ResetStalePushForTest)
		if rec := k1134Get(h, RESTAction(), raGV, k547RAGVR.Resource, k547RAName, 0, k547Changed); rec.Code != http.StatusOK {
			t.Fatalf("PRECONDITION: the cold RESTAction GET fills the cell (code=%d)", rec.Code)
		}
		check(t, h, "restactions", RESTAction(), raGV, k547RAGVR.Resource, k547RAName, "restactions")
	})

	t.Run("widgets_l1_hit", func(t *testing.T) {
		h := newK547Harness(t, k547DenyBackground)
		h.warm()
		cache.ResetStalePushForTest()
		t.Cleanup(cache.ResetStalePushForTest)
		check(t, h, "widgets", Widgets(), widgetGV, k547WidgetGVR.Resource, k547WName, "widgets")
	})

	t.Run("widgetcontent_shell_hit", func(t *testing.T) {
		h := newK547Harness(t, k547DenyBackground)
		ctx := k547ReqCtx(0)
		contentKey, handle, inputs := dispatchWidgetContentKey(ctx,
			k547WidgetGVR.Group, k547WidgetGVR.Version, k547WidgetGVR.Resource, k547WNS, k547WName, -1, -1,
			effectiveKeyExtras(ctx, h.widgetU.Object, k547Extras(k547Changed)))
		if handle == nil || contentKey == "" {
			t.Fatalf("PRECONDITION: no widgetContent key")
		}
		shell := k547Widget()
		shell.Object["status"] = map[string]any{"widgetData": map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "reason": "v0"}}}}
		raw, _ := json.Marshal(shell.Object)
		putFixture(handle, contentKey, &cache.ResolvedEntry{RawJSON: raw, Inputs: inputs})
		cache.ResetStalePushForTest()
		t.Cleanup(cache.ResetStalePushForTest)
		check(t, h, "widgetContent", Widgets(), widgetGV, k547WidgetGVR.Resource, k547WName, "widgetContent")

		// Control: the counter sees the derivation when a serve DOES open (a
		// stale-derived shell), so its 0 above is a reading, not a blind spot.
		putFixture(handle, contentKey, &cache.ResolvedEntry{RawJSON: raw, Inputs: inputs, StaleDerived: true})
		n := k1134Derivations(t)
		if rec := k1134Get(h, Widgets(), widgetGV, k547WidgetGVR.Resource, k547WName, 0, k547Changed); rec.Code != http.StatusOK ||
			rec.Header().Get(refreshClassHeader) != "widgetContent" {
			t.Fatalf("PRECONDITION: the stale-derived shell is served as a content hit (code=%d class=%q)",
				rec.Code, rec.Header().Get(refreshClassHeader))
		}
		if s := k1062Layer(cache.StaleLayerAPIStage); s.Serves != 1 || n.Load() != 1 {
			t.Fatalf("control: a stale armed hit opens one serve (%d) and derives the digest once (%d)", s.Serves, n.Load())
		}
	})
}

// TestIssue1134_EveryHitSiteHandsNoteStaleHitTheLazyDerivation is the structural
// pin: the only subject function a production hit site may hand to
// cache.NoteStaleHit is staleSubjectOf itself, read from the package's source.
// The derivation count above cannot see an eager redact.User in a closure (see
// the file comment); this sees any closure, whatever it calls. The call count
// keeps a renamed or moved site from passing with nothing inspected.
func TestIssue1134_EveryHitSiteHandsNoteStaleHitTheLazyDerivation(t *testing.T) {
	const wantSites = 2 // staleHitWritePoint (both L1 hits), the widgetContent shell hit
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("PRECONDITION: read the package source: %v", err)
	}
	fset := token.NewFileSet()
	sites := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("PRECONDITION: parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "NoteStaleHit" {
				return true
			}
			sites++
			last := call.Args[len(call.Args)-1]
			if id, ok := last.(*ast.Ident); !ok || id.Name != "staleSubjectOf" {
				t.Errorf("RED (%s): NoteStaleHit must be handed staleSubjectOf itself: anything else can carry a digest derived on every hit",
					fset.Position(call.Pos()))
			}
			return true
		})
	}
	if sites != wantSites {
		t.Fatalf("PRECONDITION: %d NoteStaleHit call(s) in the package, want %d: a new hit site needs an arm above", sites, wantSites)
	}
}
