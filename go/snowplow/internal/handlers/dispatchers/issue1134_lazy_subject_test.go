// issue1134_lazy_subject_test.go — #1134 (the #1130 review's survivor): a
// FRESH armed hit derives no requester digest, at every hit site, through the
// real handlers (the #547 harness: real widgets and RESTAction handlers, real
// store, real refresher). #1062 times a stale serve per requester, and the
// requester's subject is a keyed digest (cache.StaleSubject, ~9 allocations);
// NoteStaleHit derives it only when a serve opens, so a fresh hit — nearly
// every hit — allocates nothing for it. That property was pinned only inside
// NoteStaleHit: a hit site that computed staleSubjectOf(ctx) eagerly and
// wrapped the result in a closure passed every arm.
//
// The digest is counted where every hit site derives it (staleSubjectOf,
// staleSubjectHookForTest): the handler allocates on its own, so a whole-hit
// AllocsPerRun has no baseline that excludes the digest, while a derivation
// that does not happen allocates nothing. Sites: the RESTAction L1 hit
// (restactions.go), the per-cohort widgets L1 hit and the identity-free
// widgetContent shell hit (widgets.go).
//
// Mutation → RED: at any one of the three sites, an eager staleSubjectOf(ctx)
// wrapped in a closure; or staleHitWritePoint doing the same.
package dispatchers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// k1134Derivations counts staleSubjectOf calls while installed.
func k1134Derivations(t *testing.T) *atomic.Int64 {
	t.Helper()
	n := &atomic.Int64{}
	f := func() { n.Add(1) }
	staleSubjectHookForTest.Store(&f)
	t.Cleanup(func() { staleSubjectHookForTest.Store(nil) })
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
