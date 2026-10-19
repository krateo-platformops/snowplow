//go:build unit || integration

package dispatchers

// issue777_bprime_dispatch_test.go — #777 (b′) arms B1–B8 through the REAL
// Widgets().ServeHTTP Put gate: a widget with no extras-reading spec surface is
// exempt from the F6 undeclared-extras quarantine (the Put is accepted under
// the unchanged declared-only key, and the exempt counter moves), and a widget
// with ANY such surface — even null, empty, malformed or unknown — is still
// declined. The predicate itself and the resolve-side properties (B7, B8, B9,
// B10) are in internal/resolvers/widgets/issue777_bprime_test.go.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type b777Result struct {
	served, persisted []byte
	key               string
	resolves          int
	exemptDelta       uint64
	declineDelta      uint64
}

// b777Serve drives one /call for a widget with the given spec and ?extras=.
// Without an apiRef the resolve is the REAL widgets.Resolve (hermetic: an
// apiRef-less widget never dials; only the tail CRD-schema validate error is
// swallowed, as a3Serve does). With an apiRef the resolver is a fake that
// returns the widget: the Put gate reads the CR, not the resolve, and a real
// apiRef resolve would fail before the gate. fresh=false keeps L1 (the B1 hit).
func b777Serve(t *testing.T, spec map[string]any, extras map[string]any, fresh bool) b777Result {
	t.Helper()
	if fresh {
		cache.ResetResolvedCacheForTest()
	}
	var res b777Result
	cr := h1WidgetUnstructured(spec)
	_, hasApiRef := spec["apiRef"]
	restore := installWidgetFakes(t, cr, func() bool { return true },
		func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
			res.resolves++
			if hasApiRef {
				return h1WidgetUnstructured(spec), nil
			}
			out, _ := widgets.Resolve(ctx, opts)
			if out == nil {
				out = &unstructured.Unstructured{Object: map[string]any{}}
			}
			return out, nil
		})
	defer restore()

	reqCtx := h1ReqCtx(h1User)
	key, handle, _ := dispatchCacheLookupKey(reqCtx, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource, h1NS, h1WName, -1, -1,
		effectiveKeyExtras(reqCtx, cr.Object, extras))
	if handle == nil || key == "" {
		t.Fatalf("PRECONDITION: expected a live cache handle and key (key=%q)", key)
	}
	res.key = key

	target := "/call"
	if len(extras) > 0 {
		b, err := json.Marshal(extras)
		if err != nil {
			t.Fatalf("marshal extras: %v", err)
		}
		target += "?extras=" + url.QueryEscape(string(b))
	}
	ex0, dec0 := WidgetUndeclaredExtrasExemptTotal(), widgetSkippedUndeclaredExtrasPutTotal.Load()
	rec := httptest.NewRecorder()
	Widgets().ServeHTTP(rec, httptest.NewRequest("GET", target, nil).WithContext(reqCtx))
	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	res.exemptDelta = WidgetUndeclaredExtrasExemptTotal() - ex0
	res.declineDelta = widgetSkippedUndeclaredExtrasPutTotal.Load() - dec0
	res.served = rec.Body.Bytes()
	if entry, hit := handle.Get(key); hit {
		res.persisted = entry.RawJSON
	}
	return res
}

func b777ExemptSpec() map[string]any {
	return map[string]any{
		"widgetData": map[string]any{"label": "chrome-777"},
		"keyExtras":  []any{"projects"},
	}
}

// B1 — an exempt widget caches under undeclared route/query extras, and a
// second request with different undeclared extras HITS the same cell.
func TestB777_B1_ExemptWidgetCachesAndASecondRequestHits(t *testing.T) {
	h1BuildWatcher(t)
	a := b777Serve(t, b777ExemptSpec(), map[string]any{"name": "a", "ns": "x", "q": "z"}, true)
	if a.persisted == nil {
		t.Fatalf("#777 B1 RED: an extras-free widget's Put was declined under undeclared extras (decline +%d)", a.declineDelta)
	}
	if a.exemptDelta != 1 || a.declineDelta != 0 {
		t.Fatalf("B1: exempt +%d decline +%d, want +1/+0", a.exemptDelta, a.declineDelta)
	}
	b := b777Serve(t, b777ExemptSpec(), map[string]any{"name": "b"}, false)
	if b.key != a.key {
		t.Fatalf("B1: the key moved with undeclared extras (A=%q B=%q) — the declared-only key must be unchanged", a.key, b.key)
	}
	if b.resolves != 0 {
		t.Fatalf("B1: the second request resolved (%d) instead of hitting the cell the first one stored", b.resolves)
	}
	if !bytes.Equal(a.served, b.served) {
		t.Fatalf("B1: bodies differ across undeclared extras:\nA %s\nB %s", a.served, b.served)
	}
}

// B11 — an exempt PARENT (the app-shell shape: static widgetData + static
// resourcesRefs to child widgets, no keyExtras) changes nothing about its
// children. A child is its own /call, built from the static ref (no extras ride
// the path — B7), keyed by ITS OWN declared keys over whatever extras the client
// sends it. So the child's key is the same before and after the parent's exempt
// Put, ignores the page's route params, and still partitions on what the child
// declares.
func TestB777_B11_ExemptParentLeavesChildKeysUnchanged(t *testing.T) {
	h1BuildWatcher(t)
	ctx := h1ReqCtx(h1User)
	childCR := h1WidgetUnstructured(map[string]any{
		"widgetData":         map[string]any{},
		"keyExtras":          []any{"projects"},
		"widgetDataTemplate": []any{map[string]any{"forPath": "p", "expression": "${ .projects }"}},
	})
	childKey := func(extras map[string]any) string {
		k, _, _ := dispatchCacheLookupKey(ctx, "widgets", h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource,
			h1NS, "child-777", -1, -1, effectiveKeyExtras(ctx, childCR.Object, extras))
		if k == "" {
			t.Fatal("PRECONDITION B11: no child key derived")
		}
		return k
	}
	page := map[string]any{"projects": "p1", "namespace": "team-a", "name": "comp-1"}
	before := childKey(page)

	parent := map[string]any{
		"widgetData": map[string]any{"hasSider": true, "sider": map[string]any{"resourceRefId": "child-777"}},
		"resourcesRefs": map[string]any{"items": []any{map[string]any{
			"id": "child-777", "apiVersion": h1WidgetGVR.Group + "/" + h1WidgetGVR.Version,
			"resource": h1WidgetGVR.Resource, "namespace": h1NS, "name": "child-777", "verb": "GET",
		}}},
	}
	r := b777Serve(t, parent, page, true)
	if r.persisted == nil || r.exemptDelta != 1 {
		t.Fatalf("PRECONDITION B11: the app-shell-shaped parent must be exempt and Put (stored=%v exempt +%d)", r.persisted != nil, r.exemptDelta)
	}
	if bytes.Contains(r.persisted, []byte("comp-1")) || bytes.Contains(r.persisted, []byte("team-a")) {
		t.Fatalf("B11: the exempt parent's stored body carries the page's route params: %s", r.persisted)
	}
	if after := childKey(page); after != before {
		t.Fatalf("#777 B11 RED: the child's key moved after the parent's exempt Put (%q → %q)", before, after)
	}
	if other := childKey(map[string]any{"projects": "p1", "namespace": "team-b", "name": "comp-2"}); other != before {
		t.Fatalf("B11: the child's key moved with undeclared route params (%q vs %q)", other, before)
	}
	if childKey(map[string]any{"projects": "p2"}) == before {
		t.Fatal("B11: the child's declared key (projects) no longer partitions its cell")
	}
}

// B2–B6 (and B7, B8) — which widgets the exemption reaches.
func TestB777_B2toB8_QuarantineStillAppliesWhereTheBodyCanReadExtras(t *testing.T) {
	h1BuildWatcher(t)
	undeclared := map[string]any{"name": "a", "ns": "x"}
	with := func(k string, v any) map[string]any {
		s := b777ExemptSpec()
		s[k] = v
		return s
	}
	declined := []struct {
		name string
		spec map[string]any
	}{
		{"B2_apiRef", with("apiRef", map[string]any{"name": "ra-777", "namespace": h1NS})},
		{"B3_widgetDataTemplate", with("widgetDataTemplate", []any{map[string]any{"forPath": "x", "expression": "${ 1 }"}})},
		{"B4_resourcesRefsTemplate", with("resourcesRefsTemplate", []any{})},
		{"B5_malformed_widgetDataTemplate", with("widgetDataTemplate", "not-a-list")},
		{"B6_unknown_field", with("futureThing", map[string]any{})},
	}
	for _, c := range declined {
		t.Run(c.name, func(t *testing.T) {
			r := b777Serve(t, c.spec, undeclared, true)
			if r.persisted != nil || r.declineDelta != 1 || r.exemptDelta != 0 {
				t.Fatalf("#777 %s RED: want declined (no Put, decline +1, exempt +0); got stored=%v decline +%d exempt +%d",
					c.name, r.persisted != nil, r.declineDelta, r.exemptDelta)
			}
		})
	}

	t.Run("B7_static_resourcesRefs_exempt", func(t *testing.T) {
		r := b777Serve(t, with("resourcesRefs", map[string]any{"items": []any{}}), undeclared, true)
		if r.persisted == nil || r.exemptDelta != 1 || r.declineDelta != 0 {
			t.Fatalf("B7: a static-resourcesRefs widget must be exempt; stored=%v exempt +%d decline +%d",
				r.persisted != nil, r.exemptDelta, r.declineDelta)
		}
	})

	t.Run("B8_identity_extras_unchanged", func(t *testing.T) {
		// Identity keys never reached the quarantine (A2/A6): the Put follows
		// today's rule, the strip keeps "evil" out of the body, and the
		// exemption does not count it — it is not an undeclared extra.
		r := b777Serve(t, b777ExemptSpec(), map[string]any{"username": "evil-777"}, true)
		if r.persisted == nil || r.declineDelta != 0 {
			t.Fatalf("B8: an identity-only request must be Put exactly as before; stored=%v decline +%d", r.persisted != nil, r.declineDelta)
		}
		if r.exemptDelta != 0 {
			t.Fatalf("#777 B8 RED: the exemption counted an identity key (+%d) — it must sit after the identity handling", r.exemptDelta)
		}
		if bytes.Contains(r.served, []byte("evil-777")) || bytes.Contains(r.persisted, []byte("evil-777")) {
			t.Fatalf("B8: the undeclared identity extra reached the body: %s", r.persisted)
		}
	})

	t.Run("declared_only_request_not_counted", func(t *testing.T) {
		r := b777Serve(t, b777ExemptSpec(), map[string]any{"projects": "p1"}, true)
		if r.persisted == nil || r.exemptDelta != 0 || r.declineDelta != 0 {
			t.Fatalf("a fully-declared request is a plain Put (no exemption); stored=%v exempt +%d decline +%d",
				r.persisted != nil, r.exemptDelta, r.declineDelta)
		}
	})
}
