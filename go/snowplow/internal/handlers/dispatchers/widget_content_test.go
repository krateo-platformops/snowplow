// widget_content_test.go — Ship G (0.30.16x): unit coverage for the
// identity-free widget content L1 layer.
//
// Covers:
//   - TestWidgetContentKey_IdentityFree (AC-G.3): the content key MUST
//     hash to the same cell across admin and cyberjoker (Username/Groups
//     dropped from ComputeKey for CacheEntryClassWidgetContent).
//   - TestWidgetContentKey_DistinctFromWidgetsClass: the class string is
//     part of the hash — a widgetContent key for the same (gvr, ns, name)
//     hashes to a different cell from a per-user "widgets" key.
//   - TestWidgetContentKey_VariesByPerPage / VariesByPage: pagination is
//     part of the content key.
//   - TestDispatchWidgetContentKey_LayerOffReturnsNilHandle (AC-G.6): the
//     toggle WIDGET_CONTENT_L1_ENABLED=false bypasses the layer.
//   - TestDispatchWidgetContentKey_CacheOffReturnsNilHandle (AC-G.6):
//     CACHE_ENABLED=false short-circuits to nil handle.
//   - TestDispatchWidgetContentKey_IdentityOmittedFromKey: even when
//     UserInfo is on the context, the returned inputs MUST carry no
//     Username/Groups (defence in depth).
//   - TestGateWidgetEnvelope_NoIdentityFailsClosed: a request with no
//     UserInfo MUST get (nil, false).
//   - TestGateWidgetEnvelope_OverwritesAllowedFlags: the gate overwrites
//     per-item `allowed` under the request identity.
//   - TestGateWidgetEnvelope_PreservesOtherFields: identity-invariant
//     fields (id, path, verb, payload, widgetData) are passed through.
//   - TestGateWidgetEnvelope_ConcurrentSafe (-race): N goroutines run
//     the gate over a SHARED raw envelope under different identities;
//     no data race (the unmarshal produces a fresh map per call).
//   - TestRestVerbToKube: the verb-mapping is correct and round-trips.

package dispatchers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// --- shared test helpers (Ship G) ----------------------------------------

func someWidgetContentGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels",
	}
}

func newUnstructuredWidget(ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Panel",
		"metadata":   map[string]any{"namespace": ns, "name": name},
	}}
}

// setNested writes val at path obj[fields[0]][fields[1]]... creating
// intermediate maps as needed. Returns an error on a non-map intermediate.
func setNested(obj map[string]any, val any, fields ...string) error {
	cur := obj
	for i, f := range fields {
		if i == len(fields)-1 {
			cur[f] = val
			return nil
		}
		next, ok := cur[f].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[f] = next
		}
		cur = next
	}
	return nil
}

// --- key-shape tests ------------------------------------------------------

func TestWidgetContentKey_IdentityFree(t *testing.T) {
	// Ship 0.30.242 H.c-layered Phase 2c — identity is folded as
	// BindingUID for identity-bound classes; the widgetContent class
	// IGNORES it. Both keys carry a non-empty BindingUID to PROVE it is
	// ignored for this class.
	adminKey := cache.ComputeKey(cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "panels",
		Namespace:       "fireworks-app",
		Name:            "dashboard-summary",
		BindingUID:      "uid-admin-deadbeefcafebabe",
		PerPage:         5,
		Page:            1,
	})
	cjKey := cache.ComputeKey(cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "panels",
		Namespace:       "fireworks-app",
		Name:            "dashboard-summary",
		BindingUID:      "uid-cj-feedfacefeedface",
		PerPage:         5,
		Page:            1,
	})
	if adminKey != cjKey {
		t.Fatalf("widgetContent key MUST be identity-free across admin vs cyberjoker;\n  admin=%s\n  cj   =%s", adminKey, cjKey)
	}
}

func TestWidgetContentKey_DistinctFromWidgetsClass(t *testing.T) {
	wcKey := cache.ComputeKey(cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "panels",
		Namespace:       "ns",
		Name:            "w",
		PerPage:         -1,
		Page:            -1,
	})
	widgetsKey := cache.ComputeKey(cache.ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "panels",
		Namespace:       "ns",
		Name:            "w",
		BindingUID:      "uid-deadbeef", // any non-empty identity fold
		PerPage:         -1,
		Page:            -1,
	})
	if wcKey == widgetsKey {
		t.Fatalf("widgetContent key MUST differ from widgets-class key for the same (gvr, ns, name); got identical %s", wcKey)
	}
}

func TestWidgetContentKey_VariesByPerPage(t *testing.T) {
	base := cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "panels",
		Namespace:       "ns",
		Name:            "w",
		PerPage:         5,
		Page:            1,
	}
	k1 := cache.ComputeKey(base)
	base.PerPage = 10
	k2 := cache.ComputeKey(base)
	if k1 == k2 {
		t.Fatalf("widgetContent key MUST vary by PerPage; both perPage=5 and perPage=10 hashed to %s", k1)
	}
}

func TestWidgetContentKey_VariesByPage(t *testing.T) {
	base := cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "panels",
		Namespace:       "ns",
		Name:            "w",
		PerPage:         5,
		Page:            1,
	}
	k1 := cache.ComputeKey(base)
	base.Page = 2
	k2 := cache.ComputeKey(base)
	if k1 == k2 {
		t.Fatalf("widgetContent key MUST vary by Page; both page=1 and page=2 hashed to %s", k1)
	}
}

// --- dispatcher helper tests ---------------------------------------------

func TestDispatchWidgetContentKey_CacheOffReturnsNilHandle(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	_, h, in := dispatchWidgetContentKey(context.Background(),
		"g", "v", "r", "ns", "name", -1, -1, nil)
	if h != nil {
		t.Fatalf("CACHE_ENABLED=false must yield nil handle, got %T", h)
	}
	if in != nil {
		t.Fatalf("CACHE_ENABLED=false must yield nil inputs, got %+v", in)
	}
}

func TestDispatchWidgetContentKey_LayerOffReturnsNilHandle(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "false")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	_, h, in := dispatchWidgetContentKey(context.Background(),
		"g", "v", "r", "ns", "name", -1, -1, nil)
	if h != nil {
		t.Fatalf("WIDGET_CONTENT_L1_ENABLED=false must yield nil handle, got %T", h)
	}
	if in != nil {
		t.Fatalf("WIDGET_CONTENT_L1_ENABLED=false must yield nil inputs, got %+v", in)
	}
}

func TestDispatchWidgetContentKey_IdentityOmittedFromKey(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	// Even when a UserInfo is on the context, dispatchWidgetContentKey
	// MUST NOT key on identity — the layer is identity-free by design.
	// Two calls under different identities must produce the same key.
	adminCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "admin", Groups: []string{"system:authenticated"}}))
	cjCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "cyberjoker", Groups: []string{"narrow:rbac"}}))

	adminKey, _, adminIn := dispatchWidgetContentKey(adminCtx,
		"g", "v", "r", "ns", "name", -1, -1, nil)
	cjKey, _, cjIn := dispatchWidgetContentKey(cjCtx,
		"g", "v", "r", "ns", "name", -1, -1, nil)
	if adminKey != cjKey {
		t.Fatalf("widgetContent key MUST be identical across identities;\n  admin=%s\n  cj   =%s", adminKey, cjKey)
	}
	if adminIn == nil || cjIn == nil {
		t.Fatalf("inputs must be non-nil when layer is on")
	}
	if adminIn.BindingUID != "" {
		t.Fatalf("identity-free inputs must carry empty BindingUID; got admin BindingUID=%q",
			adminIn.BindingUID)
	}
	_ = cjIn
}

// --- gate tests ----------------------------------------------------------

// fakeEnvelope builds a minimal resolved widget envelope shape. It mimics
// the marshal output of widgets.Resolve: a CR (apiVersion+kind+metadata)
// with a status.resourcesRefs.items[] slice. Each item carries the
// fields gateWidgetEnvelope reads (path, verb, allowed).
func fakeEnvelope(items []map[string]any) []byte {
	envelope := map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Panel",
		"metadata":   map[string]any{"name": "dashboard", "namespace": "fireworks-app"},
		"status": map[string]any{
			"widgetData": map[string]any{
				"title": "Dashboard",
				"actions": map[string]any{
					"create": []any{
						map[string]any{"resourceRefId": "create-composition"},
					},
				},
			},
			"resourcesRefs": map[string]any{
				"items": items,
			},
		},
	}
	raw, _ := json.Marshal(envelope)
	return raw
}

func TestGateWidgetEnvelope_NoIdentityFailsClosed(t *testing.T) {
	// No UserInfo on ctx → fail-closed (nil, false). The caller
	// (widgets.go) falls through to the per-user L1 lookup, which
	// symmetrically nil-checks UserInfo.
	raw := fakeEnvelope([]map[string]any{
		{"id": "list-comp", "path": "/call?resource=compositions&apiVersion=composition.krateo.io/v1", "verb": "GET", "allowed": true},
	})
	gated, served := gateWidgetEnvelope(context.Background(), raw)
	if served {
		t.Fatalf("gate must fail closed without identity on ctx; got served=true, gated=%q", gated)
	}
	if gated != nil {
		t.Fatalf("fail-closed must return nil bytes; got %q", gated)
	}
}

func TestGateWidgetEnvelope_OverwritesAllowedFlags(t *testing.T) {
	// The gate MUST overwrite every status.resourcesRefs.items[].allowed
	// under the request identity via rbac.UserCan. In a test environment
	// without a typed-RBAC snapshot wired, rbac.UserCan falls back to
	// the SelfSubjectAccessReview path (or returns false defensively).
	// What we assert here is the OVERWRITE happens — the stored
	// allowed=true never makes it to the gated body unchanged.
	//
	// Stored envelope: every item carries allowed=true (SA-evaluated).
	// Gated body: items[].allowed is whatever rbac.UserCan returns for
	// the request identity (in this test, with no live typed-RBAC, it
	// is false). What we CHECK is that the gated body's allowed values
	// were derived from the request identity, NOT served verbatim.
	rawItems := []map[string]any{
		{"id": "a", "path": "/call?resource=compositions&apiVersion=composition.krateo.io/v1&namespace=fireworks-app", "verb": "GET", "allowed": true},
		{"id": "b", "path": "/call?resource=widgets&apiVersion=widgets.templates.krateo.io/v1&namespace=fireworks-app", "verb": "GET", "allowed": true},
	}
	raw := fakeEnvelope(rawItems)
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "cyberjoker", Groups: []string{"system:authenticated"}}))
	gated, served := gateWidgetEnvelope(ctx, raw)
	if !served {
		t.Fatalf("gate must serve under valid identity; got served=false")
	}
	// Decode the gated body and inspect each item's allowed flag.
	var decoded map[string]any
	if err := json.Unmarshal(gated, &decoded); err != nil {
		t.Fatalf("gated body must be valid JSON: %v", err)
	}
	status, _ := decoded["status"].(map[string]any)
	if status == nil {
		t.Fatalf("gated body missing status")
	}
	rr, _ := status["resourcesRefs"].(map[string]any)
	if rr == nil {
		t.Fatalf("gated body missing status.resourcesRefs")
	}
	items, _ := rr["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("gated body must preserve item count (2); got %d", len(items))
	}
	// Every item's allowed MUST now be a fresh evaluation under THIS
	// identity. Without a typed-RBAC snapshot in the test environment
	// every item lands at allowed=false (the fail-closed default). The
	// load-bearing assertion: the stored allowed=true is NOT preserved
	// verbatim. If we ever serve the cached body without running the
	// gate, this test fails.
	for i, raw := range items {
		it, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("item %d is not a map", i)
		}
		allowed, ok := it["allowed"].(bool)
		if !ok {
			t.Fatalf("item %d missing/non-bool allowed", i)
		}
		// In the test env (no RBAC snapshot) the gate evaluates to
		// allowed=false; the assertion is that the SA's true was
		// OVERWRITTEN, not that the new value is any specific bool.
		if allowed {
			t.Logf("item %d allowed=true (typed-RBAC snapshot may be present in test env)", i)
		}
	}
	// And: the cached body's per-item allowed flags were all true; the
	// gated body's per-item allowed flags MUST NOT all be true (the gate
	// fired). With cyberjoker's identity in the test env (no RBAC
	// snapshot wired), every item lands false. We assert AT LEAST ONE
	// item's allowed flag changed value — the gate clearly fired.
	anyChanged := false
	for _, raw := range items {
		it := raw.(map[string]any)
		if it["allowed"].(bool) != true {
			anyChanged = true
			break
		}
	}
	if !anyChanged {
		t.Fatalf("gate did NOT overwrite SA-evaluated allowed=true under request identity — gateWidgetEnvelope is a no-op?")
	}
}

func TestGateWidgetEnvelope_PreservesIdentityInvariantFields(t *testing.T) {
	// Fields the design declares identity-invariant (status.widgetData,
	// each item's id/path/verb) MUST round-trip byte-equivalent through
	// the gate. Only the `allowed` flag is rewritten.
	rawItems := []map[string]any{
		{"id": "list-comp", "path": "/call?resource=compositions&apiVersion=composition.krateo.io/v1", "verb": "GET", "allowed": true},
	}
	raw := fakeEnvelope(rawItems)
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "admin", Groups: []string{"system:authenticated"}}))
	gated, served := gateWidgetEnvelope(ctx, raw)
	if !served {
		t.Fatalf("expected served=true")
	}
	var decoded map[string]any
	if err := json.Unmarshal(gated, &decoded); err != nil {
		t.Fatalf("invalid gated JSON: %v", err)
	}
	// apiVersion / kind / metadata round-trip unchanged.
	if decoded["apiVersion"] != "widgets.templates.krateo.io/v1beta1" {
		t.Errorf("apiVersion not preserved: %v", decoded["apiVersion"])
	}
	if decoded["kind"] != "Panel" {
		t.Errorf("kind not preserved: %v", decoded["kind"])
	}
	// status.widgetData preserved verbatim.
	status, _ := decoded["status"].(map[string]any)
	wd, _ := status["widgetData"].(map[string]any)
	if wd["title"] != "Dashboard" {
		t.Errorf("status.widgetData.title not preserved: %v", wd["title"])
	}
	// Per-item id/path/verb preserved.
	rr, _ := status["resourcesRefs"].(map[string]any)
	items, _ := rr["items"].([]any)
	it, _ := items[0].(map[string]any)
	if it["id"] != "list-comp" {
		t.Errorf("item.id not preserved: %v", it["id"])
	}
	if it["path"] != "/call?resource=compositions&apiVersion=composition.krateo.io/v1" {
		t.Errorf("item.path not preserved: %v", it["path"])
	}
	if it["verb"] != "GET" {
		t.Errorf("item.verb not preserved: %v", it["verb"])
	}
}

// TestGateWidgetEnvelope_ConcurrentSafe runs the gate N times in parallel
// over the SAME cached raw envelope under different identities. The gate
// does json.Unmarshal per call so each goroutine operates on its OWN
// fresh map tree — no shared mutation. -race must report clean.
func TestGateWidgetEnvelope_ConcurrentSafe(t *testing.T) {
	rawItems := []map[string]any{
		{"id": "a", "path": "/call?resource=compositions&apiVersion=composition.krateo.io/v1&namespace=ns", "verb": "GET", "allowed": true},
		{"id": "b", "path": "/call?resource=widgets&apiVersion=widgets.templates.krateo.io/v1&namespace=ns", "verb": "GET", "allowed": true},
		{"id": "c", "path": "/call?resource=panels&apiVersion=widgets.templates.krateo.io/v1&namespace=ns", "verb": "POST", "allowed": true},
	}
	raw := fakeEnvelope(rawItems)

	const goroutines = 16
	const iterations = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	identities := []string{"admin", "cyberjoker", "operator", "viewer"}

	for g := 0; g < goroutines; g++ {
		go func(uname string) {
			defer wg.Done()
			ctx := xcontext.BuildContext(context.Background(),
				xcontext.WithUserInfo(jwtutil.UserInfo{Username: uname, Groups: []string{"system:authenticated"}}))
			for i := 0; i < iterations; i++ {
				gated, served := gateWidgetEnvelope(ctx, raw)
				if !served {
					t.Errorf("goroutine %s iter %d: served=false unexpected", uname, i)
					return
				}
				// Cheap sanity check: gated must be non-empty.
				if len(gated) == 0 {
					t.Errorf("goroutine %s iter %d: gated bytes empty", uname, i)
					return
				}
			}
		}(identities[g%len(identities)])
	}
	wg.Wait()

	// And: the cached raw envelope MUST be byte-unchanged after the
	// parallel run — the gate works on a deep-copy via json.Unmarshal.
	expected := fakeEnvelope(rawItems)
	if !bytes.Equal(raw, expected) {
		t.Fatalf("cached raw envelope mutated under concurrent gate execution")
	}
}

// --- verb mapping --------------------------------------------------------

func TestRestVerbToKube(t *testing.T) {
	cases := []struct {
		rest string
		kube string
		ok   bool
	}{
		{http.MethodGet, "get", true},
		{http.MethodPost, "create", true},
		{http.MethodPut, "update", true},
		{http.MethodPatch, "patch", true},
		{http.MethodDelete, "delete", true},
		{"get", "get", true}, // case-insensitive
		{"FOO", "", false},
		{"", "", false},
	}
	for _, c := range cases {
		got, ok := restVerbToKube(c.rest)
		if ok != c.ok {
			t.Errorf("restVerbToKube(%q): ok = %v, want %v", c.rest, ok, c.ok)
		}
		if got != c.kube {
			t.Errorf("restVerbToKube(%q): kube = %q, want %q", c.rest, got, c.kube)
		}
	}
}

// --- populate path -------------------------------------------------------

// TestPopulateWidgetContentL1_PutHappensWhenEnabled drives the F2 walker
// Put site directly: a synthetic resolved widget envelope is handed to
// populateWidgetContentL1; the resolved cache MUST then carry an entry
// under the identity-free key.
func TestPopulateWidgetContentL1_PutHappensWhenEnabled(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	// Build a minimal in+res pair the way widgets.Resolve would.
	in := newUnstructuredWidget("fireworks-app", "dashboard-summary")
	res := newUnstructuredWidget("fireworks-app", "dashboard-summary")
	// Stamp a resourcesRefs.items[] with SA-evaluated allowed=true.
	_ = setNested(res.Object, []any{
		map[string]any{"id": "i", "path": "/call?resource=x&apiVersion=v1", "verb": "GET", "allowed": true},
	}, "status", "resourcesRefs", "items")

	gvr := someWidgetContentGVR()
	populateWidgetContentL1(context.Background(), gvr, in, 5, 1, res, 0)

	// The cache MUST now hold the entry under the identity-free key.
	c := cache.ResolvedCache()
	if c == nil {
		t.Fatalf("ResolvedCache nil under cache=on")
	}
	key := cache.ComputeKey(cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           gvr.Group,
		Version:         gvr.Version,
		Resource:        gvr.Resource,
		Namespace:       "fireworks-app",
		Name:            "dashboard-summary",
		PerPage:         5,
		Page:            1,
	})
	entry, hit := c.Get(key)
	if !hit {
		t.Fatalf("populate did not Put under expected identity-free key")
	}
	if entry == nil || len(entry.RawJSON) == 0 {
		t.Fatalf("populate Put nil/empty entry")
	}
	if entry.Inputs == nil || entry.Inputs.CacheEntryClass != cache.CacheEntryClassWidgetContent {
		t.Fatalf("entry.Inputs missing or wrong class: %+v", entry.Inputs)
	}
	if entry.Inputs.BindingUID != "" {
		t.Fatalf("entry.Inputs MUST carry empty identity-fold for widgetContent; got %+v", entry.Inputs)
	}

	// Counter: widgetContentStoreTotal MUST have advanced.
	if cache.ResolvedCache().Stats().WidgetContentStoreTotal == 0 {
		t.Fatalf("widget_content_store_total did not advance on Put")
	}
}

// TestPopulateWidgetContentL1_NoOpWhenLayerOff: with the layer flag off,
// populateWidgetContentL1 MUST NOT Put — back-compat / removable.
func TestPopulateWidgetContentL1_NoOpWhenLayerOff(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "false")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	in := newUnstructuredWidget("ns", "w")
	res := newUnstructuredWidget("ns", "w")
	populateWidgetContentL1(context.Background(), someWidgetContentGVR(), in, 5, 1, res, 0)

	c := cache.ResolvedCache()
	if c == nil {
		t.Fatalf("ResolvedCache should be non-nil under CACHE_ENABLED=true")
	}
	if c.Stats().WidgetContentStoreTotal != 0 {
		t.Fatalf("widget_content_store_total advanced under layer-off (%d) — populate must be a no-op",
			c.Stats().WidgetContentStoreTotal)
	}
}

// TestPopulateWidgetContentL1_NoOpWhenCacheOff: with the cache subsystem
// off (CACHE_ENABLED=false), populateWidgetContentL1 MUST NOT Put.
func TestPopulateWidgetContentL1_NoOpWhenCacheOff(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	in := newUnstructuredWidget("ns", "w")
	res := newUnstructuredWidget("ns", "w")
	populateWidgetContentL1(context.Background(), someWidgetContentGVR(), in, 5, 1, res, 0)

	// ResolvedCache MUST be nil under CACHE_ENABLED=false — nothing to inspect.
	if c := cache.ResolvedCache(); c != nil {
		t.Fatalf("ResolvedCache must be nil when CACHE_ENABLED=false; got %v", c)
	}
}

// TestRegisterRefreshHandlers_WidgetContentRegistered (AC-G.5): after
// RegisterRefreshHandlers runs, the refresher registry MUST carry a
// handler for the widgetContent class. Without it, an entry off the
// refresher queue hits skippedNoHandler and silently TTL-only
// (the 0.30.116→117 AC-E3 defect, prevented here at Ship G).
func TestRegisterRefreshHandlers_WidgetContentRegistered(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	// Drive the production registration (saRC=nil is the
	// outside-cluster / unit-test posture; refresh runs identity-only).
	RegisterRefreshHandlers(nil)

	// Verify via the existing cross-package test seam.
	if cache.RefreshFuncForTest(cache.CacheEntryClassWidgetContent) == nil {
		t.Fatalf("AC-G.5 FAIL: no refresh handler registered for widgetContent class — dep-driven refresh would TTL-only")
	}
}

// TestPopulateWidgetContentL1_RecordsDepEdges (AC-G.5 architect re-review
// defect-fix): the widgetContent Put MUST also record dep edges into the
// DepTracker so K8s informer events on any referenced object dirty-mark
// the entry and the refresher re-resolves it. Without this, the entry is
// TTL-only stale-forever — same regression shape as 0.30.117 (Ship E AC-E3).
//
// The walker mirrors the per-user widgets.go:148-195 path: install
// WithL1KeyContext on the resolve ctx (so inner-call dep recording fires),
// then after Put call recordWidgetDeps for self-dep + apiRef + resourcesRefs
// deps (Edge types 1+2). This test pins the latter half of the contract.
func TestPopulateWidgetContentL1_RecordsDepEdges(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	// Build a representative resolved widget envelope: spec.apiRef points
	// at a RestAction (edge type 2), spec.resourcesRefs.items[] carries one
	// render ref (edge type 1) and one action-only ref (must be filtered
	// per Revision 14). Same shape deps_extract_test.go's widgetForTest()
	// uses — keeps the falsifier aligned.
	res := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Panel",
		"metadata": map[string]any{
			"name":      "dashboard-summary",
			"namespace": "fireworks-app",
		},
		"spec": map[string]any{
			"apiRef": map[string]any{
				"name":      "compositions-list",
				"namespace": "krateo-system",
			},
			"resourcesRefs": map[string]any{
				"items": []any{
					// Render ref — MUST be recorded.
					map[string]any{
						"id":         "render-1",
						"apiVersion": "composition.krateo.io/v1",
						"resource":   "compositions",
						"namespace":  "bench-ns-01",
						"name":       "bench-app-01-01",
					},
					// Action-only ref — MUST be filtered.
					map[string]any{
						"id":         "action-1",
						"apiVersion": "v1",
						"resource":   "pods",
						"namespace":  "bench-ns-01",
						"name":       "viewlogs-pod",
					},
				},
			},
		},
		"status": map[string]any{
			"widgetData": map[string]any{
				"actions": map[string]any{
					"navigate": []any{
						map[string]any{"resourceRefId": "action-1"},
					},
				},
			},
		},
	}}
	// The walker's `in` carries metadata only; res IS the resolved CR (spec
	// preserved). populateWidgetContentL1 passes `res` to recordWidgetDeps
	// — see widgets.go:195 per-user path.
	in := newUnstructuredWidget("fireworks-app", "dashboard-summary")

	gvr := schema.GroupVersionResource{
		Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels",
	}

	// Baseline: dep counters at zero (ResetDepsForTest).
	beforeRecord := cache.Deps().Stats().RecordTotal
	if beforeRecord != 0 {
		t.Fatalf("RecordTotal baseline = %d; want 0 after ResetDepsForTest", beforeRecord)
	}

	populateWidgetContentL1(context.Background(), gvr, in, 5, 1, res, 0)

	// AC-G.5 PRIMARY ASSERTION: the dep tracker MUST have advanced.
	afterRecord := cache.Deps().Stats().RecordTotal
	if afterRecord <= beforeRecord {
		t.Fatalf("AC-G.5 FAIL: populate did not record any dep edges; RecordTotal advanced %d -> %d. "+
			"Without dep edges the widgetContent entry is TTL-only stale-forever "+
			"(same regression shape as 0.30.117 / Ship E AC-E3).",
			beforeRecord, afterRecord)
	}

	// Compute the key the populate site Puts under.
	key := cache.ComputeKey(cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           gvr.Group,
		Version:         gvr.Version,
		Resource:        gvr.Resource,
		Namespace:       "fireworks-app",
		Name:            "dashboard-summary",
		PerPage:         5,
		Page:            1,
	})

	// AC-G.5 SECONDARY ASSERTIONS: verify the SPECIFIC edges per
	// recordWidgetDeps contract (self-dep + apiRef + render-ref).
	wantPos := []struct {
		gvr  schema.GroupVersionResource
		ns   string
		name string
		desc string
	}{
		// Self-dep — DELETE on the widget CR evicts the content entry.
		{gvr, "fireworks-app", "dashboard-summary", "self-dep"},
		// Edge type 2 — spec.apiRef -> RestAction.
		{schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}, "krateo-system", "compositions-list", "apiRef -> RestAction"},
		// Edge type 1 — render-ref.
		{schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "compositions"}, "bench-ns-01", "bench-app-01-01", "render-ref"},
	}
	deps := cache.Deps()
	for _, c := range wantPos {
		matched := deps.CollectMatchesForTest(c.gvr, c.ns, c.name)
		if _, ok := matched[key]; !ok {
			t.Errorf("AC-G.5: expected %s edge (%s/%s/%s -> widgetContent-key) but not recorded; got matched=%v",
				c.desc, c.gvr, c.ns, c.name, matched)
		}
	}

	// Action-only ref MUST NOT be recorded (Revision 14 filter).
	actionGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	if negMatched := deps.CollectMatchesForTest(actionGVR, "bench-ns-01", "viewlogs-pod"); len(negMatched) > 0 {
		if _, ok := negMatched[key]; ok {
			t.Errorf("AC-G.5: action-only ref WAS recorded as a render dep — Revision 14 filter broken")
		}
	}
}
