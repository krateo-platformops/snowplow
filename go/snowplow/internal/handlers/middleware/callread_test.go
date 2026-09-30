// callread_test.go — #186 key-parity + body-decode falsifiers.
//
// The load-bearing constraint (arch/pm C2): a body-supplied `extras` must fold
// into the SAME cache key as the query-supplied `extras`, or the same logical
// request cold-navigates. These arms decode the SAME extras through the TWO
// GENUINELY INDEPENDENT channels — the real BodyExtrasDecode envelope-extract
// (POST body) vs the real util.ParseExtras query path (GET ?extras=) — and
// assert they converge on one decoded map and one HashExtras. C2c: the two
// channels are separate code paths, not one helper called twice, so a
// divergence is caught for real.
package middleware

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
)

// realChartExtras models the autopilot previewBlueprint payload shape (#186):
// nested objects, arrays, ints and strings — the exact class of input that
// 431s in the URL today. Both channels must decode it to the SAME map.
func realChartExtras() map[string]any {
	return map[string]any{
		"chart": map[string]any{
			"Chart.yaml":  "apiVersion: v2\nname: demo\nversion: 0.1.0\n",
			"values.yaml": "replicaCount: 3\nimage:\n  tag: latest\n",
		},
		"replicas": float64(3),
		"flags":    []any{"a", "b", "c"},
		"nested":   map[string]any{"deep": map[string]any{"n": float64(42)}},
	}
}

// decodeViaBody runs the REAL BodyExtrasDecode middleware over a POST body
// `{"extras": <J>}` and returns the extras map it stashed on the context —
// the genuine body channel (envelope-extract), NOT a shared helper.
func decodeViaBody(t *testing.T, extras map[string]any) map[string]any {
	t.Helper()
	envelope, err := json.Marshal(map[string]any{"extras": extras})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	var captured map[string]any
	var seen bool
	h := BodyExtrasDecode(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, seen = util.ExtrasFromContext(r.Context())
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/call/read?apiVersion=templates.krateo.io/v1&resource=restactions&name=demo&namespace=krateo-system", bytes.NewReader(envelope))
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("BodyExtrasDecode returned %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !seen {
		t.Fatal("BodyExtrasDecode did not stash extras on the context — the context-first util.ParseExtras would fall back to the (empty) query, diverging the key")
	}
	return captured
}

// decodeViaQuery runs the REAL util.ParseExtras query path over `?extras=<J>`
// — the genuine query channel, independent of the body path above.
func decodeViaQuery(t *testing.T, extras map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(extras)
	if err != nil {
		t.Fatalf("marshal extras: %v", err)
	}
	req := httptest.NewRequest("GET", "/call?extras="+url.QueryEscape(string(raw)), nil)
	got, err := util.ParseExtras(req)
	if err != nil {
		t.Fatalf("ParseExtras query: %v", err)
	}
	return got
}

// TestBodyExtras_KeyParity_TwoRealChannels — the load-bearing key-parity
// falsifier at the map/hash level (#186 C2b/C2c). The two channels are
// genuinely independent code paths — body envelope-extract vs query direct —
// yet must produce byte-identical decoded maps and therefore the identical
// HashExtras. RED-provable: any channel that diverges the map (dropping the
// envelope extract, ignoring the context in ParseExtras, or a mistyped decode)
// reds the pre-hash deep-equal AND the hash equality.
func TestBodyExtras_KeyParity_TwoRealChannels(t *testing.T) {
	extras := realChartExtras()
	mapB := decodeViaBody(t, extras)
	mapQ := decodeViaQuery(t, extras)

	// Pre-hash, field-level (feedback_key_parity_golden_real_inputs_prehash_diff):
	// a divergence reds at the FIELD, not the opaque digest.
	if !reflect.DeepEqual(mapB, mapQ) {
		t.Fatalf("PRE-HASH MAP DIVERGENCE: the body channel and the query channel decoded different maps.\n body=%#v\nquery=%#v", mapB, mapQ)
	}
	// And they must fold to the identical HashExtras → identical ComputeKey.
	if hb, hq := cache.HashExtras(mapB), cache.HashExtras(mapQ); hb != hq {
		t.Fatalf("HASH DIVERGENCE: body HashExtras=%q query HashExtras=%q — the two channels would derive DIFFERENT cache keys → cold-nav / cache miss for the same logical request", hb, hq)
	}
}

// TestParseExtras_QueryOnlyCaller_Unaffected — the /rbac-shape transparency
// proof (#186 Q3). GET /rbac calls util.ParseExtras with NO body-decode
// middleware on its route, so its request context never carries a stashed
// extras. The context-first ParseExtras must therefore fall through to the
// query verbatim — byte-identical to pre-#186 — and an absent extras must
// still be the historical empty, non-nil map.
func TestParseExtras_QueryOnlyCaller_Unaffected(t *testing.T) {
	extras := map[string]any{"foo": "bar", "n": float64(7)}
	got := decodeViaQuery(t, extras)
	if !reflect.DeepEqual(got, extras) {
		t.Fatalf("query-only caller changed: got %#v want %#v", got, extras)
	}
	req := httptest.NewRequest("GET", "/call", nil)
	empty, err := util.ParseExtras(req)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("absent extras must be an empty non-nil map with nil err; got %#v err=%v", empty, err)
	}
}

// TestBodyExtras_MalformedBody_400 — a malformed body is rejected at the edge,
// never forwarded to a resolver as (silently) empty extras.
func TestBodyExtras_MalformedBody_400(t *testing.T) {
	called := false
	h := BodyExtrasDecode(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/call/read", strings.NewReader("{not json"))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed body must be 400; got %d", rec.Code)
	}
	if called {
		t.Fatal("malformed body must NOT reach the next handler")
	}
}

// TestBodyExtras_OverCap_Rejected — a body over CallReadMaxBodyBytes is
// truncated by the LimitReader → JSON decode fails mid-token → 400. The fixed
// DoS ceiling holds; an oversize body never reaches a resolver.
func TestBodyExtras_OverCap_Rejected(t *testing.T) {
	big := strings.Repeat("x", CallReadMaxBodyBytes+1024)
	raw := `{"extras":{"blob":"` + big + `"}}`
	called := false
	h := BodyExtrasDecode(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/call/read", strings.NewReader(raw))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("over-cap body must be 400 (truncated → invalid JSON); got %d", rec.Code)
	}
	if called {
		t.Fatal("over-cap body must NOT reach the next handler")
	}
}

// TestBodyExtras_WrapperExtractsInner — the {"extras":{...}} envelope (Q2) is
// unwrapped to its inner map; a bare top-level field is NOT treated as extras.
func TestBodyExtras_WrapperExtractsInner(t *testing.T) {
	body := []byte(`{"extras":{"a":1},"somethingElse":"ignored"}`)
	var got map[string]any
	var seen bool
	h := BodyExtrasDecode(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, seen = util.ExtrasFromContext(r.Context())
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/call/read", bytes.NewReader(body))
	h.ServeHTTP(rec, req)
	if rec.Code != 200 || !seen {
		t.Fatalf("wrapper decode: code=%d seen=%v", rec.Code, seen)
	}
	if len(got) != 1 || got["a"] != float64(1) {
		t.Fatalf("wrapper must extract the inner .extras map only; got %#v", got)
	}
}

// TestBodyExtras_EmptyBoundary_KeyParity — locks the nil==empty=="e0" invariant
// at the empty boundary (#186; arch + pm both independently checked it). A POST
// /call/read with an extras-less body stashes a NIL map; the query path with no
// ?extras= returns an EMPTY non-nil map. They differ in nil-ness, but both fold
// to the SAME HashExtras sentinel "e0" (cache.HashExtras short-circuits any
// len-0 map), so "no extras" derives ONE key on both channels — no cold-nav at
// the empty boundary. RED-provable: a future HashExtras that distinguished nil
// from empty would diverge the two channels here.
func TestBodyExtras_EmptyBoundary_KeyParity(t *testing.T) {
	// Body channel: an extras-less body → BodyExtrasDecode stashes a nil map.
	var mapB map[string]any
	var seen bool
	h := BodyExtrasDecode(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mapB, seen = util.ExtrasFromContext(r.Context())
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/call/read", strings.NewReader(`{}`))
	h.ServeHTTP(rec, req)
	if rec.Code != 200 || !seen {
		t.Fatalf("empty body: code=%d seen=%v (BodyExtrasDecode must stash even a nil extras)", rec.Code, seen)
	}
	if mapB != nil {
		t.Fatalf("an extras-less body must stash a nil map; got %#v", mapB)
	}

	// Query channel: no ?extras= → the historical empty non-nil map.
	mapQ, err := util.ParseExtras(httptest.NewRequest("GET", "/call", nil))
	if err != nil {
		t.Fatalf("ParseExtras: %v", err)
	}
	if mapQ == nil || len(mapQ) != 0 {
		t.Fatalf("absent query extras must be an empty non-nil map; got %#v", mapQ)
	}

	// The two representations of "no extras" MUST fold to the same key.
	hb, hq := cache.HashExtras(mapB), cache.HashExtras(mapQ)
	if hb != hq {
		t.Fatalf("EMPTY-BOUNDARY KEY DIVERGENCE: nil-extras (body) HashExtras=%q vs empty-extras (query) HashExtras=%q — 'no extras' must derive ONE key on both channels", hb, hq)
	}
	if hb != "e0" {
		t.Fatalf("expected the empty-extras hash sentinel %q; got %q", "e0", hb)
	}
}
