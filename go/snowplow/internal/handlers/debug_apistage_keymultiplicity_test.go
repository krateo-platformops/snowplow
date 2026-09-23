package handlers

// #247 — the attribution instrument's WIRE shape.
//
// The four new ResolvedEntryMeta fields are only an instrument if a reader can
// actually see them at the vantage they are read from. /debug/apistage
// serialises cache.ResolvedEntryMeta directly (no handler DTO), so this arm
// holds the JSON key names and the two-key attribution end to end — through
// the HTTP handler, not through the struct.
//
// Vantage matters here specifically: the alternative instrument considered for
// #247 was two fields on the dispatch diag log line, which is log.Info against
// a chart that ships LOG_LEVEL=warn and therefore emits nothing in production.
// This arm is what makes "readable" a tested property rather than a claim.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestDebugApistage_CarriesKeyMultiplicityAttributionFields(t *testing.T) {
	// SELF-ARMING, and it FAILS rather than skips if the store is absent.
	// A conditional t.Skip here would make the arm silently vacuous in the
	// default suite run — the instrument's own wire shape would then be
	// unverified exactly when nobody is looking
	// (feedback_silent_skip_breaks_convergence_proof).
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("ResolvedCache() nil with CACHE_ENABLED=true — cannot verify the wire shape")
	}

	base := cache.ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "tables",
		Namespace:       "krateo-system",
		Name:            "issue247-wire-shape",
		BindingUID:      "C:issue247-wire-0001",
		RBACSubGen:      5,
		PerPage:         10,
		Page:            1,
		Extras:          map[string]any{"compositionId": "comp-a"},
	}
	sibling := base
	sibling.RBACSubGen = 7 // differs ONLY in sub-gen — the #247 mechanism

	kBase := cache.ComputeKey(base)
	kSib := cache.ComputeKey(sibling)
	if kBase == kSib {
		t.Fatalf("precondition: the two inputs must derive distinct keys")
	}
	inBase, inSib := base, sibling
	store.Put(kBase, &cache.ResolvedEntry{RawJSON: []byte(`{"ok":true}`), Inputs: &inBase})
	store.Put(kSib, &cache.ResolvedEntry{RawJSON: []byte(`{"ok":true}`), Inputs: &inSib})

	rec := httptest.NewRecorder()
	DebugApistage()(rec, httptest.NewRequest(http.MethodGet, "/debug/apistage", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	// The reader greps the raw JSON for these keys; assert on the bytes, not
	// only on the decoded struct.
	raw := rec.Body.String()
	for _, key := range []string{`"rbacSubGen"`, `"extrasHash"`, `"perPage"`, `"page"`} {
		if !strings.Contains(raw, key) {
			t.Errorf("/debug/apistage JSON does not carry %s — the instrument is not "+
				"readable at the vantage it is read from", key)
		}
	}

	var body struct {
		Entries []cache.ResolvedEntryMeta `json:"entries"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	byKey := map[string]cache.ResolvedEntryMeta{}
	for _, m := range body.Entries {
		byKey[m.KeyHash] = m
	}
	mBase, okB := byKey[kBase]
	mSib, okS := byKey[kSib]
	if !okB || !okS {
		t.Fatalf("both cells must be in the walk (base=%v sibling=%v)", okB, okS)
	}

	// The whole point: two keys, one logical cell, attributable to sub-gen.
	if mBase.RBACSubGen == mSib.RBACSubGen {
		t.Errorf("sub-gen siblings are not attributable on the wire: both read %d", mBase.RBACSubGen)
	}
	if mBase.ExtrasHash != mSib.ExtrasHash {
		t.Errorf("sub-gen siblings must share an extras identity: %q vs %q",
			mBase.ExtrasHash, mSib.ExtrasHash)
	}
	if mBase.PerPage != mSib.PerPage || mBase.Page != mSib.Page {
		t.Errorf("sub-gen siblings must share pagination: %d/%d vs %d/%d",
			mBase.PerPage, mBase.Page, mSib.PerPage, mSib.Page)
	}
	if mBase.CacheEntryClass == "" || mSib.CacheEntryClass == "" {
		t.Errorf("rbacSubGen must never travel without cacheEntryClass on the same row")
	}

	// The single-key path carries them too (it is the path that also adds
	// bodySha256, i.e. the one used to decide whether siblings hold equal bytes).
	rec2 := httptest.NewRecorder()
	DebugApistage()(rec2, httptest.NewRequest(http.MethodGet, "/debug/apistage?key_hash="+kBase, nil))
	if rec2.Code != http.StatusOK {
		t.Fatalf("single-key status = %d, want 200", rec2.Code)
	}
	var one struct {
		Entries []cache.ResolvedEntryMeta `json:"entries"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &one); err != nil {
		t.Fatalf("decode single-key: %v", err)
	}
	if len(one.Entries) != 1 {
		t.Fatalf("single-key lookup returned %d entries, want 1", len(one.Entries))
	}
	if got := one.Entries[0]; got.RBACSubGen != base.RBACSubGen || got.ExtrasHash != mBase.ExtrasHash {
		t.Errorf("single-key row disagrees with the walk: subgen %d/%d extrasHash %q/%q",
			got.RBACSubGen, base.RBACSubGen, got.ExtrasHash, mBase.ExtrasHash)
	}
	if one.Entries[0].BodySHA256 == "" {
		t.Errorf("single-key row should still carry bodySha256 — it is the oracle for " +
			"whether two sub-gen siblings hold equal bytes")
	}
}

// TestDebugApistage_ZeroSubGenStillSerialises holds the encoding decision that
// rbacSubGen is NOT omitempty. A zero sub-gen is one of the field's two
// READINGS — "this class never stamps" — not the absence of a value. If it
// were omitted, a reader would have to recover a measured reading from a
// missing JSON key, which is the same error the ExtrasHash ""/"e0" split
// exists to avoid. Every identity-free class row carries a zero, so this is
// the common case, not an edge one.
func TestDebugApistage_ZeroSubGenStillSerialises(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("ResolvedCache() nil with CACHE_ENABLED=true")
	}

	in := cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "tables",
		Namespace:       "krateo-system",
		Name:            "issue247-zero-subgen",
		PerPage:         -1, // the seed path's unpaginated tuple
		Page:            -1,
		// RBACSubGen deliberately unset — widgetContent never stamps it.
	}
	key := cache.ComputeKey(in)
	stamped := in
	store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"ok":true}`), Inputs: &stamped})

	rec := httptest.NewRecorder()
	DebugApistage()(rec, httptest.NewRequest(http.MethodGet, "/debug/apistage?key_hash="+key, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	// Decode into a raw map so an ABSENT key is distinguishable from a zero.
	var raw struct {
		Entries []map[string]any `json:"entries"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(raw.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(raw.Entries))
	}
	row := raw.Entries[0]

	if _, present := row["rbacSubGen"]; !present {
		t.Errorf("rbacSubGen is ABSENT on a zero-sub-gen row — a reader must not have to " +
			"recover the 'this class never stamps' reading from a missing key")
	}
	if got, ok := row["rbacSubGen"].(float64); !ok || got != 0 {
		t.Errorf("rbacSubGen = %v, want 0", row["rbacSubGen"])
	}
	// The disambiguator must travel with it.
	if got, _ := row["cacheEntryClass"].(string); got != cache.CacheEntryClassWidgetContent {
		t.Errorf("cacheEntryClass = %q, want %q — rbacSubGen=0 is unreadable without it",
			got, cache.CacheEntryClassWidgetContent)
	}
	// Negative pagination must survive too (it is data, not absence).
	for _, f := range []string{"perPage", "page"} {
		v, present := row[f]
		if !present {
			t.Errorf("%s is ABSENT — -1 is the seed path's unpaginated tuple, not a missing value", f)
			continue
		}
		if got, ok := v.(float64); !ok || got != -1 {
			t.Errorf("%s = %v, want -1", f, v)
		}
	}
	// An entry WITH a key-inputs record and empty extras carries the sentinel.
	if got, _ := row["extrasHash"].(string); got != "e0" {
		t.Errorf("extrasHash = %q, want the %q empty-extras sentinel", got, "e0")
	}
}
