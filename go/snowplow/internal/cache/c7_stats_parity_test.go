// c7_stats_parity_test.go — 1.12.6 C7: the four-surface parity arms.
//
// Three metric drifts shipped in 1.12.6 before this commit, all in
// hand-written mirrors of a snapshot struct. These arms do not check a list
// of names — a list in a test drifts exactly like a list in the mirror. They
// ask the TYPE (stats_by_tag.go) which stats exist and then check each
// surface against that answer:
//
//	1. every numeric field of every published stats struct is tagged or
//	   explicitly excluded (a new field cannot be silently unpublished);
//	2. the expvar surface publishes every derived stat, read back through the
//	   real /debug/vars handler;
//	3. docs/architecture/observability.md names every published stat of every
//	   family (tagged families AND the two map families, snowplow_deps and
//	   snowplow_resolved_cache);
//	4. distinct values set on every field come back under the right key —
//	   the derivation, not just the key set (a swapped or duplicated tag is
//	   a wrong number on a dashboard, which is worse than a missing one).
//
// The OTLP half lives in internal/metrics/otlp_export_c7_test.go (it needs
// the exporter). RED on the commit that adds the tags without converting the
// mirrors: relist_bridge_* and the broadcaster fields are undocumented.

package cache

import (
	"encoding/json"
	"expvar"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestC7_StatsByTag_EveryNumericFieldIsTaggedOrExcluded(t *testing.T) {
	fams := TaggedStatFamilies()
	if len(fams) < 3 {
		t.Fatalf("TaggedStatFamilies() lists %d families; the three converted ones must be registered", len(fams))
	}
	for _, f := range fams {
		if len(f.Untagged) != 0 {
			t.Errorf("%s: numeric field(s) with no stat tag: %v — a field a snapshot carries must say where it is "+
				"published (`stat:\"<key>\"`) or that it is not (`stat:\"-\"` + a comment)", f.Name(), f.Untagged)
		}
		if len(f.Specs) == 0 {
			t.Errorf("%s: no published stats derived — every tag missing?", f.Name())
		}
		seen := map[string]string{}
		for _, s := range f.Specs {
			if prev, dup := seen[s.Stat]; dup {
				t.Errorf("%s: stat %q is carried by two fields (%s, %s) — the second silently overwrites the first on every surface",
					f.Name(), s.Stat, prev, s.Field)
			}
			seen[s.Stat] = s.Field
			if s.Kind != "counter" && s.Kind != "gauge" {
				t.Errorf("%s.%s: kind %q", f.Name(), s.Field, s.Kind)
			}
		}
	}
}

// The mutation probe: a struct with an untagged numeric field and a
// duplicated tag must be caught by the same helpers the arm above uses. If
// this passes by accident the arm is not reading the type.
func TestC7_StatsByTag_MutationProbe_UntaggedAndDuplicateAreCaught(t *testing.T) {
	type probe struct {
		A uint64 `stat:"a"`
		B uint64 // untagged — the drift shape
		C string // non-numeric, never counted
		D uint64 `stat:"a"` // duplicate of A
		E uint64 `stat:"-"` // explicit exclusion — allowed
	}
	if got := untaggedNumericFields(reflect.TypeOf(probe{})); !reflect.DeepEqual(got, []string{"B"}) {
		t.Fatalf("untaggedNumericFields = %v; want [B] — the helper is not reading the type", got)
	}
	specs := statSpecsOf(reflect.TypeOf(probe{}))
	if len(specs) != 2 || specs[0].Stat != "a" || specs[1].Stat != "a" {
		t.Fatalf("statSpecsOf = %+v; want two specs both named a (the duplicate must surface, not collapse)", specs)
	}
	m := statsByTag(probe{A: 1, D: 2})
	if len(m) != 1 {
		t.Fatalf("statsByTag on a duplicated tag = %v; one key expected (the arm above catches the duplicate)", m)
	}
}

// expvarVars reads /debug/vars through the real handler.
func expvarVars(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	rec := httptest.NewRecorder()
	expvar.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/vars", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/debug/vars returned %d", rec.Code)
	}
	var all map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &all); err != nil {
		t.Fatalf("decoding /debug/vars: %v", err)
	}
	return all
}

// publishedStatKeys is the KEY SET of every stats family, derived without
// any process-global state (N1): the tagged families from their struct
// tags, snowplow_deps from its flattening (its key set does not depend on
// values — Deps() is always non-nil), snowplow_resolved_cache from the
// flattening of a ZERO ResolvedCacheStats (the live map is empty until a
// store is published, and a sibling test's reset can empty it again). Map
// families are keyed by expvar name → stat set; the per-stat refresher
// family contributes its full expvar keys under "".
func publishedStatKeys() map[string]map[string]bool {
	out := map[string]map[string]bool{"": {}}
	for _, f := range TaggedStatFamilies() {
		if f.Expvar != "" {
			s := map[string]bool{}
			for _, sp := range f.Specs {
				s[sp.Stat] = true
			}
			out[f.Expvar] = s
			continue
		}
		for _, sp := range f.Specs {
			out[""][f.ExpvarKey(sp)] = true
		}
	}
	deps := map[string]bool{}
	for k := range DepsStatsByStat() {
		deps[k] = true
	}
	out["snowplow_deps"] = deps
	rc := map[string]bool{}
	for k := range resolvedCacheStatsByStatOf(ResolvedCacheStats{}) {
		rc[k] = true
	}
	out["snowplow_resolved_cache"] = rc
	if len(rc) == 0 || len(deps) == 0 {
		panic("publishedStatKeys: a static key set came back empty")
	}
	return out
}

func registerAllExpvarForTest() {
	RegisterExpvarForTest() // refresher + crd_discovery (+ fallthrough, controller health)
	RegisterRefreshBroadcasterExpvarForTest()
	RegisterDepsExpvarForTest()
}

func TestC7_Expvar_PublishesEveryDerivedStat(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	registerAllExpvarForTest()
	all := expvarVars(t)

	for _, f := range TaggedStatFamilies() {
		if f.Expvar != "" {
			raw, ok := all[f.Expvar]
			if !ok {
				t.Errorf("%s: /debug/vars has no %q key", f.Name(), f.Expvar)
				continue
			}
			var m map[string]json.RawMessage
			if err := json.Unmarshal(raw, &m); err != nil {
				t.Errorf("%s: %q is not a map: %v", f.Name(), f.Expvar, err)
				continue
			}
			for _, s := range f.Specs {
				if _, ok := m[s.Stat]; !ok {
					t.Errorf("%s: expvar %s lacks stat %q (field %s) — a missing key is a dashboard panel reading \"no data\"",
						f.Name(), f.Expvar, s.Stat, s.Field)
				}
			}
			if len(m) != len(f.Specs) {
				t.Errorf("%s: expvar %s publishes %d keys, the type derives %d — a key not backed by a tagged field",
					f.Name(), f.Expvar, len(m), len(f.Specs))
			}
			continue
		}
		derived := map[string]bool{}
		for _, s := range f.Specs {
			derived[f.ExpvarKey(s)] = true
			if _, ok := all[f.ExpvarKey(s)]; !ok {
				t.Errorf("%s: /debug/vars has no %q (stat %s, field %s)", f.Name(), f.ExpvarKey(s), s.Stat, s.Field)
			}
		}
		// Reverse direction: the per-stat keys are LITERALS (the C0 structural
		// guard derives the cache-off key set from literal expvar.Publish
		// arguments), so a literal not backed by a tagged field must fail here.
		for key := range all {
			if strings.HasPrefix(key, f.ExpvarPrefix) && !derived[key] {
				t.Errorf("%s: /debug/vars publishes %q but no tagged field derives it — a literal key with no source of truth", f.Name(), key)
			}
		}
	}
}

// observabilityDoc reads docs/architecture/observability.md relative to the
// package directory.
func observabilityDoc(t *testing.T) string {
	t.Helper()
	p := filepath.Join("..", "..", "docs", "architecture", "observability.md")
	b, err := os.ReadFile(p) // #nosec G304 -- fixed repo-relative path
	if err != nil {
		t.Fatalf("reading %s: %v", p, err)
	}
	return string(b)
}

func TestC7_Docs_EveryPublishedStatIsDocumented(t *testing.T) {
	doc := observabilityDoc(t)
	// State-free (N1): every key set comes from the types / static flattenings.
	for family, keys := range publishedStatKeys() {
		for name := range keys {
			if !strings.Contains(doc, "`"+name+"`") {
				t.Errorf("observability.md does not name `%s` (%s) — an operator cannot read a counter the doc does not list",
					name, map[bool]string{true: family, false: "top-level expvar"}[family != ""])
			}
		}
	}
}

// setDistinct sets every tagged field of a struct pointer to base+i (i in
// spec order) and returns the expected stat -> value map.
func setDistinct(t *testing.T, ptr any, base int64) map[string]int64 {
	t.Helper()
	v := reflect.ValueOf(ptr).Elem()
	want := map[string]int64{}
	i := int64(0)
	for _, s := range statSpecsOf(v.Type()) {
		f := v.FieldByName(s.Field)
		i++
		switch f.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			f.SetInt(base + i)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			f.SetUint(uint64(base + i))
		case reflect.Float32, reflect.Float64:
			f.SetFloat(float64(base + i))
		}
		want[s.Stat] = base + i
	}
	return want
}

func TestC7_Wiring_DistinctValuesReachEveryDerivedStat(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	registerAllExpvarForTest()

	crd := &CRDDiscoveryStats{}
	wantCRD := setDistinct(t, crd, 100)
	SetCRDDiscoveryStatsForTest(crd)
	t.Cleanup(func() { SetCRDDiscoveryStatsForTest(nil) })

	rb := &RefreshBroadcasterStats{}
	wantRB := setDistinct(t, rb, 200)
	SetRefreshBroadcasterStatsForTest(rb)
	t.Cleanup(func() { SetRefreshBroadcasterStatsForTest(nil) })

	all := expvarVars(t)
	check := func(family string, raw json.RawMessage, want map[string]int64) {
		var got map[string]float64
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("%s: %v", family, err)
		}
		for k, w := range want {
			if int64(got[k]) != w {
				t.Errorf("%s.%s = %v; want %d — the value did not travel from the field to the key", family, k, got[k], w)
			}
		}
	}
	check("snowplow_crd_discovery", all["snowplow_crd_discovery"], wantCRD)
	check("snowplow_refresh_broadcaster", all["snowplow_refresh_broadcaster"], wantRB)
	for k, w := range wantCRD {
		if got := CRDDiscoveryStatsByStat()[k]; got != w {
			t.Errorf("CRDDiscoveryStatsByStat()[%s] = %d; want %d", k, got, w)
		}
	}

	// The refresher family has no override seam: drive the real atomics.
	// Every counter stat must be reachable by AddRefresherPoolCounterForTest
	// or AddRefreshTerminalCountersForTest — an unreachable stat is a failure,
	// not a skip.
	resetRefresherForTest()
	t.Cleanup(resetRefresherForTest)
	ResetRefreshTerminalForTest()
	t.Cleanup(ResetRefreshTerminalForTest)
	AddRefreshTerminalCountersForTest(301, 302, 303, 304, 5)
	terminal := map[string]int64{"drop_evict": 301, "drop_evict_suspended": 302, "suppressed_set": 303, "suppressed_skips": 304, "suppressed_keys": 5}
	wantR := map[string]int64{}
	for i, s := range refresherStatSpecs() {
		if w, ok := terminal[s.Stat]; ok {
			wantR[s.Stat] = w
			continue
		}
		if s.Kind == "gauge" { // queue_depth: live Len(), asserted present only
			continue
		}
		n := uint64(400 + i)
		if !AddRefresherPoolCounterForTest(s.Stat, n) {
			t.Fatalf("refresher stat %q (field %s) has no test bump — extend AddRefresherPoolCounterForTest", s.Stat, s.Field)
		}
		wantR[s.Stat] = int64(n)
	}
	got := RefresherStatsByStat()
	all = expvarVars(t)
	fam := TaggedStatFamilies()[2]
	for _, s := range fam.Specs {
		w, ok := wantR[s.Stat]
		if !ok {
			continue
		}
		if got[s.Stat] != w {
			t.Errorf("RefresherStatsByStat()[%s] = %d; want %d", s.Stat, got[s.Stat], w)
		}
		var ev float64
		if err := json.Unmarshal(all[fam.ExpvarKey(s)], &ev); err != nil || int64(ev) != w {
			t.Errorf("expvar %s = %v (err %v); want %d", fam.ExpvarKey(s), ev, err, w)
		}
	}
}
