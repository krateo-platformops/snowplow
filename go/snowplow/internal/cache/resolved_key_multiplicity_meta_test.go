package cache

// #247 — the key-multiplicity attribution instrument.
//
// ResolvedEntryMeta now carries every ComputeKey input except the class-
// constant resolvedKeyVersion, so two resident keys for ONE (object, identity)
// diff to a NAMED component. These arms hold that property, hold the two
// readings the instrument is allowed to have, and hold the one place it can go
// silently wrong (a replace-in-place that re-stamps the entry but not its
// derived extras identity).
//
// The pre-implementation blindness capture these arms invert is archived at
// scratchpad/falsifier-247/01-blindness-before-5c444db.log: four genuinely
// distinct ComputeKey values for one logical cell, projecting byte-identical
// metadata on every attributable field.

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// oneLogicalCell is a single (class, gvr, ns, name, bindingUID) coordinate —
// the "logical cell" PM C5.1 counts. Every variant below differs from it in
// exactly ONE ComputeKey component.
func oneLogicalCell() ResolvedKeyInputs {
	return ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "tables",
		Namespace:       "krateo-system",
		Name:            "incident-auditrecords",
		BindingUID:      "C:1dd07773-aaaa-bbbb-cccc-000000000001",
		RBACSubGen:      5,
		PerPage:         10,
		Page:            1,
		Extras:          map[string]any{"compositionId": "comp-a"},
	}
}

func putInputs(t *testing.T, c *ResolvedCacheStore, in ResolvedKeyInputs, body string) string {
	t.Helper()
	inputs := in
	key := ComputeKey(inputs)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(body), Inputs: &inputs})
	return key
}

func metaByKey(t *testing.T, c *ResolvedCacheStore) map[string]ResolvedEntryMeta {
	t.Helper()
	out := map[string]ResolvedEntryMeta{}
	c.RangeMetadata(func(m ResolvedEntryMeta) bool {
		out[m.KeyHash] = m
		return true
	})
	return out
}

// TestResolvedMeta_AttributesKeyMultiplicityToANamedComponent is the #247
// instrument's reason to exist, and the direct inversion of the archived
// blindness capture. Four keys for one logical cell, each differing in exactly
// one component; the surface must name which one for every pair.
func TestResolvedMeta_AttributesKeyMultiplicityToANamedComponent(t *testing.T) {
	c := newResolvedCache(20, 1<<20, time.Hour)

	base := oneLogicalCell()

	bySubgen := base
	bySubgen.RBACSubGen = 7 // the #247 mechanism

	byExtras := base
	byExtras.Extras = map[string]any{"compositionId": "comp-b"} // SAME cardinality

	byPage := base
	byPage.Page = 2 // the portal#235 pagination hypothesis

	kBase := putInputs(t, c, base, `{"ok":true}`)
	kSubgen := putInputs(t, c, bySubgen, `{"ok":true}`)
	kExtras := putInputs(t, c, byExtras, `{"ok":true}`)
	kPage := putInputs(t, c, byPage, `{"ok":true}`)

	keys := map[string]bool{kBase: true, kSubgen: true, kExtras: true, kPage: true}
	if len(keys) != 4 {
		t.Fatalf("precondition: expected 4 distinct ComputeKey values, got %d", len(keys))
	}

	metas := metaByKey(t, c)
	if len(metas) != 4 {
		t.Fatalf("expected 4 resident rows, got %d", len(metas))
	}
	mBase, mSubgen, mExtras, mPage := metas[kBase], metas[kSubgen], metas[kExtras], metas[kPage]

	// The coordinate is shared — these really are one logical cell.
	for name, m := range map[string]ResolvedEntryMeta{"subgen": mSubgen, "extras": mExtras, "page": mPage} {
		if m.CacheEntryClass != mBase.CacheEntryClass || m.Resource != mBase.Resource ||
			m.Namespace != mBase.Namespace || m.Name != mBase.Name || m.BindingUID != mBase.BindingUID {
			t.Fatalf("%s variant is not the same logical cell as base: %+v vs %+v", name, m, mBase)
		}
	}

	// (1) sub-gen variant: RBACSubGen differs, everything else identical.
	if mSubgen.RBACSubGen == mBase.RBACSubGen {
		t.Errorf("sub-gen variant not attributable: both rows read rbacSubGen=%d", mBase.RBACSubGen)
	}
	if mSubgen.ExtrasHash != mBase.ExtrasHash {
		t.Errorf("sub-gen variant must NOT differ in extrasHash: %q vs %q", mSubgen.ExtrasHash, mBase.ExtrasHash)
	}
	if mSubgen.PerPage != mBase.PerPage || mSubgen.Page != mBase.Page {
		t.Errorf("sub-gen variant must NOT differ in pagination: %d/%d vs %d/%d",
			mSubgen.PerPage, mSubgen.Page, mBase.PerPage, mBase.Page)
	}

	// (2) extras variant: ExtrasHash differs at IDENTICAL cardinality. This is
	// the case extras_len on the dispatch diag log cannot see, and it is the
	// case #247's Extras exclusion turns on.
	if mExtras.ExtrasHash == mBase.ExtrasHash {
		t.Errorf("extras variant not attributable: both rows read extrasHash=%q", mBase.ExtrasHash)
	}
	if mExtras.RBACSubGen != mBase.RBACSubGen {
		t.Errorf("extras variant must NOT differ in rbacSubGen: %d vs %d", mExtras.RBACSubGen, mBase.RBACSubGen)
	}

	// (3) pagination variant: Page differs, sub-gen and extras do not.
	if mPage.Page == mBase.Page {
		t.Errorf("pagination variant not attributable: both rows read page=%d", mBase.Page)
	}
	if mPage.RBACSubGen != mBase.RBACSubGen || mPage.ExtrasHash != mBase.ExtrasHash {
		t.Errorf("pagination variant must differ ONLY in pagination: subgen %d/%d extras %q/%q",
			mPage.RBACSubGen, mBase.RBACSubGen, mPage.ExtrasHash, mBase.ExtrasHash)
	}

	// PM C5.1's denominator is now computable: the logical-cell tuple is
	// derivable from the row alone. Pre-change, 3 of its 8 components were
	// absent from every surface.
	logical := func(m ResolvedEntryMeta) string {
		return fmt.Sprintf("%s|%s/%s/%s|%s|%s|%s|%d|%d|%s",
			m.CacheEntryClass, m.Group, m.Version, m.Resource,
			m.Namespace, m.Name, m.BindingUID, m.PerPage, m.Page, m.ExtrasHash)
	}
	if logical(mBase) != logical(mSubgen) {
		t.Errorf("C5.1: base and its sub-gen sibling must be the SAME logical cell "+
			"(the fix merges them); got\n  %s\n  %s", logical(mBase), logical(mSubgen))
	}
	for _, m := range []ResolvedEntryMeta{mExtras, mPage} {
		if logical(m) == logical(mBase) {
			t.Errorf("C5.1: a legitimately-separated variant must be a DISTINCT logical cell; got %s", logical(m))
		}
	}
}

// TestResolvedMeta_ExtrasHashTracksReplaceInPlace is the arm for the one way
// this instrument goes silently WRONG rather than silently absent: Put's
// replace-in-place branch re-stamps entry and bytes, and a derived field that
// is not re-stamped alongside them projects the PRIOR extras identity against
// the NEW body. The refresher re-Puts every cell it refreshes through exactly
// this branch.
func TestResolvedMeta_ExtrasHashTracksReplaceInPlace(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)

	in := oneLogicalCell()
	key := ComputeKey(in)

	first := in
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &first})
	before := metaByKey(t, c)[key].ExtrasHash
	if before == "" || before == "e0" {
		t.Fatalf("precondition: expected a real extras hash on the first Put, got %q", before)
	}

	// Replace IN PLACE under the same key with different extras. (Contrived
	// against ComputeKey, deliberately: the point is that the projection
	// follows the entry the item currently holds, whatever put it there.)
	second := in
	second.Extras = map[string]any{"compositionId": "comp-REPLACED"}
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":2}`), Inputs: &second})

	after := metaByKey(t, c)[key]
	if after.ExtrasHash == before {
		t.Fatalf("replace-in-place did NOT re-stamp extrasHash: still %q — the surface is "+
			"attributing the new body to the PRIOR extras identity", before)
	}
	if want := HashExtras(second.Extras); after.ExtrasHash != want {
		t.Fatalf("extrasHash after replace = %q, want %q (HashExtras of the entry the item now holds)",
			after.ExtrasHash, want)
	}
	if after.RawJSONBytes != len(`{"v":2}`) {
		t.Fatalf("precondition: the replace did not take effect (rawJSONBytes=%d)", after.RawJSONBytes)
	}
}

// TestResolvedMeta_ExtrasHashIsTheComputeKeyDerivation holds the no-drift
// property: the projected hash is the SAME canonicalisation ComputeKey folds,
// so extras-equal ⟺ hash-equal. Two arms — insertion order must not matter,
// and a same-cardinality VALUE change must be visible.
func TestResolvedMeta_ExtrasHashIsTheComputeKeyDerivation(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)

	// Order-independence: two maps built in opposite insertion orders.
	a := oneLogicalCell()
	a.Extras = map[string]any{"alpha": 1, "beta": 2, "gamma": 3}
	b := oneLogicalCell()
	b.Extras = map[string]any{"gamma": 3, "beta": 2, "alpha": 1}

	if ka, kb := ComputeKey(a), ComputeKey(b); ka != kb {
		t.Fatalf("precondition: ComputeKey is order-dependent, which it must not be (%s vs %s)", ka, kb)
	}
	key := putInputs(t, c, a, `{"ok":true}`)
	ha := metaByKey(t, c)[key].ExtrasHash

	c2 := newResolvedCache(10, 1<<20, time.Hour)
	key2 := putInputs(t, c2, b, `{"ok":true}`)
	hb := metaByKey(t, c2)[key2].ExtrasHash

	if ha != hb {
		t.Errorf("extrasHash is insertion-order dependent: %q vs %q — it has drifted from "+
			"the canonicaliseExtras ComputeKey folds", ha, hb)
	}
	if ha != HashExtras(a.Extras) {
		t.Errorf("extrasHash = %q, want HashExtras(extras) = %q — a second derivation has appeared",
			ha, HashExtras(a.Extras))
	}

	// Same cardinality, different value: the extras_len blindness.
	c3 := newResolvedCache(10, 1<<20, time.Hour)
	v1 := oneLogicalCell()
	v1.Extras = map[string]any{"compositionId": "one"}
	v2 := oneLogicalCell()
	v2.Extras = map[string]any{"compositionId": "two"}
	k1 := putInputs(t, c3, v1, `{"ok":true}`)
	k2 := putInputs(t, c3, v2, `{"ok":true}`)
	m := metaByKey(t, c3)
	if len(v1.Extras) != len(v2.Extras) {
		t.Fatalf("precondition: the two extras maps must have equal cardinality")
	}
	if m[k1].ExtrasHash == m[k2].ExtrasHash {
		t.Errorf("a same-cardinality extras VALUE change is invisible — extrasHash is no "+
			"sharper than extras_len (both %q)", m[k1].ExtrasHash)
	}
}

// TestResolvedMeta_SubGenZeroIsReadableOnlyWithClass holds the limitation
// sentence in behaviour: rbacSubGen == 0 has two regimes, and CacheEntryClass
// is what separates them. Every identity-free class must project a zero AND a
// non-empty class on the same row.
func TestResolvedMeta_SubGenZeroIsReadableOnlyWithClass(t *testing.T) {
	c := newResolvedCache(20, 1<<20, time.Hour)

	// The classes whose construction sites never set RBACSubGen — enumerated
	// by grep at 5c444db, not from a named set:
	//   dispatchers/helpers.go:193        widgetContent
	//   dispatchers/widget_content.go:87  widgetContent
	//   restactions/api/apistage.go:66    apistage
	//   ra_full_list_slice.go:88          raFullList
	identityFree := []string{CacheEntryClassWidgetContent, CacheEntryClassApistage, CacheEntryClassRAFullList}

	keys := map[string]string{}
	for _, class := range identityFree {
		in := oneLogicalCell()
		in.CacheEntryClass = class
		in.RBACSubGen = 0 // as every one of those sites leaves it
		keys[class] = putInputs(t, c, in, `{"ok":true}`)
	}
	// A stamped class whose subject genuinely has not moved — the OTHER
	// reading of the same zero.
	stamped := oneLogicalCell()
	stamped.CacheEntryClass = "widgets"
	stamped.RBACSubGen = 0
	stampedKey := putInputs(t, c, stamped, `{"ok":true}`)

	metas := metaByKey(t, c)
	for _, class := range identityFree {
		m := metas[keys[class]]
		if m.RBACSubGen != 0 {
			t.Errorf("class %q must fold a constant zero sub-gen, got %d", class, m.RBACSubGen)
		}
		if m.CacheEntryClass == "" {
			t.Errorf("class %q row projects an EMPTY cacheEntryClass — rbacSubGen=0 is then "+
				"unreadable, which is exactly the two-regimes-one-number this field must not become", class)
		}
	}
	if m := metas[stampedKey]; m.RBACSubGen != 0 || m.CacheEntryClass != "widgets" {
		t.Errorf("stamped-class zero row = subgen %d class %q, want 0/widgets", m.RBACSubGen, m.CacheEntryClass)
	}
	// The two regimes really are distinguishable from the row alone.
	if metas[keys[CacheEntryClassApistage]].CacheEntryClass == metas[stampedKey].CacheEntryClass {
		t.Errorf("the two readings of rbacSubGen=0 are NOT separable on the row")
	}
}

// TestResolvedMeta_NoInputsProjectsEmptyNotSentinel holds the distinction
// between "no key-inputs record" ("") and "a record whose extras fold was
// empty" ("e0"). Collapsing them makes an absent record read as a measured one.
func TestResolvedMeta_NoInputsProjectsEmptyNotSentinel(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)

	c.Put("no-inputs-key", &ResolvedEntry{RawJSON: []byte(`{"ok":true}`)})

	emptyExtras := oneLogicalCell()
	emptyExtras.Extras = nil
	withInputs := putInputs(t, c, emptyExtras, `{"ok":true}`)

	metas := metaByKey(t, c)
	if got := metas["no-inputs-key"].ExtrasHash; got != "" {
		t.Errorf("an entry with NO Inputs must project extrasHash=\"\", got %q", got)
	}
	if got := metas[withInputs].ExtrasHash; got != "e0" {
		t.Errorf("an entry WITH Inputs and empty extras must project the \"e0\" sentinel, got %q", got)
	}
}

// TestResolvedMeta_NilInputsZerosAreDiscriminatedByClass makes the documented
// discriminator a TESTED property rather than a comment. Because rbacSubGen,
// perPage and page are not omitempty, a nil-Inputs row serialises 0/0/0 —
// numerically indistinguishable from a genuine zero sub-gen or the genuine
// page-independent raFullList fold. The only thing separating them is
// CacheEntryClass == "" on the same row, so that separation must hold.
func TestResolvedMeta_NilInputsZerosAreDiscriminatedByClass(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)

	c.Put("no-inputs-key", &ResolvedEntry{RawJSON: []byte(`{"ok":true}`)})

	// A genuine raFullList fold: page-independent 0/0, WITH an Inputs record.
	raFull := oneLogicalCell()
	raFull.CacheEntryClass = CacheEntryClassRAFullList
	raFull.RBACSubGen = 0
	raFull.PerPage = 0
	raFull.Page = 0
	raFullKey := putInputs(t, c, raFull, `{"ok":true}`)

	metas := metaByKey(t, c)
	absent, real := metas["no-inputs-key"], metas[raFullKey]

	// Numerically identical on all three fields — that is the ambiguity.
	if absent.RBACSubGen != real.RBACSubGen || absent.PerPage != real.PerPage || absent.Page != real.Page {
		t.Fatalf("precondition: the two rows should be numerically identical "+
			"(absent %d/%d/%d vs real %d/%d/%d)",
			absent.RBACSubGen, absent.PerPage, absent.Page,
			real.RBACSubGen, real.PerPage, real.Page)
	}
	// And separable only by class.
	if absent.CacheEntryClass != "" {
		t.Errorf("a nil-Inputs row must project an EMPTY cacheEntryClass, got %q — "+
			"without it, its zeros are indistinguishable from measured ones", absent.CacheEntryClass)
	}
	if real.CacheEntryClass != CacheEntryClassRAFullList {
		t.Errorf("the raFullList row must name its class, got %q", real.CacheEntryClass)
	}
}

// TestResolvedMeta_ExtrasHashIsAHashNotTheExtras is the leak arm for the new
// field, the sibling of TestRangeMetadata_BehaviorallyEmitsNoBody. Extras can
// carry per-identity values; the projection must never reproduce one.
func TestResolvedMeta_ExtrasHashIsAHashNotTheExtras(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)

	const sentinel = "SECRET-EXTRAS-DO-NOT-LEAK-9c2b1"
	in := oneLogicalCell()
	in.Extras = map[string]any{"compositionId": sentinel, "nested": map[string]any{"deep": sentinel}}
	key := putInputs(t, c, in, `{"ok":true}`)

	m := metaByKey(t, c)[key]
	if m.ExtrasHash == "" {
		t.Fatalf("precondition: expected a populated extrasHash")
	}
	if strings.Contains(m.ExtrasHash, sentinel) {
		t.Fatalf("extrasHash reproduced an extras VALUE: %q", m.ExtrasHash)
	}
	// And nothing else on the row carries it either.
	if s := fmt.Sprintf("%+v", m); strings.Contains(s, sentinel) {
		t.Fatalf("an extras value reached the metadata projection: %s", s)
	}
	// It is a hex SHA-256, i.e. fixed width and hex-only.
	if len(m.ExtrasHash) != 64 {
		t.Fatalf("extrasHash = %q (len %d), want a 64-char hex SHA-256", m.ExtrasHash, len(m.ExtrasHash))
	}
	for _, r := range m.ExtrasHash {
		if !strings.ContainsRune("0123456789abcdef", r) {
			t.Fatalf("extrasHash is not hex: %q", m.ExtrasHash)
		}
	}
}

// TestResolvedMeta_ExtrasHashRaceUnderConcurrentPutAndWalk is the concurrency
// arm required because extrasHash is a new field on a SHARED store-internal
// struct (lruItem), written under c.mu by both Put branches and read under
// c.mu by every walk. A content-equivalence check cannot cover that; this runs
// real concurrent writers against real concurrent readers and must be run with
// -race.
//
// It also holds the PAIRING invariant, which is the part a race detector alone
// would not catch: a projected row's extrasHash must be the hash of the extras
// of the entry the row's OTHER fields came from — not merely of some entry once
// Put under that key.
//
// That distinction is load-bearing and was found by the RED control matrix. A
// first version of this arm accepted any hash belonging to the key's variant
// set, and it stayed GREEN under control 1 (replace-in-place not re-stamping
// extrasHash) — a stale hash is still a member of that set, so the arm could
// not see the exact defect it existed for. The fix is to make each Put's
// variant identifiable from a DIFFERENT projected field that is read off
// entry rather than off item: ItemsCount carries the variant index, extrasHash
// comes from the item, and the two must agree row by row. A hash that lags its
// entry by even one generation now reds.
func TestResolvedMeta_ExtrasHashRaceUnderConcurrentPutAndWalk(t *testing.T) {
	c := newResolvedCache(512, 1<<22, time.Hour)

	const (
		nKeys     = 16
		nVariants = 8
		nWriters  = 8
		nReaders  = 4
		nRounds   = 60
	)

	// Per key, a set of variants. wantHash[k][v] is the ONLY extrasHash
	// legitimate for a row of key k whose ItemsCount identifies variant v.
	keys := make([]string, nKeys)
	variants := make([][]ResolvedKeyInputs, nKeys)
	wantHash := make([][]string, nKeys)
	for k := 0; k < nKeys; k++ {
		base := oneLogicalCell()
		base.Name = fmt.Sprintf("widget-%02d", k)
		keys[k] = ComputeKey(base)
		for v := 0; v < nVariants; v++ {
			in := base
			in.Extras = map[string]any{
				"compositionId": fmt.Sprintf("comp-%02d-%d", k, v),
				"nested":        map[string]any{"v": v},
			}
			variants[k] = append(variants[k], in)
			wantHash[k] = append(wantHash[k], HashExtras(in.Extras))
		}
		// Every variant must be distinguishable, or the arm proves nothing.
		seen := map[string]bool{}
		for _, h := range wantHash[k] {
			if seen[h] {
				t.Fatalf("precondition: key %d has two variants with the same extras hash", k)
			}
			seen[h] = true
		}
	}

	// entryForVariant stamps the variant index into Items, which
	// metaForItemLocked projects as ItemsCount off the ENTRY — the independent
	// coordinate the extrasHash (projected off the ITEM) is checked against.
	entryForVariant := func(k, v, w, r int) *ResolvedEntry {
		in := variants[k][v]
		items := make([]*unstructured.Unstructured, v+1)
		for i := range items {
			items[i] = &unstructured.Unstructured{
				Object: map[string]any{"metadata": map[string]any{"name": in.Name}},
			}
		}
		return &ResolvedEntry{
			RawJSON: []byte(fmt.Sprintf(`{"w":%d,"r":%d,"v":%d}`, w, r, v)),
			Items:   items,
			Inputs:  &in,
		}
	}

	// Seed every key so readers always find rows.
	for k := 0; k < nKeys; k++ {
		c.Put(keys[k], entryForVariant(k, 0, -1, -1))
	}

	keyIndex := map[string]int{}
	for k, key := range keys {
		keyIndex[key] = k
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var mu sync.Mutex
	var failures []string
	fail := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		if len(failures) < 20 {
			failures = append(failures, fmt.Sprintf(format, args...))
		}
	}

	// checkPairing is the invariant: ItemsCount comes off the ENTRY and names
	// the variant; ExtrasHash comes off the ITEM. They must describe the same
	// generation. A replace that re-stamps one and not the other reds here.
	checkPairing := func(via string, m ResolvedEntryMeta) {
		k, ok := keyIndex[m.KeyHash]
		if !ok {
			fail("%s emitted an unknown key %q", via, m.KeyHash)
			return
		}
		if m.ExtrasHash == "" {
			fail("%s: key %d projected an EMPTY extrasHash — a Put branch left the "+
				"derived field unstamped", via, k)
			return
		}
		v := m.ItemsCount - 1
		if v < 0 || v >= nVariants {
			fail("%s: key %d projected itemsCount=%d, outside the variant range", via, k, m.ItemsCount)
			return
		}
		if m.ExtrasHash != wantHash[k][v] {
			which := "a hash belonging to no variant of this key"
			for other, h := range wantHash[k] {
				if h == m.ExtrasHash {
					which = fmt.Sprintf("variant %d's hash", other)
					break
				}
			}
			fail("%s: key %d row is variant %d (itemsCount=%d) but carries %s — "+
				"extrasHash is torn from the entry it describes", via, k, v, m.ItemsCount, which)
		}
	}

	for w := 0; w < nWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < nRounds; r++ {
				for k := 0; k < nKeys; k++ {
					c.Put(keys[k], entryForVariant(k, (w+r)%nVariants, w, r))
				}
			}
		}(w)
	}

	for rd := 0; rd < nReaders; rd++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				c.RangeMetadata(func(m ResolvedEntryMeta) bool {
					checkPairing("walk", m)
					return true
				})
				c.RangeMetadataBatched(4, func(metas []ResolvedEntryMeta, _ time.Duration) bool {
					for _, m := range metas {
						checkPairing("batched walk", m)
					}
					return true
				})
			}
		}()
	}

	// Writers finish on their own; readers run until told to stop.
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	time.Sleep(200 * time.Millisecond)
	close(stop)
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(failures) > 0 {
		t.Fatalf("%d pairing failure(s) under concurrent Put/walk:\n  %s",
			len(failures), strings.Join(failures, "\n  "))
	}

	// Final state: every key's hash still pairs with its own entry.
	for key, m := range metaByKey(t, c) {
		k := keyIndex[key]
		v := m.ItemsCount - 1
		if v < 0 || v >= nVariants || m.ExtrasHash != wantHash[k][v] {
			t.Fatalf("post-run key %d is variant %d but holds extrasHash %q, want %q",
				k, v, m.ExtrasHash, wantHash[k][max(0, min(v, nVariants-1))])
		}
	}
}
