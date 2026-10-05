package cache

// issue378_remint_test.go — #378 P1 (brief issuecomment-5990884686): the store
// half of the zero-cold-navigation fix for warm cells reaching the 24h C5 cap.
//
// The 24h cap stays HARD (owner Q1). What changes is WHO resets a cell's birth:
// the refresher's terminal write (ReplaceIfGenRefresh) re-mints a resident cell
// whose BornAt age is inside the lead window [maxAge-L, maxAge), L = min(TTL,
// maxAge/2), so a cell the refresher keeps body-fresh never reaches the cap
// under a customer. Every other write inherits BornAt.
//
// Every arm here ages cells by REAL elapse under short store bounds; none uses
// SetBornAtForTest to put a cell past the cap. New store surface is reached
// through an interface assertion / the stat map, so this file compiles on the
// parent and its RED there is a runtime failure that names the missing piece.
//
// ARMS
//   D1  detector reads non-zero DURING the defect: K=3 classes × M=4 warm cells
//       cross the cap with no re-mint; 12 customer GETs → evict_max_age_warm_
//       customer_total Δ=12 and oldest_warm_born_age_seconds > maxAge.
//   D2  discrimination: a COLD past-cap cell evicted by a customer GET moves
//       evict_max_age_total only; a warm past-cap cell read by GetNoTouch moves
//       only …_internal_total; a warm body-expired cell read by a customer moves
//       evict_ttl_warm_customer_total.
//   F2c every non-refresher write method (enumerated by reflection) of a
//       resident in-window cell INHERITS BornAt; only ReplaceIfGenRefresh resets.
//   F4a no amplification over ≥4 reaper ticks: 2,000 boot-born cells × 3
//       classes; real resolves equal the same harness with the window predicate
//       forced false (the RefreshFunc writes through ReplaceIfGen — the
//       window-false twin — a seam that lives only in this file). Exactly one
//       BornAt reset per cell.
//   F4b #375 carried: a dep moves mid-resolve during an in-window refresh; the
//       re-mint is accepted with a fresh BornAt AND remarked; the follow-up
//       refresh inherits the new BornAt.

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// r378RefreshPutter is the #378 refresher-terminal carrier, reached through an
// interface so this file compiles where it does not exist yet.
type r378RefreshPutter interface {
	ReplaceIfGenRefresh(ctx context.Context, key string, entry *ResolvedEntry, capturedGen uint64) bool
}

func r378RefreshPut(t testing.TB, c *ResolvedCacheStore) func(context.Context, string, *ResolvedEntry, uint64) bool {
	t.Helper()
	rp, ok := any(c).(r378RefreshPutter)
	if !ok {
		t.Fatalf("#378 RED: *ResolvedCacheStore has no ReplaceIfGenRefresh — the refresher terminal has no re-mint carrier, " +
			"so a warm cell the refresher keeps body-fresh still reaches the C5 cap under a customer")
	}
	return rp.ReplaceIfGenRefresh
}

// r378Stat reads one snowplow_resolved_cache stat; an absent stat is RED.
func r378Stat(t testing.TB, c *ResolvedCacheStore, name string) int64 {
	t.Helper()
	m := resolvedCacheStatsByStatOf(c.Stats())
	v, ok := m[name]
	if !ok {
		t.Fatalf("#378 RED: snowplow_resolved_cache has no stat %q (evict_max_age_total=%d evict_ttl_total=%d — "+
			"the defect is there but nothing names it)", name, m["evict_max_age_total"], m["evict_ttl_total"])
	}
	return v
}

func r378SleepUntil(at time.Time) {
	if d := time.Until(at); d > 0 {
		time.Sleep(d)
	}
}

// TestIssue378_D1_WarmCustomerMaxAgeEvict_ReadsNonZeroDuringDefect — K=3 × M=4.
func TestIssue378_D1_WarmCustomerMaxAgeEvict_ReadsNonZeroDuringDefect(t *testing.T) {
	const maxAge = 3 * time.Second
	c := newResolvedCache(0, 0, 30*time.Second)
	c.maxEntryAge = maxAge
	var keys []string
	for _, class := range []string{"restactions", "widgets", CacheEntryClassRAFullList} {
		for m := 0; m < 4; m++ {
			in := ResolvedKeyInputs{CacheEntryClass: class, Namespace: "d1", Name: class + "-" + string(rune('a'+m))}
			k := ComputeKey(in)
			c.Put(k, &ResolvedEntry{RawJSON: []byte(`{"d1":1}`), Inputs: &in})
			keys = append(keys, k)
		}
	}
	t0 := time.Now()
	// Customer traffic keeps every cell warm across its lifetime.
	r378SleepUntil(t0.Add(1500 * time.Millisecond))
	for _, k := range keys {
		if _, ok := c.Get(k); !ok {
			t.Fatalf("SETUP: %s must still be served before the cap", k)
		}
	}
	// Cross the cap by real elapse (lifetime is whole seconds: > maxAge needs ≥ maxAge+1s).
	r378SleepUntil(t0.Add(maxAge + 1200*time.Millisecond))
	c.reapPastMaxEntryAge()
	if got := c.Stats().WarmPastMaxAge; got != 12 {
		t.Fatalf("SETUP: the reaper must keep all 12 warm past-cap cells (C3), warm_past_max_age=%d", got)
	}
	if got := r378Stat(t, c, "oldest_warm_born_age_seconds"); got <= int64(maxAge.Seconds()) {
		t.Fatalf("#378 D1 RED: oldest_warm_born_age_seconds=%d, want > maxAge=%v while 12 warm cells are past the cap", got, maxAge)
	}
	cust0 := r378Stat(t, c, "evict_max_age_warm_customer_total")
	int0 := r378Stat(t, c, "evict_max_age_warm_internal_total")
	age0 := c.Stats().EvictMaxAgeTotal
	for _, k := range keys {
		if _, ok := c.Get(k); ok {
			t.Fatalf("SETUP: a past-cap cell must be evicted on read (the hard cap); %s was served", k)
		}
	}
	if d := r378Stat(t, c, "evict_max_age_warm_customer_total") - cust0; d != 12 {
		t.Fatalf("#378 D1 RED: 12 warm cells were max-age-evicted under customer GETs (cold navigations), "+
			"evict_max_age_warm_customer_total Δ=%d, want 12", d)
	}
	if d := r378Stat(t, c, "evict_max_age_warm_internal_total") - int0; d != 0 {
		t.Fatalf("#378 D1: customer GETs moved the internal counter by %d", d)
	}
	if d := c.Stats().EvictMaxAgeTotal - age0; d != 12 {
		t.Fatalf("#378 D1: evict_max_age_total Δ=%d, want 12 (the existing counter is unchanged)", d)
	}
}

// TestIssue378_D2_Discrimination — cold vs warm, customer vs internal, maxAge vs TTL.
func TestIssue378_D2_Discrimination(t *testing.T) {
	const ttl, maxAge = 2 * time.Second, 3 * time.Second
	c := newResolvedCache(0, 0, ttl)
	c.maxEntryAge = maxAge
	key := func(n string) string {
		return ComputeKey(ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "d2-" + n})
	}
	in := func(n string) *ResolvedKeyInputs {
		return &ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "d2-" + n}
	}
	cold, seeded, warm, ttlWarm := key("cold"), key("seeded"), key("warm"), key("ttl")
	c.Put(cold, &ResolvedEntry{RawJSON: []byte(`1`), Inputs: in("cold")})
	c.Put(seeded, &ResolvedEntry{RawJSON: []byte(`1`), Inputs: in("seeded"), SeededAtBoot: true})
	c.Put(warm, &ResolvedEntry{RawJSON: []byte(`1`), Inputs: in("warm")})
	c.Put(ttlWarm, &ResolvedEntry{RawJSON: []byte(`1`), Inputs: in("ttl")})
	t0 := time.Now()

	r378SleepUntil(t0.Add(1200 * time.Millisecond))
	if _, ok := c.Get(warm); !ok {
		t.Fatal("SETUP: warm must be served")
	}
	if _, ok := c.Get(ttlWarm); !ok {
		t.Fatal("SETUP: ttl must be served")
	}

	// (c) a WARM customer-read cell whose BODY expired: evict_ttl_warm_customer_total.
	r378SleepUntil(t0.Add(2300 * time.Millisecond))
	ttlW0 := r378Stat(t, c, "evict_ttl_warm_customer_total")
	ttl0 := c.Stats().EvictTTLTotal
	if _, ok := c.Get(ttlWarm); ok {
		t.Fatal("SETUP: a body past its TTL must be evicted on read")
	}
	if d := r378Stat(t, c, "evict_ttl_warm_customer_total") - ttlW0; d != 1 {
		t.Fatalf("#378 D2 RED: a warm cell TTL-evicted under a customer GET moved evict_ttl_warm_customer_total by %d, want 1", d)
	}
	if d := c.Stats().EvictTTLTotal - ttl0; d != 1 {
		t.Fatalf("#378 D2: evict_ttl_total Δ=%d, want 1", d)
	}
	// Keep the three max-age arms body-fresh (a plain gen-guarded REPLACE inherits
	// BornAt and lastRead), and re-read `warm` so it stays customer-warm.
	for name, k := range map[string]string{"cold": cold, "seeded": seeded, "warm": warm} {
		e := &ResolvedEntry{RawJSON: []byte(`2`), Inputs: in(name), SeededAtBoot: k == seeded}
		if !c.ReplaceIfGen(context.Background(), k, e, c.CaptureGen(k)) {
			t.Fatalf("SETUP: refresh-style re-Put of %s refused", name)
		}
	}
	if _, ok := c.Get(warm); !ok {
		t.Fatal("SETUP: warm must be served after its re-Put")
	}

	r378SleepUntil(t0.Add(maxAge + 300*time.Millisecond))
	cust0 := r378Stat(t, c, "evict_max_age_warm_customer_total")
	int0 := r378Stat(t, c, "evict_max_age_warm_internal_total")
	age0 := c.Stats().EvictMaxAgeTotal

	// (a) COLD past-cap cell, customer GET: evict_max_age_total only.
	if _, ok := c.Get(cold); ok {
		t.Fatal("SETUP: cold past-cap cell must be evicted")
	}
	if d := r378Stat(t, c, "evict_max_age_warm_customer_total") - cust0; d != 0 {
		t.Fatalf("#378 D2 RED: a COLD past-cap cell moved the warm-customer counter by %d — the P5 trigger would fire on a cell nobody browses", d)
	}
	if d := c.Stats().EvictMaxAgeTotal - age0; d != 1 {
		t.Fatalf("#378 D2: cold evict moved evict_max_age_total by %d, want 1", d)
	}

	// (b) WARM (seeded) past-cap cell, INTERNAL read: …_internal_total only.
	if _, ok := c.GetNoTouch(seeded); ok {
		t.Fatal("SETUP: seeded past-cap cell must be evicted on GetNoTouch (the internal orphan backstop)")
	}
	if d := r378Stat(t, c, "evict_max_age_warm_internal_total") - int0; d != 1 {
		t.Fatalf("#378 D2 RED: a warm past-cap cell evicted by GetNoTouch moved …_internal_total by %d, want 1", d)
	}
	if d := r378Stat(t, c, "evict_max_age_warm_customer_total") - cust0; d != 0 {
		t.Fatalf("#378 D2 RED: an INTERNAL read moved the customer counter by %d", d)
	}

	// Positive control: a WARM past-cap cell, customer GET → the P5 trigger.
	if _, ok := c.Get(warm); ok {
		t.Fatal("SETUP: warm past-cap cell must be evicted")
	}
	if d := r378Stat(t, c, "evict_max_age_warm_customer_total") - cust0; d != 1 {
		t.Fatalf("#378 D2 RED: a warm past-cap customer GET moved the warm-customer counter by %d, want 1", d)
	}
	if d := c.Stats().EvictMaxAgeTotal - age0; d != 3 {
		t.Fatalf("#378 D2: evict_max_age_total Δ=%d over the three past-cap arms, want 3 (unchanged semantics)", d)
	}
}

// r378SynthArgs builds the argument list for a store write method m on key (the
// arm-9 synthesis, extended to every Put*/Replace* shape).
func r378SynthArgs(t *testing.T, c *ResolvedCacheStore, m reflect.Method, key string, in ResolvedKeyInputs) ([]reflect.Value, bool) {
	t.Helper()
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	mt := m.Type
	args := []reflect.Value{reflect.ValueOf(c)}
	for j := 1; j < mt.NumIn(); j++ {
		pt := mt.In(j)
		switch {
		case pt == ctxType:
			args = append(args, reflect.ValueOf(context.Background()))
		case pt.Kind() == reflect.String:
			args = append(args, reflect.ValueOf(key))
		case pt == reflect.TypeOf(&ResolvedEntry{}):
			inCopy := in
			args = append(args, reflect.ValueOf(&ResolvedEntry{RawJSON: []byte(`{"v":"rewrite"}`), Inputs: &inCopy}))
		case pt.Kind() == reflect.Uint64:
			args = append(args, reflect.ValueOf(c.CaptureGen(key)))
		case pt == reflect.TypeOf(ResolvedKeyInputs{}):
			args = append(args, reflect.ValueOf(in))
		case pt == reflect.TypeOf(map[string]any{}):
			args = append(args, reflect.ValueOf(map[string]any{"items": []any{map[string]any{"v": "rewrite"}}}))
		default:
			t.Errorf("#378 F2c: write method %s has a parameter of type %s this enumeration cannot synthesize — "+
				"extend it so the method is DRIVEN, never silently skipped", m.Name, pt)
			return nil, false
		}
	}
	return args, true
}

// TestIssue378_F2c_OnlyTheRefresherTerminalReMints — every exported Put*/Replace*
// method of the store, called on a RESIDENT cell whose BornAt age is INSIDE the
// lead window, must INHERIT BornAt — the keepwarm / seed / gvr-discovered /
// #258 / customer carriers all write through these. Only ReplaceIfGenRefresh
// (the refresher terminal) resets it.
func TestIssue378_F2c_OnlyTheRefresherTerminalReMints(t *testing.T) {
	const maxAge = 4 * time.Second // L = min(TTL=60s, maxAge/2) = 2s → window [2s, 4s)
	c := newResolvedCache(0, 0, 60*time.Second)
	c.maxEntryAge = maxAge
	st := reflect.TypeOf(c)
	type cell struct {
		m    reflect.Method
		key  string
		in   ResolvedKeyInputs
		born time.Time
	}
	var cells []cell
	for i := 0; i < st.NumMethod(); i++ {
		m := st.Method(i)
		if !(strings.HasPrefix(m.Name, "Put") || strings.HasPrefix(m.Name, "Replace")) || m.Name == "ReplaceIfGenRefresh" {
			continue
		}
		in := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, Namespace: "f2c", Name: m.Name}
		k := ComputeKey(in)
		inCopy := in
		c.Put(k, &ResolvedEntry{RawJSON: []byte(`{"items":[{"v":"born"}]}`), Inputs: &inCopy})
		e, _ := c.GetNoTouch(k)
		cells = append(cells, cell{m: m, key: k, in: in, born: e.BornAt})
	}
	names := map[string]bool{}
	for _, x := range cells {
		names[x.m.Name] = true
	}
	for _, must := range []string{"Put", "PutIfGen", "ReplaceIfGen", "PutThenRemark", "PutRAFullList", "PutRAFullListIfGen"} {
		if !names[must] {
			t.Fatalf("#378 F2c: the enumeration did not find %s — the filter is broken (found %v)", must, names)
		}
	}
	time.Sleep(maxAge/2 + 300*time.Millisecond) // every cell is now inside [maxAge-L, maxAge)

	for _, x := range cells {
		before, ok := c.GetNoTouch(x.key)
		if !ok {
			t.Fatalf("SETUP: %s cell not resident", x.m.Name)
		}
		args, ok := r378SynthArgs(t, c, x.m, x.key, x.in)
		if !ok {
			continue
		}
		x.m.Func.Call(args)
		after, ok := c.GetNoTouch(x.key)
		if !ok || after == before {
			t.Errorf("#378 F2c: %s did not write the resident in-window cell — the inherit claim would be vacuous", x.m.Name)
			continue
		}
		if !after.BornAt.Equal(x.born) {
			t.Errorf("#378 F2c RED: %s RESET BornAt of a resident in-window cell (%v → %v); only the refresher terminal "+
				"(ReplaceIfGenRefresh) may re-mint — any other setter lets a keepwarm/seed/customer write extend the C5 cap",
				x.m.Name, x.born, after.BornAt)
		}
	}
	t.Logf("#378 F2c enumerated write methods: %v", func() []string {
		var out []string
		for _, x := range cells {
			out = append(out, x.m.Name)
		}
		return out
	}())

	// The refresher terminal on the same in-window state RESETS BornAt (F2b at the store).
	put := r378RefreshPut(t, c)
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "f2c", Name: "refresh"}
	k := ComputeKey(in)
	c.Put(k, &ResolvedEntry{RawJSON: []byte(`1`), Inputs: &in})
	e0, _ := c.GetNoTouch(k)
	time.Sleep(maxAge/2 + 300*time.Millisecond)
	if !put(context.Background(), k, &ResolvedEntry{RawJSON: []byte(`2`), Inputs: &in}, c.CaptureGen(k)) {
		t.Fatal("ReplaceIfGenRefresh refused a live in-window cell")
	}
	e1, _ := c.GetNoTouch(k)
	if !e1.BornAt.After(e0.BornAt) {
		t.Fatalf("#378 F2c: ReplaceIfGenRefresh in the window must reset BornAt (%v → %v)", e0.BornAt, e1.BornAt)
	}
}

// r378RunF4aArm runs one F4a harness: 2,000 boot-born cells × 3 classes through
// the REAL refresher pool and the REAL reaper tick (#316 proactive enqueue), for
// ≥ 4 ticks across the lead window. write is the refresher terminal under test.
// Returns real resolves (Δcompleted − Δskipped_no_entry) and BornAt resets per key.
func r378RunF4aArm(t *testing.T, writeFor func(c *ResolvedCacheStore) func(context.Context, string, *ResolvedEntry, uint64) bool) (real int64, resets map[string]int, cells int) {
	t.Helper()
	cleanup := withCleanRefresher(t, 4, 0)
	defer cleanup()
	t.Setenv(envResolvedCacheTTLSeconds, "8")          // refresh enqueued past 3/4·TTL = 6s
	t.Setenv(envResolvedCacheMaxEntryAgeSeconds, "12") // L = min(8, 6) = 6 → window [6s, 12s)
	t.Setenv(envResolvedCacheSummaryEvery, "1")        // the reaper tick
	c := ResolvedCache()
	write := writeFor(c)

	const perClass = 2000
	classes := []string{"restactions", "widgets", CacheEntryClassWidgetContent}
	born := time.Now()
	var keys []string
	for _, class := range classes {
		for i := 0; i < perClass; i++ {
			in := ResolvedKeyInputs{CacheEntryClass: class, Namespace: "f4a", Name: class + "-" + itoa378(i)}
			k := ComputeKey(in)
			c.Put(k, &ResolvedEntry{RawJSON: []byte(`{"boot":1}`), Inputs: &in, SeededAtBoot: true, CreatedAt: born, BornAt: born})
			keys = append(keys, k)
		}
	}
	var mu sync.Mutex
	births := map[string]map[int64]bool{}
	for _, k := range keys {
		births[k] = map[int64]bool{born.UnixNano(): true}
	}
	var calls atomic.Int64
	for _, class := range classes {
		RegisterRefreshFunc(class, func(ctx context.Context, k string, used ResolvedKeyInputs) error {
			calls.Add(1)
			gen0 := c.CaptureGen(k)
			rctx := WithL1KeyContext(ctx, k)
			// The production refresher builds a FRESH entry (no SeededAtBoot, no BornAt).
			e := &ResolvedEntry{RawJSON: []byte(`{"refreshed":1}`), Inputs: &used}
			if write(rctx, k, e, gen0) {
				mu.Lock()
				births[k][e.BornAt.UnixNano()] = true
				mu.Unlock()
			}
			return nil
		})
	}
	r0 := RefresherStatsByStat()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	// Run past the window start, across ≥ 4 reaper ticks inside [6s, 12s), and
	// stop before the earliest possible SECOND window entry of a re-minted cell.
	r378SleepUntil(born.Add(11500 * time.Millisecond))
	r1 := RefresherStatsByStat()
	real = (r1["completed"] - r0["completed"]) - (r1["skipped_no_entry"] - r0["skipped_no_entry"])
	resets = map[string]int{}
	mu.Lock()
	for _, k := range keys {
		resets[k] = len(births[k]) - 1
	}
	mu.Unlock()
	t.Logf("F4a arm: cells=%d handler calls=%d real resolves=%d (completed Δ=%d skipped_no_entry Δ=%d)",
		len(keys), calls.Load(), real, r1["completed"]-r0["completed"], r1["skipped_no_entry"]-r0["skipped_no_entry"])
	return real, resets, len(keys)
}

func itoa378(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// TestIssue378_F4a_NoAmplification_MultiTick — C3.
func TestIssue378_F4a_NoAmplification_MultiTick(t *testing.T) {
	if testing.Short() {
		t.Skip("F4a runs two ~12s real-elapse arms")
	}
	// Control: the window predicate forced false — the same terminal write minus
	// the re-mint is exactly ReplaceIfGen (test-only seam: chosen here, never in
	// production code).
	ctlReal, ctlResets, n := r378RunF4aArm(t, func(c *ResolvedCacheStore) func(context.Context, string, *ResolvedEntry, uint64) bool {
		return c.ReplaceIfGen
	})
	for k, r := range ctlResets {
		if r != 0 {
			t.Fatalf("CONTROL: the window-false twin reset BornAt of %s %d times", k, r)
		}
	}
	gotReal, resets, _ := r378RunF4aArm(t, func(c *ResolvedCacheStore) func(context.Context, string, *ResolvedEntry, uint64) bool {
		return r378RefreshPut(t, c)
	})
	if ctlReal < int64(n) {
		t.Fatalf("SETUP: the control refreshed %d < %d cells — the arm never reached the window", ctlReal, n)
	}
	if gotReal != ctlReal {
		t.Fatalf("#378 F4a RED: the re-mint arm ran %d real resolves vs %d with the window predicate forced false — "+
			"re-minting must ride refreshes that happen anyway, never add one (0.30.185 amplification)", gotReal, ctlReal)
	}
	bad := 0
	for _, r := range resets {
		if r != 1 {
			bad++
		}
	}
	if bad != 0 {
		t.Fatalf("#378 F4a RED: %d/%d cells did not get EXACTLY one BornAt reset across the window", bad, len(resets))
	}
}

// TestIssue378_F4b_DepMovesMidResolve_ReMintIsRemarked — #375 carried.
func TestIssue378_F4b_DepMovesMidResolve_ReMintIsRemarked(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	t.Setenv(envResolvedCacheTTLSeconds, "60")
	t.Setenv(envResolvedCacheMaxEntryAgeSeconds, "4") // L = min(60, 2) = 2 → window [2s, 4s)
	c := ResolvedCache()
	put := r378RefreshPut(t, c)
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "f4b"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in})
	e0, _ := c.GetNoTouch(key)
	time.Sleep(2300 * time.Millisecond) // into the window by real elapse

	var mu sync.Mutex
	var born []time.Time
	var accepted []bool
	var calls atomic.Int32
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, used ResolvedKeyInputs) error {
		n := calls.Add(1)
		gen0 := c.CaptureGen(k)
		rctx := WithL1KeyContext(ctx, k)
		Deps().Record(rctx, k, g375A, "ns", "f4b-dep")
		if n == 1 {
			Deps().OnUpdate(g375A, "ns", "f4b-dep") // the dep moves INSIDE the in-window re-resolve
		}
		e := &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &used}
		ok := put(rctx, k, e, gen0)
		mu.Lock()
		born, accepted = append(born, e.BornAt), append(accepted, ok)
		mu.Unlock()
		return nil
	})
	remark0 := Deps().Stats().MovedRemarkTotal
	remint0 := r378Stat(t, c, "remint_total")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	enqueueRefreshForTest(key)
	waitFor(t, 5*time.Second, "the remarked follow-up refresh", func() bool { return calls.Load() >= 2 })
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if !accepted[0] || !accepted[1] {
		t.Fatalf("#378 F4b: refreshes accepted=%v, want both", accepted)
	}
	if !born[0].After(e0.BornAt) {
		t.Fatalf("#378 F4b RED: the in-window refresh whose dep moved mid-resolve was not re-minted (BornAt %v → %v)", e0.BornAt, born[0])
	}
	if d := Deps().Stats().MovedRemarkTotal - remark0; d < 1 {
		t.Fatalf("#378 F4b RED: the re-mint was not remarked (moved_remark_total Δ=%d) — #375's PUT-THEN-REMARK must ride the re-mint carrier", d)
	}
	if !born[1].Equal(born[0]) {
		t.Fatalf("#378 F4b RED: the follow-up refresh (new BornAt age ≈0, outside the window) must INHERIT the re-minted "+
			"BornAt %v, got %v", born[0], born[1])
	}
	if d := r378Stat(t, c, "remint_total") - remint0; d != 1 {
		t.Fatalf("#378 F4b: remint_total Δ=%d, want exactly 1", d)
	}
	if e, ok := c.GetNoTouch(key); !ok || !e.BornAt.Equal(born[0]) {
		t.Fatalf("#378 F4b: the resident cell must carry the re-minted BornAt")
	}
}
