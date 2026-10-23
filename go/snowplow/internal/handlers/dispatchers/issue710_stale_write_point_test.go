//go:build integration

// #710 PR-2b — the D2 write-point guarantee at the production dispatchers
// (architect ruling, #710 issuecomment-6195582848): a stale serve without a
// guaranteed push path never happens.
//
//   - D2-15  a stale serve whose Put is declined is never written: the
//     response is the FRESH body (re-run with D2 disabled), withdrawn{unarmable}
//     moves, and the content debt still clears.
//     Mutation → RED: drop the write-point guard (staleWritePoint returns false
//     without withdrawing).
//   - D2-15b the re-run is declined too: still a fresh body (unarmed), and the
//     re-run never re-enters D2 — exactly one stale decision.
//     Mutation → RED: re-run without cache.WithStaleServeDisabled.
//   - D2-15c the stale content is read inside a NESTED RESTAction resolve
//     under a widget; the widget's Put is declined: identical to D2-15 (the
//     outermost dispatcher saw StaleDerived).
//     Mutation → RED: a sink not inherited by nested resolves (e.g. the nested
//     resolve re-scopes to its own sink).
//   - D2-15d one customer GET through the withdraw + re-run path is counted
//     ONCE: the L1 lookup counters move exactly as much as a plain cold GET's
//     (the re-run resolves, it never reads the cell), and nothing was written to
//     the ResponseWriter (no header, no WriteHeader, no body) before the re-run.
//     Mutation → RED: no re-run marker (the re-run reads the cell / counts its
//     lookup again).
//   - D2-19b a #547 demand revalidation (a user's hit on a known-stale
//     identity-bound cell) resolves at the bypass class at BOTH gates.
//   - D2-19c a D2 debt's dependent re-resolve runs through the production
//     refresh func (resolveOnceProd stamps the refresher origin) and still
//     resolves at the bypass class at the content gate and as a CUSTOMER tree
//     at the nested gate; a plain refresher resolve stays background (control).
//     Mutation → RED: the customer-triggered check after the origin rule.
//   - D2-15e (architect F1) an EXTERNAL-TTL widget whose RA reads a known-
//     stale content cell: the stale-derived body is never stored (the external
//     Put is never armed); the response and the resident cell are fresh; and
//     when the re-run is declined (a stage error) no stale cell is left behind.
//     Mutation → RED: the external branch Puts regardless.
//   - D2-17  invariant sweep: every stale-derived body written in these
//     scenarios had an accepted Put + the armed header + an owed-push mark.
//     Mutation → RED: CommitStaleServe skipped (no owed mark) or a stale body
//     written unarmed.
package dispatchers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// k710Write is one customer body write observed at the write point.
type k710Write struct {
	key                 string
	staleDerived, armed bool
	owed                bool
}

// k710Puts records every customer-dispatcher Put attempt (site, stale-derived).
func k710Puts(t *testing.T) func() []k710Put {
	t.Helper()
	var mu sync.Mutex
	var got []k710Put
	prev := stalePutHookForTest
	stalePutHookForTest = func(key, site string, staleDerived bool) {
		mu.Lock()
		got = append(got, k710Put{key: key, site: site, staleDerived: staleDerived})
		mu.Unlock()
	}
	t.Cleanup(func() { stalePutHookForTest = prev })
	return func() []k710Put {
		mu.Lock()
		defer mu.Unlock()
		return append([]k710Put(nil), got...)
	}
}

type k710Put struct {
	key, site    string
	staleDerived bool
}

// k710WriteNoStaleOutsidePutIfGen is the F1 invariant: a stale-derived body is
// only ever offered to the armable PutIfGen.
func k710WriteNoStaleOutsidePutIfGen(t *testing.T, arm string, puts []k710Put) {
	t.Helper()
	for _, p := range puts {
		if p.staleDerived && p.site != "putifgen" {
			t.Fatalf("%s RED: a stale-derived body was stored at %q — only the armable PutIfGen may ever store one", arm, p.site)
		}
	}
}

// k710Writes installs the D2-17 write-point observer.
func k710Writes(t *testing.T) func() []k710Write {
	t.Helper()
	var mu sync.Mutex
	var got []k710Write
	prev := staleWriteHookForTest
	staleWriteHookForTest = func(key string, staleDerived, armed bool) {
		w := k710Write{key: key, staleDerived: staleDerived, armed: armed, owed: cache.StaleOwed(key)}
		mu.Lock()
		got = append(got, w)
		mu.Unlock()
	}
	t.Cleanup(func() { staleWriteHookForTest = prev })
	return func() []k710Write {
		mu.Lock()
		defer mu.Unlock()
		return append([]k710Write(nil), got...)
	}
}

// k710KnownStaleFound is F3c's setup: the shared `found` content cell (a
// cluster LIST read by the RA's stage) is KNOWN-STALE (its refresh flipped to
// not-servable) while the informer holds comp-1 at v1. Background resolves are
// allowed afterwards so a D2 debt can re-resolve its dependents.
func k710KnownStaleFound(t *testing.T) (*k547Harness, string) {
	t.Helper()
	// #854 — pins the PLAIN content cell: these arms need a body that really is
	// stale, and an incremental cell applies the change instead (see
	// TestIssue710_D2_17i for D2 over an incremental cell).
	return k710KnownStaleFoundAnchoring(t, false)
}

// k710KnownStaleFoundAnchoring is k710KnownStaleFound with the content cell
// anchored as an incremental one (anchor) or kept plain.
func k710KnownStaleFoundAnchoring(t *testing.T, anchor bool) (*k547Harness, string) {
	t.Helper()
	t.Cleanup(cache.SetListCellAnchoringForTest(anchor))
	h := newK547Harness(t, k547DenyBackground)
	h.contentMode = true
	cache.RegisterRefreshFunc(cache.CacheEntryClassApistage, func(context.Context, string, cache.ResolvedKeyInputs) error {
		return cache.ErrContentNotServable
	})
	for k := range k547Cohorts {
		for _, m := range []int{0, 2} {
			if r, code := h.get(k, m); code != 200 || r != "v0" {
				t.Fatalf("warm (k=%d,m=%d): code=%d reason=%q", k, m, code, r)
			}
		}
	}
	var contentKey string
	n := 0
	cache.ResolvedCache().RangeMetadata(func(m cache.ResolvedEntryMeta) bool {
		if m.CacheEntryClass == cache.CacheEntryClassApistage {
			contentKey = m.KeyHash
			n++
		}
		return true
	})
	if n != 1 {
		t.Fatalf("PRECONDITION: %d apistage content cells after warm, want exactly the one `found` cluster LIST", n)
	}
	h.update(k547Changed, "v1")
	if !k547WaitFor(t, 30*time.Second, "content cell known-stale", func() bool {
		_, r, ok := cache.KnownStale(contentKey)
		return ok && r == "not_servable"
	}) {
		t.Fatalf("PRECONDITION: the `found` content cell never became known-stale")
	}
	h.allowBackground.Store(true)
	return h, contentKey
}

// k710AssertFreshWithdrawn is D2-15's assertion set, shared by 15/15b/15c.
func k710AssertFreshWithdrawn(t *testing.T, arm, served string, code int, contentKey string,
	ss0, wd0 uint64, writes []k710Write, wantArmed bool) {
	t.Helper()
	if code != 200 || served != "v1" {
		t.Fatalf("%s RED: served %q (code %d), want the FRESH v1 — a stale body whose Put was declined has no push path "+
			"and must never be written", arm, served, code)
	}
	st := restactionsapi.StaleServeStatsSnapshot()
	if d := st.ServeTotal - ss0; d != 1 {
		t.Fatalf("%s: stale_serve_total moved %d, want exactly 1 (the first resolve's decision; the re-run must never re-enter D2)", arm, d)
	}
	if d := st.WithdrawnUnarmable - wd0; d != 1 {
		t.Fatalf("%s: stale_serve_withdrawn_total{unarmable} moved %d, want 1", arm, d)
	}
	for _, w := range writes {
		if w.staleDerived {
			t.Fatalf("%s RED: a stale-derived body was written (key armed=%v owed=%v)", arm, w.armed, w.owed)
		}
	}
	if len(writes) != 1 {
		t.Fatalf("%s: %d body writes observed, want exactly 1 (the re-run's)", arm, len(writes))
	}
	if writes[0].armed != wantArmed {
		t.Fatalf("%s: the re-run's write armed=%v, want %v", arm, writes[0].armed, wantArmed)
	}
	// The content debt is not withdrawn with the dependent: it still clears.
	if !k547WaitFor(t, 10*time.Second, "debt cleared", func() bool {
		_, _, ks := cache.KnownStale(contentKey)
		return restactionsapi.StaleServeStatsSnapshot().DebtOpen == 0 && !ks
	}) {
		t.Fatalf("%s: the content debt never cleared after the withdrawal (%+v)", arm, restactionsapi.StaleServeStatsSnapshot())
	}
}

func TestIssue710_D2_15_ADeclinedStaleServeIsNeverWrittenTheResponseIsFresh(t *testing.T) {
	h, contentKey := k710KnownStaleFound(t)
	writes := k710Writes(t)
	h.customerExternal.Store(1) // the first resolve's Put is declined; the re-run's is accepted
	s0 := restactionsapi.StaleServeStatsSnapshot()
	r, code := h.getRA(0, k547Changed)
	k710AssertFreshWithdrawn(t, "D2-15", r, code, contentKey, s0.ServeTotal, s0.WithdrawnUnarmable, writes(), true)
}

func TestIssue710_D2_15b_TheReRunNeverReEntersD2EvenWhenItIsDeclinedToo(t *testing.T) {
	h, contentKey := k710KnownStaleFound(t)
	writes := k710Writes(t)
	h.customerExternal.Store(-1) // every customer resolve declines, the re-run included
	// The content stays KNOWN-STALE through the whole request (the debt's refill
	// is parked), so a re-run with D2 still enabled WOULD be served stale again.
	release := restactionsapi.HoldStaleDebtRefillsForTest()
	t.Cleanup(release)
	s0 := restactionsapi.StaleServeStatsSnapshot()
	type res struct {
		r    string
		code int
	}
	done := make(chan res, 1)
	go func() { r, code := h.getRA(0, k547Changed); done <- res{r, code} }()
	var got res
	select {
	case got = <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("D2-15b RED: the withdrawn GET did not return — its re-run re-entered D2 and withdrew again")
	}
	if d := restactionsapi.StaleServeStatsSnapshot().ServeTotal - s0.ServeTotal; d != 1 {
		t.Fatalf("D2-15b RED: %d stale decisions for one GET with the content still known-stale — the re-run re-entered D2", d)
	}
	release()
	k710AssertFreshWithdrawn(t, "D2-15b", got.r, got.code, contentKey, s0.ServeTotal, s0.WithdrawnUnarmable, writes(), false)
}

func TestIssue710_D2_15c_ANestedStaleServeUnderADeclinedWidgetIsNeverWritten(t *testing.T) {
	h, contentKey := k710KnownStaleFound(t)
	writes := k710Writes(t)
	h.customerExternal.Store(1)
	s0 := restactionsapi.StaleServeStatsSnapshot()
	// The widget's resolve runs the RA's `found` stage in-process (a nested
	// RESTAction resolve under the widget dispatcher's ctx and sink).
	r, code := h.get(0, k547Changed)
	k710AssertFreshWithdrawn(t, "D2-15c", r, code, contentKey, s0.ServeTotal, s0.WithdrawnUnarmable, writes(), true)
}

func TestIssue710_D2_17_EveryStaleWriteHasAnAcceptedPutAnArmedHeaderAndAnOwedMark(t *testing.T) {
	h, _ := k710KnownStaleFound(t)
	writes := k710Writes(t)
	puts := k710Puts(t)
	// The debt refill is PARKED across both requests, so the content stays
	// KNOWN-STALE and the declined pass below really carries a stale body: a
	// write point that let it through unarmed must turn THIS sweep RED (S11).
	release := restactionsapi.HoldStaleDebtRefillsForTest()
	t.Cleanup(release)
	// A committed stale serve (widget, cohort 0) …
	if r, code := h.get(0, k547Changed); code != 200 || r != "v0" {
		t.Fatalf("D2-17: cohort 0 served %q (code %d), want the stale v0 shown at once", r, code)
	}
	// … a declined one (RA, cohort 1). Its response is checked AFTER the sweep,
	// so a stale write the write point let through is named by the sweep's own
	// invariant, not by this request's body …
	h.customerExternal.Store(1)
	r1, code1 := h.getRA(1, k547Changed)
	release()
	// … and the replace converging.
	k547WaitFor(t, 10*time.Second, "widget replaced", func() bool {
		rr, ok := h.residentReason(h.key(0, k547Changed))
		return ok && rr == "v1"
	})
	nStale := 0
	for _, w := range writes() {
		if !w.staleDerived {
			continue
		}
		nStale++
		if !w.armed || !w.owed {
			t.Fatalf("D2-17 RED: a stale-derived body was written with armed=%v owed=%v — every stale write needs an "+
				"accepted Put, the armed header and an owed-push mark", w.armed, w.owed)
		}
		if e, ok := cache.ResolvedCache().GetNoTouch(w.key); ok && e.StaleDerived {
			t.Fatalf("D2-17: the stale-derived cell is still resident after the replace window")
		}
	}
	if nStale == 0 {
		t.Fatalf("D2-17: no stale-derived write observed — the sweep is vacuous")
	}
	k710WriteNoStaleOutsidePutIfGen(t, "D2-17", puts())
	if code1 != 200 || r1 != "v1" {
		t.Fatalf("D2-17: cohort 1's declined RA served %q (code %d), want fresh v1", r1, code1)
	}
}

// TestIssue710_D2_17i — #854: D2 over an INCREMENTAL content cell. The cell
// is marked known-stale exactly as above (its refresh reports not-servable),
// but its informer is servable and it applies the change itself. An armable
// customer read still takes the D2 path (show at once, open the debt), and is
// shown the cell's CURRENT generation, never an older one: no reader is served
// content older than a fill would read now (#854 C5). The write point keeps
// its invariant: every stale-derived write has an accepted Put, the armed
// header and an owed-push mark.
// Mutation → RED: drop the Snapshot the stale-serve read takes in
// apistageContentServe's HIT branch (the read is served the generation the
// cell published before the change, v0).
func TestIssue710_D2_17i_AnIncrementalCellUnderAKnownStaleMarkServesItsCurrentGeneration(t *testing.T) {
	h, contentKey := k710KnownStaleFoundAnchoring(t, true)
	e, ok := cache.ResolvedCache().GetNoTouch(contentKey)
	if !ok || e.ListCell() == nil {
		t.Fatalf("PRECONDITION: the `found` content cell is not an incremental cell")
	}
	writes := k710Writes(t)
	puts := k710Puts(t)
	release := restactionsapi.HoldStaleDebtRefillsForTest()
	t.Cleanup(release)
	s0 := restactionsapi.StaleServeStatsSnapshot()
	r, code := h.get(0, k547Changed)
	if d := restactionsapi.StaleServeStatsSnapshot().ServeTotal - s0.ServeTotal; d != 1 {
		t.Fatalf("PRECONDITION: %d D2 stale decisions for the GET, want 1 (the arm must read through D2)", d)
	}
	if code != 200 || r != "v1" {
		t.Fatalf("D2-17i RED: cohort 0 was served %q (code %d) through D2 over an incremental cell, want its current generation v1", r, code)
	}
	release()
	for _, w := range writes() {
		if w.staleDerived && (!w.armed || !w.owed) {
			t.Fatalf("D2-17i RED: a stale-derived body was written with armed=%v owed=%v", w.armed, w.owed)
		}
	}
	k710WriteNoStaleOutsidePutIfGen(t, "D2-17i", puts())
}

// k710Writer records whether anything reached the wire.
type k710Writer struct {
	h           http.Header
	wroteHeader bool
	code        int
	body        []byte
}

func (w *k710Writer) Header() http.Header { return w.h }
func (w *k710Writer) WriteHeader(code int) {
	if !w.wroteHeader {
		w.wroteHeader, w.code = true, code
	}
}
func (w *k710Writer) Write(b []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	w.body = append(w.body, b...)
	return len(b), nil
}

// k710GetRA is getRA through a recording writer.
func k710GetRA(h *k547Harness, k, m int) (string, *k710Writer) {
	ex, _ := json.Marshal(k547Extras(m))
	u := "/call?apiVersion=" + url.QueryEscape(k547RAGVR.Group+"/"+k547RAGVR.Version) +
		"&resource=" + k547RAGVR.Resource + "&name=" + k547RAName + "&namespace=" + k547WNS +
		"&extras=" + url.QueryEscape(string(ex))
	w := &k710Writer{h: http.Header{}}
	RESTAction().ServeHTTP(w, httptest.NewRequest("GET", u, nil).WithContext(k547ReqCtx(k)))
	var obj struct {
		Status struct {
			Conditions []struct {
				Reason string `json:"reason"`
			} `json:"conditions"`
		} `json:"status"`
	}
	if json.Unmarshal(w.body, &obj) != nil || len(obj.Status.Conditions) == 0 {
		return "", w
	}
	return obj.Status.Conditions[0].Reason, w
}

func k710Lookups() (hit, miss int64) {
	st := cache.ResolvedCacheStatsByStat()
	return st["hit_total"], st["miss_total"]
}

func TestIssue710_D2_15d_AWithdrawnServeIsCountedOnceAndNothingIsWrittenBeforeTheReRun(t *testing.T) {
	h, contentKey := k710KnownStaleFound(t)
	var rerunSaw []string
	prev := staleRerunHookForTest
	staleRerunHookForTest = func(wri http.ResponseWriter) {
		w, ok := wri.(*k710Writer)
		if !ok {
			rerunSaw = append(rerunSaw, "not the arm's writer")
			return
		}
		if w.wroteHeader || len(w.body) > 0 {
			rerunSaw = append(rerunSaw, "status/body written before the re-run")
		}
		for _, hk := range []string{refreshKeyHeader, refreshClassHeader, util.HeaderStageOutcomes} {
			if w.h.Get(hk) != "" {
				rerunSaw = append(rerunSaw, "header "+hk+" set before the re-run")
			}
		}
		// Determinism: the re-run sees the content state the baseline GET sees
		// (refilled), so the comparison isolates what the re-run COUNTS.
		k547WaitFor(t, 10*time.Second, "debt cleared before the re-run", func() bool {
			_, _, ks := cache.KnownStale(contentKey)
			return !ks && restactionsapi.StaleServeStatsSnapshot().DebtOpen == 0
		})
	}
	t.Cleanup(func() { staleRerunHookForTest = prev })

	h.customerExternal.Store(1)
	wd0 := restactionsapi.StaleServeStatsSnapshot().WithdrawnUnarmable
	h0, m0 := k710Lookups()
	r, w := k710GetRA(h, 0, k547Changed)
	h1, m1 := k710Lookups()
	if r != "v1" || w.code != http.StatusOK {
		t.Fatalf("D2-15d: the withdrawn GET served %q (code %d), want fresh v1", r, w.code)
	}
	if len(rerunSaw) != 0 {
		t.Fatalf("D2-15d RED: the ResponseWriter was not pristine at the re-run: %v", rerunSaw)
	}
	if st := restactionsapi.StaleServeStatsSnapshot(); st.WithdrawnUnarmable-wd0 != 1 {
		t.Fatalf("PRECONDITION: the GET did not take the withdraw + re-run path (%+v)", st)
	}
	// Baseline: the same request shape, plain cold (no stale serve, no decline),
	// once the content debt has cleared.
	if !k547WaitFor(t, 10*time.Second, "debt cleared", func() bool {
		_, _, ks := cache.KnownStale(contentKey)
		return !ks && restactionsapi.StaleServeStatsSnapshot().DebtOpen == 0
	}) {
		t.Fatalf("PRECONDITION: the content debt never cleared")
	}
	h2, m2 := k710Lookups()
	if r, _ := k710GetRA(h, 1, k547Changed); r != "v1" {
		t.Fatalf("baseline cold GET served %q, want v1", r)
	}
	h3, m3 := k710Lookups()
	t.Logf("lookups: withdrawn GET hit+%d miss+%d; plain cold GET hit+%d miss+%d", h1-h0, m1-m0, h3-h2, m3-m2)
	if (h1-h0)+(m1-m0) != (h3-h2)+(m3-m2) || m1-m0 != m3-m2 {
		t.Fatalf("D2-15d RED: the withdrawn GET moved the L1 lookup counters by hit+%d miss+%d, a plain cold GET by "+
			"hit+%d miss+%d — the re-run counted a second request", h1-h0, m1-m0, h3-h2, m3-m2)
	}
}

func TestIssue710_D2_19b_ADemandRevalidationBypassesBothGates(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	h.driveChangedToKnownStale("v1")
	if r, code := h.get(0, k547Changed); code != 200 {
		t.Fatalf("D2-19b: GET code %d (served %q)", code, r)
	}
	var seen []k547GateSeen
	if !k547WaitFor(t, 10*time.Second, "demand revalidation resolved", func() bool {
		seen = nil
		for _, g := range h.gates() {
			if g.demand {
				seen = append(seen, g)
			}
		}
		return len(seen) > 0
	}) {
		t.Fatalf("PRECONDITION: no demand revalidation resolve observed (%+v)", h.gates())
	}
	for _, g := range seen {
		if !g.triggered || g.fill == "background" || g.nestedBg {
			t.Fatalf("D2-19b RED: a demand revalidation resolved at %+v — a customer is waiting on it: it must bypass "+
				"the content gate and be a customer tree at the nested gate", g)
		}
	}
}

func TestIssue710_D2_19c_ADebtDependentReResolveBypassesBothGatesAheadOfTheOriginRule(t *testing.T) {
	h, _ := k710KnownStaleFound(t)
	if r, code := h.get(0, k547Changed); code != 200 || r != "v0" {
		t.Fatalf("D2-19c: cohort 0 served %q (code %d), want the stale v0", r, code)
	}
	if !k547WaitFor(t, 10*time.Second, "widget replaced", func() bool {
		rr, ok := h.residentReason(h.key(0, k547Changed))
		return ok && rr == "v1"
	}) {
		t.Fatalf("PRECONDITION: the stale-derived widget cell was never replaced")
	}
	var dep, plain []k547GateSeen
	for _, g := range h.gates() {
		switch {
		case g.bg && g.triggered:
			dep = append(dep, g)
		case g.bg:
			plain = append(plain, g)
		}
	}
	if len(dep) == 0 {
		t.Fatalf("PRECONDITION: no debt-lane dependent re-resolve observed (%+v)", h.gates())
	}
	for _, g := range dep {
		if g.fill != "debt" || g.nestedBg {
			t.Fatalf("D2-19c RED: the dependent re-resolve (refresher origin + customer-triggered) classed fill=%q nestedBg=%v — "+
				"want the bypass class at the content gate and a customer tree at the nested gate", g.fill, g.nestedBg)
		}
	}
	// Control: a resolve under the refresher origin WITHOUT the marker stays
	// background at both gates.
	ctl := cache.WithBackgroundResolveOrigin(context.Background(), cache.BackgroundOriginRefresher)
	if c := restactionsapi.ContentFillClassForTest(ctl); c != "background" || !restactionsapi.NestedResolveBackgroundForTest(ctl) {
		t.Fatalf("D2-19c control: a plain refresher ctx classed fill=%q — the bypass leaked to background work", c)
	}
	_ = plain
}

func k710ExternalStaleFound(t *testing.T) *k547Harness {
	t.Helper()
	h, _ := k710KnownStaleFound(t)
	h.widgetU.SetAnnotations(map[string]string{externalCacheTTLAnnotation: "300"})
	h.external.Store(true) // every resolve of the widget touches a genuine external endpoint
	return h
}

func TestIssue710_D2_15e_AnExternalTTLWidgetNeverStoresAStaleDerivedBody(t *testing.T) {
	h := k710ExternalStaleFound(t)
	puts := k710Puts(t)
	s0 := restactionsapi.StaleServeStatsSnapshot()
	r, code := h.get(0, k547Changed)
	if code != 200 || r != "v1" {
		t.Fatalf("D2-15e: served %q (code %d), want the FRESH v1 — an external-TTL response can never be pushed", r, code)
	}
	st := restactionsapi.StaleServeStatsSnapshot()
	if st.ServeTotal-s0.ServeTotal != 1 || st.WithdrawnUnarmable-s0.WithdrawnUnarmable != 1 {
		t.Fatalf("PRECONDITION: the first pass must have been served stale and withdrawn (serve %d, withdrawn %d)",
			st.ServeTotal-s0.ServeTotal, st.WithdrawnUnarmable-s0.WithdrawnUnarmable)
	}
	k710WriteNoStaleOutsidePutIfGen(t, "D2-15e", puts())
	e, ok := cache.ResolvedCache().GetNoTouch(h.key(0, k547Changed))
	if !ok || !e.ExternalTTL || e.StaleDerived {
		t.Fatalf("D2-15e: the resident cell is resident=%v externalTTL=%v staleDerived=%v — want the re-run's fresh external-TTL cell",
			ok, ok && e.ExternalTTL, ok && e.StaleDerived)
	}
	if rr, _ := h.residentReason(h.key(0, k547Changed)); rr != "v1" {
		t.Fatalf("D2-15e RED: the resident cell holds %q, want v1", rr)
	}
}

func TestIssue710_D2_15e_ADeclinedReRunLeavesNoStaleExternalCellBehind(t *testing.T) {
	h := k710ExternalStaleFound(t)
	h.stageErrOnRerun.Store(true)
	puts := k710Puts(t)
	r, code := h.get(0, k547Changed)
	if code != 200 || r != "v1" {
		t.Fatalf("D2-15e(b): served %q (code %d), want the fresh v1 (the re-run's body, partial or not, is fresh)", r, code)
	}
	k710WriteNoStaleOutsidePutIfGen(t, "D2-15e(b)", puts())
	if rr, ok := h.residentReason(h.key(0, k547Changed)); ok && rr == "v0" {
		t.Fatalf("D2-15e(b) RED: a STALE external-TTL cell (v0) is resident after a withdrawn serve whose re-run was declined — " +
			"stale baked into a fresh-looking cell for up to the external TTL")
	}
}
