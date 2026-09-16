// issue216_f6b_gone_forget_test.go — 1.12.7 F6b: a GONE verdict must stop the
// per-binding seed from replaying a harvested in-memory copy.
//
// WHY THIS ARM IS SHAPED THE WAY IT IS. The seam under test is exactly the
// thing a test double would fake, so faking any part of it proves nothing.
// Three rules therefore bind this file:
//
//   - It calls the PRODUCTION registrar (registerHarvesterGoneForgetHook) and
//     never passes its own closure to cache.RegisterGoneForgetHook. A hook the
//     test wired itself would demonstrate only that the test can call a
//     function.
//   - It drives the REAL cache entry point with an absent verdict —
//     cache.Deps().OnDelete, which IS OnObjectEvent(..., objAbsent), the single
//     decision site the dep-event worker calls. No double, and no crossed state
//     installed by hand.
//   - It asserts an OUTCOME on two real invocations with opposite results, not
//     that a hook fired. Seed once with the record present, seed again after
//     the verdict, and the second pass must not reach the widget at all.
//
// WHAT IT OBSERVES, AND THE ONE HOP IT CANNOT REACH. The observable is the
// production diagnostic seedOneWidget emits for every widget it is entered for
// (dispatch.cache_key.computed at site="seed", helpers.go), captured off the
// default logger. seedOneWidget itself is NOT stubbed — the real primitive runs
// on both passes, driven by the real seedScopeYielding over the real target
// enumerator.
//
// It stops one hop short of the Put, and deliberately so: reaching handle.Put
// hermetically is not possible in this package, because widgets.Resolve calls
// crdschema.ValidateObjectStatus, which needs a live apiserver GET for the CRD
// over the SA transport. That reach limit is pre-existing and already recorded
// in the repo (seed_resolves_counter_test.go), and no test in this package
// crosses it. Entry into the real seed primitive for a coordinate is the
// strongest available discriminator, and it is the right one for the mechanism
// under test: F6b changes whether the seed pass ever reaches the widget, not
// what it does once there.
//
// WHY ENTRY IS A SOUND PROXY FOR THE WRITE — AND WHAT WOULD INVALIDATE IT.
// (PM verify on dfa0958, condition A. This block is the load-bearing
// assumption of this file; if you change seedOneWidget, read it first.)
//
// The discriminator above is ENTRY into seedOneWidget for the coordinate. That
// is only sufficient because, for a widget of this shape, entry IMPLIES the
// Put. Here is every gate between the observable and the write, with the
// reason each one cannot intervene — enumerated from the code rather than
// asserted, because the whole argument rests on the list being complete.
// Line numbers are phase1_pip_seed.go at the time of writing.
//
// The observable is the diagnostic at :1124, emitted immediately after the key
// derivation. After it:
//
//	:1129  handle == nil || key == ""   — L1 off or no identity. The fixture
//	                                      enables the cache and installs a
//	                                      cohort identity, so neither holds.
//	:1139  BindingUID == ""             — RBAC fail-closed. buildFixCWatcher
//	                                      grants userGranted a real binding on
//	                                      this GVR, so the re-derivation yields
//	                                      a non-empty first-match UID.
//	:1162  seedSkipDecision             — boot fresh-skip / keepwarm age-skip.
//	                                      The arm runs seedModeBoot against a
//	                                      freshly reset store, so there is no
//	                                      live cell to skip on.
//	:1173  enterSeedUnit error          — only a ctx cancelled while blocked on
//	                                      the admission bound.
//	:1274  declineSeedPutOnError        — needs a swallowed stage error or an
//	                                      external-endpoint touch.
//	:1283  declineWidgetUAFPut          — needs a userAccessFilter refilter,
//	                                      which can only arrive through the
//	                                      widget's apiRef RESTAction chain.
//	:1299  handle.Put                   — the write.
//
// The last two are the DECLINE gates, and they are the structural half of the
// argument: all three of their triggers — stage error, external touch, UAF
// refilter — are produced by resolving a widget's apiRef chain. A widget with
// no apiRef resolves entirely from the harvested in-memory copy and touches
// nothing that can raise any of them. This is the same property that makes the
// replay loop possible in the first place: the seed never fetches the object,
// so nothing can 404 and no decline guard can fire. The earlier gates are the
// fixture half — each is satisfied by construction above, not by luck.
//
// THE HAZARD THIS COMMENT EXISTS FOR: a NEW gate added anywhere between :1124
// and :1299 would break the implication silently. The arm would still observe
// entry, still pass, and would no longer be evidence that the seed writes.
// Nothing in this file can detect that. If you add a gate there, either move
// the observable below it or replace it with a direct write assertion.
//
// (Note that in-process the resolve at :1257 itself fails, because
// crdschema.ValidateObjectStatus needs a live CRD GET — see the reach limit
// above. The implication documented here is about the PRODUCTION path, which
// is what makes entry a valid proxy for it.)
//
// RED on the base: delete the forgetCoordinate calls in
// registerHarvesterGoneForgetHook, or the notifyObjectGone fire in
// OnObjectEvent's absent branch, and the second pass seeds the widget again.
package dispatchers

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// f6bSyncBuf is a mutex-guarded slog capture sink.
//
// WHY NOT A BARE bytes.Buffer. slog.SetDefault is process-wide, and this
// process holds at least one long-lived goroutine that logs on a timer and is
// never stopped: startDispatchSummary (internal/resolvers/restactions/api),
// a 60s ticker started lazily on the first informer dispatch. Whenever one of
// its ticks lands inside a test's capture window, an unsynchronised buffer is
// written by that goroutine while the test reads it — a genuine data race.
// TestF4bLeverA_R4_SummaryIsWholeBootCumulative takes exactly that race
// intermittently under -count=3. Guarding the sink costs nothing and keeps
// this file out of that class entirely.
type f6bSyncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *f6bSyncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *f6bSyncBuf) snapshot() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

// f6bSeedEntriesFor counts how many times the REAL seed primitive was entered
// for the named widget during a captured pass. Parses the production
// dispatch.cache_key.computed diagnostic rather than any test-only signal.
func f6bSeedEntriesFor(captured []byte, ns, name string) int {
	type line struct {
		Msg       string `json:"msg"`
		Site      string `json:"site"`
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
	}
	n := 0
	for _, raw := range bytes.Split(captured, []byte("\n")) {
		if len(raw) == 0 {
			continue
		}
		var l line
		if err := json.Unmarshal(raw, &l); err != nil {
			continue
		}
		if l.Msg == "dispatch.cache_key.computed" && l.Site == "seed" &&
			l.Namespace == ns && l.Name == name {
			n++
		}
	}
	return n
}

// f6bHarvestedStillHolds reports whether the nav harvester's snapshot still
// carries the coordinate — the state the seed pass reads.
func f6bHarvestedStillHolds(h *navWidgetHarvester, gvr schema.GroupVersionResource, ns, name string) bool {
	for _, e := range h.snapshot() {
		if e.GVR == gvr && e.W != nil && e.W.GetNamespace() == ns && e.W.GetName() == name {
			return true
		}
	}
	return false
}

func TestIssue216_F6b_GoneVerdictStopsTheSeedReplayingAHarvestedCopy(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()

	// Real RBAC snapshot + servable widget GVR, so dispatchCacheLookupKey runs
	// the production derivation and the target enumerator has a real binding.
	buildFixCWatcher(t)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{fixCWidgetGVR})
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)

	cache.ResetGoneForgetHooksForTest()
	t.Cleanup(cache.ResetGoneForgetHooksForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	const (
		ns   = "krateo-system"
		name = "dashboard-flex"
	)

	// The harvested copy — produced by the production harvest path, not by
	// writing into the map.
	nav := newNavWidgetHarvester()
	w := &unstructured.Unstructured{}
	w.SetNamespace(ns)
	w.SetName(name)
	w.SetGroupVersionKind(schema.GroupVersionKind{
		Group: fixCWidgetGVR.Group, Version: fixCWidgetGVR.Version, Kind: "Flex",
	})
	nav.harvestNavWidget(w, fixCWidgetGVR, -1, -1, -1, -1)
	content := newContentPrewarmHarvester()

	// THE PRODUCTION REGISTRAR. Not a closure this test wrote.
	registerHarvesterGoneForgetHook(rePrewarmDeps{navHarv: nav, harvester: content})

	seedPass := func() int {
		var buf f6bSyncBuf
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
		defer slog.SetDefault(prev)

		// A bounded parent deadline: the real widgets.Resolve cannot complete
		// in-process (no apiserver for the CRD validate), and the assertion is
		// on seed ENTRY, which happens before it. The deadline only stops the
		// doomed tail from costing wall-clock.
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		// The real seed pass, over whatever the harvester currently holds.
		_ = seedScopeYielding(ctx, nil, nav.snapshot(), endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot)
		return f6bSeedEntriesFor(buf.snapshot(), ns, name)
	}

	// PASS 1 — the record is present, so the seed reaches the widget.
	if got := seedPass(); got == 0 {
		t.Fatalf("premise: the first seed pass never reached widget %s/%s (0 seed entries); "+
			"the arm cannot discriminate anything if the seed does not run", ns, name)
	}

	// THE REAL CACHE ENTRY POINT, with an absent verdict. OnDelete is
	// OnObjectEvent(gvr, ns, name, objAbsent) verbatim — the same decision site
	// and the same branch the dep-event worker drives after probing a synced
	// informer. Nothing is installed by hand.
	cache.Deps().OnDelete(fixCWidgetGVR, ns, name)

	// THE LOAD-BEARING ASSERTION. The snapshot the seed reads must no longer
	// carry the coordinate. The second seed pass below overlaps with this one
	// — both read the same map — and it earns its place by proving the seed
	// consumes the SNAPSHOT rather than some cached target list it built
	// earlier. But this is the assertion that pins the mechanism, so do not
	// drop or weaken it on the grounds that the second pass covers it: if the
	// pass were ever to stop reading the harvester, this check would still be
	// the one that catches a forget that did not happen.
	if f6bHarvestedStillHolds(nav, fixCWidgetGVR, ns, name) {
		t.Fatalf("RED (F6b): after an ABSENT verdict for %s/%s the nav harvester still holds the "+
			"harvested copy. The per-binding seed re-resolves that copy on every pass and never "+
			"fetches the object, so a deleted widget is written back into L1 forever and eviction "+
			"cannot win", ns, name)
	}

	// PASS 2 — the seed must not reach the widget at all.
	if got := seedPass(); got != 0 {
		t.Fatalf("RED (F6b): the second seed pass entered the seed primitive %d time(s) for %s/%s "+
			"AFTER the cache derived an authoritative GONE verdict for it. The forget hook did not "+
			"reach the harvester, so the replay loop is intact", got, ns, name)
	}
}

// TestIssue216_F6b_LiveCoordinateIsNeverForgotten — the over-forget guard. A
// gone verdict for a DIFFERENT object must not disturb the harvested set.
// Remove-on-positive-evidence has to be precise, or the hook becomes a way to
// empty the warm set.
func TestIssue216_F6b_LiveCoordinateIsNeverForgotten(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	cache.ResetGoneForgetHooksForTest()
	t.Cleanup(cache.ResetGoneForgetHooksForTest)
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
	nav := newNavWidgetHarvester()
	for _, n := range []string{"keep-me", "delete-me"} {
		w := &unstructured.Unstructured{}
		w.SetNamespace("krateo-system")
		w.SetName(n)
		w.SetGroupVersionKind(schema.GroupVersionKind{Group: gvr.Group, Version: gvr.Version, Kind: "Flex"})
		// Two pagination tuples for the same widget: a forget must take both,
		// and a forget for a different widget must take neither.
		nav.harvestNavWidget(w, gvr, -1, -1, -1, -1)
		nav.harvestNavWidget(w, gvr, 5, 1, 5, 1)
	}
	content := newContentPrewarmHarvester()
	registerHarvesterGoneForgetHook(rePrewarmDeps{navHarv: nav, harvester: content})

	if n := len(nav.snapshot()); n != 4 {
		t.Fatalf("premise: expected 4 harvested entries, got %d", n)
	}

	cache.Deps().OnDelete(gvr, "krateo-system", "delete-me")

	if f6bHarvestedStillHolds(nav, gvr, "krateo-system", "delete-me") {
		t.Fatalf("F6b: delete-me survived its own gone verdict (both pagination tuples must go)")
	}
	if !f6bHarvestedStillHolds(nav, gvr, "krateo-system", "keep-me") {
		t.Fatalf("RED (F6b over-forget): a gone verdict for delete-me also dropped the LIVE widget " +
			"keep-me. Removal must be exact — a hook that over-matches empties the warm set and " +
			"turns every deletion into a batch of cold navigations")
	}
	if n := len(nav.snapshot()); n != 2 {
		t.Fatalf("F6b: expected exactly keep-me's 2 entries to remain, got %d", n)
	}

	// A verdict for a coordinate that was never harvested must be inert.
	cache.Deps().OnDelete(gvr, "krateo-system", "never-seen")
	if n := len(nav.snapshot()); n != 2 {
		t.Fatalf("F6b: a verdict for an unharvested coordinate changed the set (now %d entries)", n)
	}
}

// TestIssue216_F6b_ExistsVerdictNeverForgets — remove on positive evidence.
// An object that EXISTS is dirty-mark territory and must never cost a
// harvested copy. The uncertain/degraded verdicts are unexported states, so
// the arm that pins THEM lives next to them, in the cache package
// (TestIssue216_F6b_OnlyTheAbsentVerdictFires).
func TestIssue216_F6b_ExistsVerdictNeverForgets(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	cache.ResetGoneForgetHooksForTest()
	t.Cleanup(cache.ResetGoneForgetHooksForTest)
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
	nav := newNavWidgetHarvester()
	w := &unstructured.Unstructured{}
	w.SetNamespace("krateo-system")
	w.SetName("uncertain-flex")
	w.SetGroupVersionKind(schema.GroupVersionKind{Group: gvr.Group, Version: gvr.Version, Kind: "Flex"})
	nav.harvestNavWidget(w, gvr, -1, -1, -1, -1)
	registerHarvesterGoneForgetHook(rePrewarmDeps{navHarv: nav, harvester: newContentPrewarmHarvester()})

	// The object EXISTS — an ADD/UPDATE verdict. Dirty-mark territory, never a
	// reason to drop a harvested copy.
	cache.Deps().OnUpdate(gvr, "krateo-system", "uncertain-flex")
	if !f6bHarvestedStillHolds(nav, gvr, "krateo-system", "uncertain-flex") {
		t.Fatalf("RED (F6b): an EXISTS verdict dropped the harvested copy. Only an absent verdict may")
	}
	cache.Deps().OnAdd(gvr, "krateo-system", "uncertain-flex")
	if !f6bHarvestedStillHolds(nav, gvr, "krateo-system", "uncertain-flex") {
		t.Fatalf("RED (F6b): an ADD verdict dropped the harvested copy. Only an absent verdict may")
	}
}
