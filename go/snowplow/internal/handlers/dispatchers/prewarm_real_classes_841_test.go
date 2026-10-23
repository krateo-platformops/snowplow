package dispatchers

// prewarm_real_classes_841_test.go — #841: seed only identities a real
// requester can hit, in order of known population; the seed-origin counters
// show seeded cells being hit.
//
//   S1 TestS841_LearnedMultiBindingSeededFirst_UnmatchedBaseNeverSeeded — the
//      bench run 12 shape: every known user holds ≥2 bindings, so no
//      representative matches anyone. The learned class is the FIRST identity
//      seeded, is warm when the latch fires, and no representative is ever
//      resolved.
//   S2 TestS841_MatchedBaseKept_UnmatchedBaseDropped — a representative a known
//      group-only user matches is kept (and serves that user); a bound group no
//      known user holds is not seeded.
//   S3 TestS841_SeededCellsAreHitByTheMatchingRequester — the seed-origin
//      counters: cells seeded per class, a customer Get on each counts a hit and
//      a first-hit cell, and a refresh re-Put keeps the origin.
//   S4 TestS841_ClassSeedScopeSeedsNewlyMatchedRepresentative — a login whose
//      class equals an unseeded representative seeds that representative's
//      cells through the class-seed scope.
//   S7 TestS841_SeedOrderIsByKnownPopulation — a higher-population class
//      seeds before a lower one, whatever its source.
//   S8 TestS841_KeptBaseCohortsOrderedByPopulation — the same between two
//      kept base cohorts.
//   S9 TestS841_BackstopReservesTheBaseUnitsStillToCome — a learned class that
//      outranks a kept base cohort is taken pre-latch only if the base units
//      still to come fit too.
//
// The harness is #262's (learned_identity_262_harness_test.go): REAL RBAC
// informer, Secrets informer → learned registry, enumeration wrapper, seed
// primitives and keys; resolver edges stubbed.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const s841Ops = "l262-ops" // a bound group no known user holds

// s841Event is one seed resolve (widget or RESTAction) or the latch fire, in
// the order they happened.
type s841Event struct {
	kind   string // "widget" | "ra" | "latch"
	user   string
	groups []string
}

type s841Recorder struct {
	mu     sync.Mutex
	events []s841Event
}

func (r *s841Recorder) add(e s841Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *s841Recorder) snapshot() []s841Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]s841Event(nil), r.events...)
}

// s841Record wraps the stubbed resolver edges (install AFTER l262Stubs) and the
// latch fire observer.
func s841Record(t *testing.T) *s841Recorder {
	t.Helper()
	r := &s841Recorder{}
	who := func(ctx context.Context) (string, []string) {
		ui, err := xcontext.UserInfo(ctx)
		if err != nil {
			return "?", nil
		}
		return ui.Username, append([]string(nil), ui.Groups...)
	}
	innerW := widgetsResolveFn
	widgetsResolveFn = func(ctx context.Context, o widgets.ResolveOptions) (*widgets.Widget, error) {
		u, g := who(ctx)
		r.add(s841Event{kind: "widget", user: u, groups: g})
		return innerW(ctx, o)
	}
	innerGet := seedObjectsGetFn
	seedObjectsGetFn = func(ctx context.Context, ref templatesv1.ObjectReference) objects.Result {
		u, g := who(ctx)
		r.add(s841Event{kind: "ra", user: u, groups: g})
		return innerGet(ctx, ref)
	}
	prevObs := firstNavFireObserver
	firstNavFireObserver = func(string) { r.add(s841Event{kind: "latch"}) }
	t.Cleanup(func() {
		widgetsResolveFn, seedObjectsGetFn, firstNavFireObserver = innerW, innerGet, prevObs
	})
	return r
}

func s841HasGroup(groups []string, g string) bool {
	for _, x := range groups {
		if x == g {
			return true
		}
	}
	return false
}

// ── S1 ───────────────────────────────────────────────────────────────────────

func TestS841_LearnedMultiBindingSeededFirst_UnmatchedBaseNeverSeeded(t *testing.T) {
	env := l262Setup(t, l262Opts{
		extraUsers:   []string{"dave"},
		groupCohorts: []string{s841Ops},
		secrets: []*corev1.Secret{
			l262ClientconfigSecret(t, "dave-clientconfig", "dave", []string{l262Group}, time.Now().Add(-time.Hour), "1"),
		},
	})
	l262Stubs(t, l262StubOpts{})
	l262WaitLearned(t, 1)
	cache.ResetSeedOriginStatsForTest()
	t.Cleanup(cache.ResetSeedOriginStatsForTest)
	rec := s841Record(t)
	ensureFirstNavLatch()
	bootNavProgressState.reset()
	t.Cleanup(bootNavProgressState.reset)

	daveCtx := l262CustomerCtx("dave", []string{l262Group})
	if w, _ := l262Warm(t, env, daveCtx); w != 0 {
		t.Fatalf("PRECONDITION: a fresh process holds no warm cell for dave, got %d", w)
	}

	l262Boot(t, env, seedModeBoot)

	ev := rec.snapshot()
	if len(ev) == 0 || ev[0].kind != "widget" || ev[0].user != "dave" {
		t.Fatalf("S1 RED: the first seed resolve must be the learned multi-binding class's nav widget, got %+v", ev)
	}
	latchAt, lastDaveWidget := -1, -1
	for i, e := range ev {
		switch {
		case e.kind == "latch":
			latchAt = i
		case e.user == "":
			t.Fatalf("S1 RED: a representative no known requester matches was seeded (event %d: %+v); "+
				"its cells serve no one", i, e)
		case e.kind == "widget" && e.user == "dave":
			lastDaveWidget = i
		}
	}
	if latchAt < 0 {
		t.Fatal("S1 RED: the first-nav latch never fired")
	}
	if lastDaveWidget > latchAt {
		t.Fatalf("S1 RED: the latch fired (event %d) before the learned class's last nav widget (event %d); "+
			"with no representative kept it must still wait for every seeded identity's nav widgets", latchAt, lastDaveWidget)
	}
	if w, n := l262Warm(t, env, daveCtx); w != n {
		t.Fatalf("S1 RED: dave finds %d/%d units warm after the boot seed", w, n)
	}
	// The readiness-exit accounting counts the learned class's nav units: the
	// identity set the latch waited for.
	if snap := bootNavProgressState.snapshot(); !snap.armed || snap.total != len(env.widgets) ||
		snap.processed != snap.total || snap.cohorts != 1 {
		t.Fatalf("S1 RED: readiness nav progress %+v, want armed with total=processed=%d over 1 identity",
			snap, len(env.widgets))
	}
	repCtx := l262CustomerCtx("", []string{l262Group})
	if w, _ := l262Warm(t, env, repCtx); w != 0 {
		t.Fatalf("S1 RED: %d cell(s) were seeded under the unmatched group representative", w)
	}
	if kept, unmatched := cache.BaseTargetCounts(); kept != 0 || unmatched == 0 {
		t.Fatalf("base targets kept=%d unmatched=%d, want 0 kept and >0 unmatched", kept, unmatched)
	}
	if seeded, _ := cache.LearnedAdmissionStats(); seeded != 1 {
		t.Fatalf("learned classes seeded=%d, want 1 (the probe admits it with nothing measured)", seeded)
	}
}

// ── S2 ───────────────────────────────────────────────────────────────────────

func TestS841_MatchedBaseKept_UnmatchedBaseDropped(t *testing.T) {
	now := time.Now()
	env := l262Setup(t, l262Opts{
		extraUsers:   []string{"dave"},
		groupCohorts: []string{s841Ops},
		secrets: []*corev1.Secret{
			l262ClientconfigSecret(t, "dave-clientconfig", "dave", []string{l262Group}, now.Add(-time.Hour), "1"),
			// erin holds ONLY the group's bindings: the group representative's key is hers.
			l262ClientconfigSecret(t, "erin-clientconfig", "erin", []string{l262Group}, now.Add(-2*time.Hour), "1"),
		},
	})
	l262Stubs(t, l262StubOpts{})
	l262WaitLearned(t, 2)
	rec := s841Record(t)

	l262Boot(t, env, seedModeBoot)

	repSeeded := false
	for _, e := range rec.snapshot() {
		if e.user != "" {
			continue
		}
		if s841HasGroup(e.groups, s841Ops) {
			t.Fatalf("S2 RED: the %s representative was seeded, but no known requester holds that group", s841Ops)
		}
		if s841HasGroup(e.groups, l262Group) {
			repSeeded = true
		}
	}
	if !repSeeded {
		t.Fatal("S2 RED: the group representative erin matches was not seeded")
	}
	erinCtx := l262CustomerCtx("erin", []string{l262Group})
	if w, n := l262Warm(t, env, erinCtx); w != n {
		t.Fatalf("S2 RED: erin (group-only, served by the representative) finds %d/%d warm", w, n)
	}
	if w, n := l262Warm(t, env, l262CustomerCtx("dave", []string{l262Group})); w != n {
		t.Fatalf("S2 RED: dave finds %d/%d warm", w, n)
	}
	if kept, unmatched := cache.BaseTargetCounts(); kept == 0 || unmatched == 0 {
		t.Fatalf("base targets kept=%d unmatched=%d, want both >0", kept, unmatched)
	}
}

// ── S3 ───────────────────────────────────────────────────────────────────────

func TestS841_SeededCellsAreHitByTheMatchingRequester(t *testing.T) {
	now := time.Now()
	env := l262Setup(t, l262Opts{
		extraUsers: []string{"dave"},
		secrets: []*corev1.Secret{
			l262ClientconfigSecret(t, "dave-clientconfig", "dave", []string{l262Group}, now.Add(-time.Hour), "1"),
			l262ClientconfigSecret(t, "erin-clientconfig", "erin", []string{l262Group}, now.Add(-2*time.Hour), "1"),
		},
	})
	l262Stubs(t, l262StubOpts{})
	l262WaitLearned(t, 2)
	cache.ResetSeedOriginStatsForTest()
	t.Cleanup(cache.ResetSeedOriginStatsForTest)

	l262Boot(t, env, seedModeBoot)

	daveKeys, handle := l262Keys(t, env, l262CustomerCtx("dave", []string{l262Group}))
	erinKeys, _ := l262Keys(t, env, l262CustomerCtx("erin", []string{l262Group}))
	n := uint64(len(daveKeys))
	if s := cache.SeedOriginStats(cache.SeedOriginLearned); s.Seeded < n || s.Hits != 0 {
		t.Fatalf("after the seed: learned seeded=%d hits=%d, want ≥%d seeded and 0 hits", s.Seeded, s.Hits, n)
	}
	// The matching requester reads every cell (the customer Get funnel).
	for _, k := range daveKeys {
		if _, ok := handle.Get(k); !ok {
			t.Fatalf("PRECONDITION: dave's seeded cell %q is resident", k)
		}
	}
	if s := cache.SeedOriginStats(cache.SeedOriginLearned); s.Hits != n || s.CellsHit != n {
		t.Fatalf("S3 RED: dave read %d learned-seeded cells; counters hits=%d cells_hit=%d", n, s.Hits, s.CellsHit)
	}
	// A refresher re-Put (no origin on the new entry) keeps the cell's origin;
	// a second read is a hit but not a new first-hit cell.
	prev, _ := handle.GetNoTouch(daveKeys[0])
	handle.Put(daveKeys[0], &cache.ResolvedEntry{RawJSON: prev.RawJSON, Inputs: prev.Inputs})
	for _, k := range daveKeys {
		handle.Get(k)
	}
	if s := cache.SeedOriginStats(cache.SeedOriginLearned); s.Hits != 2*n || s.CellsHit != n {
		t.Fatalf("S3 RED: after a refresh re-Put and a second read: hits=%d cells_hit=%d, want %d/%d", s.Hits, s.CellsHit, 2*n, n)
	}
	// erin's cells are the kept representative's: base origin.
	for _, k := range erinKeys {
		handle.Get(k)
	}
	if s := cache.SeedOriginStats(cache.SeedOriginBase); s.Hits != uint64(len(erinKeys)) || s.CellsHit != uint64(len(erinKeys)) {
		t.Fatalf("S3 RED: erin read %d representative cells; base hits=%d cells_hit=%d", len(erinKeys), s.Hits, s.CellsHit)
	}
	// A cell traffic created is never attributed, even after a seed re-Put.
	trafficKey := "s841-traffic-cell"
	handle.Put(trafficKey, &cache.ResolvedEntry{RawJSON: []byte(`{}`)})
	handle.Put(trafficKey, &cache.ResolvedEntry{RawJSON: []byte(`{}`), SeedOrigin: cache.SeedOriginLearned})
	before := cache.SeedOriginStats(cache.SeedOriginLearned)
	handle.Get(trafficKey)
	if after := cache.SeedOriginStats(cache.SeedOriginLearned); after != before {
		t.Fatalf("S3 RED: a seed re-Put claimed a traffic-created cell (%+v → %+v)", before, after)
	}
}

// ── S4 ───────────────────────────────────────────────────────────────────────

func TestS841_ClassSeedScopeSeedsNewlyMatchedRepresentative(t *testing.T) {
	env := l262Setup(t, l262Opts{
		extraUsers: []string{"dave"},
		secrets: []*corev1.Secret{
			l262ClientconfigSecret(t, "dave-clientconfig", "dave", []string{l262Group}, time.Now().Add(-time.Hour), "1"),
		},
	})
	l262Stubs(t, l262StubOpts{})
	l262WaitLearned(t, 1)
	l262Boot(t, env, seedModeBoot) // no one matches the group representative yet
	_ = cache.DrainPendingLearnedClasses()

	erinCtx := l262CustomerCtx("erin", []string{l262Group})
	if w, _ := l262Warm(t, env, erinCtx); w != 0 {
		t.Fatalf("PRECONDITION: the unmatched representative was not seeded, but erin finds %d warm", w)
	}
	// erin logs in: a group-only class, equal to the representative.
	sec := l262ClientconfigSecret(t, "erin-clientconfig", "erin", []string{l262Group}, time.Now(), "1")
	if _, err := env.kube.CoreV1().Secrets(l262AuthnNS).Create(context.Background(), sec, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	l262WaitLearned(t, 2)
	if err := rePrewarmLearnedClasses(context.Background(), env.deps); err != nil {
		t.Fatalf("rePrewarmLearnedClasses: %v", err)
	}
	if w, n := l262Warm(t, env, erinCtx); w != n {
		t.Fatalf("S4 RED: after erin's login the class-seed scope left %d/%d of her (representative) units cold", n-w, n)
	}
}

// ── S7: the seed order is by known population, never by source ───────────────

// s841FirstSeed returns the index of the first widget resolve whose requester
// matches pred, -1 if none.
func s841FirstSeed(ev []s841Event, pred func(s841Event) bool) int {
	for i, e := range ev {
		if e.kind == "widget" && pred(e) {
			return i
		}
	}
	return -1
}

func TestS841_SeedOrderIsByKnownPopulation(t *testing.T) {
	isRep := func(e s841Event) bool { return e.user == "" }
	isTeam := func(e s841Event) bool { return strings.HasPrefix(e.user, "team") }
	isDave := func(e s841Event) bool { return e.user == "dave" }
	// secrets: `team` users hold devs+ops (a learned class with no representative
	// of its own), `solo` users hold devs only (they match the devs
	// representative), dave holds devs + a User binding (a one-user learned
	// class, and the NEWEST login, so newest-first alone would put him first).
	run := func(t *testing.T, team, solo int) []s841Event {
		now := time.Now()
		var secrets []*corev1.Secret
		for i := 0; i < team; i++ {
			u := "team" + itoaLine(i)
			secrets = append(secrets, l262ClientconfigSecret(t, u+"-clientconfig", u,
				[]string{l262Group, s841Ops}, now.Add(-time.Duration(10+i)*time.Hour), "1"))
		}
		for i := 0; i < solo; i++ {
			u := "solo" + itoaLine(i)
			secrets = append(secrets, l262ClientconfigSecret(t, u+"-clientconfig", u,
				[]string{l262Group}, now.Add(-time.Duration(30+i)*time.Hour), "1"))
		}
		secrets = append(secrets, l262ClientconfigSecret(t, "dave-clientconfig", "dave",
			[]string{l262Group}, now.Add(-time.Minute), "1"))
		env := l262Setup(t, l262Opts{extraUsers: []string{"dave"}, groupCohorts: []string{s841Ops}, secrets: secrets})
		l262Stubs(t, l262StubOpts{})
		l262WaitLearned(t, team+solo+1)
		rec := s841Record(t)
		ensureFirstNavLatch()
		l262Boot(t, env, seedModeBoot)
		return rec.snapshot()
	}

	t.Run("learned class shared by 3 before a base cohort matched by 1", func(t *testing.T) {
		ev := run(t, 3, 1)
		team, rep, dave := s841FirstSeed(ev, isTeam), s841FirstSeed(ev, isRep), s841FirstSeed(ev, isDave)
		if team < 0 || rep < 0 || dave < 0 {
			t.Fatalf("NON-VACUITY: every class must seed (team=%d rep=%d dave=%d)", team, rep, dave)
		}
		if !(team < rep && team < dave) {
			t.Fatalf("S7 RED: the 3-user learned class must seed first; first seeds team=%d rep=%d dave=%d", team, rep, dave)
		}
		if !(rep < dave) {
			t.Fatalf("S7 RED: at equal population (1) the base cohort precedes the learned class; rep=%d dave=%d", rep, dave)
		}
	})
	t.Run("base cohort matched by 3 before a learned class shared by 2", func(t *testing.T) {
		ev := run(t, 2, 3)
		team, rep, dave := s841FirstSeed(ev, isTeam), s841FirstSeed(ev, isRep), s841FirstSeed(ev, isDave)
		if team < 0 || rep < 0 || dave < 0 {
			t.Fatalf("NON-VACUITY: every class must seed (team=%d rep=%d dave=%d)", team, rep, dave)
		}
		if !(rep < team && team < dave) {
			t.Fatalf("S7 RED: want base(3) < team(2) < dave(1, newest); first seeds rep=%d team=%d dave=%d", rep, team, dave)
		}
	})
}

// S8: two KEPT base cohorts with different populations — the base list itself
// is ordered by population (all of the larger cohort's nav units before any of
// the smaller's), in both directions so neither the NavOrder/rank tie-breaks
// nor the group names can pass it by coincidence.
func TestS841_KeptBaseCohortsOrderedByPopulation(t *testing.T) {
	repOf := func(group string) func(s841Event) bool {
		return func(e s841Event) bool { return e.user == "" && s841HasGroup(e.groups, group) }
	}
	run := func(t *testing.T, devs, ops int) []s841Event {
		now := time.Now()
		var secrets []*corev1.Secret
		for i := 0; i < devs; i++ {
			u := "devsonly" + itoaLine(i)
			secrets = append(secrets, l262ClientconfigSecret(t, u+"-clientconfig", u,
				[]string{l262Group}, now.Add(-time.Duration(1+i)*time.Hour), "1"))
		}
		for i := 0; i < ops; i++ {
			u := "opsonly" + itoaLine(i)
			secrets = append(secrets, l262ClientconfigSecret(t, u+"-clientconfig", u,
				[]string{s841Ops}, now.Add(-time.Duration(20+i)*time.Hour), "1"))
		}
		env := l262Setup(t, l262Opts{groupCohorts: []string{s841Ops}, secrets: secrets})
		l262Stubs(t, l262StubOpts{})
		l262WaitLearned(t, devs+ops)
		rec := s841Record(t)
		ensureFirstNavLatch()
		l262Boot(t, env, seedModeBoot)
		return rec.snapshot()
	}
	// assertFirst: every nav unit of the `first` cohort seeds before any of `second`'s.
	assertFirst := func(t *testing.T, ev []s841Event, first, second string) {
		t.Helper()
		lastFirst, firstSecond, nFirst, nSecond := -1, -1, 0, 0
		for i, e := range ev {
			if e.kind != "widget" {
				continue
			}
			if e.user != "" {
				t.Fatalf("PRECONDITION: every known user matches a cohort, so no learned class is seeded; got %+v", e)
			}
			switch {
			case repOf(first)(e):
				lastFirst, nFirst = i, nFirst+1
			case repOf(second)(e):
				if firstSecond < 0 {
					firstSecond = i
				}
				nSecond++
			}
		}
		if nFirst == 0 || nSecond == 0 {
			t.Fatalf("NON-VACUITY: both kept cohorts must seed nav units (%s=%d %s=%d)", first, nFirst, second, nSecond)
		}
		if lastFirst > firstSecond {
			t.Fatalf("S8 RED: the more-populated cohort %s must seed all its nav units before %s's first "+
				"(last %s unit at %d, first %s unit at %d)", first, second, first, lastFirst, second, firstSecond)
		}
	}
	t.Run("devs cohort matched by 3 before ops cohort matched by 1", func(t *testing.T) {
		assertFirst(t, run(t, 3, 1), l262Group, s841Ops)
	})
	t.Run("ops cohort matched by 3 before devs cohort matched by 1", func(t *testing.T) {
		assertFirst(t, run(t, 1, 3), s841Ops, l262Group)
	})
}

// S9: a learned class that OUTRANKS a kept base cohort by population goes
// first only while it leaves room for the base units still to come — those are
// the latch's mandatory set, so taking the class when it and the base no longer
// both fit would push readiness onto the backstop.
//
// Fixture (virtual seed clock, every widget 40ms, 2 nav widgets, the widget
// cost already measured — a resumed boot chunk, so no probe): teamA (3 users,
// devs+ops) and teamB (2 users, devs+qa) both outrank the devs cohort (1
// group-only user), and the admission puts both pre-latch (their 160ms fit the
// 200ms backstop). teamA + the base units (160ms) fit; after teamA, 120ms are
// left: teamB alone (80ms) fits, teamB + the base units (160ms) do not. teamB
// must wait for after the latch, the base units seed, and the latch fires
// before the backstop. Without the reservation teamB is taken and the base
// units push the latch past the backstop.
func TestS841_BackstopReservesTheBaseUnitsStillToCome(t *testing.T) {
	const unit = 40 * time.Millisecond
	const qa = "l262-qa"
	now := time.Now()
	var secrets []*corev1.Secret
	add := func(prefix string, n int, groups []string, age time.Duration) {
		for i := 0; i < n; i++ {
			u := prefix + itoaLine(i)
			secrets = append(secrets, l262ClientconfigSecret(t, u+"-clientconfig", u, groups, now.Add(-age-time.Duration(i)*time.Minute), "1"))
		}
	}
	add("teamA", 3, []string{l262Group, s841Ops}, 10*time.Hour)
	add("teamB", 2, []string{l262Group, qa}, 5*time.Hour)
	add("erin", 1, []string{l262Group}, 20*time.Hour)
	env := l262Setup(t, l262Opts{groupCohorts: []string{s841Ops, qa}, secrets: secrets, widgets: 2, ras: 2})
	clk := l262VirtualClock(t)
	l262Stubs(t, l262StubOpts{sleep: unit, clock: clk})
	l262WaitLearned(t, 6)
	widgetUnitCost.add(unit) // measured by an earlier chunk of this boot: no probe

	var mu sync.Mutex
	first := map[string]time.Time{} // class prefix → first widget resolve instant
	var fired time.Time
	inner := widgetsResolveFn
	widgetsResolveFn = func(ctx context.Context, o widgets.ResolveOptions) (*widgets.Widget, error) {
		if ui, err := xcontext.UserInfo(ctx); err == nil {
			cls := "base"
			switch {
			case strings.HasPrefix(ui.Username, "teamA"):
				cls = "teamA"
			case strings.HasPrefix(ui.Username, "teamB"):
				cls = "teamB"
			}
			mu.Lock()
			if _, ok := first[cls]; !ok {
				first[cls] = clk.now()
			}
			mu.Unlock()
		}
		return inner(ctx, o)
	}
	t.Cleanup(func() { widgetsResolveFn = inner })
	prevObs := firstNavFireObserver
	firstNavFireObserver = func(string) { mu.Lock(); fired = clk.now(); mu.Unlock() }
	t.Cleanup(func() { firstNavFireObserver = prevObs })

	latch := ensureFirstNavLatch()
	start := clk.now()
	deadline := start.Add(5 * unit) // teamA (2 units) + 3 units: after teamA, teamB XOR the base fits
	latch.setBackstopDeadline(deadline)

	l262Boot(t, env, seedModeBoot)

	mu.Lock()
	defer mu.Unlock()
	if fired.IsZero() {
		t.Fatal("NON-VACUITY: the first-nav latch never fired")
	}
	for _, c := range []string{"teamA", "teamB", "base"} {
		if first[c].IsZero() {
			t.Fatalf("NON-VACUITY: class %s never seeded its nav widgets (%v)", c, first)
		}
	}
	if !first["teamA"].Before(first["base"]) {
		t.Fatalf("PRECONDITION: the 3-user class seeds before the 1-user base cohort (%v)", first)
	}
	t.Logf("S9: latch +%v, backstop +%v, teamB first +%v, base first +%v",
		fired.Sub(start), deadline.Sub(start), first["teamB"].Sub(start), first["base"].Sub(start))
	if !fired.Before(deadline) {
		t.Fatalf("S9 RED: the latch fired at +%v, at or after the backstop +%v — a learned class was taken "+
			"pre-latch without leaving room for the base units still to come", fired.Sub(start), deadline.Sub(start))
	}
	if first["teamB"].Before(fired) {
		t.Fatalf("S9 RED: teamB seeded pre-latch (+%v) although it and the remaining base units do not both fit "+
			"before the backstop", first["teamB"].Sub(start))
	}
}

// knowEveryRepresentative registers, through the live-traffic path (S2,
// cache.ObserveLiveIdentity), one known requester per representative of gvrs:
// the representative's own user, or for a group representative a member who
// holds only that group's bindings. Under #841 a representative is a seed
// target only while a known requester derives its key; fixtures written before
// #841 seeded every representative, and this states that precondition
// explicitly instead of relaxing the production rule. Call it after the
// binding index is built; the registry is reset on cleanup.
func knowEveryRepresentative(t testing.TB, gvrs ...schema.GroupVersionResource) {
	t.Helper()
	t.Cleanup(cache.ResetLearnedIdentitiesForTest)
	exp := time.Now().Add(time.Hour)
	for _, gvr := range gvrs {
		for _, tg := range cache.EnumeratePrewarmTargetsForGVR(gvr, "list") {
			user := tg.Subject.Username
			if user == "" {
				user = "s841-member-of-" + strings.Join(tg.Subject.Groups, "+")
			}
			cache.ObserveLiveIdentity(user, tg.Subject.Groups, exp)
		}
	}
}
