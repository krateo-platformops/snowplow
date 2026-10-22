package dispatchers

// prewarm_real_classes_841_test.go — #841: seed only identities a real
// requester can hit; the drain yields between widgets; the seed-origin
// counters show seeded cells being hit.
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
//   S5 TestS841_WalkYieldsBeforeEveryWidget / S6 TestS841_DrainYieldsMidPage —
//      the pagination drain steps aside for a customer between the widgets of
//      ONE page, not only between pages.
//
// The harness is #262's (learned_identity_262_harness_test.go): REAL RBAC
// informer, Secrets informer → learned registry, enumeration wrapper, seed
// primitives and keys; resolver edges stubbed.

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

// ── S5 / S6: the drain yields between widgets ───────────────────────────────

// s841Widget is a minimal widget CR the walk harvests before it resolves.
func s841Widget(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Panel",
		"metadata":   map[string]any{"namespace": "krateo-system", "name": name},
		"spec":       map[string]any{},
	}}
}

var s841PanelGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}

func TestS841_WalkYieldsBeforeEveryWidget(t *testing.T) {
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()

	harv := newNavWidgetHarvester()
	w := newPhase1Walker(nil, "test-authn-ns", withNavWidgetHarvester(harv))
	done := markCustomerInFlight()
	yieldBefore := prewarmEngineSingleton().yieldTotal.Load()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	go func() {
		_ = w.walk(ctx, s841Widget("s841-w"), s841PanelGVR, 1, 1, 5, -1, -1)
		close(finished)
	}()
	time.Sleep(150 * time.Millisecond)
	if n := len(harv.snapshot()); n != 0 {
		done()
		t.Fatalf("S5 RED: the walk reached widget %d while a customer was in flight; it must yield before every widget", n)
	}
	if prewarmEngineSingleton().yieldTotal.Load() == yieldBefore {
		done()
		t.Fatal("S5 RED: the walk never parked on the engine yield")
	}
	done()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("the walk did not finish within 5s of the customer completing")
	}
	if n := len(harv.snapshot()); n != 1 {
		t.Fatalf("after the customer completed the walk harvested %d widget(s), want 1", n)
	}
}

func TestS841_DrainYieldsMidPage(t *testing.T) {
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()

	const children = 4
	// Page 2 reveals `children` child widgets and is the last page. The customer
	// arrives WHILE page 2 resolves — after the page-top yield, before any child.
	var release func()
	var arrived atomic.Bool
	installFakePaginationSeams(t, func(page int) (*unstructured.Unstructured, error) {
		env := fakePageEnvelope(page, false)
		items := make([]any, 0, children)
		for i := 0; i < children; i++ {
			items = append(items, map[string]any{
				"id":   "c" + itoaLine(i),
				"verb": "GET",
				"path": "/call?resource=panels&apiVersion=widgets.templates.krateo.io/v1beta1&namespace=krateo-system&name=s841-child-" + itoaLine(i),
			})
		}
		env.Object["status"].(map[string]any)["resourcesRefs"].(map[string]any)["items"] = items
		release = markCustomerInFlight()
		arrived.Store(true)
		return env, nil
	})
	var childFetches atomic.Int64
	prevChild := paginationFetchChildFn
	paginationFetchChildFn = func(_ context.Context, ref templatesv1.ObjectReference) objects.Result {
		childFetches.Add(1)
		return objects.Result{Unstructured: s841Widget(ref.Name), GVR: s841PanelGVR}
	}
	t.Cleanup(func() { paginationFetchChildFn = prevChild })

	harv := newNavWidgetHarvester()
	w := newPhase1Walker(nil, "test-authn-ns", withNavWidgetHarvester(harv))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	go func() {
		iterateApiRefPages(ctx, w, fakePage1Driven(), schema.GroupVersionResource{
			Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "datagrids"},
			fakePage1Driven(), 1, 5, 5, 5, "test-authn-ns")
		close(finished)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !arrived.Load() {
		if time.Now().After(deadline) {
			t.Fatal("the drain never resolved page 2")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Bounded: from the customer's arrival, no child widget is walked — at most
	// the one child CR already being fetched is read.
	time.Sleep(150 * time.Millisecond)
	if n := len(harv.snapshot()); n != 0 {
		release()
		t.Fatalf("S6 RED: %d child widget(s) of the page were walked while a customer was in flight; "+
			"the drain must yield between the widgets of one page", n)
	}
	if f := childFetches.Load(); f > 1 {
		release()
		t.Fatalf("S6 RED: %d child CRs fetched while a customer was in flight, want ≤1", f)
	}
	release()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("the drain did not finish within 5s of the customer completing")
	}
	if n := len(harv.snapshot()); n != children {
		t.Fatalf("after the customer completed the drain walked %d/%d children", n, children)
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
