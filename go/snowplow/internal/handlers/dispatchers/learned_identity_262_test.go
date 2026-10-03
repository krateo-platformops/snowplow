package dispatchers

// learned_identity_262_test.go — #262 falsifiers F1–F5 (F6 privacy lives in
// learned_identity_262_privacy_test.go; the #436 arm in
// rotated_effective_groups_436_test.go). F7 (Chrome across a roll) is the
// post-deploy acceptance and is not here.
//
//   F1 TestF1_262_LearnedClassWarmAfterRestart — a user with an extra User
//      binding, known ONLY from their clientconfig Secret (a fresh process: no
//      traffic), finds EVERY nav widget and RESTAction warm after the boot seed.
//   F2 TestF2_262_RotationOnPreviouslyUnboundGroup — the user presents a group
//      nothing binds (so they share the group representative's cells); a binding
//      then lands on that group through the real informer → flush → hook path.
//      Their key moves away from the representative's; the #258 reseed must warm
//      the NEW key.
//   F3 TestF3_262_LoginSecretUpdateSeedsNewClass — a re-login with a new group
//      set (clientconfig Secret UPDATE) through the real informer → registry →
//      hook → engine worker → class-seed scope; the new key is warm, latency
//      measured.
//   F4 TestF4_262_BoundHolds_EngineBindsFirst / ..._MemoryBindsFirst — the
//      self-adapting bound: newest-first prefix, the unseeded counter, the
//      engine capacity identity, and a keepwarm cycle inside the interval; and
//      the memory side binding first.
//   F5 TestF5_262_Precision_SameGroupUsersAddNoTargets — 1000 same-group users
//      add ZERO learned targets (lossless dedup); the one user with an extra
//      binding among them adds exactly one class.

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// ── F1 ───────────────────────────────────────────────────────────────────────

func TestF1_262_LearnedClassWarmAfterRestart(t *testing.T) {
	now := time.Now()
	env := l262Setup(t, l262Opts{
		extraUsers: []string{"dave"},
		secrets: []*corev1.Secret{
			l262ClientconfigSecret(t, "dave-clientconfig", "dave", []string{l262Group}, now.Add(-time.Hour), "1"),
			l262ClientconfigSecret(t, "erin-clientconfig", "erin", []string{l262Group}, now.Add(-2*time.Hour), "1"),
			// not a login artifact: ignored
			{ObjectMeta: metav1.ObjectMeta{Name: "authn-jwt-signing-key", Namespace: l262AuthnNS}, Data: map[string][]byte{"private.pem": []byte("x")}},
		},
	})
	l262Stubs(t, l262StubOpts{})
	l262WaitLearned(t, 2)

	daveCtx := l262CustomerCtx("dave", []string{l262Group})
	erinCtx := l262CustomerCtx("erin", []string{l262Group})
	daveKeys, _ := l262Keys(t, env, daveCtx)
	erinKeys, _ := l262Keys(t, env, erinCtx)
	for i := range daveKeys {
		if daveKeys[i] == erinKeys[i] {
			t.Fatalf("PRECONDITION: dave's extra binding must give him a key no group member derives (unit %d)", i)
		}
	}
	if w, _ := l262Warm(t, env, daveCtx); w != 0 {
		t.Fatalf("PRECONDITION: a fresh process holds no warm cell for dave, got %d", w)
	}

	l262Boot(t, env, seedModeBoot)

	warm, total := l262Warm(t, env, daveCtx)
	if total != len(env.widgets)+len(env.ras) || total < 5 {
		t.Fatalf("fixture: %d units, want every widget and RESTAction", total)
	}
	if warm != total {
		t.Fatalf("F1 RED: dave (extra User binding, known only from his clientconfig Secret) finds %d/%d "+
			"nav units warm after the boot seed; every widget and RESTAction must be warm", warm, total)
	}
	// The group-only member is served by the representative's cells (no learned
	// seed needed, none spent).
	if w, n := l262Warm(t, env, erinCtx); w != n {
		t.Fatalf("erin (group-only) finds %d/%d warm", w, n)
	}
	if r, s, u := cache.LearnedClassCounts(); r != 2 || s != 2 || u != 0 {
		t.Fatalf("counters: registered=%d from_secrets=%d unparseable=%d, want 2/2/0", r, s, u)
	}
	if seeded, unseeded := cache.LearnedAdmissionStats(); seeded != 1 || unseeded != 0 {
		t.Fatalf("admission: seeded=%d unseeded_capacity=%d, want 1/0 (erin is not distinct)", seeded, unseeded)
	}
}

// ── F2 ───────────────────────────────────────────────────────────────────────

func TestF2_262_RotationOnPreviouslyUnboundGroup(t *testing.T) {
	registerS258WideningHook(t)
	s258ResetSink()
	const auditors = "l262-auditors" // presented by frank, bound by nothing (yet)
	env := l262Setup(t, l262Opts{
		secrets: []*corev1.Secret{
			l262ClientconfigSecret(t, "frank-clientconfig", "frank", []string{l262Group, auditors}, time.Now().Add(-time.Hour), "1"),
		},
	})
	l262Stubs(t, l262StubOpts{})
	l262WaitLearned(t, 1)

	frankCtx := l262CustomerCtx("frank", []string{l262Group, auditors})
	repCtx := l262CustomerCtx("", []string{l262Group})
	before, _ := l262Keys(t, env, frankCtx)
	repKeys, _ := l262Keys(t, env, repCtx)
	for i := range before {
		if before[i] != repKeys[i] {
			t.Fatalf("PRECONDITION: with %s unbound frank shares the group representative's key (unit %d)", auditors, i)
		}
	}
	if n := len(l262LearnedTargets(h1WidgetGVR)); n != 0 {
		t.Fatalf("PRECONDITION: frank is not DISTINCT before the grant; the wrapper emitted %d learned target(s)", n)
	}
	l262Boot(t, env, seedModeBoot)
	if w, n := l262Warm(t, env, frankCtx); w != n {
		t.Fatalf("PRECONDITION: frank is warm through the representative before the grant (%d/%d)", w, n)
	}

	// THE ROTATION: a binding on the previously unbound group, through the real
	// informer → bindings delta → pending bump → publish → flush → hook.
	rb := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: l262XNS, Name: "l262-auditors", UID: types.UID("uid-l262-auditors")},
		Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: auditors}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "l262-extra"},
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.dyn.Resource(l262RBGVR).Namespace(l262XNS).Create(context.Background(),
		&unstructured.Unstructured{Object: obj}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var rotated cache.RotatedSubjectSet
	deadline := time.Now().Add(10 * time.Second)
	for {
		if rs, ok := s258RotatedForGroup(auditors); ok {
			rotated = rs
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the binding ADD never produced an RBAC-shift flush naming the group")
		}
		cache.RebuildRBACSnapshotForTest(env.rw)
		time.Sleep(20 * time.Millisecond)
	}
	after, _ := l262Keys(t, env, frankCtx)
	for i := range after {
		if after[i] == before[i] {
			t.Fatalf("PRECONDITION: the grant must move frank's key (unit %d)", i)
		}
	}
	if w, _ := l262Warm(t, env, frankCtx); w != 0 {
		t.Fatalf("PRECONDITION: frank's post-grant keys are cold before the reseed, got %d warm", w)
	}

	// The REAL #258 handler with the REAL rotated set.
	if err := rePrewarmRBACShift(context.Background(), env.deps, rotated); err != nil {
		t.Fatalf("rePrewarmRBACShift: %v", err)
	}
	if w, n := l262Warm(t, env, frankCtx); w != n {
		t.Fatalf("F2 RED: after a binding landed on frank's previously unbound group, %d/%d of his NEW "+
			"keys are warm; the rotation reseed must warm every one", w, n)
	}
}

// s258RotatedForGroup returns the first captured set in which a member of
// group rotated.
func s258RotatedForGroup(group string) (cache.RotatedSubjectSet, bool) {
	s258WideningSink.mu.Lock()
	defer s258WideningSink.mu.Unlock()
	for _, rs := range s258WideningSink.sets {
		if rs.Rotated("someone", []string{group}) && !rs.Rotated("someone", nil) {
			return rs, true
		}
	}
	return cache.RotatedSubjectSet{}, false
}

// ── F3 ───────────────────────────────────────────────────────────────────────

func TestF3_262_LoginSecretUpdateSeedsNewClass(t *testing.T) {
	const ops = "l262-ops"
	env := l262Setup(t, l262Opts{
		groupCohorts: []string{ops},
		secrets: []*corev1.Secret{
			l262ClientconfigSecret(t, "gina-clientconfig", "gina", []string{l262Group}, time.Now().Add(-time.Hour), "1"),
		},
	})
	l262Stubs(t, l262StubOpts{})
	l262WaitLearned(t, 1)
	l262Boot(t, env, seedModeBoot) // the boot pass; gina (group-only) is not distinct
	_ = cache.DrainPendingLearnedClasses()

	// The engine worker, with the production scope handler and hook.
	e := newTestEngine()
	e.scopeHandler = makeBootScopeHandler(env.deps)
	registerEngineLearnedClassHook(e)
	wctx, wcancel := context.WithCancel(context.Background())
	t.Cleanup(wcancel)
	go e.runWorker(wctx)

	newCtx := l262CustomerCtx("gina", []string{l262Group, ops})
	if w, _ := l262Warm(t, env, newCtx); w != 0 {
		t.Fatalf("PRECONDITION: gina's post-login keys are cold, got %d warm", w)
	}

	// THE LOGIN: authn re-issues gina's clientconfig with her new group set.
	upd := l262ClientconfigSecret(t, "gina-clientconfig", "gina", []string{l262Group, ops}, time.Now(), "2")
	start := time.Now()
	if _, err := env.kube.CoreV1().Secrets(l262AuthnNS).Update(context.Background(), upd, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := start.Add(10 * time.Second)
	var latency time.Duration
	for {
		if w, n := l262Warm(t, env, newCtx); w == n {
			latency = time.Since(start)
			break
		}
		if time.Now().After(deadline) {
			w, n := l262Warm(t, env, newCtx)
			t.Fatalf("F3 RED: a re-login with a new group set (clientconfig Secret UPDATE) left %d/%d of the "+
				"new class's units cold after 10s", n-w, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("F3 seed latency (Secret UPDATE → every unit of the new class warm): %v", latency)
	if r, _, _ := cache.LearnedClassCounts(); r != 1 {
		t.Fatalf("the re-login REPLACES gina's class: registered=%d, want 1", r)
	}
}

// ── F4 ───────────────────────────────────────────────────────────────────────

// l262ManyUsers returns n users with an extra binding each (n distinct classes)
// and their Secrets, NotBefore increasing with the index (user n-1 newest).
func l262ManyUsers(t *testing.T, n int) ([]string, []*corev1.Secret) {
	users := make([]string, n)
	secrets := make([]*corev1.Secret, n)
	base := time.Now().Add(-time.Duration(n+1) * time.Minute)
	for i := 0; i < n; i++ {
		users[i] = "u" + strconv.Itoa(i)
		secrets[i] = l262ClientconfigSecret(t, users[i]+"-clientconfig", users[i], []string{l262Group},
			base.Add(time.Duration(i)*time.Minute), "1")
	}
	return users, secrets
}

func TestF4_262_BoundHolds_EngineBindsFirst(t *testing.T) {
	// keepwarm interval = TTL×3/4 = 750ms; every resolve costs ~40ms, so the
	// interval affords ~18 units: 4 base + some, never all 5 learned classes.
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "1")
	users, secrets := l262ManyUsers(t, 5)
	env := l262Setup(t, l262Opts{extraUsers: users, secrets: secrets, widgets: 2, ras: 2})
	l262Stubs(t, l262StubOpts{sleep: 40 * time.Millisecond})
	l262WaitLearned(t, 5)

	l262Boot(t, env, seedModeBoot)

	seeded, unseeded := cache.LearnedAdmissionStats()
	capm := l262CapacityMap(t)
	us := func(k string) time.Duration { return time.Duration(capm[k].(int64)) * time.Microsecond }
	tW, tR := us("t_widget_us"), us("t_ra_us")
	interval := keepwarmSweepInterval()
	baseW, baseR := capm["base_widget_units"].(int), capm["base_ra_units"].(int)
	budget, used := us("budget_us"), us("admitted_cost_us")
	t.Logf("F4 engine bound: t_widget=%v t_ra=%v interval=%v base_units=%d+%d budget=%v admitted_cost=%v seeded=%d unseeded_capacity=%d bound=%v",
		tW, tR, interval, baseW, baseR, budget, used, seeded, unseeded, capm["bound"])

	if !capm["t_widget_measured"].(bool) || !capm["t_ra_measured"].(bool) || tW < 40*time.Millisecond || tR < 40*time.Millisecond {
		t.Fatalf("both per-kind costs must be MEASURED from the seeded units (each ≥40ms): widget %v/%v ra %v/%v",
			capm["t_widget_measured"], tW, capm["t_ra_measured"], tR)
	}
	want := interval - time.Duration(baseW)*tW - time.Duration(baseR)*tR
	if d := budget - want; d < -time.Millisecond || d > time.Millisecond {
		t.Fatalf("budget identity: budget=%v, want interval − W_base·t_widget − R_base·t_ra = %v", budget, want)
	}
	if seeded < 1 || unseeded < 1 || seeded+unseeded != 5 {
		t.Errorf("F4 RED: the bound must admit some and leave some out: seeded=%d unseeded_capacity=%d (5 distinct)", seeded, unseeded)
	}
	if capm["bound"] != "engine" {
		t.Errorf("bound=%v, want engine", capm["bound"])
	}
	classCost := 2*tW + 2*tR // 2 widgets + 2 RESTActions per class
	if used > budget || used+classCost <= budget {
		t.Errorf("the admitted prefix must be the LONGEST that fits: admitted_cost=%v budget=%v (one class costs %v)", used, budget, classCost)
	}
	// NEWEST FIRST: the admitted classes are exactly the `seeded` newest.
	for i, u := range users {
		w, n := l262Warm(t, env, l262CustomerCtx(u, []string{l262Group}))
		newest := i >= len(users)-seeded
		if newest && w != n {
			t.Errorf("F4 RED: %s is among the %d newest classes but only %d/%d warm", u, seeded, w, n)
		}
		if !newest && w != 0 {
			t.Errorf("F4 RED: %s is older than every admitted class yet %d units are warm (not newest-first)", u, w)
		}
	}

	// THE CYCLE FITS THE INTERVAL: once every cell is past the keepwarm
	// age-skip threshold, one keepwarm pass re-resolves the base cohort and the
	// admitted classes — inside one sweep interval.
	time.Sleep(keepwarmAgeSkipThreshold() + 50*time.Millisecond)
	start := time.Now()
	l262Boot(t, env, seedModeKeepwarm)
	cycle := time.Since(start)
	t.Logf("F4 keepwarm cycle: %v (interval %v)", cycle, interval)
	if cycle > interval {
		t.Fatalf("F4 RED: a keepwarm cycle over the admitted set took %v, longer than the %v sweep interval", cycle, interval)
	}
}

func TestF4_262_BoundHolds_MemoryBindsFirst(t *testing.T) {
	users, secrets := l262ManyUsers(t, 5)
	env := l262Setup(t, l262Opts{extraUsers: users, secrets: secrets, widgets: 2, ras: 2})
	l262Stubs(t, l262StubOpts{})
	l262WaitLearned(t, 5)

	// The adaptive admission ceiling (GOMEMLIMIT-relative) leaves room for
	// exactly two classes' cells at the store's measured mean entry size, read
	// at the instant the bound is applied. The engine side is ample (TTL 3600s).
	const unitsPerClass = 4
	const limit = int64(1) << 40
	restore := cache.SetAdmissionRuntimeSeamsForTest(
		func() int64 { return limit },
		func() int64 {
			store := cache.ResolvedCache()
			avg := int64(1)
			if n := int64(store.Len()); n > 0 {
				avg = store.Bytes() / n
			}
			target := 2*unitsPerClass*avg + avg/2
			return limit - limit/8 - target
		})
	t.Cleanup(restore)

	l262Boot(t, env, seedModeBoot)

	capm := l262CapacityMap(t)
	seeded, unseeded := cache.LearnedAdmissionStats()
	t.Logf("F4 memory bound: memory_headroom_bytes=%v avg_entry_bytes=%v budget_us=%v seeded=%d unseeded_capacity=%d bound=%v",
		capm["memory_headroom_bytes"], capm["avg_entry_bytes"], capm["budget_us"], seeded, unseeded, capm["bound"])
	if capm["bound"] != "memory" || seeded != 2 || unseeded != 3 {
		t.Fatalf("F4 RED (memory): bound=%v seeded=%d unseeded=%d, want memory / 2 / 3", capm["bound"], seeded, unseeded)
	}
	for i, u := range users {
		w, n := l262Warm(t, env, l262CustomerCtx(u, []string{l262Group}))
		if newest := i >= len(users)-2; newest != (w == n && n > 0) || (!newest && w != 0) {
			t.Fatalf("memory bound must admit the 2 NEWEST classes: %s warm %d/%d", u, w, n)
		}
	}
}

func l262CapacityMap(t *testing.T) map[string]any {
	t.Helper()
	m := cache.LearnedAdmissionCapacity()
	if m == nil {
		t.Fatal("no learned admission was recorded")
	}
	return m
}

// ── F5 ───────────────────────────────────────────────────────────────────────

func TestF5_262_Precision_SameGroupUsersAddNoTargets(t *testing.T) {
	const n = 1000
	secrets := make([]*corev1.Secret, 0, n+1)
	base := time.Now().Add(-48 * time.Hour)
	for i := 0; i < n; i++ {
		u := "member" + strconv.Itoa(i)
		secrets = append(secrets, l262ClientconfigSecret(t, u+"-clientconfig", u, []string{l262Group},
			base.Add(time.Duration(i)*time.Second), "1"))
	}
	// The control: ONE user with an extra binding among them.
	secrets = append(secrets, l262ClientconfigSecret(t, "hana-clientconfig", "hana", []string{l262Group}, base, "1"))
	env := l262Setup(t, l262Opts{extraUsers: []string{"hana"}, secrets: secrets})
	l262Stubs(t, l262StubOpts{})
	l262WaitLearned(t, n+1)

	for _, gvr := range []schema.GroupVersionResource{h1WidgetGVR, l262TargetGVR} {
		baseN := len(enumerateBasePrewarmTargetsFn(gvr, "list"))
		start := time.Now()
		all := enumeratePrewarmTargetsForGVRFn(gvr, "list")
		cost := time.Since(start)
		learned := l262LearnedTargets(gvr)
		t.Logf("F5 %s: base targets=%d learned targets=%d (registry %d classes) wrapper cost=%v",
			gvr.Resource, baseN, len(all)-baseN, n+1, cost)
		if len(learned) != 1 || learned["hana"] != 1 {
			t.Fatalf("F5 RED: %d same-group users + 1 user with an extra binding must add EXACTLY one learned "+
				"target (hana) for %s; got %v", n, gvr.Resource, learned)
		}
	}
	l262Boot(t, env, seedModeBoot)
	if seeded, unseeded := cache.LearnedAdmissionStats(); seeded != 1 || unseeded != 0 {
		t.Fatalf("F5: seeded=%d unseeded_capacity=%d, want 1/0", seeded, unseeded)
	}
	if r, s, _ := cache.LearnedClassCounts(); r != n+1 || s != n+1 {
		t.Fatalf("F5: registered=%d from_secrets=%d, want %d", r, s, n+1)
	}
	if w, k := l262Warm(t, env, l262CustomerCtx("hana", []string{l262Group})); w != k {
		t.Fatalf("F5 control: hana warm %d/%d", w, k)
	}
	if w, k := l262Warm(t, env, l262CustomerCtx("member7", []string{l262Group})); w != k {
		t.Fatalf("F5: a same-group member is served by the representative: warm %d/%d", w, k)
	}
}
