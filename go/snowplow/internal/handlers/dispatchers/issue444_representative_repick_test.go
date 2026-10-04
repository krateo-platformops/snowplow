// issue444_representative_repick_test.go — #444: a representative that drifts out
// of its cell's RBAC class must not leave the cell un-refreshable until its TTL.
//
// Before #444 the refresher declined + SUPPRESSED such a cell ("TTL is the bound,
// #424 R1"): every remaining member kept hitting the pre-drift body after any dep
// change, for up to the TTL. #444: re-pick an in-class representative — the
// canonical group representative, else a recent hitter — and refresh under it;
// if none is in the class, EVICT (a cold, correct refill).
//
// REAL BOUNDARY: the cell is minted and hit by the real restActionHandler.ServeHTTP
// over a real ResourceWatcher + published RBAC snapshot (the #423 ps harness); the
// drift is a real RoleBinding CREATE; the dep change is a real informer UPDATE;
// the refresh is the real refresher loop → resolveAndPopulateL1 → the real
// restactions resolver (only the RA CR fetch is supplied — the ps harness serves
// the CR through fetchObjectFn, not the informer).
//
// ARMS (all RED on main 22bcbf0d+#446: the cell keeps the pre-update body)
//   TestIssue444_GroupRepReplacesDriftedRepresentative — group-only class.
//   TestIssue444_RecentHitterReplacesDriftedRepresentative — the class holds a
//       User-subject binding naming carol AND dave, so ("", groups) is not in it;
//       dave hit the cell, so he is the replacement. His username never reaches
//       a refresher log line.
//   TestIssue444_NoInClassRepresentative_Evicts — same class, dave never hit:
//       nobody verifiable is in the class → the cell is evicted, and dave's next
//       call resolves fresh.
// The raFullList carrier is TestIssue431_RepresentativeDriftStillDeclines.

package dispatchers

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

// i444CarolDave: one RoleBinding naming User:carol AND User:dave on target-reader
// in ns x — a class the canonical group representative is NOT a member of.
func i444CarolDave() runtime.Object {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: psTargetNS, Name: "carol-dave-target", UID: types.UID("uid-rb-444-cd")},
		Subjects: []rbacv1.Subject{
			{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psCarol},
			{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psDave},
		},
		RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "target-reader"},
	}
}

type i444Env struct {
	a        psArm
	dyn      *dynamicfake.FakeDynamicClient
	srvURL   string
	cr       *unstructured.Unstructured
	key      string
	stored   cache.ResolvedKeyInputs
	saEP     *endpoints.Endpoint
	saRC     *rest.Config
	carolCtx context.Context
	daveCtx  context.Context
}

func i444Setup(t *testing.T, extra ...runtime.Object) *i444Env {
	t.Helper()
	k423Env(t)
	a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a, extra...)
	srv := psFakeAPIServer(t, a, k423Counters())
	psSeedClientconfigs(t, srv.URL)
	e := &i444Env{a: a, dyn: dyn, srvURL: srv.URL, cr: k423ListCR(a),
		carolCtx: psUserCtx(a, psCarol), daveCtx: psUserCtx(a, psDave),
		saEP: &endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-sa"}, saRC: &rest.Config{Host: srv.URL}}
	cKey, _ := k423RAKey(t, e.carolCtx)
	if dKey, _ := k423RAKey(t, e.daveCtx); dKey != cKey {
		t.Fatalf("PRE: carol and dave must share one cell")
	}
	e.key = cKey
	if rec := psServeCR(t, e.cr, srv.URL, e.carolCtx); !psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("SETUP: carol's body must carry x/y; body=%s", psTrunc(rec.Body.String(), 300))
	}
	ent, ok := cache.ResolvedCache().Get(e.key)
	if !ok || ent.Inputs == nil || ent.Inputs.RepresentativeUsername != psCarol {
		t.Fatalf("SETUP: carol's body must be cached with carol as representative")
	}
	e.stored = *ent.Inputs
	return e
}

// loop starts the real refresher for the restactions class.
func (e *i444Env) loop(t *testing.T) {
	t.Helper()
	i187RefresherEnv(t)
	e.seam(t)
	refresh := func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		return resolveAndPopulateL1(ctx, in, e.saEP, e.saRC)
	}
	cache.RegisterRefreshFunc("restactions", refresh)
	ctx, cancel := context.WithCancel(context.Background())
	cache.StartRefresher(ctx)
	t.Cleanup(func() { cancel(); cache.ResetRefresherForTest() })
}

// seam supplies the RA CR to the refresher's real restactions re-resolve.
func (e *i444Env) seam(t *testing.T) {
	t.Helper()
	restore := setResolveOnceForTest(func(ctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		ctx = cache.WithBackgroundResolve(ctx)
		return resolveRestActionForRefresh(ctx, objects.Result{GVR: h1RAGVR, Unstructured: e.cr.DeepCopy()}, in, psAuthnNS)
	})
	t.Cleanup(restore)
}

// drift gives carol a binding of her own: her binding set (and sub-gen) move,
// dave's do not — he still derives the cell's key.
func (e *i444Env) drift(t *testing.T) {
	t.Helper()
	// A User RoleBinding of carol's own (in ns z: it changes no data she reads,
	// only her binding set). Waited on her KEY moving — her `get` on the target is
	// already allowed through the group, so a permission poll would not wait.
	rb := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: psOtherNS, Name: "carol-444-own", UID: types.UID("uid-rb-444-carol")},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psCarol}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "pod-reader"},
	}
	m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
	if _, err := e.dyn.Resource(k424RBGVR).Namespace(psOtherNS).Create(context.Background(),
		&unstructured.Unstructured{Object: m}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("drift grant: %v", err)
	}
	if !i444Wait(5*time.Second, func() bool { ck, _ := k423RAKey(t, e.carolCtx); return ck != e.key }) {
		t.Fatalf("PRE: carol's own key must have moved")
	}
	if dk, _ := k423RAKey(t, e.daveCtx); dk != e.key {
		t.Fatalf("PRE: dave must still derive the cell's key")
	}
	if d := identityClassDrift(context.Background(), &e.stored, psCarol, psGroups(e.a)); d == "" {
		t.Fatalf("PRE: carol (the representative) must have drifted out of the cell's class")
	}
}

func (e *i444Env) body() string {
	if ent, ok := cache.ResolvedCache().Get(e.key); ok {
		return string(ent.RawJSON)
	}
	return "<absent>"
}

func i444Wait(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

const i444New = "after-444-update"

func TestIssue444_GroupRepReplacesDriftedRepresentative(t *testing.T) {
	a := psArm{target: psConfigmapsGVR}
	e := i444Setup(t, k423PortalReaders(a)...)
	e.drift(t)
	before := representativeRepickForTest(repSourceGroup)
	e.loop(t)
	i431UpdateCM(t, e.dyn, i444New)
	if !i444Wait(10*time.Second, func() bool { return strings.Contains(e.body(), i444New) }) {
		t.Fatalf("#444 STALE: the representative drifted and the dep change was never refreshed for the remaining "+
			"members; body=%s", psTrunc(e.body(), 300))
	}
	if representativeRepickForTest(repSourceGroup) <= before {
		t.Fatalf("the replacement must have been the canonical group representative")
	}
	ent, _ := cache.ResolvedCache().Get(e.key)
	if ent.Inputs.RepresentativeUsername != "" {
		t.Fatalf("the re-Put must carry the new (group) representative, got a username")
	}
	if rec := psServeCR(t, e.cr, e.srvURL, e.daveCtx); !strings.Contains(rec.Body.String(), i444New) {
		t.Fatalf("dave must be served the refreshed body; got %s", psTrunc(rec.Body.String(), 300))
	}
}

func TestIssue444_RecentHitterReplacesDriftedRepresentative(t *testing.T) {
	a := psArm{target: psConfigmapsGVR}
	e := i444Setup(t, append(k423PortalReaders(a), i444CarolDave())...)
	// The canonical group representative is NOT in this class (it lacks the
	// carol+dave binding), so only a hitter can stand in.
	g := e.stored
	if d := identityClassDrift(context.Background(), &g, "", append(psGroups(e.a), "system:authenticated")); d == "" {
		t.Fatalf("PRE: the group representative must be outside this class, else the arm cannot reach the hitter path")
	}
	if rec := psServeCR(t, e.cr, e.srvURL, e.daveCtx); !psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("SETUP: dave's hit must serve the cell")
	}
	e.drift(t)

	before := representativeRepickForTest(repSourceHitter)
	e.loop(t)
	i431UpdateCM(t, e.dyn, i444New)
	if !i444Wait(10*time.Second, func() bool { return strings.Contains(e.body(), i444New) }) {
		t.Fatalf("#444 STALE: no in-class replacement was used; body=%s", psTrunc(e.body(), 300))
	}
	if representativeRepickForTest(repSourceHitter) <= before {
		t.Fatalf("the replacement must have been a recent hitter")
	}
	ent, _ := cache.ResolvedCache().Get(e.key)
	if ent.Inputs.RepresentativeUsername != psDave {
		t.Fatalf("the re-Put must carry dave (the in-class hitter) as representative")
	}
}

// TestIssue444_PromotedHitterNeverLogged — the refresh that promotes a recent
// hitter logs through the refresher's ctx logger (debug: the "re-resolved +
// stored" line carries the representative); the hitter's username must appear
// only as its sha256 label (#444, #262 redaction rules). Driven directly (no
// loop) so every line of the promoting refresh lands in the capture.
//
// Mutation: dropping the redaction at promotion makes this RED (the stored line
// names dave).
func TestIssue444_PromotedHitterNeverLogged(t *testing.T) {
	a := psArm{target: psConfigmapsGVR}
	e := i444Setup(t, append(k423PortalReaders(a), i444CarolDave())...)
	if rec := psServeCR(t, e.cr, e.srvURL, e.daveCtx); !psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("SETUP: dave's hit must serve the cell")
	}
	e.drift(t)
	e.seam(t)

	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(logger))
	before := representativeRepickForTest(repSourceHitter)
	if err := resolveAndPopulateL1(ctx, e.stored, e.saEP, e.saRC); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if representativeRepickForTest(repSourceHitter) <= before {
		t.Fatalf("PRE: the refresh must have promoted the recent hitter")
	}
	out := logBuf.String()
	if !strings.Contains(out, "re-resolved + stored") {
		t.Fatalf("PRE: the promoting refresh's stored line must be captured, else the arm cannot fail; log=%s", out)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, psDave) {
			t.Fatalf("PRIVACY: a promoted recent hitter's username reached a log line: %s", line)
		}
	}
}

func TestIssue444_NoInClassRepresentative_Evicts(t *testing.T) {
	a := psArm{target: psConfigmapsGVR}
	e := i444Setup(t, append(k423PortalReaders(a), i444CarolDave())...)
	e.drift(t) // dave never hit: no verifiable in-class identity remains
	before := representativeRepickForTest("evicted")
	evict0 := cache.ResolvedCache().Stats().EvictNoRepresentativeTotal
	e.loop(t)
	i431UpdateCM(t, e.dyn, i444New)
	if !i444Wait(10*time.Second, func() bool { return e.body() == "<absent>" }) {
		t.Fatalf("#444: a cell with no in-class representative must be EVICTED (not kept stale until TTL); body=%s",
			psTrunc(e.body(), 300))
	}
	if representativeRepickForTest("evicted") <= before ||
		cache.ResolvedCache().Stats().EvictNoRepresentativeTotal <= evict0 {
		t.Fatalf("the eviction must be the #444 no-representative eviction")
	}
	// dave's next call is a MISS: a fresh fill under his own identity, which
	// makes him the cell's (in-class) representative from then on. (The body's
	// freshness on that path is the ordinary cold-fill's — not under test here.)
	miss0 := cache.ResolvedCache().Stats().MissTotal
	psServeCR(t, e.cr, e.srvURL, e.daveCtx)
	if cache.ResolvedCache().Stats().MissTotal <= miss0 {
		t.Fatalf("dave's next call must miss the evicted cell")
	}
	ent, ok := cache.ResolvedCache().Get(e.key)
	if !ok || ent.Inputs == nil || ent.Inputs.RepresentativeUsername != psDave {
		t.Fatalf("the refill must be dave's own, with dave as the cell's representative")
	}
}

// TestIssue444_LearnedRepresentative_GroupRepickStaysRedacted — #441 x #444. The
// drifted representative is a LEARNED identity (#262); the canonical group
// representative that replaces it carries that class's group set, which #262
// treats as identity facts. Every line of the re-picking refresh must carry the
// class only as its sha256 label: neither the learned username nor its group.
func TestIssue444_LearnedRepresentative_GroupRepickStaysRedacted(t *testing.T) {
	a := psArm{target: psConfigmapsGVR}
	cache.ResetLearnedIdentitiesForTest()
	t.Cleanup(cache.ResetLearnedIdentitiesForTest)
	e := i444Setup(t, k423PortalReaders(a)...) // sets the cache-on env first
	cache.ObserveLiveIdentity(psCarol, []string{psGroup}, time.Now().Add(time.Hour))
	if !isLearnedIdentity(psCarol) {
		t.Fatalf("PRE: carol must be a learned identity")
	}
	e.drift(t)
	e.seam(t)

	var logBuf syncBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(logger))
	before := representativeRepickForTest(repSourceGroup)
	if err := resolveAndPopulateL1(ctx, e.stored, e.saEP, e.saRC); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if representativeRepickForTest(repSourceGroup) <= before {
		t.Fatalf("PRE: the group representative must have replaced the learned one")
	}
	out := logBuf.String()
	if !strings.Contains(out, "re-resolved + stored") {
		t.Fatalf("PRE: the re-picking refresh's stored line must be captured; log=%s", out)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, psCarol) || strings.Contains(line, psGroup) {
			t.Fatalf("PRIVACY (#262): a learned class's username or group reached a log line of the re-picking refresh: %s", line)
		}
	}
}

// TestIssue444_WidgetsCarrier_GroupRepReplacesDriftedRepresentative — the
// widgets class goes through the same refresher re-pick (arm per carrier). The
// resolver seam records the identity the refresh ran under; the boundary under
// test is the representative selection + the re-Put, not the widget resolver.
func TestIssue444_WidgetsCarrier_GroupRepReplacesDriftedRepresentative(t *testing.T) {
	a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a, append(k423WidgetExtras(a), k423PortalReaders(a)...)...)
	carolCtx := psUserCtx(a, psCarol)
	cr := h1WidgetUnstructured(map[string]any{})
	wKey := func(ctx context.Context) (string, *cache.ResolvedKeyInputs) {
		k, _, in := dispatchCacheLookupKey(ctx, "widgets", h1WidgetGVR.Group, h1WidgetGVR.Version,
			h1WidgetGVR.Resource, h1NS, h1WName, -1, -1, effectiveKeyExtras(ctx, cr.Object, nil))
		if in == nil || in.BindingUID == "" {
			t.Fatalf("PRE: widgets key needs a BindingUID")
		}
		return k, in
	}
	key, in := wKey(carolCtx)
	if dk, _ := wKey(psUserCtx(a, psDave)); dk != key {
		t.Fatalf("PRE: carol and dave must share one widgets cell")
	}
	cache.ResolvedCache().Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"w":"pre-drift"}`), Inputs: in})
	stored := *in

	rb := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: psOtherNS, Name: "carol-444-w", UID: types.UID("uid-rb-444-carol-w")},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psCarol}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "pod-reader"},
	}
	m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
	if _, err := dyn.Resource(k424RBGVR).Namespace(psOtherNS).Create(context.Background(),
		&unstructured.Unstructured{Object: m}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("drift grant: %v", err)
	}
	if !i444Wait(5*time.Second, func() bool { k, _ := wKey(carolCtx); return k != key }) {
		t.Fatalf("PRE: carol's widgets key must move")
	}
	if d := identityClassDrift(context.Background(), &stored, psCarol, psGroups(a)); d == "" {
		t.Fatalf("PRE: carol must have drifted out of the widgets cell's class")
	}

	var ranAs []string
	restore := setResolveOnceForTest(func(ctx context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		ui, err := xcontext.UserInfo(ctx)
		if err != nil {
			t.Fatalf("seam: no identity")
		}
		ranAs = append([]string{ui.Username}, ui.Groups...)
		return []byte(`{"w":"refreshed"}`), nil
	})
	defer restore()
	if err := resolveAndPopulateL1(context.Background(), stored, nil, nil); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if len(ranAs) == 0 || ranAs[0] != "" {
		t.Fatalf("#444: the widgets refresh must run under the canonical group representative, ran as %q", ranAs)
	}
	ent, ok := cache.ResolvedCache().Get(key)
	if !ok || string(ent.RawJSON) != `{"w":"refreshed"}` {
		t.Fatalf("#444 STALE: the widgets cell was not refreshed after its representative drifted")
	}
	if ent.Inputs.RepresentativeUsername != "" {
		t.Fatalf("the re-Put must carry the group representative")
	}
}

// syncBuffer is a goroutine-safe bytes.Buffer for capturing slog output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
