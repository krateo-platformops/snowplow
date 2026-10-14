package dispatchers

// issue643_prb2_walk_test.go — #643 PR-B2: the discovery walk stops re-dialling
// external widgets whose last walk resolve touched an external endpoint.
//
// Driven on the shared #643 harness with a REAL navigation tree: the lister
// hands the engine a root widget, rePrewarmBootScoped walks it through the REAL
// phase1Walker.walk → widgets.Resolve (apiRef → restactions.Resolve → the
// counting external endpoint; resourcesRefs → the child /call paths; the
// CRD-status validation against the endpoint's discovery + CRD answers), and
// descends into informer-served children. The arms here use ONLY APIs that
// exist on main, so each is run RED-first there; the arms that need the new
// counters / seams live in issue643_prb2_walk_seams_test.go.
//
// Arms (architect brief comment 6156637571 + rulings on #643):
//   W1      K=3 cohorts × M=2 external widgets reachable from the nav root +
//           N internal; boot, 2 gvr-discovered, 1 fresh boot: every pass after
//           the first walk dials 0 (walk AND seed), internal cells stay warm.
//   W3a/b/c retire: widget resourcesRefs edited → resolved, new child walked;
//           RA edited to internal → Put; RA deleted → walk mark dropped.
//   Cb      ruling (b): the pre-engine phase1 walk marks, the engine's boot
//           re-walk dials 0.
//   Cc-i    ruling (c): an errored external LEAF is marked; next walk dials 0.
//   Cc-ii   ruling (c): an errored external widget WITH resourcesRefs is not
//           marked, is re-dialled, and its child is discovered once the fetch
//           succeeds.
//
// Not t.Parallel(): reassigns package vars.

import (
	"context"
	"encoding/base64"
	"sync/atomic"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/redact"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// b2Ref is a static GET resourcesRefs item pointing at the panel `name`.
func b2Ref(name string) map[string]any {
	return map[string]any{
		"id": "ref-" + name, "apiVersion": h643WidgetGVR.Group + "/" + h643WidgetGVR.Version,
		"resource": h643WidgetGVR.Resource, "name": name, "namespace": h643NS, "verb": "GET",
	}
}

// b2Panel is a walkable panel: apiRef → ra, static children refs, and an
// optional child produced by spec.resourcesRefsTemplate.
func b2Panel(name, ra string, refs []string, tmplChild string) *unstructured.Unstructured {
	w := h643Widget(name, ra)
	spec := w.Object["spec"].(map[string]any)
	if len(refs) > 0 {
		items := make([]any, 0, len(refs))
		for _, r := range refs {
			items = append(items, b2Ref(r))
		}
		spec["resourcesRefs"] = map[string]any{"items": items}
	}
	if tmplChild != "" {
		spec["resourcesRefsTemplate"] = []any{map[string]any{"template": b2Ref(tmplChild)}}
	}
	return w
}

func b2ExtRA(name string) *unstructured.Unstructured {
	return h643RA(name, h643ExtSecret, "/ext/fast/"+name+"/s0", "/ext/fast/"+name+"/s1")
}

func b2IntRA(name string) *unstructured.Unstructured { return h643RA(name, "") }

// b2Tree collects a fixture's objects.
type b2Tree struct{ objs []*unstructured.Unstructured }

func (b *b2Tree) panel(name, ra string, external bool, refs []string, tmplChild string) *b2Tree {
	if external {
		b.objs = append(b.objs, b2ExtRA(ra))
	} else {
		b.objs = append(b.objs, b2IntRA(ra))
	}
	b.objs = append(b.objs, b2Panel(name, ra, refs, tmplChild))
	return b
}

// b2Setup builds the harness over tree and points the engine's lister at the
// root panel (a FRESH read per pass, as listNavigationRootsFromConfigMap does).
func b2Setup(t *testing.T, cohorts int, root string, tree *b2Tree) *h643Env {
	t.Helper()
	// The walk runs under the snowplow SA identity (withPhase1SAContext reads it
	// from the SA token's sub); the SA is bound to the same reader role, as the
	// real SA is bound */* on a cluster.
	sa := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "b2-sa-reader"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "h643-reader"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: b2SAName, Namespace: h643AuthnNS}},
	}
	env := h643Setup(t, h643Opts{cohorts: cohorts, objs: tree.objs, typed: []runtime.Object{sa}, walk: true, things: 2})
	env.deps.saEP.Token = b2SAToken()
	env.deps.lister = func(ctx context.Context) ([]navigationRoot, error) {
		u, err := env.dyn.Resource(h643WidgetGVR).Namespace(h643NS).Get(ctx, root, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return []navigationRoot{{Root: u, GVR: h643WidgetGVR}}, nil
	}
	env.engine.scopeHandler = makeBootScopeHandler(env.deps)
	b2ResetWalkChildren(t)
	h643Budget(t, nil)
	return env
}

const b2SAName = "b2-snowplow"

// b2SAToken is an unsigned SA JWT whose sub is the walk SA (only sub is read).
func b2SAToken() string {
	enc := base64.RawURLEncoding
	hdr := enc.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	body := enc.EncodeToString([]byte(`{"sub":"system:serviceaccount:` + h643AuthnNS + `:` + b2SAName + `","iss":"kubernetes/serviceaccount"}`))
	return hdr + "." + body + "." + enc.EncodeToString([]byte("sig"))
}

// b2StubSeeds turns both seed primitives into no-ops (production seams), so the
// endpoint's dial count is the WALK's alone.
func b2StubSeeds(t *testing.T) {
	t.Helper()
	pw, pr := seedOneWidgetFn, seedOneRestactionFn
	seedOneWidgetFn = func(context.Context, navWidgetEntry, string, seedScopeMode) error { return nil }
	seedOneRestactionFn = func(context.Context, redact.Label, templatesv1.ObjectReference, string, seedScopeMode) error {
		return nil
	}
	t.Cleanup(func() { seedOneWidgetFn, seedOneRestactionFn = pw, pr })
}

func b2ResetWalkChildren(t *testing.T) {
	clear := func() {
		phase1WalkChildren.Range(func(k, _ any) bool { phase1WalkChildren.Delete(k); return true })
	}
	clear()
	t.Cleanup(clear)
}

// b2WalkObs is how many times the walk recorded `name`'s children (0 = never
// walked) — recordWalkChildren's coverage reading.
func b2WalkObs(name string) uint64 {
	v, ok := phase1WalkChildren.Load(h643WidgetGVR.String() + "|" + h643NS + "|" + name)
	if !ok {
		return 0
	}
	e := v.(*atomic.Pointer[phase1WalkChildrenEntry]).Load()
	if e == nil {
		return 0
	}
	return e.Observations
}

// b2WCKey is the walk's key for a panel reached by a no-slice /call path or as
// a root: the identity-free content key at the (-1,-1) tuple.
func b2WCKey(name string) string {
	k, _ := widgetContentL1Key(h643WidgetGVR, h643NS, name, -1, -1)
	return k
}

func b2Update(t *testing.T, env *h643Env, gvr schema.GroupVersionResource, name string, mutate func(u *unstructured.Unstructured)) {
	t.Helper()
	u, err := env.dyn.Resource(gvr).Namespace(h643NS).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	mutate(u)
	u.SetResourceVersion("2")
	if _, err := env.dyn.Resource(gvr).Namespace(h643NS).Update(context.Background(), u, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update %s: %v", name, err)
	}
	ref := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: name, Namespace: h643NS},
		APIVersion: gvr.Group + "/" + gvr.Version, Resource: gvr.Resource,
	}
	h643WaitFor(t, "the informer to serve "+name+" at RV 2", func() bool {
		got := objects.Get(cache.WithInformerOnlyReads(h643CohortCtx(env)), ref)
		return got.Err == nil && got.Unstructured != nil && got.Unstructured.GetResourceVersion() == "2"
	})
}

// ── W1 ─────────────────────────────────────────────────────────────────────

func TestIssue643_PRB2_W1_WalkDialsZeroAfterTheFirstWalk(t *testing.T) {
	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-a", "ext-b", "int-a", "int-b"}, "").
		panel("ext-a", "ext-a-ra", true, nil, "").
		panel("ext-b", "ext-b-ra", true, nil, "").
		panel("int-a", "int-a-ra", false, nil, "").
		panel("int-b", "int-b-ra", false, nil, "")
	env := b2Setup(t, h643K, "b2-root", tree)
	if got := h643Cohorts(); got != h643K {
		t.Fatalf("fixture precondition: %d cohorts, want %d", got, h643K)
	}

	if d := env.pass(prewarmScope{kind: scopeKindBoot}); d == 0 {
		t.Fatalf("precondition: the first boot pass (first walk + seed) dialled nothing — the fixture never reaches the external endpoint (dials=%v)", env.ep.byPath())
	}
	for _, n := range []string{"b2-root", "ext-a", "ext-b", "int-a", "int-b"} {
		if b2WalkObs(n) != 1 {
			t.Fatalf("precondition: the first walk did not reach %s (observations=%d)", n, b2WalkObs(n))
		}
	}
	int0, _ := env.warmWidgetCells()

	for i, s := range []prewarmScope{
		{kind: scopeKindGVRDiscovered},
		{kind: scopeKindGVRDiscovered},
		{kind: scopeKindBoot}, // a fresh boot re-enqueue: attempt 0 → it re-walks
	} {
		if d := env.pass(s); d != 0 {
			t.Fatalf("RED (#643 PR-B2 W1): pass %d (%s) after the first walk dialled the external endpoint %d times, want 0 — "+
				"the walk re-resolved external widgets whose content Put it declines anyway (dials=%v)", i+1, s.kind, d, env.ep.byPath())
		}
		for _, n := range []string{"b2-root", "ext-a", "ext-b", "int-a", "int-b"} {
			if got := b2WalkObs(n); got != uint64(i+2) {
				t.Fatalf("W1: pass %d (%s) — %s recorded %d walk observations, want %d: a skip must still record and descend",
					i+1, s.kind, n, got, i+2)
			}
		}
		if in, _ := env.warmWidgetCells(); in != int0 {
			t.Fatalf("W1: pass %d (%s) left %d internal widget cells warm, want %d (internal cells re-Put as before)", i+1, s.kind, in, int0)
		}
	}
}

// ── W3 ─────────────────────────────────────────────────────────────────────

func TestIssue643_PRB2_W3a_WidgetRefsEdited_ResolvesAndWalksTheNewChild(t *testing.T) {
	b2StubSeeds(t)
	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-p"}, "").
		panel("ext-p", "ext-p-ra", true, []string{"kid-1"}, "").
		panel("kid-1", "kid-1-ra", false, nil, "").
		panel("kid-2", "kid-2-ra", false, nil, "")
	env := b2Setup(t, 1, "b2-root", tree)

	if d := env.pass(prewarmScope{kind: scopeKindBoot}); d != 2 {
		t.Fatalf("precondition: the first walk dialled %d, want 2 (ext-p's two stages)", d)
	}
	if b2WalkObs("kid-2") != 0 {
		t.Fatal("precondition: kid-2 walked before it was referenced")
	}
	if d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered}); d != 0 {
		t.Fatalf("RED (#643 PR-B2 W3a): the second walk re-dialled ext-p %d times, want 0", d)
	}

	b2Update(t, env, h643WidgetGVR, "ext-p", func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(u.Object, map[string]any{"items": []any{b2Ref("kid-1"), b2Ref("kid-2")}}, "spec", "resourcesRefs")
	})
	if d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered}); d != 2 {
		t.Fatalf("W3a: after ext-p's resourcesRefs edit the walk dialled %d, want 2 — the widget edit must retire the mark and resolve", d)
	}
	if b2WalkObs("kid-2") == 0 {
		t.Fatal("W3a: kid-2, added by the resourcesRefs edit, was never walked — stale children were replayed")
	}
	if d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered}); d != 0 {
		t.Fatalf("RED (#643 PR-B2 W3a): the walk after the re-mark dialled %d, want 0", d)
	}
	if got := b2WalkObs("kid-2"); got != 2 {
		t.Fatalf("W3a: kid-2 walk observations = %d after the replaying pass, want 2 (the re-mark carries the new child)", got)
	}
}

func TestIssue643_PRB2_W3b_RAEditedToInternal_WalkPuts(t *testing.T) {
	b2StubSeeds(t)
	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-l"}, "").
		panel("ext-l", "ext-l-ra", true, nil, "")
	env := b2Setup(t, 1, "b2-root", tree)
	key := b2WCKey("ext-l")
	store := cache.ResolvedCache()

	env.pass(prewarmScope{kind: scopeKindBoot})
	if _, ok := store.GetNoTouch(key); ok {
		t.Fatal("precondition: an external-touched walk resolve Put the content cell (GTTL-1 decline missing)")
	}
	if d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered}); d != 0 {
		t.Fatalf("RED (#643 PR-B2 W3b): the second walk re-dialled ext-l %d times, want 0", d)
	}

	internalPath := "/apis/" + h643ThingsGVR.Group + "/" + h643ThingsGVR.Version + "/namespaces/" + h643NS + "/" + h643ThingsGVR.Resource
	b2Update(t, env, h643RAGVR, "ext-l-ra", func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedSlice(u.Object, []any{map[string]any{"name": "s0", "verb": "GET", "path": internalPath}}, "spec", "api")
	})
	if d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered}); d != 0 {
		t.Fatalf("W3b: ext-l-ra is internal now; the walk dialled %d", d)
	}
	if _, ok := store.GetNoTouch(key); !ok {
		t.Fatal("W3b: after its RESTAction became internal the walk must resolve ext-l and Put its content cell — a stale mark left it cold (I-1)")
	}
	if cache.EngineDeclinedExternalSet().Marked(key) {
		t.Fatal("W3b: the walk mark survived the Put (Put-unmark)")
	}
}

func TestIssue643_PRB2_W3c_RADeleted_WalkMarkDropped(t *testing.T) {
	b2StubSeeds(t)
	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-l"}, "").
		panel("ext-l", "ext-l-ra", true, nil, "")
	env := b2Setup(t, 1, "b2-root", tree)
	key := b2WCKey("ext-l")

	env.pass(prewarmScope{kind: scopeKindBoot})
	if !cache.EngineDeclinedExternalSet().Marked(key) {
		t.Fatal("RED (#643 PR-B2 W3c): the walk did not mark ext-l after an observed external touch")
	}
	if err := env.dyn.Resource(h643RAGVR).Namespace(h643NS).Delete(context.Background(), "ext-l-ra", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete ext-l-ra: %v", err)
	}
	h643WaitFor(t, "the GONE verdict to drop the walk mark", func() bool { return !cache.EngineDeclinedExternalSet().Marked(key) })
}

// ── ruling (b): the phase1 boot walk marks ─────────────────────────────────

func TestIssue643_PRB2_Cb_Phase1WalkMarks_EngineBootReWalkDialsZero(t *testing.T) {
	b2StubSeeds(t)
	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-a", "ext-b"}, "").
		panel("ext-a", "ext-a-ra", true, nil, "").
		panel("ext-b", "ext-b-ra", true, nil, "")
	env := b2Setup(t, 1, "b2-root", tree)

	roots, err := env.deps.lister(context.Background())
	if err != nil {
		t.Fatalf("lister: %v", err)
	}
	before := env.ep.total()
	if err := resolveNavigationRoot(context.Background(), roots[0].Root, roots[0].GVR, env.deps.saEP, env.deps.saRC,
		env.deps.authnNS, env.deps.harvester, env.deps.navHarv, nil); err != nil {
		t.Fatalf("phase1 walk: %v", err)
	}
	if d := env.ep.total() - before; d != 4 {
		t.Fatalf("precondition: the phase1 walk dialled %d, want 4 (2 external widgets × 2 stages)", d)
	}
	if d := env.pass(prewarmScope{kind: scopeKindBoot}); d != 0 {
		t.Fatalf("RED (#643 PR-B2 ruling b): the engine's boot re-walk dialled %d after the phase1 walk had already resolved "+
			"every external widget, want 0 (dials=%v)", d, env.ep.byPath())
	}
	if b2WalkObs("ext-a") != 2 || b2WalkObs("ext-b") != 2 {
		t.Fatalf("Cb: the engine re-walk must still record + descend the replayed widgets (obs ext-a=%d ext-b=%d, want 2)",
			b2WalkObs("ext-a"), b2WalkObs("ext-b"))
	}
}

// ── ruling (c): errored resolves ───────────────────────────────────────────

// b2Erroring turns panel `name` into one whose resolve ERRORS after its
// external touch while the endpoint serves numeric rows (the default): its
// widgetDataTemplate writes the external value into widgetData.title, which the
// CRD declares a string, so the status validation fails — the 057 shape. With
// b2StringRows on, the same widget resolves cleanly: the error is data-driven,
// with no change to the widget.
func b2Erroring(tree *b2Tree, name string) {
	for _, o := range tree.objs {
		if o.GetName() == name && o.GetKind() == "Panel" {
			_ = unstructured.SetNestedSlice(o.Object, []any{map[string]any{
				"forPath": ".title", "expression": "${ .s0.rows[0].v }",
			}}, "spec", "widgetDataTemplate")
		}
	}
}

func b2StringRows(env *h643Env, on bool) { env.ep.stringRows.Store(on) }

func TestIssue643_PRB2_Cci_ErroredExternalLeaf_MarkedAndNotReDialled(t *testing.T) {
	b2StubSeeds(t)
	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-l"}, "").
		panel("ext-l", "ext-l-ra", true, nil, "")
	b2Erroring(tree, "ext-l")
	env := b2Setup(t, 1, "b2-root", tree)

	if d := env.pass(prewarmScope{kind: scopeKindBoot}); d == 0 {
		t.Fatalf("precondition: the first walk did not reach the external endpoint")
	}
	if b2WalkObs("ext-l") != 0 || b2WalkObs("b2-root") != 1 {
		t.Fatalf("precondition: ext-l's walk resolve must ERROR (no children recorded; obs=%d) under a walked root", b2WalkObs("ext-l"))
	}
	if d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered}); d != 0 {
		t.Fatalf("RED (#643 PR-B2 ruling c-i): an errored external LEAF was re-dialled %d times on the next walk, want 0 — "+
			"a leaf declares no children, so replaying its error path loses nothing", d)
	}
}

func TestIssue643_PRB2_Ccii_ErroredExternalWithRefs_NotMarked_ChildFoundOnceFetchSucceeds(t *testing.T) {
	b2StubSeeds(t)
	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-par"}, "").
		panel("ext-par", "ext-par-ra", true, []string{"par-kid"}, "").
		panel("par-kid", "par-kid-ra", false, nil, "")
	b2Erroring(tree, "ext-par")
	env := b2Setup(t, 1, "b2-root", tree)

	d0 := env.pass(prewarmScope{kind: scopeKindBoot})
	if d0 == 0 {
		t.Fatal("precondition: the first walk did not reach the external endpoint")
	}
	if b2WalkObs("ext-par") != 0 || b2WalkObs("par-kid") != 0 {
		t.Fatal("precondition: ext-par's walk resolve must ERROR, leaving par-kid undiscovered")
	}
	if d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered}); d != d0 {
		t.Fatalf("Cc-ii: the errored external widget WITH resourcesRefs dialled %d on the next walk, want %d (re-dialled as today: "+
			"it must not be marked)", d, d0)
	}
	b2StringRows(env, true)
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered})
	if b2WalkObs("par-kid") == 0 {
		t.Fatal("RED (#643 PR-B2 ruling c-ii): once ext-par's fetch succeeded its child par-kid was still never walked — an errored " +
			"mark on a widget that declares children stopped its subtree from ever being discovered (I-1)")
	}
}
