package dispatchers

// issue1430_served_view_memo_test.go — #1430: the served digest of a gated key
// is computed once per (answer, identity, published RBAC snapshot), and is
// always the digest the uncached gate gives (the memo on its own:
// internal/cache served_view_memo_test.go).
//
//	P1  N arms, catch-up checks and written frames of one identity on one
//	    shell under one snapshot: the gate runs once. A second identity on the
//	    same shell: once more, and its own digest.
//	P2  an RBAC publish between two arms that changes what alice is served:
//	    her next arm gates again and holds the live digest; bob, on the same
//	    key, keeps his own.
//	P3  the shell: an identical re-Put keeps the stored digest; another body,
//	    or the same body with other stage outcomes, is gated again.
//	P4  parity: over shells × identities × a sequence of RBAC states, cold,
//	    warm and in shuffled order, the memoised digest equals the uncached one.
//	P5  the widgetContent /call hit stores the digest it stamped: the same
//	    identity's arm runs no gate and holds exactly that digest. A hit on a
//	    WARM memo still stamps the digest of the bytes it wrote: it reads
//	    nothing from the memo, and replaces what the memo held.
//	P6  the /call fill reads the RBAC state before its gate: a publish that
//	    lands while the gate runs leaves nothing stored.
//	P7  the evaluator's mode: a digest gated with no watcher wired, or with the
//	    cache disabled, is never the answer once the evaluator reads the
//	    snapshot again.

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

const p1430Class = cache.CacheEntryClassWidgetContent

func p1430Setup(t *testing.T) dynamic.Interface {
	t.Helper()
	reset := func() {
		cache.ResetBindingsByGVRIndexForTest()
		cache.ResetRBACSubGenForTest()
	}
	reset()
	t.Cleanup(reset)
	dyn := h1BuildWatcherWithRA(t)
	p1430Quiet(t)
	if !cache.ServedViewRBACNow().Storable() {
		t.Fatal("PRE: the harness published no complete RBAC snapshot; nothing would be stored and every arm below would pass vacuously")
	}
	return dyn
}

func p1430Ctx(user string, groups ...string) context.Context {
	return cache.WithInformerOnlyReads(xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: groups})))
}

func p1430Put(t *testing.T, key string, body []byte, outcomes string) *cache.ResolvedEntry {
	t.Helper()
	cache.ResolvedCache().PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: body, StageOutcomes: outcomes,
		Inputs: &cache.ResolvedKeyInputs{CacheEntryClass: p1430Class, Name: key}})
	return p1430Entry(t, key)
}

func p1430Entry(t *testing.T, key string) *cache.ResolvedEntry {
	t.Helper()
	e, ok := cache.ResolvedCache().GetNoTouch(key)
	if !ok || e == nil {
		t.Fatalf("PRE: no cell under %s", key)
	}
	return e
}

// p1430Live is the uncached reference: the gate run now, digested.
func p1430Live(ctx context.Context, e *cache.ResolvedEntry) string {
	d, _ := gatedAnswerDigest(ctx, e)
	return d
}

func p1430RBACGVR(r string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: r}
}

func p1430Role(name, group, resource string, verbs ...string) *unstructured.Unstructured {
	vs := make([]any, len(verbs))
	for i, v := range verbs {
		vs[i] = v
	}
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
		"metadata": map[string]any{"name": name},
		"rules":    []any{map[string]any{"apiGroups": []any{group}, "resources": []any{resource}, "verbs": vs}}}}
}

func p1430Binding(name, role, subjectKind, subject string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
		"metadata": map[string]any{"name": name, "uid": "uid-" + name},
		"subjects": []any{map[string]any{"kind": subjectKind, "name": subject}},
		"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role}}}
}

// p1430Quiet waits until no RBAC snapshot rebuild is in flight.
func p1430Quiet(t *testing.T) {
	t.Helper()
	if err := cache.Global().WaitRBACQuiescentForTest(10 * time.Second); err != nil {
		t.Fatal(err)
	}
}

// p1430Await waits until ctx's identity may (or may not) do verb on the panel
// the shells name, and until the RBAC writer is quiet with that still true
// (the #703 follow-up publish runs in the same writer slot).
func p1430Await(t *testing.T, ctx context.Context, verb string, want bool) {
	t.Helper()
	can := func() bool {
		return rbac.UserCan(ctx, rbac.UserCanOptions{Verb: verb, GroupResource: h1WidgetGVR.GroupResource(), Namespace: h1NS, Name: "target"})
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if can() == want {
			p1430Quiet(t)
			if can() == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("PRE: the RBAC change never made %s=%v", verb, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// p1430UnderOneSnapshot runs measure until the published RBAC snapshot is the
// same before and after it, so a count taken inside it is of one snapshot: a
// publish that lands during a pass voids that pass, it does not fail it.
func p1430UnderOneSnapshot(t *testing.T, measure func()) {
	t.Helper()
	for pass := 0; pass < 50; pass++ {
		p1430Quiet(t)
		before := cache.ServedViewRBACNow()
		measure()
		if cache.ServedViewRBACNow() == before {
			return
		}
	}
	t.Fatal("PRE: the RBAC snapshot never held still for one measured pass")
}

func p1430Arm(ctx context.Context, key string) *SubscriptionRotationTracker {
	tr := NewSubscriptionRotationTracker(ctx)
	tr.trackGated(key, p1430Class)
	tr.ArmGated()
	return tr
}

func TestIssue1430_P1_OneGateAcrossEveryArmOfAnIdentity(t *testing.T) {
	p1430Setup(t)
	const key = "wc-1430-p1"
	e := p1430Put(t, key, d1324Shell(true), "")
	alice, bob := p1430Ctx("alice-1430", "team-1430"), p1430Ctx("bob-1430", "team-1430")
	live := p1430Live(alice, e)
	if live == "" {
		t.Fatal("PRE: the gate refused")
	}

	before := cache.RefreshBroadcasterStatsSnapshot()
	const arms = 25
	for i := 0; i < arms; i++ {
		tr := p1430Arm(alice, key)
		if tr.gated[key].served != live {
			t.Fatalf("P1 RED: arm %d holds %q, the uncached gate gives %q", i, tr.gated[key].served, live)
		}
		if got := BehindKeys(alice, map[string]HeldDigests{key: {Class: p1430Class, Digests: []string{live}}}); len(got) != 0 {
			t.Fatalf("P1 RED: arm %d: a tab holding the live digest is behind (%v)", i, got)
		}
		if got := BehindKeys(alice, map[string]HeldDigests{key: {Class: p1430Class, Digests: []string{"v1.00"}}}); len(got) != 1 {
			t.Fatalf("P1 RED: arm %d: a tab holding another digest is not behind", i)
		}
		tr.NoteFrameWritten(key)
		if tr.gated[key].served != live {
			t.Fatalf("P1 RED: arm %d after a frame holds %q, want %q", i, tr.gated[key].served, live)
		}
	}
	after := cache.RefreshBroadcasterStatsSnapshot()
	if runs := after.GatedViewGateRuns - before.GatedViewGateRuns; runs != 1 {
		t.Fatalf("P1 RED: the gate ran %d times across %d arms (4 digests each) of one identity on one shell under one "+
			"RBAC snapshot, want 1", runs, arms)
	}
	if reused := after.GatedViewReused - before.GatedViewReused; reused != 4*arms-1 {
		t.Fatalf("P1: reused Δ=%d, want %d", reused, 4*arms-1)
	}

	tb := p1430Arm(bob, key)
	if runs := cache.RefreshBroadcasterStatsSnapshot().GatedViewGateRuns - after.GatedViewGateRuns; runs != 1 {
		t.Fatalf("P1 RED: a second identity cost %d gate runs, want 1 — it must not be answered from the first identity's digest", runs)
	}
	if tb.gated[key].served != p1430Live(bob, e) {
		t.Fatalf("P1 RED: bob holds %q, the uncached gate gives %q", tb.gated[key].served, p1430Live(bob, e))
	}
	if n := e.ServedViewSlotsForTest(); n != 2 {
		t.Fatalf("P1: %d slots for two identities", n)
	}
}

func TestIssue1430_P2_APublishBetweenTwoArmsIsGatedAgain(t *testing.T) {
	dyn := p1430Setup(t)
	const key = "wc-1430-p2"
	e := p1430Put(t, key, d1324Shell(true), "")
	alice, bob := p1430Ctx("alice-1430", "team-1430"), p1430Ctx("bob-1430", "team-1430")
	ta, tb := p1430Arm(alice, key), p1430Arm(bob, key)
	a0, b0 := ta.gated[key].served, tb.gated[key].served
	if a0 == "" || a0 != b0 {
		t.Fatalf("PRE: alice %q and bob %q should be served the same view before the grant", a0, b0)
	}

	mk := func(gvr schema.GroupVersionResource, obj *unstructured.Unstructured) {
		t.Helper()
		if _, err := dyn.Resource(gvr).Create(context.Background(), obj, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	mk(p1430RBACGVR("clusterroles"), p1430Role("panel-deleter-1430", h1WidgetGVR.Group, h1WidgetGVR.Resource, "delete"))
	mk(p1430RBACGVR("clusterrolebindings"), p1430Binding("alice-deletes-1430", "panel-deleter-1430", "User", "alice-1430"))
	p1430Await(t, alice, "delete", true)

	// Alice's stored digest is of the snapshot before the grant, whatever
	// number of publishes followed: the first arm after them gates once and
	// discards one slot.
	var a1 string
	var runs, moved uint64
	first := true
	p1430UnderOneSnapshot(t, func() {
		before := cache.RefreshBroadcasterStatsSnapshot()
		a1 = p1430Arm(alice, key).gated[key].served
		after := cache.RefreshBroadcasterStatsSnapshot()
		if first {
			runs, moved = after.GatedViewGateRuns-before.GatedViewGateRuns, after.GatedViewRBACMoved-before.GatedViewRBACMoved
			first = false
		}
	})
	if a1 == a0 || a1 != p1430Live(alice, e) {
		t.Fatalf("P2 RED: after a grant that changes what alice is served her arm holds %q (before: %q, uncached now: %q) — "+
			"a stored digest outlived the RBAC snapshot it was gated under", a1, a0, p1430Live(alice, e))
	}
	if runs != 1 || moved != 1 {
		t.Fatalf("P2 RED: gate_runs Δ=%d snapshot_moved Δ=%d on the first arm after the grant, want 1 and 1", runs, moved)
	}
	// The live connection's own check (#1328) reads the same digest.
	if got := ta.GatedMoved(); len(got) != 1 || got[0] != key {
		t.Fatalf("P2 RED: GatedMoved=%v on alice's open connection, want [%s]", got, key)
	}
	// bob shares the key and the snapshot, not the view.
	b1 := p1430Arm(bob, key).gated[key].served
	if b1 != b0 || b1 == a1 || b1 != p1430Live(bob, e) {
		t.Fatalf("P2 RED: bob holds %q (before %q, alice now %q, uncached %q) — an identity was answered another's digest",
			b1, b0, a1, p1430Live(bob, e))
	}
	if got := tb.GatedMoved(); len(got) != 0 {
		t.Fatalf("P2 RED: bob's view did not move but GatedMoved=%v", got)
	}
}

func TestIssue1430_P3_TheDigestFollowsTheAnswerNotTheEntry(t *testing.T) {
	p1430Setup(t)
	const key = "wc-1430-p3"
	alice := p1430Ctx("alice-1430", "team-1430")
	item := func(id string) []byte {
		return fakeEnvelope([]map[string]any{{"id": id, "verb": "DELETE", "allowed": true,
			"path": "/call?resource=" + h1WidgetGVR.Resource + "&apiVersion=" + h1WidgetGVR.Group + "/" + h1WidgetGVR.Version +
				"&namespace=" + h1NS + "&name=target"}})
	}
	e0 := p1430Put(t, key, item("one"), "")
	d0 := servedDigestNow(servedViewerOf(alice), p1430Class, e0)
	if d0 == "" || d0 != p1430Live(alice, e0) {
		t.Fatalf("PRE: %q", d0)
	}

	// An identical re-Put: a new entry, the same answer.
	e1 := p1430Put(t, key, item("one"), "")
	if e1 == e0 {
		t.Fatal("PRE: the re-Put did not store a new entry")
	}
	before := cache.RefreshBroadcasterStatsSnapshot()
	if d := servedDigestNow(servedViewerOf(alice), p1430Class, e1); d != d0 {
		t.Fatalf("P3 RED: identical re-Put: %q, want %q", d, d0)
	}
	if runs := cache.RefreshBroadcasterStatsSnapshot().GatedViewGateRuns - before.GatedViewGateRuns; runs != 0 {
		t.Fatalf("P3 RED: an identical re-Put cost %d gate runs, want 0 — the digest belongs to the answer, and most "+
			"refresh Puts are identical", runs)
	}

	// Another body under the same key.
	e2 := p1430Put(t, key, item("two"), "")
	d2 := servedDigestNow(servedViewerOf(alice), p1430Class, e2)
	if d2 == d0 || d2 != p1430Live(alice, e2) {
		t.Fatalf("P3 RED: after the shell's body changed the memo answered %q (before %q, uncached %q) — a stored digest "+
			"survived a change of the bytes it was gated from", d2, d0, p1430Live(alice, e2))
	}
	if n := e2.ServedViewSlotsForTest(); n != 1 {
		t.Fatalf("P3: %d slots on the new answer, want 1", n)
	}

	// The same body, other stage outcomes: another answer (#1319).
	e3 := p1430Put(t, key, item("two"), `[{"stage":"s","reason":"denied"}]`)
	d3 := servedDigestNow(servedViewerOf(alice), p1430Class, e3)
	if d3 == d2 || d3 != p1430Live(alice, e3) {
		t.Fatalf("P3 RED: same body, other stage outcomes: %q (before %q, uncached %q)", d3, d2, p1430Live(alice, e3))
	}
}

func TestIssue1430_P4_Parity_TheMemoisedDigestIsTheUncachedOne(t *testing.T) {
	dyn := p1430Setup(t)
	call := func(resource, group, ns, name string) string {
		return "/call?resource=" + resource + "&apiVersion=" + group + "&namespace=" + ns + "&name=" + name
	}
	panels := h1WidgetGVR.Group + "/" + h1WidgetGVR.Version
	mixed := fakeEnvelope([]map[string]any{
		{"id": "del", "verb": "DELETE", "allowed": true, "path": call(h1WidgetGVR.Resource, panels, h1NS, "target")},
		{"id": "del-other", "verb": "DELETE", "allowed": true, "path": call(h1WidgetGVR.Resource, panels, h1NS, "other")},
		{"id": "get", "verb": "GET", "allowed": true, "path": call(h1WidgetGVR.Resource, panels, h1NS, "target")},
		{"id": "get-ns2", "verb": "GET", "allowed": true, "path": call(h1WidgetGVR.Resource, panels, "elsewhere", "target")},
		{"id": "cm", "verb": "GET", "allowed": false, "path": call("configmaps", "v1", h1NS, "cm")},
		{"id": "post", "verb": "POST", "allowed": true, "path": call(h1WidgetGVR.Resource, panels, h1NS, "")},
		{"id": "external", "verb": "GET", "allowed": true, "path": "https://example.invalid/x"},
		{"id": "no-verb", "path": call(h1WidgetGVR.Resource, panels, h1NS, "target")},
		{"id": "bad-verb", "verb": "TELEPORT", "path": call(h1WidgetGVR.Resource, panels, h1NS, "target")},
	})
	var withJunk map[string]any
	_ = json.Unmarshal(d1324Shell(true), &withJunk)
	items := withJunk["status"].(map[string]any)["resourcesRefs"].(map[string]any)["items"].([]any)
	withJunk["status"].(map[string]any)["resourcesRefs"].(map[string]any)["items"] = append(items, "not-an-object", 7)
	junk, _ := json.Marshal(withJunk)

	shells := []struct {
		cell, outcomes string
		body           []byte
	}{
		{cell: "p4-no-refs", body: []byte(`{"kind":"Panel","status":{"widgetData":{"title":"t"}}}`)},
		{cell: "p4-empty-items", body: fakeEnvelope(nil)},
		{cell: "p4-one-delete", body: d1324Shell(true)},
		{cell: "p4-one-delete-sa-false", body: d1324Shell(false)},
		{cell: "p4-mixed", body: mixed},
		{cell: "p4-junk-items", body: junk},
		{cell: "p4-outcomes", body: mixed, outcomes: `[{"stage":"s","reason":"denied"}]`},
		{cell: "p4-malformed", body: []byte(`{"status":`)},
	}
	entries := make([]*cache.ResolvedEntry, len(shells))
	for i, s := range shells {
		entries[i] = p1430Put(t, s.cell, s.body, s.outcomes)
	}
	identities := []struct {
		name string
		ctx  context.Context
	}{
		{"alice", p1430Ctx("alice-1430", "team-1430")},
		{"bob", p1430Ctx("bob-1430", "team-1430")},
		{"carol-no-groups", p1430Ctx("carol-1430")},
		{"dave-groups-ab", p1430Ctx("dave-1430", "team-1430", "zeta-1430")},
		{"dave-groups-ba", p1430Ctx("dave-1430", "zeta-1430", "team-1430")},
		{"dave-one-group", p1430Ctx("dave-1430", "team-1430")},
		{"harness-reader", p1430Ctx(h1User)},
		{"service-account", p1430Ctx("system:serviceaccount:" + h1NS + ":robot-1430")},
		{"no-identity", context.Background()},
	}

	rng := rand.New(rand.NewSource(1430))
	seen := map[string]bool{}
	check := func(state string) {
		t.Helper()
		type cell struct{ s, i int }
		var cells []cell
		for s := range shells {
			for i := range identities {
				cells = append(cells, cell{s, i})
			}
		}
		for pass := 0; pass < 3; pass++ { // cold, warm, warm again in another order
			rng.Shuffle(len(cells), func(a, b int) { cells[a], cells[b] = cells[b], cells[a] })
			for _, c := range cells {
				id, e := identities[c.i], entries[c.s]
				live := p1430Live(id.ctx, e)
				if got := servedDigestNow(servedViewerOf(id.ctx), p1430Class, e); got != live {
					t.Fatalf("P4 RED [%s, pass %d]: shell %s for %s: memoised %q, uncached %q", state, pass, shells[c.s].cell,
						id.name, got, live)
				}
				seen[live] = true
			}
		}
		// Group order is not part of the identity.
		for s := range shells {
			if a, b := servedDigestNow(servedViewerOf(identities[3].ctx), p1430Class, entries[s]), servedDigestNow(servedViewerOf(identities[4].ctx), p1430Class, entries[s]); a != b {
				t.Fatalf("P4 RED [%s]: the same groups in another order gave %q and %q", state, a, b)
			}
		}
	}

	alice, dave := identities[0].ctx, identities[3].ctx
	roles, bindings := dyn.Resource(p1430RBACGVR("clusterroles")), dyn.Resource(p1430RBACGVR("clusterrolebindings"))
	must := func(_ *unstructured.Unstructured, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	bg := context.Background()

	check("base")
	must(roles.Create(bg, p1430Role("deleter-1430", h1WidgetGVR.Group, h1WidgetGVR.Resource, "delete"), metav1.CreateOptions{}))
	must(bindings.Create(bg, p1430Binding("alice-deleter-1430", "deleter-1430", "User", "alice-1430"), metav1.CreateOptions{}))
	p1430Await(t, alice, "delete", true)
	check("user grant")

	must(roles.Create(bg, p1430Role("zeta-reader-1430", h1WidgetGVR.Group, h1WidgetGVR.Resource, "get", "create"), metav1.CreateOptions{}))
	must(bindings.Create(bg, p1430Binding("zeta-reads-1430", "zeta-reader-1430", "Group", "zeta-1430"), metav1.CreateOptions{}))
	p1430Await(t, dave, "get", true)
	check("group grant")

	// A rules edit: the binding set does not move, the verdicts do.
	must(roles.Update(bg, p1430Role("deleter-1430", h1WidgetGVR.Group, h1WidgetGVR.Resource, "get"), metav1.UpdateOptions{}))
	p1430Await(t, alice, "delete", false)
	check("rules edit")

	if err := bindings.Delete(bg, "zeta-reads-1430", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	p1430Await(t, dave, "get", false)
	check("revoke")

	if len(seen) < 6 {
		t.Fatalf("PRE: the table produced only %d distinct digests; it does not exercise the gate", len(seen))
	}
}

func TestIssue1430_P5_TheCallHitFillsWhatItsIdentityArmsOn(t *testing.T) {
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	h := newK547Harness(t, k547DenyBackground)
	if !cache.ServedViewRBACNow().Storable() {
		t.Fatal("PRE: the harness published no complete RBAC snapshot")
	}
	widgetGV := k547WidgetGVR.Group + "/" + k547WidgetGVR.Version
	ctx := k547ReqCtx(0)
	contentKey, handle, inputs := dispatchWidgetContentKey(ctx,
		k547WidgetGVR.Group, k547WidgetGVR.Version, k547WidgetGVR.Resource, k547WNS, k547WName, -1, -1,
		effectiveKeyExtras(ctx, h.widgetU.Object, k547Extras(k547Changed)))
	if handle == nil || contentKey == "" {
		t.Fatal("PRE: no widgetContent key")
	}
	shell := k547Widget()
	shell.Object["status"] = map[string]any{"resourcesRefs": map[string]any{"items": []any{map[string]any{
		"id": "del", "verb": "DELETE", "allowed": true,
		"path": "/call?resource=" + k547WidgetGVR.Resource + "&apiVersion=" + widgetGV + "&namespace=" + k547WNS + "&name=" + k547WName,
	}}}}
	raw, _ := json.Marshal(shell.Object)
	putFixture(handle, contentKey, &cache.ResolvedEntry{RawJSON: raw, Inputs: inputs})

	before := cache.RefreshBroadcasterStatsSnapshot()
	rec := k1134Get(h, Widgets(), widgetGV, k547WidgetGVR.Resource, k547WName, 0, k547Changed)
	if rec.Header().Get(refreshClassHeader) != p1430Class {
		t.Fatalf("PRE: not a widgetContent shell hit (class %q)", rec.Header().Get(refreshClassHeader))
	}
	stamped := rec.Header().Get(RefreshDigestHeader)
	if stamped == "" || stamped != cache.ContentDigest(rec.Body.Bytes()) {
		t.Fatalf("P5 RED: the hit stamped %q, the bytes it wrote digest to %q — the /call path must digest what it wrote, "+
			"never read the memo", stamped, cache.ContentDigest(rec.Body.Bytes()))
	}
	mid := cache.RefreshBroadcasterStatsSnapshot()
	if mid.GatedViewCallFills-before.GatedViewCallFills != 1 || mid.GatedViewReused != before.GatedViewReused {
		t.Fatalf("P5 RED: the hit made call_fills Δ=%d reused Δ=%d, want 1 and 0",
			mid.GatedViewCallFills-before.GatedViewCallFills, mid.GatedViewReused-before.GatedViewReused)
	}

	tr := NewSubscriptionRotationTracker(cache.WithInformerOnlyReads(ctx))
	tr.trackGated(contentKey, p1430Class)
	tr.ArmGated()
	after := cache.RefreshBroadcasterStatsSnapshot()
	if tr.gated[contentKey].served != stamped {
		t.Fatalf("P5 RED: the arm holds %q, the /call stamped %q", tr.gated[contentKey].served, stamped)
	}
	if runs := after.GatedViewGateRuns - mid.GatedViewGateRuns; runs != 0 {
		t.Fatalf("P5 RED: the arm that follows its identity's /call hit ran the gate %d times, want 0", runs)
	}
	if got := tr.Behind(map[string]HeldDigests{contentKey: {Class: p1430Class, Digests: []string{stamped}}}); len(got) != 0 {
		t.Fatalf("P5 RED: a tab holding the digest its /call stamped is behind (%v)", got)
	}

	// A hit on a WARM memo. The memo is made to hold a digest that is NOT the
	// one of the bytes the hit writes: if the hit read the memo, it would stamp
	// it. It must stamp what it wrote, run no arm-side gate, reuse nothing, and
	// leave the memo holding the digest it stamped.
	entry, ok := handle.Get(contentKey)
	if !ok {
		t.Fatal("PRE: the shell is gone")
	}
	viewer := servedViewerOf(ctx)
	const planted = "v1.ffffffffffffffffffffffffffffffff"
	entry.NoteServedView(viewer.id, cache.ServedViewRBACNow(), planted)
	if d := servedDigestNow(viewer, p1430Class, entry); d != planted {
		t.Fatalf("PRE: the memo is not warm with the planted digest (%q)", d)
	}
	warm := cache.RefreshBroadcasterStatsSnapshot()
	rec2 := k1134Get(h, Widgets(), widgetGV, k547WidgetGVR.Resource, k547WName, 0, k547Changed)
	hit := cache.RefreshBroadcasterStatsSnapshot()
	if got := rec2.Header().Get(RefreshDigestHeader); got != stamped || got != cache.ContentDigest(rec2.Body.Bytes()) {
		t.Fatalf("P5 RED: a /call hit on a warm memo stamped %q; the bytes it wrote digest to %q (the memo held %q) — "+
			"the hit must digest what it wrote and never read the memo", got, cache.ContentDigest(rec2.Body.Bytes()), planted)
	}
	if hit.GatedViewReused != warm.GatedViewReused || hit.GatedViewGateRuns != warm.GatedViewGateRuns ||
		hit.GatedViewCallFills-warm.GatedViewCallFills != 1 {
		t.Fatalf("P5 RED: across a /call hit on a warm memo: reused Δ=%d gate_runs Δ=%d call_fills Δ=%d, want 0, 0, 1",
			hit.GatedViewReused-warm.GatedViewReused, hit.GatedViewGateRuns-warm.GatedViewGateRuns, hit.GatedViewCallFills-warm.GatedViewCallFills)
	}
	if d := servedDigestNow(viewer, p1430Class, entry); d != stamped {
		t.Fatalf("P5 RED: after the hit the memo holds %q, want the digest the hit stamped %q", d, stamped)
	}
}

func TestIssue1430_P6_TheCallFillReadsTheRBACStateBeforeItsGate(t *testing.T) {
	p1430Setup(t)
	alice := p1430Ctx("alice-1430", "team-1430")
	e := p1430Put(t, "wc-1430-p6", d1324Shell(true), "")

	// The control: with no publish during the gate, the fill stores.
	gated, served, fill := gateNotingServedView(alice, e, func() ([]byte, bool) { return gateWidgetEnvelope(alice, e.Raw()) })
	if !served {
		t.Fatal("PRE: the gate refused")
	}
	fill(cache.AnswerDigest(gated, e.StageOutcomes))
	if n := e.ServedViewSlotsForTest(); n != 1 {
		t.Fatalf("PRE: the fill stored %d slots with no publish in flight, want 1", n)
	}

	// A publish lands while the gate runs: the verdicts may be of either
	// snapshot, so the digest names neither.
	f := p1430Put(t, "wc-1430-p6-mid", d1324Shell(true), "")
	gated, _, fill = gateNotingServedView(alice, f, func() ([]byte, bool) {
		g, s := gateWidgetEnvelope(alice, f.Raw())
		cache.RebuildRBACSnapshotForTest(cache.Global()) // a new sequence, mid-gate
		return g, s
	})
	fill(cache.AnswerDigest(gated, f.StageOutcomes))
	if n := f.ServedViewSlotsForTest(); n != 0 {
		t.Fatalf("P6 RED: a /call fill whose gate ran across an RBAC publish stored %d slot(s) — the fill site read the "+
			"RBAC state after its gate, not before", n)
	}
	// A ctx with no identity fills nothing.
	_, _, fill = gateNotingServedView(context.Background(), f, func() ([]byte, bool) { return nil, false })
	fill("v1.00000000000000000000000000000000")
	if n := f.ServedViewSlotsForTest(); n != 0 {
		t.Fatalf("P6 RED: a fill with no identity stored %d slot(s)", n)
	}
}

func TestIssue1430_P7_ADigestGatedInAnotherEvaluatorModeIsNeverTheAnswer(t *testing.T) {
	modes := map[string]func(t *testing.T) (leave func()){
		"watcher_not_wired": func(t *testing.T) func() {
			w := cache.Global()
			cache.SetGlobal(nil)
			return func() { cache.SetGlobal(w) }
		},
		"cache_disabled": func(t *testing.T) func() {
			t.Setenv("CACHE_ENABLED", "false")
			return func() { t.Setenv("CACHE_ENABLED", "true") }
		},
	}
	for name, enter := range modes {
		t.Run(name, func(t *testing.T) {
			p1430Setup(t)
			// The harness lets h1User get panels; the shell asks exactly that, so
			// the real view differs from the one a deny-everything mode gives.
			reader := p1430Ctx(h1User)
			viewer := servedViewerOf(reader)
			shell := fakeEnvelope([]map[string]any{{"id": "get", "verb": "GET", "allowed": false,
				"path": "/call?resource=" + h1WidgetGVR.Resource + "&apiVersion=" + h1WidgetGVR.Group + "/" + h1WidgetGVR.Version +
					"&namespace=" + h1NS + "&name=target"}})
			e := p1430Put(t, "wc-1430-p7-"+name, shell, "")
			real := p1430Live(reader, e)

			seq := cache.ServedViewRBACNow()
			leave := enter(t)
			other := servedDigestNow(viewer, p1430Class, e)
			if other == real || other != p1430Live(reader, e) {
				t.Fatalf("PRE: in mode %s the gate gives %q (uncached %q, real view %q); the arm needs a mode whose view differs",
					name, other, p1430Live(reader, e), real)
			}
			gated, _, fill := gateNotingServedView(reader, e, func() ([]byte, bool) { return gateWidgetEnvelope(reader, e.Raw()) })
			fill(cache.AnswerDigest(gated, e.StageOutcomes))
			leave()
			if cache.ServedViewRBACNow() != seq {
				t.Fatal("PRE: the RBAC snapshot moved; the arm must change the mode under ONE snapshot")
			}
			if got := servedDigestNow(viewer, p1430Class, e); got != real {
				t.Fatalf("P7 RED: back in the snapshot-reading mode, under the same snapshot, the memo answered %q; the uncached "+
					"gate gives %q — a digest gated in mode %s was stored", got, real, name)
			}

			// And the reverse: a digest stored while the evaluator reads the
			// snapshot is not the answer in the other mode.
			enter(t)
			if got, live := servedDigestNow(viewer, p1430Class, e), p1430Live(reader, e); got != live {
				t.Fatalf("P7 RED: in mode %s the memo answered the stored %q, the uncached gate gives %q", name, got, live)
			}
			leave()
		})
	}
}

// BenchmarkIssue1430_ArmGated is one arm over 20 gated shells of 40 actions
// each: "memoised" is every arm after an identity's first under one RBAC
// snapshot, "uncached" is the gate every arm ran before #1430.
func BenchmarkIssue1430_ArmGated(b *testing.B) {
	b.Setenv("CACHE_ENABLED", "true")
	b.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	b.Cleanup(cache.ResetResolvedCacheForTest)
	prev := cache.LiveRBACSnapshot()
	cache.PublishRBACSnapshotForTest(&cache.RBACSnapshot{Synced: true, PublishSeq: 1430})
	b.Cleanup(func() { cache.PublishRBACSnapshotForTest(prev) })
	prevW := cache.Global()
	cache.SetGlobal(&cache.ResourceWatcher{}) // the evaluator reads the snapshot only with a watcher wired
	b.Cleanup(func() { cache.SetGlobal(prevW) })
	if !cache.ServedViewRBACNow().Storable() {
		b.Fatal("PRE: nothing would be stored; the memoised arm would measure the uncached gate")
	}
	items := make([]map[string]any, 40)
	for i := range items {
		items[i] = map[string]any{"id": i, "verb": "DELETE", "allowed": true,
			"path": "/call?resource=panels&apiVersion=widgets.templates.krateo.io/v1beta1&namespace=ns&name=target"}
	}
	body := fakeEnvelope(items)
	ctx := p1430Ctx("alice-1430", "team-1430")
	keys := make([]string, 20)
	entries := make([]*cache.ResolvedEntry, len(keys))
	for i := range keys {
		keys[i] = "wc-1430-bench-" + string(rune('a'+i))
		cache.ResolvedCache().PutResolved(fixtureStoreVerdict, keys[i], &cache.ResolvedEntry{RawJSON: body,
			Inputs: &cache.ResolvedKeyInputs{CacheEntryClass: p1430Class, Name: keys[i]}})
		entries[i], _ = cache.ResolvedCache().GetNoTouch(keys[i])
	}
	b.Run("memoised", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tr := NewSubscriptionRotationTracker(ctx)
			for _, k := range keys {
				tr.trackGated(k, p1430Class)
			}
			tr.ArmGated()
		}
	})
	b.Run("uncached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, e := range entries {
				gatedAnswerDigest(ctx, e)
			}
		}
	})
}
