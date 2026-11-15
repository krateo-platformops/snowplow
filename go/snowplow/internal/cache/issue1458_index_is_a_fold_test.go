// issue1458_index_is_a_fold_test.go — #1458: the bindings index's membership
// (which binding names which role, with which subjects) is a fold of the four
// RBAC informers' events and nothing else writes it.
//
//	TestIssue1458_GeneratedHistory_IndexIsTheFoldOfTheLiveObjects
//	    random histories of RBAC events of every kind (binding create, subject
//	    edit, roleRef edit, delete; role create, rules edit, delete; namespaced
//	    and cluster-scoped), with, at EVERY position, a prewarm pass's index step
//	    (SetNavigatedGVRs over the registered GVRs) and, at random positions, a
//	    type registration (AddNavigatedGVR), a deaf watch ended by a relist, and
//	    the snapshot writer held or released. After every step the index's
//	    membership equals a fold computed independently from the fake
//	    apiserver's live objects (or, while the watch is deaf, from what it held
//	    when it went deaf), before the pass step and after it.
//	TestIssue1458_RBACInformersAreNeverTornDownReRegisteredOrSwapped
//	    every production path that tears an informer down or swaps its store,
//	    driven with the RBAC kinds, leaves the same informer registered and
//	    feeding the hooks.
//	TestIssue1458_ReadinessWaitsForTheIndexToBeReady
//	    (#1457 item 1) an RBAC informer whose initial LIST has not returned, or
//	    RBAC handlers that have not been delivered theirs: the index is not
//	    ready, the enumerators answer nothing, readiness is not complete —
//	    whatever the readiness flip and the prewarm pass do — until they are.
//	TestIssue1458_NoProductionPathRefillsMembershipFromTheSnapshot
//	    a source guard: membership is written by the event hooks' two functions
//	    alone.
package cache

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

// ── the generated history ────────────────────────────────────────────────────

var (
	h1458Namespaces = []string{"a", "b"}
	h1458RoleNames  = []string{"r0", "r1", "r2", "r3"}
	h1458BindNames  = []string{"b0", "b1", "b2", "b3", "b4"}
	h1458Groups     = []string{"g0", "g1", "g2", "g3"}
	h1458Resources  = []string{"configmaps", "pods", "secrets", "*"}
	h1458NavPool    = []schema.GroupVersionResource{
		{Version: "v1", Resource: "configmaps"}, {Version: "v1", Resource: "pods"}, {Version: "v1", Resource: "secrets"},
		{Group: "widgets.example.io", Version: "v1", Resource: "panels"},
	}
)

type h1458World struct {
	t    *testing.T
	r    *rand.Rand
	rw   *ResourceWatcher
	dyn  *dynamicfake.FakeDynamicClient
	gate *c1458Gate

	rbs   map[string]*rbacv1.RoleBinding
	crbs  map[string]*rbacv1.ClusterRoleBinding
	roles map[string]*rbacv1.Role
	crs   map[string]*rbacv1.ClusterRole

	release chan struct{} // non-nil: the snapshot writer is held
	frozen  map[string]string
	// relists left: each one doubles the reflectors' retry backoff, so a
	// history has a few.
	relists int
	history []string
	stats   *h1458Stats
}

type h1458Stats struct {
	steps, eventsWhileHeld, passesWhileHeld, gaps, relists, eventsInAGap, registrations atomic.Int64
	byOp                                                                                [12]atomic.Int64
}

func (w *h1458World) subjects() []rbacv1.Subject {
	var out []rbacv1.Subject
	for _, g := range h1458Groups {
		if w.r.Intn(2) == 0 {
			out = append(out, rbacv1.Subject{Kind: "Group", APIGroup: c1458Group, Name: g})
		}
	}
	return out
}

func (w *h1458World) rules() []rbacv1.PolicyRule {
	res := h1458Resources[w.r.Intn(len(h1458Resources))]
	groups := []string{""}
	if res == "*" {
		groups = []string{"*"}
	}
	return []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: groups, Resources: []string{res}}}
}

func (w *h1458World) must(what string, err error) {
	w.t.Helper()
	if err != nil {
		w.t.Fatalf("%s: %v\nhistory:\n%s", what, err, strings.Join(w.history, "\n"))
	}
}

// expected is the membership the index must hold: the live objects', or what
// they were when the watch went deaf.
func (w *h1458World) expected() map[string]string {
	if w.frozen != nil {
		return w.frozen
	}
	return c1458LiveMembership(w.t, w.dyn)
}

func h1458Same(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func h1458Diff(got, want map[string]string) string {
	var out []string
	for id, w := range want {
		if g, ok := got[id]; !ok {
			out = append(out, fmt.Sprintf("  MISSING from the index: %s (%s)", id, w))
		} else if g != w {
			out = append(out, fmt.Sprintf("  DIFFERS: %s: index %q, want %q", id, g, w))
		}
	}
	for id, g := range got {
		if _, ok := want[id]; !ok {
			out = append(out, fmt.Sprintf("  PHANTOM in the index: %s (%s)", id, g))
		}
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

// op applies one random step and returns its description.
func (w *h1458World) op() string {
	ctx := context.Background()
	clusterScoped := w.r.Intn(2) == 0
	ns := h1458Namespaces[w.r.Intn(len(h1458Namespaces))]
	bname := h1458BindNames[w.r.Intn(len(h1458BindNames))]
	rname := h1458RoleNames[w.r.Intn(len(h1458RoleNames))]
	bkey, rkey := ns+"/"+bname, ns+"/"+rname
	kind := w.r.Intn(12)
	w.stats.byOp[kind].Add(1)
	event := func(s string) string {
		if w.release != nil {
			w.stats.eventsWhileHeld.Add(1)
		}
		if w.frozen != nil {
			w.stats.eventsInAGap.Add(1)
		}
		return s
	}
	switch kind {
	case 0, 1, 2: // binding: create, or edit its subjects, or edit its roleRef
		if clusterScoped {
			cur := w.crbs[bname]
			if cur == nil {
				b := c1458CRB(bname, rname)
				b.Subjects = w.subjects()
				_, err := w.dyn.Resource(c1458CRBGVR).Create(ctx, c1458U(w.t, b), metav1.CreateOptions{})
				w.must("create crb", err)
				w.crbs[bname] = b
				return event(fmt.Sprintf("create ClusterRoleBinding %s -> ClusterRole %s, %d subjects", bname, rname, len(b.Subjects)))
			}
			what := "edit subjects of"
			if kind == 2 {
				cur.RoleRef.Name = rname
				what = "re-point (roleRef -> " + rname + ")"
			} else {
				cur.Subjects = w.subjects()
			}
			_, err := w.dyn.Resource(c1458CRBGVR).Update(ctx, c1458U(w.t, cur), metav1.UpdateOptions{})
			w.must("update crb", err)
			return event(fmt.Sprintf("%s ClusterRoleBinding %s (%d subjects)", what, bname, len(cur.Subjects)))
		}
		cur := w.rbs[bkey]
		if cur == nil {
			b := c1458RB(ns, bname, rname)
			b.UID = types.UID("uid-rb-" + ns + "-" + bname)
			b.Subjects = w.subjects()
			_, err := w.dyn.Resource(c1458RBGVR).Namespace(ns).Create(ctx, c1458U(w.t, b), metav1.CreateOptions{})
			w.must("create rb", err)
			w.rbs[bkey] = b
			return event(fmt.Sprintf("create RoleBinding %s -> Role %s, %d subjects", bkey, rname, len(b.Subjects)))
		}
		what := "edit subjects of"
		if kind == 2 {
			cur.RoleRef.Name = rname
			what = "re-point (roleRef -> " + rname + ")"
		} else {
			cur.Subjects = w.subjects()
		}
		_, err := w.dyn.Resource(c1458RBGVR).Namespace(ns).Update(ctx, c1458U(w.t, cur), metav1.UpdateOptions{})
		w.must("update rb", err)
		return event(fmt.Sprintf("%s RoleBinding %s (%d subjects)", what, bkey, len(cur.Subjects)))
	case 3: // binding: delete
		if clusterScoped {
			if w.crbs[bname] == nil {
				return w.op()
			}
			w.must("delete crb", w.dyn.Resource(c1458CRBGVR).Delete(ctx, bname, metav1.DeleteOptions{}))
			delete(w.crbs, bname)
			return event("delete ClusterRoleBinding " + bname)
		}
		if w.rbs[bkey] == nil {
			return w.op()
		}
		w.must("delete rb", w.dyn.Resource(c1458RBGVR).Namespace(ns).Delete(ctx, bname, metav1.DeleteOptions{}))
		delete(w.rbs, bkey)
		return event("delete RoleBinding " + bkey)
	case 4, 5: // role: create, or edit its rules
		if clusterScoped {
			cur := w.crs[rname]
			if cur == nil {
				cur = c1458CR(rname, w.rules())
				_, err := w.dyn.Resource(c1458CRGVR).Create(ctx, c1458U(w.t, cur), metav1.CreateOptions{})
				w.must("create cr", err)
				w.crs[rname] = cur
				return event("create ClusterRole " + rname)
			}
			cur.Rules = w.rules()
			_, err := w.dyn.Resource(c1458CRGVR).Update(ctx, c1458U(w.t, cur), metav1.UpdateOptions{})
			w.must("update cr", err)
			return event("edit rules of ClusterRole " + rname)
		}
		cur := w.roles[rkey]
		if cur == nil {
			cur = c1458Role(ns, rname, w.rules())
			_, err := w.dyn.Resource(c1458RoleGVR).Namespace(ns).Create(ctx, c1458U(w.t, cur), metav1.CreateOptions{})
			w.must("create role", err)
			w.roles[rkey] = cur
			return event("create Role " + rkey)
		}
		cur.Rules = w.rules()
		_, err := w.dyn.Resource(c1458RoleGVR).Namespace(ns).Update(ctx, c1458U(w.t, cur), metav1.UpdateOptions{})
		w.must("update role", err)
		return event("edit rules of Role " + rkey)
	case 6: // role: delete
		if clusterScoped {
			if w.crs[rname] == nil {
				return w.op()
			}
			w.must("delete cr", w.dyn.Resource(c1458CRGVR).Delete(ctx, rname, metav1.DeleteOptions{}))
			delete(w.crs, rname)
			return event("delete ClusterRole " + rname)
		}
		if w.roles[rkey] == nil {
			return w.op()
		}
		w.must("delete role", w.dyn.Resource(c1458RoleGVR).Namespace(ns).Delete(ctx, rname, metav1.DeleteOptions{}))
		delete(w.roles, rkey)
		return event("delete Role " + rkey)
	case 7: // a type registration widens the navigated set
		gvr := h1458NavPool[w.r.Intn(len(h1458NavPool))]
		AddNavigatedGVR(gvr)
		w.stats.registrations.Add(1)
		return "register " + gvr.Resource + " (AddNavigatedGVR)"
	case 8, 9: // the snapshot writer is held, or released
		if w.release == nil {
			ch := make(chan struct{})
			w.release = ch
			SetRebuildBarrierForTest(func() { <-ch })
			return "HOLD the snapshot writer"
		}
		w.freeWriter()
		return "RELEASE the snapshot writer"
	case 10: // the watch goes deaf
		if w.frozen != nil || w.relists == 0 {
			return w.op()
		}
		w.frozen = c1458LiveMembership(w.t, w.dyn)
		w.gate.setDeaf(true)
		w.stats.gaps.Add(1)
		return "the watch goes DEAF"
	default: // 410 Expired: a relist (ending a gap, if one is open)
		if w.relists == 0 || (w.frozen == nil && w.relists == 1) {
			return w.op() // the last one is kept for an open gap
		}
		w.relist()
		return "410 Expired: RELIST"
	}
}

// relist ends every RBAC watch with 410 Expired and waits until each informer
// has listed again and RE-OPENED its watch: the fake apiserver has no
// resourceVersion to resume from, so an event sent before the new watch is open
// would be lost for good (a real apiserver replays it).
func (w *h1458World) relist() {
	w.t.Helper()
	w.relists--
	before := w.gate.watchesOpened()
	w.frozen = nil
	w.gate.setDeaf(false)
	w.gate.expire()
	w.stats.relists.Add(1)
	if !c1458Wait(60*time.Second, func() bool {
		now := w.gate.watchesOpened()
		for _, gvr := range c1458Kinds {
			if now[gvr.Resource] <= before[gvr.Resource] {
				return false
			}
		}
		return true
	}) {
		w.t.Fatalf("the RBAC watches were not re-opened after the relist\nhistory:\n%s", strings.Join(w.history, "\n"))
	}
}

func (w *h1458World) freeWriter() {
	if w.release != nil {
		close(w.release)
		w.release = nil
		SetRebuildBarrierForTest(nil)
	}
}

// settle waits until the index holds the expected membership.
func (w *h1458World) settle(step int, what string) {
	w.t.Helper()
	var got, want map[string]string
	ok := c1458Wait(40*time.Second, func() bool {
		got, want = c1458IndexMembership(), w.expected()
		return h1458Same(got, want)
	})
	if !ok {
		w.t.Fatalf("step %d (%s): THE INDEX IS NOT THE FOLD OF THE LIVE OBJECTS (writer held=%v, watch deaf=%v):\n%s\nhistory:\n%s",
			step, what, w.release != nil, w.frozen != nil, h1458Diff(got, want), strings.Join(w.history, "\n"))
	}
}

func h1458Run(t *testing.T, seed int64, steps int, stats *h1458Stats) {
	_, dyn, gate := c1458Watcher(t, true)
	w := &h1458World{t: t, r: rand.New(rand.NewSource(seed)), rw: Global(), dyn: dyn, gate: gate, stats: stats, relists: 3,
		rbs: map[string]*rbacv1.RoleBinding{}, crbs: map[string]*rbacv1.ClusterRoleBinding{},
		roles: map[string]*rbacv1.Role{}, crs: map[string]*rbacv1.ClusterRole{}}
	t.Cleanup(w.freeWriter)
	if !BindingsByGVRIndexBuilt() {
		t.Fatalf("SETUP: the watcher's initial publish returned and the index is not ready")
	}
	// The informers' watches are open (the fake loses an event sent before).
	if !c1458Wait(30*time.Second, func() bool {
		opened := gate.watchesOpened()
		for _, gvr := range c1458Kinds {
			if opened[gvr.Resource] == 0 {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("SETUP: the RBAC watches were never opened")
	}
	for step := 1; step <= steps; step++ {
		what := w.op()
		w.history = append(w.history, fmt.Sprintf("  %3d  %s", step, what))
		stats.steps.Add(1)
		w.settle(step, what)
		// A prewarm pass's index step, at this position.
		SetNavigatedGVRs(w.rw.RegisteredGVRs())
		if w.release != nil {
			stats.passesWhileHeld.Add(1)
		}
		if got, want := c1458IndexMembership(), w.expected(); !h1458Same(got, want) {
			t.Fatalf("step %d (%s): THE PASS STEP CHANGED THE INDEX'S MEMBERSHIP (writer held=%v, watch deaf=%v):\n%s\nhistory:\n%s",
				step, what, w.release != nil, w.frozen != nil, h1458Diff(got, want), strings.Join(w.history, "\n"))
		}
	}
	// The end: everything delivered and published.
	w.freeWriter()
	if w.frozen != nil {
		w.relists = 1
		w.relist()
	}
	w.settle(steps+1, "the end")
	if err := WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		t.Fatalf("RBAC rebuild did not quiesce: %v", err)
	}
	// The published snapshot agrees with the same fold: the oracle is not the
	// index's own view.
	fromSnap := map[string]string{}
	render := func(subjects []rbacv1.Subject) string {
		var subj []string
		for _, s := range subjectsFromRBAC(subjects) {
			subj = append(subj, s.Kind+":"+s.Namespace+"/"+s.Name)
		}
		sort.Strings(subj)
		return strings.Join(subj, ",")
	}
	if !c1458Wait(20*time.Second, func() bool {
		snap := rbacSnap.Load()
		fromSnap = map[string]string{}
		for _, b := range snap.ClusterRoleBindings {
			fromSnap[string(crbBindingID(b))] = roleRefKey("", b.RoleRef) + " <- " + render(b.Subjects)
		}
		for ns, bs := range snap.RoleBindingsByNS {
			for _, b := range bs {
				fromSnap[string(rbBindingID(b))] = roleRefKey(ns, b.RoleRef) + " <- " + render(b.Subjects)
			}
		}
		return h1458Same(fromSnap, c1458LiveMembership(t, dyn))
	}) {
		t.Fatalf("the published snapshot does not hold the live bindings at the end:\n%s", h1458Diff(fromSnap, c1458LiveMembership(t, dyn)))
	}
}

func TestIssue1458_GeneratedHistory_IndexIsTheFoldOfTheLiveObjects(t *testing.T) {
	const histories, steps = 6, 50
	stats := &h1458Stats{}
	for seed := int64(1); seed <= histories; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("history-%02d", seed), func(t *testing.T) { h1458Run(t, 14580+seed, steps, stats) })
	}
	t.Logf("histories=%d steps=%d; RBAC events with the snapshot writer held=%d; pass steps with it held=%d; deaf gaps=%d, events inside one=%d; relists=%d; type registrations=%d",
		histories, stats.steps.Load(), stats.eventsWhileHeld.Load(), stats.passesWhileHeld.Load(), stats.gaps.Load(),
		stats.eventsInAGap.Load(), stats.relists.Load(), stats.registrations.Load())
	if t.Failed() {
		return
	}
	if stats.eventsWhileHeld.Load() == 0 || stats.passesWhileHeld.Load() == 0 || stats.eventsInAGap.Load() == 0 ||
		stats.relists.Load() == 0 || stats.registrations.Load() == 0 {
		t.Fatalf("VACUOUS: a position the property is about was never generated")
	}
	for i := range stats.byOp {
		if stats.byOp[i].Load() == 0 {
			t.Fatalf("VACUOUS: step kind %d was never generated", i)
		}
	}
}

// ── the RBAC informers stay ──────────────────────────────────────────────────

func TestIssue1458_RBACInformersAreNeverTornDownReRegisteredOrSwapped(t *testing.T) {
	rw, dyn, _ := c1458Watcher(t, false,
		c1458Role("a", "r", c1458Rules("configmaps")), c1458RB("a", "rb", "r", "g"),
		c1458CR("cr", c1458Rules("configmaps")), c1458CRB("crb", "cr", "g"))
	type held struct {
		informer any
		instance *storeInstance
	}
	before := map[schema.GroupVersionResource]held{}
	rw.mu.RLock()
	for _, gvr := range c1458Kinds {
		before[gvr] = held{informer: rw.informers[gvr].Informer(), instance: rw.instances[gvr]}
	}
	rw.mu.RUnlock()

	crd := func(gvr schema.GroupVersionResource, served ...string) *unstructured.Unstructured {
		var versions []any
		for _, v := range served {
			versions = append(versions, map[string]any{"name": v, "served": true, "storage": v == served[0]})
		}
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
			"metadata": map[string]any{"name": gvr.Resource + "." + gvr.Group},
			"spec":     map[string]any{"group": gvr.Group, "names": map[string]any{"plural": gvr.Resource}, "versions": versions},
		}}
	}
	c := crdDiscoverySingleton()
	attempts := []struct {
		name string
		do   func(gvr schema.GroupVersionResource)
	}{
		{"a #870 store build for a divergence", func(gvr schema.GroupVersionResource) {
			if rw.requestStoreBuild(gvr, reasonDivergence, []string{"a/rb"}) {
				t.Errorf("%s: a store build was ACCEPTED for a divergence", gvr.Resource)
			}
		}},
		{"a #870 store build for a schema change", func(gvr schema.GroupVersionResource) {
			if rw.requestStoreBuild(gvr, reasonSchema, nil) {
				t.Errorf("%s: a store build was ACCEPTED for a schema change", gvr.Resource)
			}
		}},
		{"RemoveResourceType", func(gvr schema.GroupVersionResource) { rw.RemoveResourceType(gvr) }},
		{"the DELETE of a CRD object named after the kind", func(gvr schema.GroupVersionResource) {
			triggerCRDDelete(crd(gvr, "v1"))
		}},
		{"a refused LIST with a CRD record that does not serve v1 (#807)", func(gvr schema.GroupVersionResource) {
			u := crd(gvr, "v2")
			c.recordServedVersions(u)
			defer c.forgetServedVersions(u)
			c.handleListRefused(rw, gvr)
		}},
		{"the prune of versions a CRD no longer serves (#219)", func(gvr schema.GroupVersionResource) {
			c.pruneUnservedGVRs(rw, crd(gvr, "v2"), []schema.GroupVersionResource{{Group: gvr.Group, Version: "v2", Resource: gvr.Resource}})
		}},
	}
	refused0 := RBACInformerTeardownRefusedTotal()
	for i, a := range attempts {
		for _, gvr := range c1458Kinds {
			a.do(gvr)
			rw.mu.RLock()
			gi := rw.informers[gvr]
			in := rw.instances[gvr]
			blocked := rw.unservedVersionBlockedLocked(gvr)
			rw.mu.RUnlock()
			if gi == nil || gi.Informer() != before[gvr].informer || in != before[gvr].instance {
				t.Fatalf("after %s the %s informer is no longer the one the watcher started (registered=%v, same instance=%v)",
					a.name, gvr.Resource, gi != nil, in == before[gvr].instance)
			}
			if blocked {
				t.Fatalf("after %s the %s kind is blocked from registering", a.name, gvr.Resource)
			}
			if b := rw.storeBuildForTest(gvr); b != nil {
				t.Fatalf("after %s a build copy of the %s store exists", a.name, gvr.Resource)
			}
		}
		// The informers still feed the hooks and the snapshot: a binding
		// created now reaches both.
		name := fmt.Sprintf("after-attempt-%d", i)
		b := c1458RB("a", name, "r", "g-"+name)
		if _, err := dyn.Resource(c1458RBGVR).Namespace("a").Create(context.Background(), c1458U(t, b), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create binding: %v", err)
		}
		crbObj := c1458CRB(name, "cr", "g-"+name)
		if _, err := dyn.Resource(c1458CRBGVR).Create(context.Background(), c1458U(t, crbObj), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create cluster binding: %v", err)
		}
		if !c1458Wait(10*time.Second, func() bool {
			return h1458Same(c1458IndexMembership(), c1458LiveMembership(t, dyn))
		}) {
			t.Fatalf("after %s the index no longer follows the live bindings:\n%s", a.name,
				h1458Diff(c1458IndexMembership(), c1458LiveMembership(t, dyn)))
		}
		if !c1458Wait(10*time.Second, func() bool {
			snap := rbacSnap.Load()
			if snap == nil || len(snap.ClusterRolesByName) == 0 || len(snap.RolesByNSName) == 0 {
				return false
			}
			for _, rb := range snap.RoleBindingsByNS["a"] {
				if rb.Name == name {
					return true
				}
			}
			return false
		}) {
			t.Fatalf("after %s the RBAC snapshot no longer follows the live objects", a.name)
		}
	}
	if got := RBACInformerTeardownRefusedTotal() - refused0; got == 0 {
		t.Fatalf("no teardown was refused: the attempts never reached the teardown")
	}
}

// ── ready, and readiness ─────────────────────────────────────────────────────

// r1458Start starts a watcher over seed, installed as Global, WITHOUT waiting
// for anything. prep runs on the fake client before the watcher is built.
func r1458Start(t *testing.T, prep func(*dynamicfake.FakeDynamicClient), seed ...runtime.Object) (*ResourceWatcher, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	untyped := make([]runtime.Object, 0, len(seed))
	for _, o := range seed {
		untyped = append(untyped, c1458U(t, o))
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		c1458CRBGVR: "ClusterRoleBindingList", c1458CRGVR: "ClusterRoleList", c1458RBGVR: "RoleBindingList",
		c1458RoleGVR: "RoleList", c1458CMGVR: "ConfigMapList",
	}, untyped...)
	if prep != nil {
		prep(dyn)
	}
	ResetPhase1DoneForTest()
	wctx, cancel := context.WithCancel(context.Background())
	rw, err := NewResourceWatcher(wctx, dyn)
	if err != nil {
		cancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	prev := Global()
	SetGlobal(rw)
	t.Cleanup(func() {
		SetGlobal(prev)
		rw.Stop()
		cancel()
		_ = WaitRBACRebuildQuiesceForTest(30 * time.Second)
		ResetBindingsByGVRIndexForTest()
		ResetPendingSubGenBumpsForTest()
		ResetRBACSubGenForTest()
		ResetPhase1DoneForTest()
	})
	return rw, dyn
}

func r1458World(n int) []runtime.Object {
	var seed []runtime.Object
	for i := 0; i < n; i++ {
		nm := fmt.Sprintf("o-%d", i)
		seed = append(seed, c1458Role("a", nm, c1458Rules("configmaps")), c1458CR(nm, c1458Rules("configmaps")),
			c1458RB("a", nm, nm, "g-"+nm), c1458CRB(nm, nm, "g-"+nm))
	}
	return seed
}

// r1458NotReady asserts the not-ready state, whatever the readiness flip and a
// prewarm pass do.
func r1458NotReady(t *testing.T, rw *ResourceWatcher, why string) {
	t.Helper()
	MarkPhase1Done() // the readiness flip (the settle gives the navigated set)
	SetNavigatedGVRs(append(rw.RegisteredGVRs(), c1458CMGVR))
	if BindingsByGVRIndexBuilt() {
		t.Fatalf("%s: THE INDEX IS READY", why)
	}
	if n := len(EnumeratePrewarmTargetsForGVR(c1458CMGVR, "list")); n != 0 {
		t.Fatalf("%s: the enumerator answers %d prewarm targets from an index that is not ready", why, n)
	}
	if IdentityAuthorisedForGVR(c1458CMGVR, "nobody", []string{"g-o-0"}) {
		t.Fatalf("%s: the index authorises a prewarm identity before it is ready", why)
	}
	if st := RBACReadiness(); st.Complete {
		t.Fatalf("%s: READINESS IS COMPLETE with the bindings index not ready", why)
	}
}

func TestIssue1458_ReadinessWaitsForTheIndexToBeReady(t *testing.T) {
	const n = 40
	t.Run("an-RBAC-initial-LIST-has-not-returned", func(t *testing.T) {
		// The roles LIST is refused until released (the fake client runs its
		// reactors under one lock, so a reactor must not block).
		var open atomic.Bool
		release := func() { open.Store(true) }
		rw, dyn := r1458Start(t, func(dyn *dynamicfake.FakeDynamicClient) {
			dyn.PrependReactor("list", "roles", func(clienttesting.Action) (bool, runtime.Object, error) {
				if open.Load() {
					return false, nil, nil
				}
				return true, nil, fmt.Errorf("the roles LIST is not answered yet (test)")
			})
		}, r1458World(n)...)
		// The other three kinds are delivered: the index already holds their
		// bindings (membership is written from the first event on).
		if !c1458Wait(15*time.Second, func() bool { return len(c1458IndexMembership()) == 2*n }) {
			t.Fatalf("the bindings the delivered informers hold never reached the index: %d of %d", len(c1458IndexMembership()), 2*n)
		}
		r1458NotReady(t, rw, "the roles informer's initial LIST has not returned")
		st := RBACReadiness()
		t.Logf("readiness: complete=%v unsynced=%v publish pending=%v; index ready=%v, bindings held=%d",
			st.Complete, st.Unsynced, st.PublishPending, BindingsByGVRIndexBuilt(), len(c1458IndexMembership()))
		if len(st.Unsynced) != 1 || st.Unsynced[0] != "roles" {
			t.Fatalf("readiness must name the informer it waits on: unsynced=%v, want [roles]", st.Unsynced)
		}

		release()
		if err := rw.WaitInitialRBACPublishForTest(30 * time.Second); err != nil {
			t.Fatal(err)
		}
		if !BindingsByGVRIndexBuilt() || !RBACReadiness().Complete {
			t.Fatalf("after the LIST returned: index ready=%v readiness complete=%v, want both", BindingsByGVRIndexBuilt(), RBACReadiness().Complete)
		}
		c1458DiffMembership(t, "at ready", c1458IndexMembership(), c1458LiveMembership(t, dyn))
		r1458NoBumpBeforeReady(t)
		if got := len(EnumeratePrewarmTargetsForGVR(c1458CMGVR, "list")); got == 0 {
			t.Fatalf("at ready the enumerator answers no prewarm target for a navigated GVR every binding grants")
		}
	})

	t.Run("the-RBAC-handlers-have-not-been-delivered-their-initial-lists", func(t *testing.T) {
		// Each RBAC handler stops after the first object of its initial list:
		// the informers' STORES sync, the handlers have not been delivered.
		gate := make(chan struct{})
		released := false
		hold := func() { <-gate }
		rbacInitialListAddDeliveredHookForTest.Store(&hold)
		release := func() {
			if !released {
				released = true
				rbacInitialListAddDeliveredHookForTest.Store(nil)
				close(gate)
			}
		}
		rw, dyn := r1458Start(t, nil, r1458World(n)...)
		t.Cleanup(release)
		sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := rw.WaitForCacheSync(sctx, 30*time.Second); err != nil {
			t.Fatalf("the informers' stores did not sync: %v", err)
		}
		if !c1458Wait(15*time.Second, func() bool { s := rbacSnap.Load(); return s != nil && s.Synced && len(s.ClusterRoleBindings) == n }) {
			t.Fatalf("the complete snapshot was never published")
		}
		r1458NotReady(t, rw, "the RBAC handlers have not been delivered their initial lists")
		st := RBACReadiness()
		held := len(c1458IndexMembership())
		t.Logf("readiness: complete=%v unsynced=%v publish pending=%v; snapshot complete; index ready=%v, bindings held=%d of %d",
			st.Complete, st.Unsynced, st.PublishPending, BindingsByGVRIndexBuilt(), held, 2*n)
		if len(st.Unsynced) != 0 || !st.PublishPending {
			t.Fatalf("readiness must say every informer synced and the initial publish is pending: %+v", st)
		}
		if held >= 2*n {
			t.Fatalf("SETUP: the handlers were delivered everything (%d bindings): nothing was held", held)
		}

		release()
		if err := rw.WaitInitialRBACPublishForTest(30 * time.Second); err != nil {
			t.Fatal(err)
		}
		if !BindingsByGVRIndexBuilt() || !RBACReadiness().Complete {
			t.Fatalf("after the delivery: index ready=%v readiness complete=%v, want both", BindingsByGVRIndexBuilt(), RBACReadiness().Complete)
		}
		// READY MEANS COMPLETE: read at the moment the initial publish returned.
		c1458DiffMembership(t, "at ready", c1458IndexMembership(), c1458LiveMembership(t, dyn))
		r1458NoBumpBeforeReady(t)
	})
}

// r1458NoBumpBeforeReady: the bindings delivered before the index was ready
// recorded no sub-generation bump (bumps stay off until ready, as before
// #1458), and nothing in an initial list owes the #682 rotation.
func r1458NoBumpBeforeReady(t *testing.T) {
	t.Helper()
	if err := WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	if got := RBACSubGenForSubject("nobody-1458", []string{"g-o-0"}); got != 0 {
		t.Errorf("the sub-generation of a subject bound only by initial-list bindings is %d at ready, want 0: "+
			"a binding delivered before ready recorded a bump, or the epoch rotated", got)
	}
	if got := RBACSubGenEpoch(); got != 0 {
		t.Errorf("the #682 epoch is %d after a boot with no RBAC edit, want 0", got)
	}
}

// TestIssue1458_AWatcherStartsTheIndexEmpty — the index is a fold of ONE
// watcher's events: what an earlier watcher's events left in it is not the
// next watcher's world (production builds one watcher; the test suites build
// many in one process).
func TestIssue1458_AWatcherStartsTheIndexEmpty(t *testing.T) {
	func() {
		t.Setenv("CACHE_ENABLED", "true")
		dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
			c1458CRBGVR: "ClusterRoleBindingList", c1458CRGVR: "ClusterRoleList", c1458RBGVR: "RoleBindingList",
			c1458RoleGVR: "RoleList", c1458CMGVR: "ConfigMapList",
		}, c1458U(t, c1458CR("cr-first", c1458Rules("configmaps"))), c1458U(t, c1458CRB("crb-first", "cr-first", "g-first")))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		first, err := NewResourceWatcher(ctx, dyn)
		if err != nil {
			t.Fatalf("NewResourceWatcher: %v", err)
		}
		defer first.Stop()
		if err := first.WaitInitialRBACPublishForTest(30 * time.Second); err != nil {
			t.Fatal(err)
		}
		if !BindingsIndexNamesSubjectForTest("Group", "", "g-first") {
			t.Fatalf("SETUP: the first watcher's binding is not in the index")
		}
	}()
	_, dyn, _ := c1458Watcher(t, false, c1458CR("cr-second", c1458Rules("configmaps")), c1458CRB("crb-second", "cr-second", "g-second"))
	if BindingsIndexNamesSubjectForTest("Group", "", "g-first") {
		t.Errorf("the index still holds the binding an EARLIER watcher's events recorded: it is not this watcher's world")
	}
	c1458DiffMembership(t, "the second watcher", c1458IndexMembership(), c1458LiveMembership(t, dyn))
}

// ── the source guard ─────────────────────────────────────────────────────────

// TestIssue1458_NoProductionPathRefillsMembershipFromTheSnapshot — the index's
// membership maps (entries, byRole) are written by enrolLocked and unrolLocked
// alone (and emptied by resetLocked); those are reached only from the binding
// event hooks' two functions (applyBindingAdd, applyBindingDelete), the test
// seed and the test reset. A build, a pass or a publish that wrote membership
// would have to call one of them, or write the maps itself: both fail here.
func TestIssue1458_NoProductionPathRefillsMembershipFromTheSnapshot(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	callers := map[string]map[string]bool{} // callee -> callers
	writers := map[string]bool{}            // functions that write entries / byRole
	files := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		files++
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			isMembership := func(x ast.Expr) bool {
				// <anything>.entries[...] or <anything>.byRole[...], or the maps themselves
				if ix, ok := x.(*ast.IndexExpr); ok {
					x = ix.X
				}
				sel, ok := x.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "entries" && sel.Sel.Name != "byRole") {
					return false
				}
				recv, ok := sel.X.(*ast.Ident)
				return ok && recv.Name == "idx"
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.CallExpr:
					name := ""
					switch fun := n.Fun.(type) {
					case *ast.Ident:
						name = fun.Name
					case *ast.SelectorExpr:
						name = fun.Sel.Name
					}
					if name == "delete" && len(n.Args) > 0 && isMembership(n.Args[0]) {
						writers[fn.Name.Name] = true
					}
					if name != "" {
						if callers[name] == nil {
							callers[name] = map[string]bool{}
						}
						callers[name][fn.Name.Name] = true
					}
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if isMembership(lhs) {
							writers[fn.Name.Name] = true
						}
					}
				}
				return true
			})
		}
	}
	if files < 50 {
		t.Fatalf("the guard parsed %d files: it is not looking at the package", files)
	}
	set := func(m map[string]bool) string {
		var out []string
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	check := func(what string, got map[string]bool, want ...string) {
		t.Helper()
		sort.Strings(want)
		if set(got) != strings.Join(want, ",") {
			t.Errorf("%s: [%s], want [%s] — the bindings index's membership has a writer the event hooks do not own (#1458)",
				what, set(got), strings.Join(want, ","))
		}
	}
	check("functions that write the membership maps", writers, "enrolLocked", "resetLocked", "unrolLocked")
	check("callers of enrolLocked", callers["enrolLocked"], "recordBindingLocked")
	check("callers of recordBindingLocked", callers["recordBindingLocked"], "SeedBindingsIndexFromSnapshotForTest", "applyBindingAdd")
	check("callers of unrolLocked", callers["unrolLocked"], "applyBindingDelete")
	check("callers of applyBindingAdd", callers["applyBindingAdd"], "onBindingAdd", "onBindingUpdate")
	check("callers of applyBindingDelete", callers["applyBindingDelete"], "onBindingDelete", "onBindingUpdate")
	check("callers of resetLocked", callers["resetLocked"], "ResetBindingsByGVRIndexForTest", "SeedBindingsIndexFromSnapshotForTest", "beginBindingsIndexForWatcher")
	check("callers of SeedBindingsIndexFromSnapshotForTest", callers["SeedBindingsIndexFromSnapshotForTest"])
	check("callers of the hooks' onBindingAdd", callers["onBindingAdd"], "rbacSnapshotEventHandlers")

	// And no other package's production code stands an index up from the snapshot.
	var users []string
	err = filepath.WalkDir("../..", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		if strings.Contains(string(b), "SeedBindingsIndexFromSnapshotForTest") {
			users = append(users, filepath.ToSlash(path))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 1 || !strings.HasSuffix(users[0], "internal/cache/bindings_by_gvr.go") {
		t.Errorf("production files that name SeedBindingsIndexFromSnapshotForTest: %v, want only its definition", users)
	}
}
