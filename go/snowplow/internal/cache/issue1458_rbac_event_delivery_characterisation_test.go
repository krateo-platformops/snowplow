// issue1458_rbac_event_delivery_characterisation_test.go — #1458 design, the
// premise of "the bindings index is a pure fold of the RBAC informers' events":
// every change of an RBAC object reaches the binding and role hooks as an add,
// an update or a delete, also across a relist, so a fold of those calls equals
// the live objects.
//
// CHARACTERISATION tests: they pin what the handlers RECEIVE today; no
// production code is changed. A real ResourceWatcher over the fake apiserver.
// The recorder is one more handler extension on the four RBAC kinds, attached
// through the same adapter chain as the RBAC snapshot handler (the one that
// calls the hooks), so it receives what that handler receives.
//
//	TestIssue1458Char_RelistAfterAGap      the watch goes deaf, the world
//	    changes behind it, the watch is ended with 410 Expired and the
//	    reflector relists. Recorded: the calls for each of the four kinds.
//	    Asserted: the created objects arrive as adds, the changed ones as
//	    updates carrying the new object, the deleted ones as deletes carrying a
//	    DeletedFinalStateUnknown tombstone with the last known object; and the
//	    index the real hooks maintain equals the live bindings afterwards.
//	TestIssue1458Char_RBACStoresAreNeverSwapped   the #870 store swap refuses
//	    the four RBAC kinds (their informers come from the shared factory).
//	TestIssue1458Char_InitialListReachesTheHandlerOnceBeforeItsHasSynced
//	    every existing object reaches the handler's ADD exactly once, flagged
//	    as initial, before the handler registration's HasSynced turns true; and
//	    no call arrives while nothing changes (resync period 0).
//	TestIssue1458Char_UnconvertibleObject  an object the typed conversion
//	    refuses: what the handler receives, what the hooks and the snapshot do.
//
// NOT covered (no real apiserver): streaming initial lists (WatchListClient,
// forced off in this package's TestMain), real watch timing.
package cache

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	clientcache "k8s.io/client-go/tools/cache"
)

const c1458Group = "rbac.authorization.k8s.io"

var (
	c1458RoleGVR = schema.GroupVersionResource{Group: c1458Group, Version: "v1", Resource: "roles"}
	c1458RBGVR   = schema.GroupVersionResource{Group: c1458Group, Version: "v1", Resource: "rolebindings"}
	c1458CRGVR   = schema.GroupVersionResource{Group: c1458Group, Version: "v1", Resource: "clusterroles"}
	c1458CRBGVR  = schema.GroupVersionResource{Group: c1458Group, Version: "v1", Resource: "clusterrolebindings"}
	c1458CMGVR   = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	c1458Kinds   = []schema.GroupVersionResource{c1458RoleGVR, c1458RBGVR, c1458CRGVR, c1458CRBGVR}
)

// c1458Call is one handler call.
type c1458Call struct {
	resource string
	call     string // add | update | delete
	key      string
	initial  bool
	objType  string
	oldType  string
	tomb     bool
	// subjects of the delivered binding (new side; the tombstone's object for a delete)
	subjects    int
	oldSubjects int
	rules       int
}

func (c c1458Call) String() string {
	s := fmt.Sprintf("%-19s %-6s %-22s obj=%s", c.resource, c.call, c.key, c.objType)
	if c.call == "update" {
		s += " old=" + c.oldType
	}
	if c.initial {
		s += " initial"
	}
	if c.tomb {
		s += " TOMBSTONE"
	}
	return s
}

type c1458Rec struct {
	mu    sync.Mutex
	calls []c1458Call
	// regs: the registration of a second, directly attached recorder per kind
	// (the extension attach discards its registration).
	regs   map[schema.GroupVersionResource]clientcache.ResourceEventHandlerRegistration
	direct map[schema.GroupVersionResource]*c1458Direct
}

// c1458Direct counts the initial-list adds of one kind, attached directly so
// its registration is kept.
type c1458Direct struct {
	mu       sync.Mutex
	initial  map[string]int
	nonInit  int
	otherOps int
}

func (d *c1458Direct) OnAdd(obj interface{}, isInInitialList bool) {
	k, _ := clientcache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	d.mu.Lock()
	defer d.mu.Unlock()
	if isInInitialList {
		d.initial[k]++
		return
	}
	d.nonInit++
}
func (d *c1458Direct) OnUpdate(_, _ interface{}) { d.mu.Lock(); d.otherOps++; d.mu.Unlock() }
func (d *c1458Direct) OnDelete(_ interface{})    { d.mu.Lock(); d.otherOps++; d.mu.Unlock() }

func c1458Describe(obj interface{}) (typ string, subjects, rules int) {
	switch o := obj.(type) {
	case *rbacv1.RoleBinding:
		return "*RoleBinding", len(o.Subjects), 0
	case *rbacv1.ClusterRoleBinding:
		return "*ClusterRoleBinding", len(o.Subjects), 0
	case *rbacv1.Role:
		return "*Role", 0, len(o.Rules)
	case *rbacv1.ClusterRole:
		return "*ClusterRole", 0, len(o.Rules)
	case *unstructured.Unstructured:
		return "*Unstructured", 0, 0
	case nil:
		return "nil", 0, 0
	default:
		return fmt.Sprintf("%T", obj), 0, 0
	}
}

type c1458Handler struct {
	rec *c1458Rec
	res string
}

func (h c1458Handler) OnAdd(obj interface{}, isInInitialList bool) {
	k, _ := clientcache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	typ, subj, rules := c1458Describe(obj)
	h.rec.add(c1458Call{resource: h.res, call: "add", key: k, initial: isInInitialList, objType: typ, subjects: subj, rules: rules})
}

func (h c1458Handler) OnUpdate(oldObj, newObj interface{}) {
	k, _ := clientcache.DeletionHandlingMetaNamespaceKeyFunc(newObj)
	typ, subj, rules := c1458Describe(newObj)
	oldTyp, oldSubj, _ := c1458Describe(oldObj)
	h.rec.add(c1458Call{resource: h.res, call: "update", key: k, objType: typ, oldType: oldTyp, subjects: subj, oldSubjects: oldSubj, rules: rules})
}

func (h c1458Handler) OnDelete(obj interface{}) {
	k, _ := clientcache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	c := c1458Call{resource: h.res, call: "delete", key: k}
	if tomb, ok := obj.(clientcache.DeletedFinalStateUnknown); ok {
		c.tomb = true
		obj = tomb.Obj
	}
	c.objType, c.subjects, c.rules = c1458Describe(obj)
	h.rec.add(c)
}

func (r *c1458Rec) add(c c1458Call) {
	r.mu.Lock()
	r.calls = append(r.calls, c)
	r.mu.Unlock()
}

func (r *c1458Rec) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// since returns the calls recorded from index i on.
func (r *c1458Rec) since(i int) []c1458Call {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]c1458Call(nil), r.calls[i:]...)
}

// c1458InstallRecorder registers the recorder as a handler extension on the
// four RBAC kinds and restores the registry at cleanup.
func c1458InstallRecorder(t *testing.T) *c1458Rec {
	t.Helper()
	rec := &c1458Rec{
		regs:   map[schema.GroupVersionResource]clientcache.ResourceEventHandlerRegistration{},
		direct: map[schema.GroupVersionResource]*c1458Direct{},
	}
	handlerExtMu.Lock()
	saved := append([]HandlerExtension(nil), handlerExtensions...)
	handlerExtMu.Unlock()
	t.Cleanup(func() {
		handlerExtMu.Lock()
		handlerExtensions = saved
		handlerExtMu.Unlock()
	})
	RegisterHandlerExtension(HandlerExtension{
		Name:      "test.c1458.recorder/" + t.Name(),
		Predicate: isTypedRBACGVR,
		Handlers: func(rw *ResourceWatcher, gvr schema.GroupVersionResource) clientcache.ResourceEventHandler {
			// Called under rw.mu, before the informer runs. A second recorder
			// is attached directly, to keep its registration (HasSynced).
			if gi := rw.informers[gvr]; gi != nil {
				d := &c1458Direct{initial: map[string]int{}}
				if reg, err := gi.Informer().AddEventHandler(d); err == nil {
					rec.mu.Lock()
					rec.regs[gvr] = reg
					rec.direct[gvr] = d
					rec.mu.Unlock()
				}
			}
			return c1458Handler{rec: rec, res: gvr.Resource}
		},
	})
	return rec
}

// c1458Gate proxies every RBAC watch of the fake apiserver, so a test can make
// the watch deaf (events dropped) and end it with 410 Expired.
type c1458Gate struct {
	mu      sync.Mutex
	deaf    bool
	current map[string]*watch.RaceFreeFakeWatcher
	sources map[string]watch.Interface
	opened  map[string]int
}

func c1458InstallGate(t *testing.T, dyn *dynamicfake.FakeDynamicClient) *c1458Gate {
	t.Helper()
	g := &c1458Gate{current: map[string]*watch.RaceFreeFakeWatcher{}, sources: map[string]watch.Interface{}, opened: map[string]int{}}
	dyn.PrependWatchReactor("*", func(a clienttesting.Action) (bool, watch.Interface, error) {
		wa, ok := a.(clienttesting.WatchAction)
		if !ok || !isTypedRBACGVR(wa.GetResource()) {
			return false, nil, nil
		}
		src, err := dyn.Tracker().Watch(wa.GetResource(), wa.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		out := watch.NewRaceFreeFake()
		res := wa.GetResource().Resource
		g.mu.Lock()
		if prev := g.sources[res]; prev != nil {
			prev.Stop()
		}
		g.current[res], g.sources[res] = out, src
		g.opened[res]++
		g.mu.Unlock()
		go func() {
			for ev := range src.ResultChan() {
				g.mu.Lock()
				deaf := g.deaf
				g.mu.Unlock()
				if deaf || out.IsStopped() {
					continue
				}
				out.Action(ev.Type, ev.Object)
			}
		}()
		return true, out, nil
	})
	t.Cleanup(func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		for _, s := range g.sources {
			s.Stop()
		}
	})
	return g
}

func (g *c1458Gate) setDeaf(v bool) { g.mu.Lock(); g.deaf = v; g.mu.Unlock() }

func (g *c1458Gate) watchesOpened() map[string]int {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := map[string]int{}
	for k, v := range g.opened {
		out[k] = v
	}
	return out
}

// expire ends every RBAC watch with 410 Expired: the reflector relists.
func (g *c1458Gate) expire() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, w := range g.current {
		w.Error(&metav1.Status{Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired, Message: "too old resource version"})
	}
}

func c1458U(t *testing.T, obj runtime.Object) *unstructured.Unstructured {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatalf("to unstructured: %v", err)
	}
	return &unstructured.Unstructured{Object: m}
}

func c1458Rules(resource string) []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{resource}}}
}

func c1458Role(ns, name string, rules []rbacv1.PolicyRule) *rbacv1.Role {
	return &rbacv1.Role{TypeMeta: metav1.TypeMeta{APIVersion: c1458Group + "/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Rules: rules}
}

func c1458CR(name string, rules []rbacv1.PolicyRule) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{TypeMeta: metav1.TypeMeta{APIVersion: c1458Group + "/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules}
}

func c1458Groups(names ...string) []rbacv1.Subject {
	var out []rbacv1.Subject
	for _, n := range names {
		out = append(out, rbacv1.Subject{Kind: "Group", APIGroup: c1458Group, Name: n})
	}
	return out
}

func c1458RB(ns, name, role string, groups ...string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{TypeMeta: metav1.TypeMeta{APIVersion: c1458Group + "/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID("uid-rb-" + name)},
		Subjects:   c1458Groups(groups...), RoleRef: rbacv1.RoleRef{APIGroup: c1458Group, Kind: "Role", Name: role}}
}

func c1458CRB(name, role string, groups ...string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{TypeMeta: metav1.TypeMeta{APIVersion: c1458Group + "/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-crb-" + name)},
		Subjects:   c1458Groups(groups...), RoleRef: rbacv1.RoleRef{APIGroup: c1458Group, Kind: "ClusterRole", Name: role}}
}

// c1458Watcher builds a real watcher over the fake apiserver holding seed.
func c1458Watcher(t *testing.T, gate bool, seed ...runtime.Object) (*ResourceWatcher, *dynamicfake.FakeDynamicClient, *c1458Gate) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	// Every object is held UNSTRUCTURED by the fake apiserver, as the dynamic
	// client's own writes are: its List refuses a mix of typed and untyped.
	untyped := make([]runtime.Object, 0, len(seed))
	for _, o := range seed {
		untyped = append(untyped, c1458U(t, o))
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		c1458CRBGVR: "ClusterRoleBindingList", c1458CRGVR: "ClusterRoleList", c1458RBGVR: "RoleBindingList",
		c1458RoleGVR: "RoleList", c1458CMGVR: "ConfigMapList",
	}, untyped...)
	var g *c1458Gate
	if gate {
		g = c1458InstallGate(t, dyn)
	}
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
	})
	sctx, sc := context.WithTimeout(context.Background(), 30*time.Second)
	defer sc()
	if err := rw.WaitForCacheSync(sctx, 30*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	if err := rw.WaitInitialRBACPublishForTest(30 * time.Second); err != nil {
		t.Fatalf("initial RBAC publish: %v", err)
	}
	return rw, dyn, g
}

// c1458Stable waits until the recorder has received nothing for quiet.
func c1458Stable(rec *c1458Rec, quiet, max time.Duration) {
	deadline := time.Now().Add(max)
	last, since := rec.len(), time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		if n := rec.len(); n != last {
			last, since = n, time.Now()
			continue
		}
		if time.Since(since) >= quiet {
			return
		}
	}
}

func c1458Wait(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// c1458IndexMembership renders what the index holds: binding → role key and
// subjects. Every role the binding is filed under is rendered (exactly one,
// when the index is right: a binding left under a role it no longer names
// shows as two), and a binding filed under a role with no entry shows too.
func c1458IndexMembership() map[string]string {
	idx := bindingsByGVRSingleton()
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	rolesOf := map[bindingID][]string{}
	for rk, set := range idx.membersByRole {
		for id := range set {
			rolesOf[id] = append(rolesOf[id], rk)
		}
	}
	out := map[string]string{}
	for id, e := range idx.members {
		var subj []string
		for _, s := range e.subjects {
			subj = append(subj, s.Kind+":"+s.Namespace+"/"+s.Name)
		}
		sort.Strings(subj)
		roles := rolesOf[id]
		sort.Strings(roles)
		out[string(id)] = strings.Join(roles, "+") + " <- " + strings.Join(subj, ",")
		if e.role != strings.Join(roles, "+") {
			out[string(id)] += " (entry names role " + e.role + ")"
		}
		delete(rolesOf, id)
	}
	for id, roles := range rolesOf {
		sort.Strings(roles)
		out[string(id)] = strings.Join(roles, "+") + " <- (NO ENTRY)"
	}
	return out
}

// c1458LiveMembership is the same rendering computed from the live objects of
// the fake apiserver, independently of every informer.
func c1458LiveMembership(t *testing.T, dyn *dynamicfake.FakeDynamicClient) map[string]string {
	t.Helper()
	out := map[string]string{}
	render := func(subjects []rbacv1.Subject) string {
		var subj []string
		for _, s := range subjectsFromRBAC(subjects) {
			subj = append(subj, s.Kind+":"+s.Namespace+"/"+s.Name)
		}
		sort.Strings(subj)
		return strings.Join(subj, ",")
	}
	crbs, err := dyn.Resource(c1458CRBGVR).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list crbs: %v", err)
	}
	for i := range crbs.Items {
		o := &rbacv1.ClusterRoleBinding{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(crbs.Items[i].Object, o); err != nil {
			continue
		}
		out[string(crbBindingID(o))] = roleRefKey("", o.RoleRef) + " <- " + render(o.Subjects)
	}
	rbs, err := dyn.Resource(c1458RBGVR).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list rbs: %v", err)
	}
	for i := range rbs.Items {
		o := &rbacv1.RoleBinding{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(rbs.Items[i].Object, o); err != nil {
			continue
		}
		out[string(rbBindingID(o))] = roleRefKey(o.Namespace, o.RoleRef) + " <- " + render(o.Subjects)
	}
	return out
}

func c1458DiffMembership(t *testing.T, what string, got, want map[string]string) {
	t.Helper()
	for id, w := range want {
		if g, ok := got[id]; !ok {
			t.Errorf("%s: live binding %s (%s) is NOT in the index", what, id, w)
		} else if g != w {
			t.Errorf("%s: binding %s: index holds %q, live is %q", what, id, g, w)
		}
	}
	for id, g := range got {
		if _, ok := want[id]; !ok {
			t.Errorf("%s: the index holds %s (%s), which is not a live binding", what, id, g)
		}
	}
}

func c1458Find(calls []c1458Call, resource, key string) []c1458Call {
	var out []c1458Call
	for _, c := range calls {
		if c.resource == resource && c.key == key {
			out = append(out, c)
		}
	}
	return out
}

func TestIssue1458Char_RelistAfterAGap(t *testing.T) {
	rec := c1458InstallRecorder(t)
	seed := []runtime.Object{
		c1458Role("a", "r-keep", c1458Rules("configmaps")),
		c1458Role("a", "r-del", c1458Rules("configmaps")),
		c1458Role("a", "r-edit", c1458Rules("configmaps")),
		c1458CR("cr-keep", c1458Rules("configmaps")),
		c1458CR("cr-del", c1458Rules("configmaps")),
		c1458CR("cr-edit", c1458Rules("configmaps")),
		c1458RB("a", "rb-keep", "r-keep", "g-keep"),
		c1458RB("a", "rb-subj", "r-keep", "g-subj"),
		c1458RB("a", "rb-del", "r-keep", "g-del"),
		c1458RB("a", "rb-on-del-role", "r-del", "g-roledel"),
		c1458RB("a", "rb-on-edit-role", "r-edit", "g-roleedit"),
		c1458CRB("crb-keep", "cr-keep", "g-keep"),
		c1458CRB("crb-subj", "cr-keep", "g-subj"),
		c1458CRB("crb-del", "cr-keep", "g-del"),
		c1458CRB("crb-on-del-role", "cr-del", "g-roledel"),
		c1458CRB("crb-on-edit-role", "cr-edit", "g-roleedit"),
	}
	_, dyn, gate := c1458Watcher(t, true, seed...)
	// The index is the one this watcher's own events fed (no seed); a pass's
	// index step gives it its navigated set.
	SetNavigatedGVRs([]schema.GroupVersionResource{c1458CMGVR})
	c1458DiffMembership(t, "at ready", c1458IndexMembership(), c1458LiveMembership(t, dyn))
	ResetPendingSubGenBumpsForTest()
	ResetRBACSubGenForTest()
	c1458Stable(rec, 200*time.Millisecond, 10*time.Second)
	opened0 := gate.watchesOpened()
	sg := func(group string) uint64 { return RBACSubGenForSubject("nobody-1458", []string{group}) }
	before := map[string]uint64{}
	for _, g := range []string{"g-keep", "g-subj", "g-del", "g-roledel", "g-roleedit", "g-new", "g-added"} {
		before[g] = sg(g)
	}

	// --- the watch goes deaf; the world changes behind it.
	gate.setDeaf(true)
	mark := rec.len()
	ctx := context.Background()
	must := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	_, err := dyn.Resource(c1458RBGVR).Namespace("a").Create(ctx, c1458U(t, c1458RB("a", "rb-new", "r-keep", "g-new")), metav1.CreateOptions{})
	must("create rb-new", err)
	_, err = dyn.Resource(c1458CRBGVR).Create(ctx, c1458U(t, c1458CRB("crb-new", "cr-keep", "g-new")), metav1.CreateOptions{})
	must("create crb-new", err)
	_, err = dyn.Resource(c1458RBGVR).Namespace("a").Update(ctx, c1458U(t, c1458RB("a", "rb-subj", "r-keep", "g-subj", "g-added")), metav1.UpdateOptions{})
	must("update rb-subj", err)
	_, err = dyn.Resource(c1458CRBGVR).Update(ctx, c1458U(t, c1458CRB("crb-subj", "cr-keep", "g-subj", "g-added")), metav1.UpdateOptions{})
	must("update crb-subj", err)
	must("delete rb-del", dyn.Resource(c1458RBGVR).Namespace("a").Delete(ctx, "rb-del", metav1.DeleteOptions{}))
	must("delete crb-del", dyn.Resource(c1458CRBGVR).Delete(ctx, "crb-del", metav1.DeleteOptions{}))
	must("delete r-del", dyn.Resource(c1458RoleGVR).Namespace("a").Delete(ctx, "r-del", metav1.DeleteOptions{}))
	must("delete cr-del", dyn.Resource(c1458CRGVR).Delete(ctx, "cr-del", metav1.DeleteOptions{}))
	_, err = dyn.Resource(c1458RoleGVR).Namespace("a").Update(ctx, c1458U(t, c1458Role("a", "r-edit", c1458Rules("pods"))), metav1.UpdateOptions{})
	must("update r-edit", err)
	_, err = dyn.Resource(c1458CRGVR).Update(ctx, c1458U(t, c1458CR("cr-edit", c1458Rules("pods"))), metav1.UpdateOptions{})
	must("update cr-edit", err)

	time.Sleep(300 * time.Millisecond)
	if n := rec.len() - mark; n != 0 {
		t.Fatalf("SETUP: %d handler calls arrived while the watch was deaf: the gap is not a gap", n)
	}
	if got := c1458IndexMembership(); got["R:a/uid-rb-rb-new"] != "" {
		t.Fatalf("SETUP: the index already holds the binding created behind the deaf watch")
	}

	// --- 410 Expired: the reflector relists.
	gate.setDeaf(false)
	gate.expire()
	arrived := func(resource, key, call string) func() bool {
		return func() bool {
			for _, c := range c1458Find(rec.since(mark), resource, key) {
				if c.call == call {
					return true
				}
			}
			return false
		}
	}
	for _, w := range []struct{ res, key, call string }{
		{"rolebindings", "a/rb-new", "add"}, {"clusterrolebindings", "crb-new", "add"},
		{"rolebindings", "a/rb-del", "delete"}, {"clusterrolebindings", "crb-del", "delete"},
		{"roles", "a/r-del", "delete"}, {"clusterroles", "cr-del", "delete"},
		{"rolebindings", "a/rb-subj", "update"}, {"clusterrolebindings", "crb-subj", "update"},
		{"roles", "a/r-edit", "update"}, {"clusterroles", "cr-edit", "update"},
	} {
		if !c1458Wait(15*time.Second, arrived(w.res, w.key, w.call)) {
			t.Errorf("NEVER ARRIVED after the relist: %s of %s %s", w.call, w.res, w.key)
		}
	}
	c1458Stable(rec, 300*time.Millisecond, 10*time.Second)
	if err := WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		t.Fatalf("RBAC rebuild did not quiesce: %v", err)
	}
	opened1 := gate.watchesOpened()
	for _, gvr := range c1458Kinds {
		if opened1[gvr.Resource] <= opened0[gvr.Resource] {
			t.Errorf("SETUP: the %s watch was not re-opened: no relist happened", gvr.Resource)
		}
	}

	calls := rec.since(mark)
	sort.SliceStable(calls, func(i, j int) bool {
		if calls[i].resource != calls[j].resource {
			return calls[i].resource < calls[j].resource
		}
		return calls[i].key < calls[j].key
	})
	for _, c := range calls {
		extra := ""
		switch {
		case strings.HasSuffix(c.resource, "bindings") && c.call == "update":
			extra = fmt.Sprintf(" subjects %d -> %d", c.oldSubjects, c.subjects)
		case strings.HasSuffix(c.resource, "bindings"):
			extra = fmt.Sprintf(" subjects=%d", c.subjects)
		}
		t.Logf("after the relist: %s%s", c, extra)
	}

	one := func(res, key string) c1458Call {
		t.Helper()
		got := c1458Find(calls, res, key)
		if len(got) != 1 {
			t.Errorf("%s %s: %d calls after the relist, want exactly 1: %v", res, key, len(got), got)
			return c1458Call{}
		}
		return got[0]
	}
	// created behind the gap: one add, not flagged initial, typed.
	for _, w := range [][2]string{{"rolebindings", "a/rb-new"}, {"clusterrolebindings", "crb-new"}} {
		if c := one(w[0], w[1]); c.call != "add" || c.initial || strings.HasPrefix(c.objType, "*Unstr") || c.subjects != 1 {
			t.Errorf("%s %s created behind the gap: got %v (subjects=%d), want one typed non-initial add", w[0], w[1], c, c.subjects)
		}
	}
	// changed behind the gap: one update, old = the last delivered object, new = the live one.
	for _, w := range [][2]string{{"rolebindings", "a/rb-subj"}, {"clusterrolebindings", "crb-subj"}} {
		if c := one(w[0], w[1]); c.call != "update" || c.oldSubjects != 1 || c.subjects != 2 {
			t.Errorf("%s %s changed behind the gap: got %v subjects %d -> %d, want one update 1 -> 2", w[0], w[1], c, c.oldSubjects, c.subjects)
		}
	}
	for _, w := range [][2]string{{"roles", "a/r-edit"}, {"clusterroles", "cr-edit"}} {
		if c := one(w[0], w[1]); c.call != "update" || strings.HasPrefix(c.objType, "*Unstr") {
			t.Errorf("%s %s changed behind the gap: got %v, want one typed update", w[0], w[1], c)
		}
	}
	// deleted behind the gap: one delete carrying a tombstone with the last known typed object.
	for _, w := range [][2]string{{"rolebindings", "a/rb-del"}, {"clusterrolebindings", "crb-del"}, {"roles", "a/r-del"}, {"clusterroles", "cr-del"}} {
		c := one(w[0], w[1])
		if c.call != "delete" || !c.tomb || strings.HasPrefix(c.objType, "*Unstr") || c.objType == "nil" {
			t.Errorf("%s %s deleted behind the gap: got %v, want one delete carrying a tombstone with the last known typed object", w[0], w[1], c)
		}
		if strings.HasSuffix(w[0], "bindings") && c.subjects != 1 {
			t.Errorf("%s %s: the tombstone's object carries %d subjects, want the last known 1", w[0], w[1], c.subjects)
		}
	}
	// untouched objects: whatever arrives must be an update with both sides typed.
	untouchedUpdates := 0
	for _, c := range calls {
		if strings.Contains(c.key, "keep") || strings.Contains(c.key, "on-") {
			untouchedUpdates++
			if c.call != "update" {
				t.Errorf("untouched %s %s: got a %s after the relist, want only updates", c.resource, c.key, c.call)
			}
		}
	}
	t.Logf("untouched objects re-delivered as updates by the relist: %d", untouchedUpdates)

	// --- the index the real hooks maintain equals the live bindings.
	c1458DiffMembership(t, "after the relist", c1458IndexMembership(), c1458LiveMembership(t, dyn))

	// --- and the real hooks bumped the subjects whose access changed.
	for _, g := range []string{"g-del", "g-roledel", "g-roleedit", "g-new", "g-added"} {
		if sg(g) <= before[g] {
			t.Errorf("the sub-generation of %s did not move across the relist (%d -> %d): the hooks did not act on its change", g, before[g], sg(g))
		}
	}
	t.Logf("sub-generations across the relist: g-keep %d->%d, g-subj %d->%d, g-del %d->%d, g-roledel %d->%d, g-roleedit %d->%d, g-new %d->%d, g-added %d->%d",
		before["g-keep"], sg("g-keep"), before["g-subj"], sg("g-subj"), before["g-del"], sg("g-del"), before["g-roledel"], sg("g-roledel"),
		before["g-roleedit"], sg("g-roleedit"), before["g-new"], sg("g-new"), before["g-added"], sg("g-added"))
}

func TestIssue1458Char_RBACStoresAreNeverSwapped(t *testing.T) {
	rw, _, _ := c1458Watcher(t, false,
		c1458Role("a", "r", c1458Rules("configmaps")), c1458RB("a", "rb", "r", "g"),
		c1458CR("cr", c1458Rules("configmaps")), c1458CRB("crb", "cr", "g"))
	for _, gvr := range c1458Kinds {
		rw.mu.RLock()
		in := rw.instances[gvr]
		rw.mu.RUnlock()
		if in == nil {
			t.Fatalf("%s has no store instance", gvr.Resource)
		}
		owned, known, _ := storeVerificationOwnership(gvr)
		t.Logf("%s: shape owned=%v; verification registry known=%v owned=%v", gvr.Resource, in.shape.owned(), known, owned)
		if in.shape.owned() || owned {
			t.Errorf("%s: the informer is OWNED, so a #870 swap can replace its store", gvr.Resource)
		}
		for _, reason := range []string{reasonDivergence, reasonSchema} {
			if rw.requestStoreBuild(gvr, reason, []string{"a/rb"}) {
				t.Errorf("%s: a store build was ACCEPTED for reason %q", gvr.Resource, reason)
			}
		}
		if b := rw.storeBuildForTest(gvr); b != nil {
			t.Errorf("%s: a build copy exists after the refused requests", gvr.Resource)
		}
	}
}

func TestIssue1458Char_InitialListReachesTheHandlerOnceBeforeItsHasSynced(t *testing.T) {
	const n = 400
	rec := c1458InstallRecorder(t)
	var seed []runtime.Object
	for i := 0; i < n; i++ {
		nm := fmt.Sprintf("o-%d", i)
		seed = append(seed, c1458Role("a", nm, c1458Rules("configmaps")), c1458CR(nm, c1458Rules("configmaps")),
			c1458RB("a", nm, nm, "g"), c1458CRB(nm, nm, "g"))
	}
	// Sample each registration's HasSynced from before the watcher starts.
	type sample struct{ initialKeys, deliveries int }
	atSync := map[schema.GroupVersionResource]sample{}
	var mu sync.Mutex
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			rec.mu.Lock()
			regs := map[schema.GroupVersionResource]clientcache.ResourceEventHandlerRegistration{}
			for k, v := range rec.regs {
				regs[k] = v
			}
			directs := map[schema.GroupVersionResource]*c1458Direct{}
			for k, v := range rec.direct {
				directs[k] = v
			}
			rec.mu.Unlock()
			for gvr, reg := range regs {
				mu.Lock()
				_, seen := atSync[gvr]
				mu.Unlock()
				if seen || !reg.HasSynced() {
					continue
				}
				// HasSynced is true: read the count AFTER it.
				d := directs[gvr]
				d.mu.Lock()
				s := sample{initialKeys: len(d.initial)}
				for _, c := range d.initial {
					s.deliveries += c
				}
				d.mu.Unlock()
				mu.Lock()
				atSync[gvr] = s
				mu.Unlock()
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()
	_, _, _ = c1458Watcher(t, false, seed...)
	if !c1458Wait(15*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(atSync) == len(c1458Kinds) }) {
		t.Fatalf("not every RBAC handler registration reported HasSynced")
	}
	close(stop)
	<-done
	for _, gvr := range c1458Kinds {
		s := atSync[gvr]
		t.Logf("%s: when the handler registration's HasSynced was first seen true, the handler had received %d initial ADDs for %d distinct objects (world: %d)",
			gvr.Resource, s.deliveries, s.initialKeys, n)
		if s.initialKeys != n || s.deliveries != n {
			t.Errorf("%s: HasSynced turned true with %d initial ADDs for %d objects delivered, want %d and %d", gvr.Resource, s.deliveries, s.initialKeys, n, n)
		}
	}
	// The extension recorder (the snapshot handler's chain) saw the same.
	c1458Stable(rec, 200*time.Millisecond, 10*time.Second)
	perKind := map[string]map[string]int{}
	for _, c := range rec.since(0) {
		if c.call != "add" || !c.initial {
			t.Errorf("during the initial list the handler received %v, want only initial adds", c)
			continue
		}
		if perKind[c.resource] == nil {
			perKind[c.resource] = map[string]int{}
		}
		perKind[c.resource][c.key]++
	}
	for _, gvr := range c1458Kinds {
		twice := 0
		for _, c := range perKind[gvr.Resource] {
			if c != 1 {
				twice++
			}
		}
		if len(perKind[gvr.Resource]) != n || twice != 0 {
			t.Errorf("%s: through the adapter chain %d distinct objects were added, %d of them not exactly once; want %d and 0",
				gvr.Resource, len(perKind[gvr.Resource]), twice, n)
		}
	}
	// Resync period 0: nothing arrives while nothing changes.
	mark := rec.len()
	time.Sleep(2 * time.Second)
	if got := rec.len() - mark; got != 0 {
		t.Errorf("%d handler calls arrived in 2 s with no change in the world (a periodic resync?)", got)
	}
	for _, gvr := range c1458Kinds {
		d := rec.direct[gvr]
		d.mu.Lock()
		if d.otherOps != 0 || d.nonInit != 0 {
			t.Errorf("%s: the direct recorder received %d updates/deletes and %d non-initial adds with no change in the world", gvr.Resource, d.otherOps, d.nonInit)
		}
		d.mu.Unlock()
	}
}

func TestIssue1458Char_UnconvertibleObject(t *testing.T) {
	rec := c1458InstallRecorder(t)
	_, dyn, _ := c1458Watcher(t, false,
		c1458Role("a", "r", c1458Rules("configmaps")), c1458RB("a", "rb-good", "r", "g-good"))
	// The index is the one this watcher's own events fed (no seed); a pass's
	// index step gives it its navigated set.
	SetNavigatedGVRs([]schema.GroupVersionResource{c1458CMGVR})
	c1458Stable(rec, 200*time.Millisecond, 10*time.Second)
	mark := rec.len()
	dropped := BindingsIndexDeltaSkippedNonTyped()

	// A RoleBinding whose subjects field is not a list: the typed conversion refuses it.
	bad := c1458U(t, c1458RB("a", "rb-bad", "r", "g-bad"))
	bad.Object["subjects"] = "not-a-list"
	ctx := context.Background()
	if _, err := dyn.Resource(c1458RBGVR).Namespace("a").Create(ctx, bad, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the unconvertible binding: %v", err)
	}
	if !c1458Wait(10*time.Second, func() bool { return len(c1458Find(rec.since(mark), "rolebindings", "a/rb-bad")) > 0 }) {
		t.Fatalf("the unconvertible binding never reached the handler")
	}
	if err := WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	inSnapshot := func(name string) bool {
		for _, rb := range rbacSnap.Load().RoleBindingsByNS["a"] {
			if rb.Name == name {
				return true
			}
		}
		return false
	}
	c := c1458Find(rec.since(mark), "rolebindings", "a/rb-bad")[0]
	_, inIndex := c1458IndexMembership()["R:a/uid-rb-rb-bad"]
	t.Logf("unconvertible binding created: the handler received %v; in the index=%v; in the snapshot=%v; hook drops counted=%d",
		c, inIndex, inSnapshot("rb-bad"), BindingsIndexDeltaSkippedNonTyped()-dropped)
	if c.call != "add" || c.objType != "*Unstructured" {
		t.Errorf("the handler received %v, want an add carrying the untyped object", c)
	}
	if inIndex || inSnapshot("rb-bad") {
		t.Errorf("the unconvertible binding is in the index (%v) or the snapshot (%v); want neither", inIndex, inSnapshot("rb-bad"))
	}
	if BindingsIndexDeltaSkippedNonTyped() == dropped {
		t.Errorf("the hook's drop was not counted")
	}

	// It is corrected: an update whose old side is untyped and whose new side is typed.
	mark = rec.len()
	if _, err := dyn.Resource(c1458RBGVR).Namespace("a").Update(ctx, c1458U(t, c1458RB("a", "rb-bad", "r", "g-bad")), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("correct the binding: %v", err)
	}
	if !c1458Wait(10*time.Second, func() bool { return len(c1458Find(rec.since(mark), "rolebindings", "a/rb-bad")) > 0 }) {
		t.Fatalf("the corrected binding never reached the handler")
	}
	if !c1458Wait(10*time.Second, func() bool { return inSnapshot("rb-bad") }) {
		t.Errorf("the corrected binding never entered the snapshot")
	}
	c = c1458Find(rec.since(mark), "rolebindings", "a/rb-bad")[0]
	_, inIndex = c1458IndexMembership()["R:a/uid-rb-rb-bad"]
	t.Logf("binding corrected: the handler received %v; in the index=%v; in the snapshot=%v", c, inIndex, inSnapshot("rb-bad"))
	if c.call != "update" || c.oldType != "*Unstructured" || c.objType != "*RoleBinding" {
		t.Errorf("the handler received %v, want an update from the untyped to the typed object", c)
	}
	if !inIndex {
		t.Errorf("the corrected binding is not in the index")
	}

	// It is broken again, then deleted.
	mark = rec.len()
	if _, err := dyn.Resource(c1458RBGVR).Namespace("a").Update(ctx, bad, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("break the binding again: %v", err)
	}
	if !c1458Wait(10*time.Second, func() bool { return !inSnapshot("rb-bad") }) {
		t.Errorf("the re-broken binding stayed in the snapshot")
	}
	_, inIndex = c1458IndexMembership()["R:a/uid-rb-rb-bad"]
	t.Logf("binding broken again: in the index=%v; in the snapshot=%v", inIndex, inSnapshot("rb-bad"))
	if inIndex {
		t.Errorf("the re-broken binding stayed in the index")
	}
	if err := dyn.Resource(c1458RBGVR).Namespace("a").Delete(ctx, "rb-bad", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !c1458Wait(10*time.Second, func() bool {
		for _, c := range c1458Find(rec.since(mark), "rolebindings", "a/rb-bad") {
			if c.call == "delete" {
				return true
			}
		}
		return false
	}) {
		t.Errorf("the delete of the unconvertible binding never reached the handler")
	}
	if err := WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	c1458DiffMembership(t, "at the end", c1458IndexMembership(), c1458LiveMembership(t, dyn))
}
