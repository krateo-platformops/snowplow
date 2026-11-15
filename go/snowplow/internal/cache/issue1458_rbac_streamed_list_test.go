// issue1458_rbac_streamed_list_test.go — #1458 design premise, with
// STREAMING lists (WatchListClient on, as production runs): every RBAC object
// reaches the four RBAC handlers as an add, an update or a delete, at start and
// across a relist, so a fold of those calls equals the live objects.
//
// ONE ARM, TWO SERVERS. c1458StreamedListArm drives a real ResourceWatcher
// through the real dynamic client and client-go's real reflector against
// whatever apiserver the rest.Config names:
//   - a REAL apiserver, in the integration-tagged cluster arm
//     (issue1458_rbac_streaming_kind_integration_test.go);
//   - an httptest server that speaks the watch-list protocol (c1458Stub, below),
//     in TestIssue1458Char_StreamedListAndRelist_WatchListStub, so client-go's
//     streaming path runs in the unit suite too. The stub is the test's own
//     server: it proves nothing about a real apiserver's side.
//
// The fake dynamic client of the other #1458 characterisation arms cannot stream
// a list (the package's TestMain forces WatchListClient off for it).
//
// NOT A PLAIN LIST IN DISGUISE. The watcher's client goes through a recording
// transport. The arm FAILS unless, for each of the four RBAC kinds, the
// reflector's requests are WATCHes carrying sendInitialEvents=true answered 200
// and none is a plain collection GET: a client that fell back to list+watch
// fails here instead of passing as a plain list. The same holds for the relist.
//
// THE RELIST is forced from the client side by the same transport: the RBAC
// watch streams go deaf (nothing more is read from them), the world is changed
// through a second, unwrapped client, the streams are ended and the next WATCH
// of each kind is answered 410 Expired. The list that follows is the server's
// streamed one.
//
// ASSERTED: at start every object of the server's LIST reached the handler's
// ADD exactly once, flagged initial, before the handler registration's
// HasSynced; across the relist the created objects arrive as adds, the changed
// ones as updates with the last delivered and the live object, the deleted ones
// as deletes carrying a tombstone with the last known typed object; and a fold
// of every recorded call equals the server's LIST.
package cache

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/rest"
	clientcache "k8s.io/client-go/tools/cache"
)

const k1458NS = "issue1458-kind"

// k1458Req is one request of the watcher's client to an RBAC collection.
type k1458Req struct {
	resource          string
	watch             bool
	sendInitialEvents bool
	status            int
	synthetic         bool
}

func (r k1458Req) String() string {
	kind := "LIST"
	if r.watch {
		kind = "WATCH"
		if r.sendInitialEvents {
			kind = "WATCH+sendInitialEvents"
		}
	}
	s := fmt.Sprintf("%s %s -> %d", kind, r.resource, r.status)
	if r.synthetic {
		s += " (injected by the test)"
	}
	return s
}

// k1458Transport records the watcher's RBAC collection requests, can make the
// RBAC watch streams deaf, and can end them and answer the next WATCH with 410.
type k1458Transport struct {
	next http.RoundTripper

	mu     sync.Mutex
	cond   *sync.Cond
	reqs   []k1458Req
	deaf   bool
	bodies []*k1458Body
	expire map[string]int
}

func newK1458Transport(next http.RoundTripper) *k1458Transport {
	t := &k1458Transport{next: next, expire: map[string]int{}}
	t.cond = sync.NewCond(&t.mu)
	return t
}

// k1458Resource names the RBAC collection a cluster-wide path addresses, or "".
func k1458Resource(path string) string {
	const prefix = "/apis/rbac.authorization.k8s.io/v1/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	switch rest := strings.TrimPrefix(path, prefix); rest {
	case "roles", "rolebindings", "clusterroles", "clusterrolebindings":
		return rest
	}
	return ""
}

func (t *k1458Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	res := k1458Resource(req.URL.Path)
	if res == "" || req.Method != http.MethodGet {
		return t.next.RoundTrip(req)
	}
	q := req.URL.Query()
	rec := k1458Req{resource: res, watch: q.Get("watch") == "true" || q.Get("watch") == "1",
		sendInitialEvents: q.Get("sendInitialEvents") == "true"}
	if rec.watch {
		t.mu.Lock()
		inject := t.expire[res] > 0
		if inject {
			t.expire[res]--
			rec.status, rec.synthetic = http.StatusGone, true
			t.reqs = append(t.reqs, rec)
		}
		t.mu.Unlock()
		if inject {
			const body = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",` +
				`"message":"too old resource version (injected by the test)","reason":"Expired","code":410}`
			return &http.Response{
				Status: "410 Gone", StatusCode: http.StatusGone, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
				Body:          io.NopCloser(strings.NewReader(body)),
				ContentLength: int64(len(body)), Request: req,
			}, nil
		}
	}
	resp, err := t.next.RoundTrip(req)
	if err != nil {
		rec.status = -1
		t.mu.Lock()
		t.reqs = append(t.reqs, rec)
		t.mu.Unlock()
		return resp, err
	}
	rec.status = resp.StatusCode
	t.mu.Lock()
	t.reqs = append(t.reqs, rec)
	if rec.watch && resp.StatusCode == http.StatusOK {
		b := &k1458Body{t: t, inner: resp.Body}
		t.bodies = append(t.bodies, b)
		resp.Body = b
	}
	t.mu.Unlock()
	return resp, nil
}

// k1458Body is an RBAC watch stream. While the transport is deaf nothing read
// from the apiserver is handed on; once ended it reads EOF.
type k1458Body struct {
	t     *k1458Transport
	inner io.ReadCloser
	ended bool // guarded by t.mu
}

func (b *k1458Body) Read(p []byte) (int, error) {
	b.t.mu.Lock()
	for b.t.deaf && !b.ended {
		b.t.cond.Wait()
	}
	ended := b.ended
	b.t.mu.Unlock()
	if ended {
		return 0, io.EOF
	}
	n, err := b.inner.Read(p)
	// A read that was already waiting on the apiserver when the stream went
	// deaf must not hand its bytes on either.
	b.t.mu.Lock()
	for b.t.deaf && !b.ended {
		b.t.cond.Wait()
	}
	ended = b.ended
	b.t.mu.Unlock()
	if ended {
		return 0, io.EOF
	}
	return n, err
}

func (b *k1458Body) Close() error { return b.inner.Close() }

func (t *k1458Transport) setDeaf() {
	t.mu.Lock()
	t.deaf = true
	t.mu.Unlock()
}

// expireAll ends every RBAC watch stream and answers the next WATCH of each
// kind with 410 Expired.
func (t *k1458Transport) expireAll() {
	t.mu.Lock()
	for _, gvr := range c1458Kinds {
		t.expire[gvr.Resource] = 1
	}
	bodies := append([]*k1458Body(nil), t.bodies...)
	for _, b := range bodies {
		b.ended = true
	}
	t.deaf = false
	t.cond.Broadcast()
	t.mu.Unlock()
	for _, b := range bodies {
		_ = b.inner.Close()
	}
}

func (t *k1458Transport) mark() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.reqs)
}

func (t *k1458Transport) since(i int) []k1458Req {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]k1458Req(nil), t.reqs[i:]...)
}

// k1458AssertStreamed fails unless, for each RBAC kind, reqs holds a streamed
// list (a 200 WATCH with sendInitialEvents) and no plain collection GET.
func k1458AssertStreamed(t *testing.T, phase string, reqs []k1458Req) {
	t.Helper()
	for _, gvr := range c1458Kinds {
		streamed, plain := 0, 0
		var seen []string
		for _, r := range reqs {
			if r.resource != gvr.Resource {
				continue
			}
			seen = append(seen, r.String())
			switch {
			case !r.watch:
				plain++
			case r.sendInitialEvents && r.status == http.StatusOK:
				streamed++
			}
		}
		t.Logf("%s: %s requests of the watcher's client: %v", phase, gvr.Resource, seen)
		if streamed == 0 {
			t.Errorf("%s: NO STREAMED LIST for %s: no WATCH with sendInitialEvents=true was answered 200 (requests: %v)", phase, gvr.Resource, seen)
		}
		if plain != 0 {
			t.Errorf("%s: %d PLAIN LIST(s) of %s: the client fell back to list+watch, or something else listed the collection "+
				"through the watcher's client (requests: %v)", phase, plain, gvr.Resource, seen)
		}
	}
}

func k1458Create(t *testing.T, dyn dynamic.Interface, gvr schema.GroupVersionResource, ns string, obj map[string]any) {
	t.Helper()
	u := &unstructured.Unstructured{Object: obj}
	var err error
	if ns != "" {
		_, err = dyn.Resource(gvr).Namespace(ns).Create(context.Background(), u, metav1.CreateOptions{})
	} else {
		_, err = dyn.Resource(gvr).Create(context.Background(), u, metav1.CreateOptions{})
	}
	if err != nil {
		t.Fatalf("create %s %s/%s: %v", gvr.Resource, ns, u.GetName(), err)
	}
}

const k1458API = "rbac.authorization.k8s.io/v1"

func k1458RoleObj(kind, ns, name, resource string) map[string]any {
	meta := map[string]any{"name": name}
	if ns != "" {
		meta["namespace"] = ns
	}
	return map[string]any{"apiVersion": k1458API, "kind": kind, "metadata": meta,
		"rules": []any{map[string]any{"apiGroups": []any{""}, "resources": []any{resource}, "verbs": []any{"get", "list"}}}}
}

func k1458BindingObj(kind, ns, name, roleKind, role string, groups ...string) map[string]any {
	meta := map[string]any{"name": name}
	if ns != "" {
		meta["namespace"] = ns
	}
	var subjects []any
	for _, g := range groups {
		subjects = append(subjects, map[string]any{"kind": "Group", "apiGroup": "rbac.authorization.k8s.io", "name": g})
	}
	return map[string]any{"apiVersion": k1458API, "kind": kind, "metadata": meta, "subjects": subjects,
		"roleRef": map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": roleKind, "name": role}}
}

// k1458Live is the apiserver's LIST of one kind: key -> subject count (0 for roles).
func k1458Live(t *testing.T, dyn dynamic.Interface, gvr schema.GroupVersionResource) map[string]int {
	t.Helper()
	l, err := dyn.Resource(gvr).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("apiserver LIST %s: %v", gvr.Resource, err)
	}
	out := map[string]int{}
	for i := range l.Items {
		k := l.Items[i].GetName()
		if ns := l.Items[i].GetNamespace(); ns != "" {
			k = ns + "/" + k
		}
		subj, _, _ := unstructured.NestedSlice(l.Items[i].Object, "subjects")
		out[k] = len(subj)
	}
	return out
}

// k1458Fold folds every recorded call of one kind: key -> subject count.
func k1458Fold(calls []c1458Call, resource string) map[string]int {
	out := map[string]int{}
	for _, c := range calls {
		if c.resource != resource {
			continue
		}
		if c.call == "delete" {
			delete(out, c.key)
			continue
		}
		out[c.key] = c.subjects
	}
	return out
}

func k1458SameFold(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

func k1458DiffFold(got, want map[string]int) []string {
	var out []string
	for k, w := range want {
		if g, ok := got[k]; !ok {
			out = append(out, fmt.Sprintf("live %s (subjects=%d) is not in the fold", k, w))
		} else if g != w {
			out = append(out, fmt.Sprintf("%s: fold has subjects=%d, live has %d", k, g, w))
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			out = append(out, fmt.Sprintf("the fold holds %s, which is not live", k))
		}
	}
	sort.Strings(out)
	return out
}

// c1458StreamedListArm is the arm (see the file comment), against the apiserver
// restCfg names.
func c1458StreamedListArm(t *testing.T, restCfg *rest.Config) {
	t.Setenv("CACHE_ENABLED", "true")
	// Production runs the RBAC informers on streamed lists; the package's
	// TestMain forces WatchListClient off for the fakes, so opt in.
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)
	ctx := context.Background()

	// The test's own client: unwrapped, never seen by the recording transport.
	plainCfg := rest.CopyConfig(restCfg)
	plainCfg.QPS, plainCfg.Burst = 100, 200
	plain, err := dynamic.NewForConfig(plainCfg)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := plain.Resource(schema.GroupVersionResource{Version: "v1", Resource: "nodes"}).List(ctx, metav1.ListOptions{}); err == nil && len(v.Items) > 0 {
		kv, _, _ := unstructured.NestedString(v.Items[0].Object, "status", "nodeInfo", "kubeletVersion")
		t.Logf("cluster: kubelet %s", kv)
	}

	// --- the world before the watcher starts.
	nsGVR := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	k1458Create(t, plain, nsGVR, "", map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": k1458NS}})
	for i := 0; i < 25; i++ {
		n := fmt.Sprintf("k1458-fill-%d", i)
		k1458Create(t, plain, c1458RoleGVR, k1458NS, k1458RoleObj("Role", k1458NS, n, "configmaps"))
		k1458Create(t, plain, c1458CRGVR, "", k1458RoleObj("ClusterRole", "", n, "configmaps"))
		k1458Create(t, plain, c1458RBGVR, k1458NS, k1458BindingObj("RoleBinding", k1458NS, n, "Role", n, "k1458-g"))
		k1458Create(t, plain, c1458CRBGVR, "", k1458BindingObj("ClusterRoleBinding", "", n, "ClusterRole", n, "k1458-g"))
	}
	for _, n := range []string{"k1458-keep", "k1458-del", "k1458-edit"} {
		k1458Create(t, plain, c1458RoleGVR, k1458NS, k1458RoleObj("Role", k1458NS, n, "configmaps"))
		k1458Create(t, plain, c1458CRGVR, "", k1458RoleObj("ClusterRole", "", n, "configmaps"))
	}
	for _, n := range []string{"k1458-b-keep", "k1458-b-subj", "k1458-b-del"} {
		k1458Create(t, plain, c1458RBGVR, k1458NS, k1458BindingObj("RoleBinding", k1458NS, n, "Role", "k1458-keep", "k1458-g"))
		k1458Create(t, plain, c1458CRBGVR, "", k1458BindingObj("ClusterRoleBinding", "", n, "ClusterRole", "k1458-keep", "k1458-g"))
	}
	before := map[string]map[string]int{}
	for _, gvr := range c1458Kinds {
		before[gvr.Resource] = k1458Live(t, plain, gvr)
		t.Logf("apiserver holds %d %s before the watcher starts", len(before[gvr.Resource]), gvr.Resource)
	}

	// --- the watcher, over the recording transport.
	rec := c1458InstallRecorder(t)
	cfg := rest.CopyConfig(restCfg)
	var tr *k1458Transport
	cfg.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		tr = newK1458Transport(rt)
		return tr
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if tr == nil {
		t.Fatalf("SETUP: the recording transport was not installed")
	}

	// Sample each handler registration's HasSynced from before the start.
	type sample struct{ keys, deliveries int }
	atSync := map[schema.GroupVersionResource]sample{}
	var smu sync.Mutex
	stop, done := make(chan struct{}), make(chan struct{})
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
				smu.Lock()
				_, seen := atSync[gvr]
				smu.Unlock()
				if seen || !reg.HasSynced() {
					continue
				}
				d := directs[gvr]
				d.mu.Lock()
				s := sample{keys: len(d.initial)}
				for _, c := range d.initial {
					s.deliveries += c
				}
				d.mu.Unlock()
				smu.Lock()
				atSync[gvr] = s
				smu.Unlock()
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()

	rw, err := NewResourceWatcher(ctx, dyn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rw.Stop)
	// Registered after Stop, so it runs before it: a stream left deaf by a
	// failed arm must not hold the reflectors Stop waits for.
	t.Cleanup(tr.expireAll)
	sctx, scancel := context.WithTimeout(ctx, 120*time.Second)
	defer scancel()
	if err := rw.WaitForCacheSync(sctx, 120*time.Second); err != nil {
		t.Fatal(err)
	}
	if !c1458Wait(60*time.Second, func() bool { smu.Lock(); defer smu.Unlock(); return len(atSync) == len(c1458Kinds) }) {
		t.Fatalf("not every RBAC handler registration reported HasSynced")
	}
	close(stop)
	<-done
	startReqs := tr.since(0)
	k1458AssertStreamed(t, "AT START", startReqs)

	// --- the initial list: every object, exactly once, before HasSynced.
	c1458Stable(rec, 500*time.Millisecond, 30*time.Second)
	initial := map[string]map[string]int{}
	for _, c := range rec.since(0) {
		if c.call == "add" && c.initial {
			if initial[c.resource] == nil {
				initial[c.resource] = map[string]int{}
			}
			initial[c.resource][c.key]++
			if strings.HasPrefix(c.objType, "*Unstr") {
				t.Errorf("AT START: %s %s reached the handler untyped", c.resource, c.key)
			}
		}
	}
	for _, gvr := range c1458Kinds {
		s := atSync[gvr]
		missing, repeated := 0, 0
		for k := range before[gvr.Resource] {
			if initial[gvr.Resource][k] == 0 {
				missing++
			}
		}
		for _, n := range initial[gvr.Resource] {
			if n != 1 {
				repeated++
			}
		}
		t.Logf("AT START: %s: the apiserver held %d; the handler received %d initial ADDs for %d distinct objects; "+
			"when its registration's HasSynced first read true it had received %d for %d",
			gvr.Resource, len(before[gvr.Resource]), len(initial[gvr.Resource]), len(initial[gvr.Resource]), s.deliveries, s.keys)
		if missing != 0 || repeated != 0 {
			t.Errorf("AT START: %s: %d objects of the apiserver's LIST never reached the handler as an initial ADD, %d reached it more than once",
				gvr.Resource, missing, repeated)
		}
		if s.keys < len(before[gvr.Resource]) || s.deliveries != s.keys {
			t.Errorf("AT START: %s: the handler registration's HasSynced read true after %d initial ADDs for %d objects; the apiserver held %d",
				gvr.Resource, s.deliveries, s.keys, len(before[gvr.Resource]))
		}
	}
	foldEqualsLive := func(phase string) {
		t.Helper()
		for _, gvr := range c1458Kinds {
			var got, want map[string]int
			ok := c1458Wait(60*time.Second, func() bool {
				got, want = k1458Fold(rec.since(0), gvr.Resource), k1458Live(t, plain, gvr)
				return k1458SameFold(got, want)
			})
			t.Logf("%s: %s: the fold of the handler's calls holds %d objects, the apiserver %d", phase, gvr.Resource, len(got), len(want))
			if !ok {
				t.Errorf("%s: %s: THE FOLD OF THE HANDLER'S CALLS IS NOT THE APISERVER'S LIST: %v", phase, gvr.Resource, k1458DiffFold(got, want))
			}
		}
	}
	foldEqualsLive("AT START")

	// --- the watch goes deaf; the world changes behind it.
	tr.setDeaf()
	mark := rec.len()
	reqMark := tr.mark()
	must := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	k1458Create(t, plain, c1458RBGVR, k1458NS, k1458BindingObj("RoleBinding", k1458NS, "k1458-b-new", "Role", "k1458-keep", "k1458-g"))
	k1458Create(t, plain, c1458CRBGVR, "", k1458BindingObj("ClusterRoleBinding", "", "k1458-b-new", "ClusterRole", "k1458-keep", "k1458-g"))
	k1458Create(t, plain, c1458RoleGVR, k1458NS, k1458RoleObj("Role", k1458NS, "k1458-new", "configmaps"))
	k1458Create(t, plain, c1458CRGVR, "", k1458RoleObj("ClusterRole", "", "k1458-new", "configmaps"))
	update := func(gvr schema.GroupVersionResource, ns, name string, mutate func(u *unstructured.Unstructured)) {
		t.Helper()
		var r dynamic.ResourceInterface = plain.Resource(gvr)
		if ns != "" {
			r = plain.Resource(gvr).Namespace(ns)
		}
		u, err := r.Get(ctx, name, metav1.GetOptions{})
		must("get "+gvr.Resource+" "+name, err)
		mutate(u)
		_, err = r.Update(ctx, u, metav1.UpdateOptions{})
		must("update "+gvr.Resource+" "+name, err)
	}
	addSubject := func(u *unstructured.Unstructured) {
		subj, _, _ := unstructured.NestedSlice(u.Object, "subjects")
		subj = append(subj, map[string]any{"kind": "Group", "apiGroup": "rbac.authorization.k8s.io", "name": "k1458-added"})
		_ = unstructured.SetNestedSlice(u.Object, subj, "subjects")
	}
	setRules := func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedSlice(u.Object, []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"pods"}, "verbs": []any{"get"}}}, "rules")
	}
	update(c1458RBGVR, k1458NS, "k1458-b-subj", addSubject)
	update(c1458CRBGVR, "", "k1458-b-subj", addSubject)
	update(c1458RoleGVR, k1458NS, "k1458-edit", setRules)
	update(c1458CRGVR, "", "k1458-edit", setRules)
	must("delete rb", plain.Resource(c1458RBGVR).Namespace(k1458NS).Delete(ctx, "k1458-b-del", metav1.DeleteOptions{}))
	must("delete crb", plain.Resource(c1458CRBGVR).Delete(ctx, "k1458-b-del", metav1.DeleteOptions{}))
	must("delete role", plain.Resource(c1458RoleGVR).Namespace(k1458NS).Delete(ctx, "k1458-del", metav1.DeleteOptions{}))
	must("delete clusterrole", plain.Resource(c1458CRGVR).Delete(ctx, "k1458-del", metav1.DeleteOptions{}))

	time.Sleep(2 * time.Second)
	if n := rec.len() - mark; n != 0 {
		t.Fatalf("SETUP: %d handler calls arrived while the watch was deaf: the gap is not a gap: %v", n, rec.since(mark))
	}

	// --- the streams end; the next WATCH of each kind is answered 410: relist.
	tr.expireAll()
	nsKey := func(res, name string) string {
		if res == "roles" || res == "rolebindings" {
			return k1458NS + "/" + name
		}
		return name
	}
	type want struct{ res, name, call string }
	wants := []want{
		{"rolebindings", "k1458-b-new", "add"}, {"clusterrolebindings", "k1458-b-new", "add"},
		{"roles", "k1458-new", "add"}, {"clusterroles", "k1458-new", "add"},
		{"rolebindings", "k1458-b-subj", "update"}, {"clusterrolebindings", "k1458-b-subj", "update"},
		{"roles", "k1458-edit", "update"}, {"clusterroles", "k1458-edit", "update"},
		{"rolebindings", "k1458-b-del", "delete"}, {"clusterrolebindings", "k1458-b-del", "delete"},
		{"roles", "k1458-del", "delete"}, {"clusterroles", "k1458-del", "delete"},
	}
	for _, w := range wants {
		w := w
		if !c1458Wait(90*time.Second, func() bool {
			for _, c := range c1458Find(rec.since(mark), w.res, nsKey(w.res, w.name)) {
				if c.call == w.call {
					return true
				}
			}
			return false
		}) {
			t.Errorf("ACROSS THE RELIST: the %s of %s %s NEVER reached the handler", w.call, w.res, w.name)
		}
	}
	c1458Stable(rec, 500*time.Millisecond, 30*time.Second)
	relistReqs := tr.since(reqMark)
	injected := map[string]bool{}
	for _, r := range relistReqs {
		if r.synthetic {
			injected[r.resource] = true
		}
	}
	for _, gvr := range c1458Kinds {
		if !injected[gvr.Resource] {
			t.Errorf("SETUP: no WATCH of %s was answered 410: its relist was not forced", gvr.Resource)
		}
	}
	k1458AssertStreamed(t, "ACROSS THE RELIST", relistReqs)

	calls := rec.since(mark)
	changed := map[string]bool{}
	for _, w := range wants {
		changed[w.res+" "+nsKey(w.res, w.name)] = true
	}
	redelivered := 0
	for _, c := range calls {
		if changed[c.resource+" "+c.key] {
			t.Logf("ACROSS THE RELIST: %s subjects %d -> %d", c, c.oldSubjects, c.subjects)
			continue
		}
		redelivered++
		if c.call != "update" {
			t.Logf("ACROSS THE RELIST: an object the test did not touch arrived as %s", c)
		}
	}
	t.Logf("ACROSS THE RELIST: %d calls for objects the test did not touch (re-delivered by the relist, or written by a controller)", redelivered)
	one := func(res, name string) c1458Call {
		t.Helper()
		got := c1458Find(calls, res, nsKey(res, name))
		if len(got) != 1 {
			t.Errorf("ACROSS THE RELIST: %s %s: %d calls, want exactly 1: %v", res, name, len(got), got)
			return c1458Call{}
		}
		return got[0]
	}
	for _, w := range wants {
		c := one(w.res, w.name)
		binding := strings.HasSuffix(w.res, "bindings")
		switch w.call {
		case "add":
			if c.call != "add" || c.initial || strings.HasPrefix(c.objType, "*Unstr") {
				t.Errorf("ACROSS THE RELIST: %s %s created behind the gap: got %v, want one typed non-initial add", w.res, w.name, c)
			}
		case "update":
			if c.call != "update" || strings.HasPrefix(c.objType, "*Unstr") || strings.HasPrefix(c.oldType, "*Unstr") {
				t.Errorf("ACROSS THE RELIST: %s %s changed behind the gap: got %v, want one typed update", w.res, w.name, c)
			}
			if binding && (c.oldSubjects != 1 || c.subjects != 2) {
				t.Errorf("ACROSS THE RELIST: %s %s: the update carries subjects %d -> %d, want 1 -> 2 (the last delivered object, then the live one)",
					w.res, w.name, c.oldSubjects, c.subjects)
			}
		case "delete":
			if c.call != "delete" || !c.tomb || strings.HasPrefix(c.objType, "*Unstr") || c.objType == "nil" {
				t.Errorf("ACROSS THE RELIST: %s %s deleted behind the gap: got %v, want one delete carrying a tombstone with the last known typed object",
					w.res, w.name, c)
			}
			if binding && c.subjects != 1 {
				t.Errorf("ACROSS THE RELIST: %s %s: the tombstone's object carries %d subjects, want the last known 1", w.res, w.name, c.subjects)
			}
		}
	}
	foldEqualsLive("ACROSS THE RELIST")

	// --- and the watch that follows the relist is live.
	mark = rec.len()
	k1458Create(t, plain, c1458RBGVR, k1458NS, k1458BindingObj("RoleBinding", k1458NS, "k1458-b-after", "Role", "k1458-keep", "k1458-g"))
	if !c1458Wait(60*time.Second, func() bool {
		return len(c1458Find(rec.since(mark), "rolebindings", k1458NS+"/k1458-b-after")) > 0
	}) {
		t.Errorf("AFTER THE RELIST: a binding created through the live watch never reached the handler")
	}
	foldEqualsLive("AFTER THE RELIST")
}
