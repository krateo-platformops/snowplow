// issue547_known_stale_test.go — #547: a cell whose latest dirty mark was not
// closed by an accepted re-Put is KNOWN-STALE, and that state outlives the
// failed refresh.
//
// THE DEFECT (architect-547 trace, #547 comment 6140421686). A portal write
// UPDATEs a composition; the informer dirty-marks every widget cell that read it;
// the refresher re-resolves the cell and FAILS under the background identity
// (a refresher-only 403, a stage error, #608's `resolve widget: unknown`). The
// refresh ends without a Put and dirty_window.go:182 DELETES the stamp: the old
// body stays resident, the cell looks exactly like a fresh one, every reload is
// a hit on the pre-change body, and stale_served_total stops counting at the
// very moment the user starts seeing stale data.
//
// THE SHAPE. Every arm drives the production boundary end to end:
//
//	customer GET-by-name /call (Widgets().ServeHTTP, ?extras={name,namespace})
//	→ cold resolve → per-cohort Put + inner-call dep edge
//	→ a REAL informer UPDATE through the dynamic fake (deps_watch UpdateFunc)
//	→ dep worker → onDirtyMark → refresher processNext → resolveAndPopulateL1
//	→ the next customer GET.
//
// K=2 cohorts (two users on two different bindings, so two per-cohort keys) ×
// M=3 compositions. Composition m=1 is the one that changes.
//
// THE INJECTION is the one seam BOTH callers resolve through (widgetsResolveFn;
// resolveWidgetForRefresh calls it since #547): it denies when
// cache.BackgroundResolveFromContext(ctx) is true and serves the customer. That
// is the measured live asymmetry — the refresher identity fails where the
// customer's own succeeds — without special-casing any resolver.
//
// ARMS
//
//	F1  drop: the background refresh fails across the whole budget and is
//	    dropped. RED on 9c233de9: the next 5 GETs serve the pre-UPDATE body.
//	F2  stage-error decline under the background identity, past K declines.
//	F4  the detector (folded into F1/F2): known_stale_served_total equals the
//	    GETs that received the pre-UPDATE body. RED: the stat does not exist.
//	F5  owner ruling 1 — the demand revalidation ALSO fails: the body keeps
//	    being served (200, never evicted), every serve is counted, and every
//	    read retries; once the cohort can resolve again, the next read heals.
//	F6  coordinator — the revalidation runs on a detached, deadline-bounded ctx
//	    carrying the requester's identity and never blocks the response.
//	F7  owner ruling 3 / coordinator — a known-stale identity-free widgetContent
//	    cell is READ THROUGH, and the fall-through never writes the shared cell.
//
// The apistage content arm (F3) lives with the content reader:
// internal/resolvers/restactions/api/issue547_content_read_through_test.go.
package dispatchers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var (
	k547CompGVR   = schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1-2-2", Resource: "fireworksapps"}
	k547WidgetGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "tables"}
)

const (
	k547CompNS  = "demo-system"
	k547WNS     = "krateo-system"
	k547WName   = "list-composition-detail-conditions"
	k547M       = 3
	k547Changed = 1 // the composition the portal write UPDATEs
)

// k547Cohorts are the K=2 cohorts: different users on different bindings, so
// each derives its own per-cohort widgets key.
var k547Cohorts = []struct{ user, group, uid string }{
	{"alice", "devs", "uid-547-a"},
	{"bob", "ops", "uid-547-b"},
}

// k547Mode selects what the BACKGROUND (refresher) identity sees.
type k547Mode int

const (
	k547DenyBackground       k547Mode = iota // a refresher-only 403 → retried → dropped
	k547StageErrorBackground                 // a continueOnError stage error → declined
)

type k547Harness struct {
	t    *testing.T
	dyn  dynamic.Interface
	mode k547Mode

	bgCalls   atomic.Int64 // background (refresher) resolves of the changed composition
	custCalls atomic.Int64 // customer-shaped resolves of the changed composition

	// failCustomer makes the CUSTOMER-shaped resolve of the changed composition
	// fail too (F5: a real failure for the cohort, not an identity mismatch).
	failCustomer atomic.Bool

	mu sync.Mutex
	// block, when set, parks a customer-shaped resolve of the changed composition
	// that is NOT a top-level customer GET (F6): entered is closed when it parks,
	// release unparks it. seen records what that resolve's ctx carried.
	block   *k547Block
	widgetU *unstructured.Unstructured
}

type k547Block struct {
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	user        string
	hasDeadline bool
}

func k547Composition(m int, reason string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k547CompGVR.Group + "/" + k547CompGVR.Version,
		"kind":       "FireworksApp",
		"metadata":   map[string]any{"name": fmt.Sprintf("comp-%d", m), "namespace": k547CompNS},
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "reason": reason},
		}},
	}}
}

func k547Widget() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k547WidgetGVR.Group + "/" + k547WidgetGVR.Version,
		"kind":       "Table",
		"metadata":   map[string]any{"name": k547WName, "namespace": k547WNS},
		// The Conditions card is keyed by the composition it renders.
		"spec": map[string]any{"keyExtras": []any{"name", "namespace"}},
	}}
}

// newK547Harness wires a cache-on watcher whose informers serve the widget CR
// and M compositions, an RBAC snapshot granting both cohorts, the resolve
// seam, and the real refresher loop behind the production resolveAndPopulateL1.
func newK547Harness(t *testing.T, mode k547Mode) *k547Harness {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	i187RefresherEnv(t)
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetRefresherForTest()

	rbacGVR := func(r string) schema.GroupVersionResource {
		return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: r}
	}
	listKinds := map[schema.GroupVersionResource]string{
		k547CompGVR:                    "FireworksAppList",
		k547WidgetGVR:                  "TableList",
		rbacGVR("clusterrolebindings"): "ClusterRoleBindingList",
		rbacGVR("clusterroles"):        "ClusterRoleList",
		rbacGVR("rolebindings"):        "RoleBindingList",
		rbacGVR("roles"):               "RoleList",
	}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "k547-reader"}, Rules: []rbacv1.PolicyRule{{
			Verbs:     []string{"get", "list"},
			APIGroups: []string{k547CompGVR.Group, k547WidgetGVR.Group},
			Resources: []string{k547CompGVR.Resource, k547WidgetGVR.Resource},
		}}},
		k547Widget(),
	}
	for _, c := range k547Cohorts {
		seed = append(seed, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "k547-" + c.user, UID: types.UID(c.uid)},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: c.user}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "k547-reader"},
		})
	}
	for m := 0; m < k547M; m++ {
		seed = append(seed, k547Composition(m, "v0"))
	}
	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	for _, gvr := range []schema.GroupVersionResource{k547CompGVR, k547WidgetGVR} {
		_, syncCh := rw.EnsureResourceType(gvr)
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("informer %s did not sync", gvr)
		}
	}
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		t.Fatalf("WaitInitialRBACPublishForTest: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})

	h := &k547Harness{t: t, dyn: dyn, mode: mode, widgetU: k547Widget()}
	r1 := setFetchObjectForTest(func(req *http.Request) objects.Result {
		return objects.Result{GVR: k547WidgetGVR, Unstructured: h.widgetU.DeepCopy()}
	})
	r2 := setCheckDispatchRBACForTest(func(context.Context, schema.GroupVersionResource, string) bool { return true })
	r3 := setWidgetsResolveForTest(h.resolve)
	t.Cleanup(func() { r3(); r2(); r1() })

	// The production RefreshFunc body, for the per-cohort class and the
	// identity-free widget content class.
	refresh := func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		return resolveAndPopulateL1(ctx, in, nil, nil)
	}
	cache.RegisterRefreshFunc("widgets", refresh)
	cache.RegisterRefreshFunc(cache.CacheEntryClassWidgetContent, refresh)
	rctx, rcancel := context.WithCancel(context.Background())
	cache.StartRefresher(rctx)
	t.Cleanup(func() { rcancel(); cache.ResetRefresherForTest() })
	return h
}

// resolve is the shared widgetsResolveFn seam. It reads the composition named
// by the extras LIVE, records the inner-call dep edge on the L1 key being
// filled (resolve.go's innerCall edge, on whatever key the caller threaded),
// and renders its conditions into status.widgetData.
func (h *k547Harness) resolve(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
	name, _ := opts.Extras["name"].(string)
	ns, _ := opts.Extras["namespace"].(string)
	changed := name == fmt.Sprintf("comp-%d", k547Changed)
	bg := cache.BackgroundResolveFromContext(ctx)
	if changed {
		if bg {
			h.bgCalls.Add(1)
		} else {
			h.custCalls.Add(1)
		}
	}
	if bg {
		switch h.mode {
		case k547DenyBackground:
			return nil, apierrors.NewForbidden(schema.GroupResource{Group: k547CompGVR.Group, Resource: k547CompGVR.Resource},
				name, errors.New("refresher identity denied (injected at the shared resolve seam)"))
		case k547StageErrorBackground:
			if s := cache.StageErrorSinkFromContext(ctx); s != nil {
				s.Bump("getComposition", "403 under the refresher identity (injected)")
			}
		}
	} else if changed {
		if h.failCustomer.Load() {
			return nil, apierrors.NewForbidden(schema.GroupResource{Group: k547CompGVR.Group, Resource: k547CompGVR.Resource},
				name, errors.New("cohort genuinely denied (injected, F5)"))
		}
		h.mu.Lock()
		b := h.block
		h.mu.Unlock()
		if b != nil {
			parked := false
			b.once.Do(func() {
				ui, _ := xcontext.UserInfo(ctx)
				_, b.hasDeadline = ctx.Deadline()
				b.user = ui.Username
				close(b.entered)
				parked = true
			})
			if parked {
				<-b.release
			}
		}
	}
	u, err := h.dyn.Resource(k547CompGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if l1 := cache.L1KeyFromContext(ctx); l1 != "" {
		cache.Deps().Record(ctx, l1, k547CompGVR, ns, name)
	}
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	out := opts.In.DeepCopy()
	out.Object["status"] = map[string]any{"widgetData": map[string]any{"conditions": conds}}
	return out, nil
}

func k547ReqCtx(k int) context.Context {
	return xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(jwtutil.UserInfo{
		Username: k547Cohorts[k].user, Groups: []string{k547Cohorts[k].group},
	}))
}

func k547Extras(m int) map[string]any {
	return map[string]any{"name": fmt.Sprintf("comp-%d", m), "namespace": k547CompNS}
}

// get issues the production GET-by-name /call for cohort k, composition m and
// returns the rendered condition reason ("" when absent) and the status code.
func (h *k547Harness) get(k, m int) (string, int) {
	return h.getCtx(k547ReqCtx(k), m)
}

func (h *k547Harness) getCtx(ctx context.Context, m int) (string, int) {
	h.t.Helper()
	ex, _ := json.Marshal(k547Extras(m))
	u := "/call?apiVersion=" + url.QueryEscape(k547WidgetGVR.Group+"/"+k547WidgetGVR.Version) +
		"&resource=" + k547WidgetGVR.Resource + "&name=" + k547WName + "&namespace=" + k547WNS +
		"&extras=" + url.QueryEscape(string(ex))
	rec := httptest.NewRecorder()
	Widgets().ServeHTTP(rec, httptest.NewRequest("GET", u, nil).WithContext(ctx))
	return k547Reason(rec.Body.Bytes()), rec.Code
}

func k547Reason(body []byte) string {
	var obj map[string]any
	if json.Unmarshal(body, &obj) != nil {
		return ""
	}
	conds, _, _ := unstructured.NestedSlice(obj, "status", "widgetData", "conditions")
	if len(conds) == 0 {
		return ""
	}
	c, _ := conds[0].(map[string]any)
	r, _ := c["reason"].(string)
	return r
}

// key is the per-cohort widgets key cohort k derives for composition m.
func (h *k547Harness) key(k, m int) string {
	h.t.Helper()
	ctx := k547ReqCtx(k)
	key, handle, _ := dispatchCacheLookupKey(ctx, "widgets",
		k547WidgetGVR.Group, k547WidgetGVR.Version, k547WidgetGVR.Resource, k547WNS, k547WName, -1, -1,
		effectiveKeyExtras(ctx, h.widgetU.Object, k547Extras(m)))
	if handle == nil || key == "" {
		h.t.Fatalf("PRECONDITION: cohort %d derives no cacheable widgets key", k)
	}
	return key
}

func (h *k547Harness) residentReason(key string) (string, bool) {
	e, ok := cache.ResolvedCache().GetNoTouch(key)
	if !ok {
		return "", false
	}
	return k547Reason(e.RawJSON), true
}

// warm fills all K×M per-cohort cells through cold customer GETs.
func (h *k547Harness) warm() {
	h.t.Helper()
	seen := map[string]bool{}
	for k := range k547Cohorts {
		for m := 0; m < k547M; m++ {
			if r, code := h.get(k, m); code != 200 || r != "v0" {
				h.t.Fatalf("warm (k=%d,m=%d): code=%d reason=%q, want 200 v0", k, m, code, r)
			}
			key := h.key(k, m)
			if r, ok := h.residentReason(key); !ok || r != "v0" {
				h.t.Fatalf("warm (k=%d,m=%d): cell not resident with v0 (ok=%v reason=%q)", k, m, ok, r)
			}
			seen[key] = true
		}
	}
	if len(seen) != len(k547Cohorts)*k547M {
		h.t.Fatalf("PRECONDITION: %d distinct cells, want K×M=%d", len(seen), len(k547Cohorts)*k547M)
	}
}

// update writes reason onto composition m through the dynamic client and waits
// until the informer UPDATE has reached OnObjectEvent.
func (h *k547Harness) update(m int, reason string) {
	h.t.Helper()
	seq0 := cache.DepEventSeqForTest()
	if _, err := h.dyn.Resource(k547CompGVR).Namespace(k547CompNS).Update(context.Background(),
		k547Composition(m, reason), metav1.UpdateOptions{}); err != nil {
		h.t.Fatalf("update comp-%d: %v", m, err)
	}
	for end := time.Now().Add(5 * time.Second); cache.DepEventSeqForTest() == seq0; time.Sleep(2 * time.Millisecond) {
		if time.Now().After(end) {
			h.t.Fatalf("the comp-%d UPDATE never reached OnObjectEvent", m)
		}
	}
}

func k547Stat(stat string) int64 { return cache.RefresherStatsByStat()[stat] }

func k547WaitFor(t *testing.T, d time.Duration, what string, cond func() bool) bool {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return cond()
}

// knownStaleServed reads the #547 detector off the published snowplow_resolved_cache
// stat map. ok=false: the stat does not exist on this build.
func k547KnownStaleServed() (int64, bool) {
	v, ok := cache.ResolvedCacheStatsByStat()["known_stale_served_total"]
	return v, ok
}

func k547KnownStaleServedReason(reason string) int64 {
	return cache.ResolvedCacheStatsByStat()["known_stale_served_"+reason+"_total"]
}

// driveChangedToKnownStale UPDATEs composition m=1 and waits until both cohorts'
// background refreshes have ENDED unfresh (dropped / declined).
func (h *k547Harness) driveChangedToKnownStale(reason string) {
	h.t.Helper()
	switch h.mode {
	case k547DenyBackground:
		d0 := k547Stat("dirty_ended_unfresh_dropped")
		h.update(k547Changed, reason)
		if !k547WaitFor(h.t, 30*time.Second, "both cells dropped", func() bool {
			return k547Stat("dirty_ended_unfresh_dropped")-d0 >= int64(len(k547Cohorts))
		}) {
			h.t.Fatalf("PRECONDITION: the background refreshes never reached the drop point (dropped +%d, bg resolves %d)",
				k547Stat("dirty_ended_unfresh_dropped")-d0, h.bgCalls.Load())
		}
	case k547StageErrorBackground:
		d0 := k547Stat("dirty_ended_unfresh_declined")
		h.update(k547Changed, reason)
		if !k547WaitFor(h.t, 15*time.Second, "both cells declined", func() bool {
			return k547Stat("dirty_ended_unfresh_declined")-d0 >= int64(len(k547Cohorts))
		}) {
			h.t.Fatalf("PRECONDITION: the background refreshes never ended declined (declined +%d)",
				k547Stat("dirty_ended_unfresh_declined")-d0)
		}
	}
	// Quiesce: nothing still queued for the changed cells.
	k547WaitFor(h.t, 5*time.Second, "queue drained", func() bool { return k547Stat("queue_depth") == 0 })
}

// assertConvergesOnRead is the GREEN contract shared by F1 and F2, per cohort:
// the FIRST GET serves the pre-UPDATE body (zero cold navigation) and starts a
// demand revalidation; the SECOND GET (and the SSE publish) carry the new
// condition. Returns how many GETs received the pre-UPDATE body.
func (h *k547Harness) assertConvergesOnRead(arm, oldReason, newReason string) int64 {
	h.t.Helper()
	var staleGets int64
	for k := range k547Cohorts {
		key := h.key(k, k547Changed)
		if r, ok := h.residentReason(key); !ok || r != oldReason {
			h.t.Fatalf("%s PRECONDITION (k=%d): after the failed refresh the cell must still be resident on the "+
				"pre-UPDATE body (ok=%v reason=%q, want %q) — never evicted", arm, k, ok, r, oldReason)
		}
		sub, unsub := cache.SubscribeRefresh(map[string]struct{}{key: {}})
		ss0 := cache.ResolvedCacheStatsByStat()["stale_served_total"]

		// GET #1 — served from the cell, never a cold navigation.
		r, code := h.get(k, k547Changed)
		if code != 200 || r != oldReason {
			unsub()
			h.t.Fatalf("%s (k=%d) GET#1: code=%d reason=%q, want 200 serving the resident body %q", arm, k, code, r, oldReason)
		}
		staleGets++

		// The demand revalidation is asynchronous: wait for the cell to turn
		// fresh, bounded. On a build without it nothing happens and GET#2..#5
		// below carry the RED.
		healed := k547WaitFor(h.t, 5*time.Second, "cell refreshed by the demand revalidation", func() bool {
			rr, _ := h.residentReason(key)
			return rr == newReason
		})
		var published bool
		if healed {
			select {
			case got := <-sub:
				published = got == key
			case <-time.After(5 * time.Second):
			}
		}
		unsub()

		for i := 2; i <= 5; i++ {
			r, code := h.get(k, k547Changed)
			if code != 200 {
				h.t.Fatalf("%s (k=%d) GET#%d: code %d", arm, k, i, code)
			}
			if r != newReason {
				staleGets++
				h.t.Fatalf("%s RED (k=%d): GET#%d still served the pre-UPDATE condition %q (want %q). The background "+
					"refresh ended without a Put, the dirty stamp was erased, and nothing re-validates the cell: "+
					"every reload is a hit on a body the system KNEW was stale (#547). stale_served_total moved %d "+
					"across these GETs; customer-identity resolves of the changed composition: %d.",
					arm, k, i, r, newReason, cache.ResolvedCacheStatsByStat()["stale_served_total"]-ss0, h.custCalls.Load())
			}
		}
		if !published {
			h.t.Fatalf("%s (k=%d): the accepted revalidation published nothing over SSE — a mounted page would "+
				"not update without a reload", arm, k)
		}
	}
	// The unchanged compositions are untouched for both cohorts.
	for k := range k547Cohorts {
		for m := 0; m < k547M; m++ {
			if m == k547Changed {
				continue
			}
			if r, code := h.get(k, m); code != 200 || r != "v0" {
				h.t.Fatalf("%s: untouched cell (k=%d,m=%d) changed: code=%d reason=%q", arm, k, m, code, r)
			}
		}
	}
	return staleGets
}

func TestIssue547_F1_DropLeavesCellKnownStaleAndTheNextReadHeals(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	h.driveChangedToKnownStale("v1")
	ks0, _ := k547KnownStaleServed()
	dropped0 := k547KnownStaleServedReason("dropped")

	staleGets := h.assertConvergesOnRead("F1", "v0", "v1")

	// F4 — the detector equals the GETs that received the pre-UPDATE body.
	ks, ok := k547KnownStaleServed()
	if !ok {
		t.Fatalf("F4 RED: snowplow_resolved_cache{stat=known_stale_served_total} does not exist")
	}
	if got := ks - ks0; got != staleGets {
		t.Fatalf("F4: known_stale_served_total moved %d, want %d (the GETs that received the pre-UPDATE body)", got, staleGets)
	}
	if got := k547KnownStaleServedReason("dropped") - dropped0; got != staleGets {
		t.Fatalf("F4: known_stale_served_dropped_total moved %d, want %d", got, staleGets)
	}
}

func TestIssue547_F2_StageErrorDeclinesPastKLeaveCellKnownStaleAndTheNextReadHeals(t *testing.T) {
	h := newK547Harness(t, k547StageErrorBackground)
	h.warm()
	// Past K: four background declines in a row (on 9c233de9 K=3, so the 4th
	// dequeue is the suppressed skip).
	for i := 1; i <= 4; i++ {
		h.driveChangedToKnownStale(fmt.Sprintf("v%d", i))
	}
	ks0, _ := k547KnownStaleServed()

	staleGets := h.assertConvergesOnRead("F2", "v0", "v4")

	ks, ok := k547KnownStaleServed()
	if !ok {
		t.Fatalf("F4 RED: snowplow_resolved_cache{stat=known_stale_served_total} does not exist")
	}
	if got := ks - ks0; got != staleGets {
		t.Fatalf("F4: known_stale_served_total moved %d, want %d", got, staleGets)
	}
}

// F5 — owner ruling 1: when the demand revalidation ALSO fails under the
// requester, keep serving the cached body (counted), retry on the next read,
// never evict.
func TestIssue547_F5_RevalidationFailureKeepsServingCountedAndRetriesOnRead(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	h.driveChangedToKnownStale("v1")
	h.failCustomer.Store(true)
	key := h.key(0, k547Changed)
	ks0, _ := k547KnownStaleServed()
	c0 := h.custCalls.Load()
	f0 := cache.ResolvedCacheStatsByStat()["known_stale_revalidation_failed_total"]

	const reads = 3
	for i := 1; i <= reads; i++ {
		r, code := h.get(0, k547Changed)
		if code != 200 || r != "v0" {
			t.Fatalf("F5 GET#%d: code=%d reason=%q — a failed revalidation must keep SERVING the cached body (200 v0), "+
				"never a cold navigation and never an error", i, code, r)
		}
		// Each read retries once (sequentially: wait for this read's attempt).
		if !k547WaitFor(t, 5*time.Second, "revalidation attempt", func() bool { return h.custCalls.Load()-c0 >= int64(i) }) {
			t.Fatalf("F5 RED: read #%d started no revalidation under the requester (attempts=%d) — "+
				"nothing retries a known-stale cell on read", i, h.custCalls.Load()-c0)
		}
		// ...and wait for it to SETTLE (released, counted failed) before the next read.
		k547WaitFor(t, 2*time.Second, "attempt settled", func() bool {
			return cache.ResolvedCacheStatsByStat()["known_stale_revalidation_failed_total"]-f0 >= int64(i)
		})
		if r, ok := h.residentReason(key); !ok || r != "v0" {
			t.Fatalf("F5: after failed revalidation #%d the cell must stay resident on v0 (ok=%v reason=%q) — never evict", i, ok, r)
		}
	}
	ks, ok := k547KnownStaleServed()
	if !ok || ks-ks0 != reads {
		t.Fatalf("F5: known_stale_served_total moved %d (exists=%v), want %d — every stale serve must be counted", ks-ks0, ok, reads)
	}

	// The cohort recovers: the next read revalidates and heals.
	h.failCustomer.Store(false)
	if r, _ := h.get(0, k547Changed); r != "v0" {
		t.Fatalf("F5: the healing read itself must still serve the cached body, got %q", r)
	}
	if !k547WaitFor(t, 5*time.Second, "healed", func() bool { r, _ := h.residentReason(key); return r == "v1" }) {
		t.Fatalf("F5: the cell never healed once the cohort could resolve again")
	}
	if r, _ := h.get(0, k547Changed); r != "v1" {
		t.Fatalf("F5: after healing GET served %q, want v1", r)
	}
}

// F6 — coordinator: the revalidation runs on a DETACHED ctx carrying the
// requester's identity with a bounded deadline, and never blocks the response.
func TestIssue547_F6_RevalidationIsDetachedBoundedAndNeverBlocksTheResponse(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	h.driveChangedToKnownStale("v1")
	b := &k547Block{entered: make(chan struct{}), release: make(chan struct{})}
	h.mu.Lock()
	h.block = b
	h.mu.Unlock()
	defer func() {
		select {
		case <-b.release:
		default:
			close(b.release)
		}
	}()

	reqCtx, cancelReq := context.WithCancel(k547ReqCtx(1))
	done := make(chan string, 1)
	go func() { r, _ := h.getCtx(reqCtx, k547Changed); done <- r }()
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		t.Fatalf("F6 RED: no revalidation under the requester started after a known-stale read")
	}
	select {
	case r := <-done:
		if r != "v0" {
			t.Fatalf("F6: the response must carry the cached body v0, got %q", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("F6: the customer response did not return while the revalidation was still running — it BLOCKED the customer")
	}
	// The request is over: its ctx is cancelled. A detached revalidation survives it.
	cancelReq()
	if b.user != k547Cohorts[1].user {
		t.Fatalf("F6: the revalidation ran as %q, want the requester %q", b.user, k547Cohorts[1].user)
	}
	if !b.hasDeadline {
		t.Fatalf("F6: the revalidation ctx carries no deadline — it must be bounded")
	}
	close(b.release)
	key := h.key(1, k547Changed)
	if !k547WaitFor(t, 5*time.Second, "healed after release", func() bool { r, _ := h.residentReason(key); return r == "v1" }) {
		t.Fatalf("F6: the detached revalidation did not complete after the request ctx was cancelled")
	}
}

// F7 — owner ruling 3 + coordinator: a known-stale IDENTITY-FREE widgetContent
// cell is READ THROUGH (never served stale), and the fall-through result is
// never written into the shared cell under the requester's identity.
func TestIssue547_F7_KnownStaleWidgetContentIsReadThroughAndNeverOverwritten(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	ctx := k547ReqCtx(0)
	contentKey, handle, inputs := dispatchWidgetContentKey(ctx,
		k547WidgetGVR.Group, k547WidgetGVR.Version, k547WidgetGVR.Resource, k547WNS, k547WName, -1, -1,
		effectiveKeyExtras(ctx, h.widgetU.Object, k547Extras(k547Changed)))
	if handle == nil || contentKey == "" {
		t.Fatalf("PRECONDITION: no widgetContent key")
	}
	shell := k547Widget()
	shell.Object["status"] = map[string]any{"widgetData": map[string]any{"conditions": []any{
		map[string]any{"type": "Ready", "status": "True", "reason": "v0"}}}}
	raw, _ := json.Marshal(shell.Object)
	handle.Put(contentKey, &cache.ResolvedEntry{RawJSON: raw, Inputs: inputs})
	cache.Deps().Record(context.Background(), contentKey, k547CompGVR, k547CompNS, fmt.Sprintf("comp-%d", k547Changed))
	if r, code := h.get(0, k547Changed); code != 200 || r != "v0" {
		t.Fatalf("PRECONDITION: the shared content cell must serve before the UPDATE (code=%d reason=%q)", code, r)
	}

	d0 := k547Stat("dirty_ended_unfresh_dropped")
	h.update(k547Changed, "v1")
	if !k547WaitFor(t, 30*time.Second, "content cell dropped", func() bool { return k547Stat("dirty_ended_unfresh_dropped") > d0 }) {
		t.Fatalf("PRECONDITION: the content cell's background refresh never reached the drop point")
	}

	for k := range k547Cohorts {
		r, code := h.get(k, k547Changed)
		if code != 200 || r != "v1" {
			t.Fatalf("F7 RED (k=%d): GET served %q (code %d), want v1 read through — a known-stale identity-free "+
				"content cell was served stale", k, r, code)
		}
	}
	e, ok := handle.GetNoTouch(contentKey)
	if !ok {
		t.Fatalf("F7: the shared content cell was evicted — read-through must not delete")
	}
	if got := k547Reason(e.RawJSON); got != "v0" {
		t.Fatalf("F7: the shared content cell now holds %q — the per-requester fall-through result was written into "+
			"the identity-free cell", got)
	}
	if e.Inputs == nil || e.Inputs.RepresentativeUsername != "" || len(e.Inputs.RepresentativeGroups) != 0 ||
		e.Inputs.CacheEntryClass != cache.CacheEntryClassWidgetContent {
		t.Fatalf("F7: the shared content cell's inputs changed identity: %+v", e.Inputs)
	}
	for k := range k547Cohorts {
		if r, ok := h.residentReason(h.key(k, k547Changed)); !ok || r != "v1" {
			t.Fatalf("F7 (k=%d): the fall-through must land in the cohort's OWN per-cohort cell (ok=%v reason=%q)", k, ok, r)
		}
	}
}
