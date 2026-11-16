package cache

// store_rv_atomicfifo_931_test.go — #931: client-go v0.36.5 with its DEFAULT
// feature gates (AtomicFIFO, UnlockWhileProcessingFIFO and InOrderInformers
// all on) on snowplow's own informer wiring.
//
// What #923 will build on, made executable here:
//
//   - A watch BOOKMARK reaches the store in order with the object events and
//     advances Store.LastStoreSyncResourceVersion() — the position the store
//     has APPLIED, not the one the reflector has published
//     (client-go@v0.36.5 controller.go processDeltas `case Bookmark`,
//     thread_safe_store.go Bookmark). It does so with snowplow's transform
//     installed (delivery record + strip) and for both informer origins:
//     a standalone informer the GVR owns AND one from the shared dynamic
//     factory.
//   - A bookmark reaches NO event handler: a later object event is the next
//     callback a handler sees.
//   - A relist is applied as ONE transaction (the atomic ReplacedAll delta):
//     the store's position becomes the LIST's resourceVersion, a key the new
//     LIST lacks is delivered to handlers as a
//     clientcache.DeletedFinalStateUnknown tombstone — the shape every
//     snowplow DeleteFunc unwraps (deps_watch.go, list_cell.go,
//     bindings_by_gvr_delta.go, secrets_transform.go) — and every handler
//     callback of the relist runs AFTER the store was replaced
//     (controller.go processReplacedAllInfo: Replace, then handlers). The
//     #854 comparator's in-flight test depends on that order (list_cell.go
//     deliveredMismatch): the indexer is written before the delivery reaches
//     the handler.
//
// BOTH REGIMES. Every arm runs twice: on the classic LIST+WATCH path the
// package TestMain selects (featuregate_watchlist_test.go), and on the
// WATCH-LIST path production takes (WatchListClient on, its default), where a
// relist streams into the reflector's temporary store, is transformed twice
// and reaches the FIFO as one Replace (reflector.go watchList). The
// silent-watch fixture serves watch-list requests when silentWatchWatchList
// is set (store_verify_repair_falsifier_test.go), and the arm proves the
// reflector used it.
//
// FALSIFIERS. Every assertion on LastStoreSyncResourceVersion reads the
// informer's own store; the expected value is the literal resourceVersion the
// arm wrote, never one derived from the store. Mutations that turn these arms
// RED (see #933):
//   - KUBE_FEATURE_AtomicFIFO=false: the store reports "" for every position,
//     so every arm fails at its FIRST position check (after the initial
//     LIST) — it proves the gate is read, not the bookmark path;
//   - client-go processDeltas `case Bookmark` not calling clientState.Bookmark
//     (controller.go:661): RED exactly at the bookmark step;
//   - client-go processReplacedAllInfo calling the handlers BEFORE
//     clientState.Replace: RED at the store-before-handler assertions of the
//     relist arm.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	clientcache "k8s.io/client-go/tools/cache"
)

// rv931Handler counts every callback an informer hands it and keeps the
// tombstones it received. With store set it also checks, inside each UPDATE
// and DELETE callback, that the store already reflects that callback.
type rv931Handler struct {
	store clientcache.Store

	mu         sync.Mutex
	adds       int
	updates    int
	deletes    int
	lastNewRV  string
	tombstones []clientcache.DeletedFinalStateUnknown
	// storeChecks counts callbacks checked against the store; storeBehind
	// names every callback that ran while the store did not yet reflect it.
	storeChecks int
	storeBehind []string
}

func (h *rv931Handler) OnAdd(_ interface{}, _ bool) {
	h.mu.Lock()
	h.adds++
	h.mu.Unlock()
}

func (h *rv931Handler) OnUpdate(_, newObj interface{}) {
	var behind string
	if h.store != nil {
		// The store must already hold THIS object: client-go writes the store
		// first, then hands the same object to the handlers.
		stored, exists, err := h.store.Get(newObj)
		if err != nil || !exists || stored != newObj {
			behind = fmt.Sprintf("UPDATE %s/%s: the store does not hold the delivered object yet (exists=%v err=%v)",
				nsOf(newObj), nameOf(newObj), exists, err)
		}
	}
	h.mu.Lock()
	h.updates++
	if u, ok := newObj.(interface{ GetResourceVersion() string }); ok {
		h.lastNewRV = u.GetResourceVersion()
	}
	if h.store != nil {
		h.storeChecks++
		if behind != "" {
			h.storeBehind = append(h.storeBehind, behind)
		}
	}
	h.mu.Unlock()
}

func (h *rv931Handler) OnDelete(obj interface{}) {
	var behind string
	if h.store != nil {
		key, err := clientcache.DeletionHandlingMetaNamespaceKeyFunc(obj)
		if err == nil {
			if _, exists, _ := h.store.GetByKey(key); exists {
				behind = "DELETE " + key + ": the store still holds the deleted key"
			}
		} else {
			behind = "DELETE: no key for the delivered object: " + err.Error()
		}
	}
	h.mu.Lock()
	h.deletes++
	if t, ok := obj.(clientcache.DeletedFinalStateUnknown); ok {
		h.tombstones = append(h.tombstones, t)
	}
	if h.store != nil {
		h.storeChecks++
		if behind != "" {
			h.storeBehind = append(h.storeBehind, behind)
		}
	}
	h.mu.Unlock()
}

func (h *rv931Handler) counts() (adds, updates, deletes int, lastNewRV string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.adds, h.updates, h.deletes, h.lastNewRV
}

func nsOf(obj interface{}) string {
	ns, _ := metaNSName(obj)
	return ns
}

func nameOf(obj interface{}) string {
	_, name := metaNSName(obj)
	return name
}

// requireDefaultAtomicGates fails the arm unless the process runs client-go's
// v1.36 defaults: the arm is about those defaults, not an override.
func requireDefaultAtomicGates(t *testing.T) {
	t.Helper()
	gates := clientfeatures.FeatureGates()
	for _, f := range []clientfeatures.Feature{
		clientfeatures.AtomicFIFO,
		clientfeatures.UnlockWhileProcessingFIFO,
		clientfeatures.InOrderInformers,
	} {
		if !gates.Enabled(f) {
			t.Errorf("precondition: client-go feature %s is off; #931 requires the v1.36 defaults", f)
		}
	}
}

// rv931Regimes are the two reflector regimes every arm runs under.
var rv931Regimes = []struct {
	name      string
	watchList bool
}{
	{"list-watch", false},
	{"watch-list (production default)", true},
}

// enterRegime selects the reflector regime for the arm. Watch-list turns the
// WatchListClient gate back on (its default; the package TestMain turns it
// off) and makes the fixture serve watch-list requests. No t.Parallel():
// SetFeatureDuringTest refuses a concurrent override.
func enterRegime(t *testing.T, watchList bool) {
	t.Helper()
	silentWatchListRequests.Store(0)
	if !watchList {
		return
	}
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)
	silentWatchWatchList = true
	t.Cleanup(func() { silentWatchWatchList = false })
}

// requireRegime proves the reflector used the regime the arm asked for: at
// least min watch-list requests under watch-list, none under list-watch.
func requireRegime(t *testing.T, watchList bool, min int64) {
	t.Helper()
	got := silentWatchListRequests.Load()
	if watchList && got < min {
		t.Fatalf("precondition: %d watch-list request(s) served, want >= %d — the arm would be asserting the LIST path under a watch-list name", got, min)
	}
	if !watchList && got != 0 {
		t.Fatalf("precondition: %d watch-list request(s) served on the list-watch regime", got)
	}
}

// rv931Informer returns gvr's informer and the watch it is blocked on, after
// checking snowplow's transform (the delivery record) is installed on it.
func rv931Informer(t *testing.T, rw *ResourceWatcher, gvr schema.GroupVersionResource) (clientcache.SharedIndexInformer, *watch.FakeWatcher) {
	t.Helper()
	rw.mu.RLock()
	gi, ok := rw.informers[gvr]
	_, recorded := rw.deliveries[gvr]
	rw.mu.RUnlock()
	if !ok {
		t.Fatalf("precondition: %s has no informer", gvr)
	}
	if !recorded {
		t.Fatalf("precondition: %s has no delivery record, so snowplow's transform is not installed and the arm would not cover it", gvr)
	}
	w := silentWatchLatest.Load()
	if w == nil {
		t.Fatalf("precondition: %s's informer holds no watch to deliver through", gvr)
	}
	return gi.Informer(), w
}

// waitStoreRV waits until the informer's store reports want as its applied
// position.
func waitStoreRV(t *testing.T, inf clientcache.SharedIndexInformer, want, what string) {
	t.Helper()
	deadline := time.Now().Add(verifyBound)
	for {
		got := inf.GetStore().LastStoreSyncResourceVersion()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("#931 RED: %s: LastStoreSyncResourceVersion = %q, want %q (published position %q)",
				what, got, want, inf.LastSyncResourceVersion())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// addCountingHandler registers h and waits until it has received the
// informer's replay, so later counts are deliveries only.
func addCountingHandler(t *testing.T, inf clientcache.SharedIndexInformer, h *rv931Handler) {
	t.Helper()
	reg, err := inf.AddEventHandler(h)
	if err != nil {
		t.Fatalf("AddEventHandler: %v", err)
	}
	t.Cleanup(func() { _ = inf.RemoveEventHandler(reg) })
	select {
	case <-reg.HasSyncedChecker().Done():
	case <-time.After(verifyBound):
		t.Fatalf("precondition: the counting handler never synced")
	}
}

func bookmarkObj(gvr schema.GroupVersionResource, rv string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(gvr.GroupVersion().String())
	u.SetKind("Panel")
	u.SetResourceVersion(rv)
	return u
}

// TestStoreRV931_BookmarkAdvancesAppliedPosition — a bookmark advances the
// store's applied position, through snowplow's transform, for an owned and a
// shared-factory informer, on both reflector regimes, and reaches no handler.
func TestStoreRV931_BookmarkAdvancesAppliedPosition(t *testing.T) {
	for _, regime := range rv931Regimes {
		for _, tc := range []struct {
			name  string
			gvr   schema.GroupVersionResource
			owned bool
			obj   func(name, rv, uid string) *unstructured.Unstructured
		}{
			{"owned standalone informer", repairGVR, true, func(n, rv, uid string) *unstructured.Unstructured { return panelObj("demo", n, rv, uid) }},
			{"shared dynamic factory informer", factory875GVR, false, factory875Obj},
		} {
			t.Run(regime.name+"/"+tc.name, func(t *testing.T) {
				enterRegime(t, regime.watchList)
				requireDefaultAtomicGates(t)
				c3Setup(t)
				resetVerificationForArm(t)
				rw, _ := silentWatchClusterFor(t, tc.gvr, tc.owned, tc.obj("panel-a", "1", "uid-a"))
				if row, ok := verificationRowFor(tc.gvr); !ok || row.ownsInformer != tc.owned {
					t.Fatalf("precondition: want ownsInformer=%v for %s (row=%+v ok=%v)", tc.owned, tc.gvr, row, ok)
				}
				requireRegime(t, regime.watchList, 1)
				inf, w := rv931Informer(t, rw, tc.gvr)

				// The initial LIST (or watch-list) was applied as one
				// transaction at the collection resourceVersion.
				waitStoreRV(t, inf, clusterListRV, "after the initial LIST")

				// An object event moves the applied position to its resourceVersion.
				w.Modify(tc.obj("panel-a", "7", "uid-a"))
				waitStoreRV(t, inf, "7", "after a watch MODIFIED at rv 7")

				h := &rv931Handler{}
				addCountingHandler(t, inf, h)
				adds0, updates0, deletes0, _ := h.counts()

				// The bookmark: no object changes, the applied position does.
				w.Action(watch.Bookmark, bookmarkObj(tc.gvr, "50"))
				waitStoreRV(t, inf, "50", "after a watch BOOKMARK at rv 50")
				if got := inf.LastSyncResourceVersion(); got != "50" {
					t.Errorf("published position after the bookmark = %q, want 50", got)
				}
				if obj, exists, err := inf.GetStore().GetByKey("demo/panel-a"); err != nil || !exists {
					t.Fatalf("the bookmark removed panel-a from the store (exists=%v err=%v)", exists, err)
				} else if rv := obj.(interface{ GetResourceVersion() string }).GetResourceVersion(); rv != "7" {
					t.Errorf("the bookmark changed panel-a's stored rv to %q, want 7", rv)
				}

				// The next object event is the next callback: the bookmark
				// reached no handler. Listener delivery is in order, so waiting
				// for rv 51 at the handler also drains anything the bookmark
				// could have sent.
				w.Modify(tc.obj("panel-a", "51", "uid-a"))
				deadline := time.Now().Add(verifyBound)
				for {
					if _, _, _, last := h.counts(); last == "51" {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("precondition: the handler never received the rv 51 update")
					}
					time.Sleep(5 * time.Millisecond)
				}
				adds, updates, deletes, _ := h.counts()
				if adds != adds0 || deletes != deletes0 || updates != updates0+1 {
					t.Errorf("#931 RED: a bookmark reached an event handler: adds %d->%d updates %d->%d (want +1, the rv 51 event) deletes %d->%d",
						adds0, adds, updates0, updates, deletes0, deletes)
				}
				waitStoreRV(t, inf, "51", "after a watch MODIFIED at rv 51")
			})
		}
	}
}

// TestStoreRV931_RelistIsOneTransactionAndDeletesAsTombstones — a relist on a
// live informer replaces the store at the LIST's resourceVersion, delivers the
// key the LIST lacks as a DeletedFinalStateUnknown, and calls every handler
// only after the store was replaced — on both reflector regimes.
func TestStoreRV931_RelistIsOneTransactionAndDeletesAsTombstones(t *testing.T) {
	for _, regime := range rv931Regimes {
		t.Run(regime.name, func(t *testing.T) {
			enterRegime(t, regime.watchList)
			requireDefaultAtomicGates(t)
			c3Setup(t)
			resetVerificationForArm(t)
			rw, dyn := silentWatchCluster(t, panelObj("demo", "panel-a", "1", "uid-a"), panelObj("demo", "panel-b", "1", "uid-b"))
			requireRegime(t, regime.watchList, 1)
			inf, w := rv931Informer(t, rw, repairGVR)
			waitStoreRV(t, inf, clusterListRV, "after the initial LIST")
			w.Action(watch.Bookmark, bookmarkObj(repairGVR, "9000"))
			waitStoreRV(t, inf, "9000", "after a watch BOOKMARK at rv 9000")

			// The handler checks the store inside every UPDATE and DELETE.
			h := &rv931Handler{store: inf.GetStore()}
			addCountingHandler(t, inf, h)
			_, updates0, deletes0, _ := h.counts()

			// panel-b goes away with no watch event; the watch then expires,
			// and the reflector relists (a LIST, or a new watch-list stream).
			if err := dyn.Resource(repairGVR).Namespace("demo").Delete(context.Background(), "panel-b", metav1.DeleteOptions{}); err != nil {
				t.Fatalf("deleting panel-b from the cluster: %v", err)
			}
			w.Error(&metav1.Status{
				Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired,
				Message: "too old resource version",
			})

			// The relist answers clusterListRV, which is BELOW the bookmark the
			// store applied: the store reports the LIST it was replaced with,
			// not a maximum. #923 must read it as "the store's position", never
			// as monotone.
			waitStoreRV(t, inf, clusterListRV, "after the relist")
			// The relist delivers panel-b's tombstone and, resync period 0
			// keeping every listener a syncing one, panel-a's same-RV update.
			deadline := time.Now().Add(verifyBound)
			for {
				if _, u, d, _ := h.counts(); d > deletes0 && u > updates0 {
					break
				}
				if time.Now().After(deadline) {
					_, u, d, _ := h.counts()
					t.Fatalf("#931 RED: the relist never delivered both panel-b's deletion and panel-a's update (updates %d->%d deletes %d->%d)",
						updates0, u, deletes0, d)
				}
				time.Sleep(5 * time.Millisecond)
			}
			requireRegime(t, regime.watchList, 2)
			if _, exists, _ := inf.GetStore().GetByKey("demo/panel-b"); exists {
				t.Errorf("panel-b is still in the store after the relist that deleted it")
			}
			if _, exists, _ := inf.GetStore().GetByKey("demo/panel-a"); !exists {
				t.Errorf("panel-a is missing from the store after the relist")
			}
			h.mu.Lock()
			tombs := append([]clientcache.DeletedFinalStateUnknown(nil), h.tombstones...)
			checks, behind := h.storeChecks, append([]string(nil), h.storeBehind...)
			h.mu.Unlock()
			if len(tombs) != 1 || tombs[0].Key != "demo/panel-b" {
				t.Fatalf("#931 RED: want exactly one DeletedFinalStateUnknown for demo/panel-b, got %+v", tombs)
			}
			if tombs[0].Obj == nil {
				t.Errorf("the tombstone carries no last-known object; snowplow's DeleteFunc reads (ns, name) from it")
			} else if ns, name := metaNSName(tombs[0].Obj); ns != "demo" || name != "panel-b" {
				t.Errorf("the tombstone's last-known object reads as %s/%s, want demo/panel-b", ns, name)
			}
			// Store before handlers (#854's in-flight invariant).
			if checks < 2 {
				t.Fatalf("precondition: only %d relist callback(s) were checked against the store, want the delete and the update", checks)
			}
			if len(behind) != 0 {
				t.Errorf("#931 RED: %d relist callback(s) ran before the store was replaced — #854's comparator would read a settled key as in flight or the reverse:\n  %v",
					len(behind), behind)
			}
		})
	}
}
