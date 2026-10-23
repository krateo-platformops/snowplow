// list_cell_854_test.go — #854 (#765 C): the incremental list content cell,
// through the production reader apistageContentServe on a real informer.
//
// Every arm compares the incremental cell with what a FULL fill of the same
// informer state stores right now: dispatchViaInformer (ListRawServable +
// filterRaw + marshalAsListRaw) and parseListEnvelope — the production functions,
// not a model of them.
//
//	F1   property: random add / update / delete / label flip / delete+recreate
//	     over a namespaced cell, a label-selector cell and a cluster-wide cell;
//	     after every step the envelope is BYTE-equal, the items deep-equal (same
//	     types: float64 numbers, an integer above 2^53, a float, an exponent form,
//	     managedFields stripped) and the store's size accounting equal (F8).
//	F1g  goldens: ADD, DELETE, in-place UPDATE, delete+recreate — byte-equal.
//	F2c  servability epoch: a watch error unanchors the cell (no event needed);
//	     the next read re-fills it in full (reason reanchor) and re-anchors.
//	F2d  lost events: a dep-worker panic, an ADD with a nil syncCh, a submit
//	     dropped onto a shut-down queue — each unanchors the cell.
//	F3   freshness: a refresher re-resolve triggered by a change reads the cell
//	     WITH the change and with no fill (the #765 A stamp is covered).
//	F4   no fill: one anchor fill, then 50 changes each followed by refresher and
//	     customer reads: zero fills, and the content key's store Put identity and
//	     generation never move (#854 C3).
//	F5a  comparator: a corrupted slot is a divergence (two passes), the cell
//	     unanchors and the next read re-fills.
//	F5c  comparator: a slot whose decoded object no longer comes from its raw is a
//	     divergence on the first pass.
//	F5m  comparator: a consumer that mutates a shared item is a divergence.
//	F7   #189: a DELETE-eviction of the content key during a read: the entry is
//	     gone, the cell is unanchored, nothing is resurrected.
//	F9   passthrough: never anchored.
package api

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"reflect"
	"testing"
	"time"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/utils/ptr"
)

const inc854OtherNS = "inc854-other"

// newInc854Watcher is newFill710Watcher with anchoring ON (the harness turns it
// off for the plain-path arms).
func newInc854Watcher(t *testing.T, n int) dynamic.Interface {
	t.Helper()
	dyn := newFill710Watcher(t, n)
	t.Cleanup(cache.SetListCellAnchoringForTest(true))
	fill710Reset(t)
	return dyn
}

func inc854Call(path string) httpcall.RequestOptions {
	return httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(http.MethodGet)}}
}

const (
	inc854NSPath       = "/apis/composition.krateo.io/v1/namespaces/" + r1Ns + "/fullstackapps"
	inc854SelectorPath = inc854NSPath + "?labelSelector=env%3Dprod"
	inc854ClusterPath  = "/apis/composition.krateo.io/v1/fullstackapps"
)

// inc854Obj is one fixture object exercising every JSON value shape the decoder
// must reproduce exactly.
func inc854Obj(ns, name string, managed int, env string) *unstructured.Unstructured {
	md := map[string]any{
		"namespace": ns, "name": name,
		"managedFields": []any{map[string]any{"manager": "inc854", "operation": "Update"}},
	}
	if env != "" {
		md["labels"] = map[string]any{"env": env}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "composition.krateo.io/v1",
		"kind":       "FullStackApp",
		"metadata":   md,
		"status": map[string]any{
			"managed": r1ManagedList(managed),
			"big":     int64(9007199254740993), // > 2^53
			"ratio":   1.5,
			"huge":    1e21, // marshals in exponent form
		},
	}}
}

// inc854Full is what a full fill of the call's partition stores right now.
func inc854Full(t *testing.T, call httpcall.RequestOptions) ([]byte, []*unstructured.Unstructured) {
	t.Helper()
	raw, ok := dispatchViaInformer(cache.WithApistageContentResolve(r1Ctx()), call)
	if !ok {
		t.Fatalf("the full dispatch of %s was not servable", call.Path)
	}
	gvr, _, _, _ := cache.ParseAPIServerPathToDep(call.Path)
	p, ok := parseListEnvelope(gvr, raw)
	if !ok {
		t.Fatalf("the full envelope of %s does not parse", call.Path)
	}
	return raw, p.items
}

func inc854Key(t *testing.T, call httpcall.RequestOptions) string {
	t.Helper()
	gvr, ns, name, _ := cache.ParseAPIServerPathToDep(call.Path)
	q := planListQuery(call.Path, name == "", ns != "")
	return cache.ComputeKey(contentKeyInputsQ(gvr, ns, name, q.canon))
}

// inc854Anchor fills the cell with a customer read and returns its cell.
func inc854Anchor(t *testing.T, store *cache.ResolvedCacheStore, call httpcall.RequestOptions) *cache.ListCell {
	t.Helper()
	_, _, _, _ = apistageContentServe(r1Ctx(), store, call, false)
	e, ok := store.GetNoTouch(inc854Key(t, call))
	if !ok {
		t.Fatalf("PRECONDITION: no content cell stored for %s", call.Path)
	}
	c := e.ListCell()
	if c == nil || !c.Anchored() {
		t.Fatalf("PRECONDITION: the content cell for %s is not an anchored incremental cell", call.Path)
	}
	return c
}

// inc854Settle waits until the dep hook has processed the latest write on
// (ns, name): its coordinate moved past before.
func inc854Settle(t *testing.T, ns string, before uint64) {
	t.Helper()
	if !fill710WaitFor(10*time.Second, func() bool {
		return cache.ContentCoordinateMovedSince(r1FsaGVR, ns, "", before)
	}) {
		t.Fatalf("the write in %s never reached the dep hook", ns)
	}
}

// inc854Same asserts the cell equals a full fill of its partition now (F1, F8).
func inc854Same(t *testing.T, step string, store *cache.ResolvedCacheStore, call httpcall.RequestOptions) {
	t.Helper()
	e, ok := store.GetNoTouch(inc854Key(t, call))
	if !ok || e.ListCell() == nil {
		t.Fatalf("%s: %s: the incremental entry is gone", step, call.Path)
	}
	v, ok := e.ListCell().Snapshot(cache.DepGenEpochNow())
	if !ok {
		t.Fatalf("%s: %s: the cell unanchored (stats %+v)", step, call.Path, cache.ListCellStatsSnapshot().Unanchors)
	}
	raw, items := inc854Full(t, call)
	if got := v.Raw(); string(got) != string(raw) {
		t.Fatalf("%s: %s: the incremental envelope is not byte-identical to a full fill\n got  %d bytes\n want %d bytes", step, call.Path, len(got), len(raw))
	}
	gotItems := v.Items()
	if len(gotItems) != len(items) {
		t.Fatalf("%s: %s: %d items, a full fill has %d", step, call.Path, len(gotItems), len(items))
	}
	for i := range items {
		if !reflect.DeepEqual(gotItems[i].Object, items[i].Object) {
			t.Fatalf("%s: %s: item %d differs from the full fill's parse (types included)\n got  %#v\n want %#v", step, call.Path, i, gotItems[i].Object, items[i].Object)
		}
	}
	plain := &cache.ResolvedEntry{RawJSON: raw, Items: items}
	if got, want := cache.EntryBytesForTest(e), cache.EntryBytesForTest(plain); got != want {
		t.Fatalf("%s: %s: F8 size accounting %d, a full fill of the same state is %d", step, call.Path, got, want)
	}
}

type inc854World struct {
	t     *testing.T
	dyn   dynamic.Interface
	exist map[[2]string]bool
}

func (w *inc854World) apply(ns, name string, op string, managed int, env string) {
	w.t.Helper()
	before := cache.DepGenEpochNow()
	res := w.dyn.Resource(r1FsaGVR).Namespace(ns)
	k := [2]string{ns, name}
	var err error
	switch op {
	case "create":
		_, err = res.Create(context.Background(), inc854Obj(ns, name, managed, env), metav1.CreateOptions{})
		w.exist[k] = true
	case "update":
		_, err = res.Update(context.Background(), inc854Obj(ns, name, managed, env), metav1.UpdateOptions{})
	case "delete":
		err = res.Delete(context.Background(), name, metav1.DeleteOptions{})
		delete(w.exist, k)
	}
	if err != nil {
		w.t.Fatalf("%s %s/%s: %v", op, ns, name, err)
	}
	inc854Settle(w.t, ns, before)
}

func TestListCell854_F1_PropertyEveryStepEqualsAFullFill(t *testing.T) {
	dyn := newInc854Watcher(t, 0)
	store := cache.ResolvedCache()
	w := &inc854World{t: t, dyn: dyn, exist: map[[2]string]bool{}}
	// A starting population in both namespaces, some labelled prod.
	for i := 0; i < 12; i++ {
		env := ""
		if i%3 == 0 {
			env = "prod"
		}
		w.apply(r1Ns, fmt.Sprintf("inc-%02d", i), "create", 1, env)
		w.apply(inc854OtherNS, fmt.Sprintf("oth-%02d", i), "create", 1, env)
	}
	calls := []httpcall.RequestOptions{inc854Call(inc854NSPath), inc854Call(inc854SelectorPath), inc854Call(inc854ClusterPath)}
	for _, c := range calls {
		inc854Anchor(t, store, c)
	}
	rng := rand.New(rand.NewSource(854))
	envs := []string{"", "prod", "dev"}
	for step := 0; step < 150; step++ {
		ns := r1Ns
		if rng.Intn(3) == 0 {
			ns = inc854OtherNS
		}
		name := fmt.Sprintf("inc-%02d", rng.Intn(18))
		if ns == inc854OtherNS {
			name = fmt.Sprintf("oth-%02d", rng.Intn(18))
		}
		k := [2]string{ns, name}
		switch {
		case !w.exist[k]:
			w.apply(ns, name, "create", 1+rng.Intn(4), envs[rng.Intn(3)])
		case rng.Intn(5) == 0:
			w.apply(ns, name, "delete", 0, "")
		case rng.Intn(6) == 0:
			// delete + recreate under the same name: a new uid, the same position
			w.apply(ns, name, "delete", 0, "")
			w.apply(ns, name, "create", 1+rng.Intn(4), envs[rng.Intn(3)])
		default:
			w.apply(ns, name, "update", 1+rng.Intn(4), envs[rng.Intn(3)]) // labels may flip in or out
		}
		for _, c := range calls {
			inc854Same(t, fmt.Sprintf("step %d", step), store, c)
		}
	}
	if fills := ContentFillStatsSnapshot().Total; fills["refresh_forced"]+fills["known_stale"]+fills["reanchor"] != 0 {
		t.Fatalf("F1: %v re-fills during the property run — the cells were maintained incrementally? (anchors only expected)", fills)
	}
}

func TestListCell854_F1g_GoldensAddDeleteUpdateRecreate(t *testing.T) {
	dyn := newInc854Watcher(t, 3)
	store := cache.ResolvedCache()
	w := &inc854World{t: t, dyn: dyn, exist: map[[2]string]bool{}}
	call := inc854Call(inc854NSPath)
	inc854Anchor(t, store, call)
	inc854Same(t, "anchor", store, call)
	w.apply(r1Ns, "fsa-00001b", "create", 2, "prod") // ADD between two existing keys
	inc854Same(t, "ADD", store, call)
	w.apply(r1Ns, "fsa-00000", "delete", 0, "") // DELETE the first key
	inc854Same(t, "DELETE", store, call)
	w.apply(r1Ns, "fsa-00002", "update", 4, "") // in-place UPDATE (the last key)
	inc854Same(t, "UPDATE", store, call)
	w.apply(r1Ns, "fsa-00001", "delete", 0, "") // delete + recreate: same position
	w.apply(r1Ns, "fsa-00001", "create", 3, "dev")
	inc854Same(t, "RECREATE", store, call)
}

func TestListCell854_F2c_ServabilityEpochUnanchorsAndTheNextReadReanchors(t *testing.T) {
	newInc854Watcher(t, 3)
	store := cache.ResolvedCache()
	call := inc854Call(inc854NSPath)
	c := inc854Anchor(t, store, call)
	before := cache.ListCellStatsSnapshot().Unanchors["servability_epoch"]
	cache.Global().FireWatchError(r1FsaGVR) // an exit from servable, no object event
	if _, ok := c.Snapshot(cache.DepGenEpochNow()); ok {
		t.Fatal("F2c RED: the cell still serves after its GVR left servable — an outage the dep hook never saw would be missed")
	}
	if got := cache.ListCellStatsSnapshot().Unanchors["servability_epoch"] - before; got != 1 {
		t.Fatalf("F2c: unanchors{servability_epoch} moved by %d, want 1", got)
	}
}

func TestListCell854_F2d_EveryLostEventSignalUnanchors(t *testing.T) {
	for _, kind := range []string{"panic", "nil_sync", "dropped_shutdown"} {
		t.Run(kind, func(t *testing.T) {
			newInc854Watcher(t, 3)
			store := cache.ResolvedCache()
			c := inc854Anchor(t, store, inc854Call(inc854NSPath))
			cache.BumpDepWatchLostEventForTest(kind)
			if _, ok := c.Snapshot(cache.DepGenEpochNow()); ok {
				t.Fatalf("F2d RED (%s): the cell still serves after an event may have been lost", kind)
			}
			// The next read re-fills in full and re-anchors.
			fill710Reset(t)
			c2 := inc854Anchor(t, store, inc854Call(inc854NSPath))
			if c2 == c {
				t.Fatalf("F2d (%s): the same cell is still stored", kind)
			}
			if got := ContentFillStatsSnapshot().Total["reanchor"]; got != 1 {
				t.Fatalf("F2d (%s): fill_total{reanchor}=%d, want 1", kind, got)
			}
			inc854Same(t, kind, store, inc854Call(inc854NSPath))
		})
	}
}

func TestListCell854_F3_RefresherReadSeesTheChangeWithNoFill(t *testing.T) {
	dyn := newInc854Watcher(t, 5)
	store := cache.ResolvedCache()
	inc854Anchor(t, store, fill710ListCall())
	fill710Reset(t)
	for m := 2; m <= 6; m++ {
		stamp765Change(t, dyn, m)
		r := stamp765Recv(t, "refresher read", stamp765Serve(stamp765RefreshCtx(), store))
		if got := fill710ManagedOf(t, r.v, stamp765Item); got != m {
			t.Fatalf("F3 RED: a refresher read after change managed=%d was served managed=%d — the cell claimed a stamp it does not contain", m, got)
		}
	}
	s := ContentFillStatsSnapshot()
	if n := s.Total["refresh_forced"] + s.Total["absent"] + s.Total["reanchor"]; n != 0 {
		t.Fatalf("F3: %d fills after the anchor (%v), want 0 — every change is applied, not re-filled", n, s.Total)
	}
	if s.RefreshReused != 5 {
		t.Fatalf("F3: refresh_reused=%d, want 5 (one per refresher read)", s.RefreshReused)
	}
}

func TestListCell854_F4_NoFillAndNoStoreWriteAcrossFiftyChanges(t *testing.T) {
	dyn := newInc854Watcher(t, 5)
	store := cache.ResolvedCache()
	key := fill710ListKey()
	inc854Anchor(t, store, fill710ListCall())
	gen0 := store.CaptureGen(key)
	seq0, ok := store.CellPutSeq(key)
	if !ok {
		t.Fatal("PRECONDITION: no live content cell")
	}
	fill710Reset(t)
	for m := 1; m <= 50; m++ {
		stamp765Change(t, dyn, 1+m%7)
		_ = stamp765Recv(t, "refresher", stamp765Serve(stamp765RefreshCtx(), store))
		if _, served, ok, _ := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok || !served {
			t.Fatalf("customer read %d not served", m)
		}
	}
	s := ContentFillStatsSnapshot()
	var fills uint64
	for _, v := range s.Total {
		fills += v
	}
	if fills != 0 {
		t.Fatalf("F4 RED: %d fills across 50 changes (%v), want 0", fills, s.Total)
	}
	if g := store.CaptureGen(key); g != gen0 {
		t.Fatalf("F4 (C3): the content key's generation moved %d → %d: a cell update must not read as a gen move", gen0, g)
	}
	if seq, _ := store.CellPutSeq(key); seq != seq0 {
		t.Fatalf("F4 (C3): the content key's Put identity moved %d → %d: a cell update must not be a store Put", seq0, seq)
	}
	inc854Same(t, "after 50", store, fill710ListCall())
	st := cache.ListCellStatsSnapshot()
	t.Logf("F4: generations=%d applied_keys=%d decodes=%d restamps=%d gen_alloc_max=%dB gen_alloc_total=%dB over %d builds rawjson_built=%d",
		st.Generations, st.AppliedKeys, st.Decodes, st.Restamps, st.GenAllocBytesMax, st.GenAllocBytesTotal, st.GenAllocCount, st.RawJSONBuilt)
}

func TestListCell854_F5a_CorruptSlotIsADivergenceAndReanchors(t *testing.T) {
	newInc854Watcher(t, 5)
	store := cache.ResolvedCache()
	c := inc854Anchor(t, store, fill710ListCall())
	if !c.CorruptSlotForTest() {
		t.Fatal("PRECONDITION: could not corrupt a slot")
	}
	if n := c.ComparePass(); n != 0 {
		t.Fatalf("F5a: the first pass reported %d divergences; a mismatch is only a suspect until a second pass confirms it", n)
	}
	if n := c.ComparePass(); n == 0 {
		t.Fatal("F5a RED: a slot whose bytes differ from the indexer survived two passes")
	}
	if c.Anchored() {
		t.Fatal("F5a: the cell is still anchored after a divergence")
	}
	if got := cache.ListCellStatsSnapshot().DivergenceTotal; got == 0 {
		t.Fatal("F5a: divergence_total did not move")
	}
	inc854Anchor(t, store, fill710ListCall())
	inc854Same(t, "re-anchored", store, fill710ListCall())
}

func TestListCell854_F5c_ObjFromRawIdentityIsCheckedOnTheFirstPass(t *testing.T) {
	newInc854Watcher(t, 5)
	store := cache.ResolvedCache()
	c := inc854Anchor(t, store, fill710ListCall())
	if !c.CorruptObjSrcForTest() {
		t.Fatal("PRECONDITION: could not corrupt objSrc")
	}
	if n := c.ComparePass(); n == 0 {
		t.Fatal("F5c RED: a slot whose decoded object no longer comes from its raw passed the comparator — serving would read the stale object")
	}
	if got := cache.ListCellStatsSnapshot().Divergences["obj_src"]; got == 0 {
		t.Fatal("F5c: divergences{obj_src} did not move")
	}
}

func TestListCell854_F5m_ConsumerMutatingASharedItemIsCaught(t *testing.T) {
	newInc854Watcher(t, 3) // 3 items: one chunk, so the rotating re-decode covers it
	store := cache.ResolvedCache()
	c := inc854Anchor(t, store, fill710ListCall())
	v, ok := c.Snapshot(cache.DepGenEpochNow())
	if !ok {
		t.Fatal("PRECONDITION: not anchored")
	}
	v.Items()[0].Object["mutated-by-a-consumer"] = true
	if n := c.ComparePass(); n == 0 {
		t.Fatal("F5m RED: a shared item mutated by a consumer was not detected")
	}
	if got := cache.ListCellStatsSnapshot().Divergences["mutated"]; got == 0 {
		t.Fatal("F5m: divergences{mutated} did not move")
	}
}

func TestListCell854_F7_DeleteEvictionUnanchorsAndNothingResurrects(t *testing.T) {
	dyn := newInc854Watcher(t, 3)
	store := cache.ResolvedCache()
	key := fill710ListKey()
	c := inc854Anchor(t, store, fill710ListCall())
	store.DeleteForTest(key)
	if c.Anchored() {
		t.Fatal("F7 RED: the cell outlived its entry")
	}
	stamp765Change(t, dyn, 5) // an event after the eviction must not reach a dead cell
	if _, ok := store.GetNoTouch(key); ok {
		t.Fatal("F7: the evicted content key came back without a read")
	}
	if cache.ListCellStatsSnapshot().Live != 0 {
		t.Fatalf("F7: %d live cells after the eviction, want 0", cache.ListCellStatsSnapshot().Live)
	}
}

func TestListCell854_F9_PassthroughNeverAnchors(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	t.Cleanup(cache.SetListCellAnchoringForTest(true))
	before := cache.ListCellStatsSnapshot().Anchors
	if c := cache.AnchorListCell(cache.ListCellAnchor{Key: "k", GVR: r1FsaGVR, Funcs: listCellFuncs(r1FsaGVR, listQuery{applicable: true})}); c != nil {
		t.Fatal("F9 RED: a cell anchored with the cache off (passthrough has no informer events)")
	}
	if got := cache.ListCellStatsSnapshot().Anchors - before; got != 0 {
		t.Fatalf("F9: anchors moved by %d", got)
	}
}
