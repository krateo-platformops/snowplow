package cache

// store_verify_recheck_872_test.go — #872: the forced pass counts a
// divergence only if it still holds against the object's CURRENT state.
//
// All arms run on the silent-watch fixture: a real ResourceWatcher whose watch
// delivers nothing, so the store holds exactly what it was seeded with, and
// the "apiserver" the forced pass talks to answers LIST from a fixed snapshot
// and GET from the cluster's current state.
//
//   - STORE AHEAD OF THE LIST. The LIST is OLDER than the store and the
//     cluster — the shape churn produces, because the store keeps applying
//     events while the LIST is in flight. Every class must be cleared by the
//     re-check: nothing published, nothing repaired.
//   - A TRUE LOSS. The store is genuinely behind the cluster (the silent watch
//     dropped the event) and the LIST agrees with the cluster. Every class must
//     still be published and repaired.
//   - A FAILED RE-CHECK. Inconclusive: nothing published or repaired, the GVR
//     not marked verified, the skip counted under its own reason.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
)

// r872Client answers LIST from list and GET from inner (the cluster's current
// state), or fails every GET with getErr. firstGetRV, when set, is the
// resourceVersion the FIRST GET answers with — the object as it was a moment
// before it moved again, which the second read then catches.
type r872Client struct {
	inner      metadata.Interface
	list       []*metav1.PartialObjectMetadata
	getErr     error
	firstGetRV string
	gets       atomic.Int64
}

func (c *r872Client) Resource(gvr schema.GroupVersionResource) metadata.Getter {
	return &r872Getter{Getter: c.inner.Resource(gvr), c: c}
}

type r872Getter struct {
	metadata.Getter
	c *r872Client
}

func (g *r872Getter) Namespace(ns string) metadata.ResourceInterface {
	return &r872Resource{ResourceInterface: g.Getter.Namespace(ns), c: g.c}
}

type r872Resource struct {
	metadata.ResourceInterface
	c *r872Client
}

func (r *r872Resource) List(context.Context, metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	out := &metav1.PartialObjectMetadataList{}
	for _, o := range r.c.list {
		out.Items = append(out.Items, *o)
	}
	return out, nil
}

func (r *r872Resource) Get(ctx context.Context, name string, opts metav1.GetOptions, sub ...string) (*metav1.PartialObjectMetadata, error) {
	n := r.c.gets.Add(1)
	if r.c.getErr != nil {
		return nil, r.c.getErr
	}
	obj, err := r.ResourceInterface.Get(ctx, name, opts, sub...)
	if err == nil && n == 1 && r.c.firstGetRV != "" {
		obj = obj.DeepCopy()
		obj.ResourceVersion = r.c.firstGetRV
	}
	return obj, err
}

// r872Object is one object as "name, rv, uid".
type r872Object struct{ name, rv, uid string }

func r872Store(objs []r872Object) []*unstructured.Unstructured {
	out := make([]*unstructured.Unstructured, 0, len(objs))
	for _, o := range objs {
		out = append(out, panelObj("demo", o.name, o.rv, o.uid))
	}
	return out
}

func r872Meta(objs []r872Object) []*metav1.PartialObjectMetadata {
	out := make([]*metav1.PartialObjectMetadata, 0, len(objs))
	for _, o := range objs {
		out = append(out, partialMeta("demo", o.name, o.rv, o.uid))
	}
	return out
}

// r872Setup seeds the store, attaches the client and returns it.
func r872Setup(t *testing.T, store, current, list []r872Object, getErr error) (*ResourceWatcher, *r872Client) {
	t.Helper()
	c3Setup(t)
	resetVerificationForArm(t)
	rw, _ := silentWatchCluster(t, r872Store(store)...)
	rec := attachMetaClient(t, rw, r872Meta(current)...)
	c := &r872Client{inner: rec.inner, list: r872Meta(list), getErr: getErr}
	rw.SetMetadataClient(c)
	return rw, c
}

func r872Published() uint64 {
	lu, ld, la, um := divergenceCounts(verifySiteForced)
	return lu + ld + la + um
}

func r872ClassCount(class string) uint64 {
	lu, ld, la, um := divergenceCounts(verifySiteForced)
	switch class {
	case "update":
		return lu
	case "delete":
		return ld
	case "add":
		return la
	case "uid":
		return um
	}
	panic(class)
}

var (
	a1 = r872Object{"panel-a", "1", "uid-a"}
	a2 = r872Object{"panel-a", "2", "uid-a"}
	ax = r872Object{"panel-a", "7", "uid-x"} // panel-a deleted and recreated
	b1 = r872Object{"panel-b", "1", "uid-b"}
)

// TestForcedPass872_StoreAheadOfTheList_IsNotALoss — the #872 artifact, per
// class. RED before #872: compareStore calls each case a divergence, the
// confirm loop cannot clear a store that is AHEAD of the LIST, and a repair is
// queued for a store that lost nothing.
func TestForcedPass872_StoreAheadOfTheList_IsNotALoss(t *testing.T) {
	for _, c := range []struct {
		name                 string
		store, current, list []r872Object
		class                string
	}{
		// Updated after the LIST: the store and the cluster are at rv 2.
		{"update", []r872Object{a2}, []r872Object{a2}, []r872Object{a1}, "update"},
		// Deleted after the LIST: gone from the store and the cluster.
		{"add", []r872Object{a2}, []r872Object{a2}, []r872Object{a2, b1}, "add"},
		// Created after the LIST: in the store and the cluster.
		{"delete", []r872Object{a2, b1}, []r872Object{a2, b1}, []r872Object{a2}, "delete"},
		// Deleted and recreated after the LIST: the store has the new uid.
		{"uid", []r872Object{ax}, []r872Object{ax}, []r872Object{a1}, "uid"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, cli := r872Setup(t, c.store, c.current, c.list, nil)
			candidates0 := storeDivergenceCandidates.Load()
			forcedVerify(context.Background(), repairGVR)

			// Non-vacuity: the LIST really disagreed with the store.
			if storeDivergenceCandidates.Load() == candidates0 {
				t.Fatalf("precondition: the stale LIST produced no candidate, so this arm tests nothing")
			}
			if got := r872Published(); got != 0 {
				t.Fatalf("#872 RED: a store AHEAD of the LIST published %d forced divergences (class %s); it lost nothing", got, c.class)
			}
			if d := storeRepairQueueDepth(); d != 0 {
				t.Fatalf("#872 RED: %d repair(s) queued for a store that lost nothing", d)
			}
			if got := storeRecheckClearedTotal.Load(); got != 1 {
				t.Errorf("store_divergence_recheck_cleared_total = %d, want 1", got)
			}
			// Bound: one GET per candidate when the first read clears it.
			if g := cli.gets.Load(); g != 1 {
				t.Errorf("re-check issued %d GETs for 1 candidate, want 1", g)
			}
			if row, ok := verificationRowFor(repairGVR); !ok || !row.verified {
				t.Errorf("a completed pass that found nothing must mark the GVR verified (row=%+v ok=%v)", row, ok)
			}
		})
	}
}

// TestForcedPass872_TrueLoss_IsStillDetectedAndRepaired — the re-check must
// not hide a real divergence, per class. The silent watch dropped the event,
// so the store is genuinely behind a cluster the LIST agrees with.
func TestForcedPass872_TrueLoss_IsStillDetectedAndRepaired(t *testing.T) {
	for _, c := range []struct {
		name           string
		store, current []r872Object
		class          string
	}{
		{"update", []r872Object{a1}, []r872Object{a2}, "update"},
		{"add", []r872Object{a2}, []r872Object{a2, b1}, "add"},
		{"delete", []r872Object{a2, b1}, []r872Object{a2}, "delete"},
		{"uid", []r872Object{a1}, []r872Object{ax}, "uid"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, cli := r872Setup(t, c.store, c.current, c.current, nil)
			forcedVerify(context.Background(), repairGVR)
			if got := r872ClassCount(c.class); got != 1 {
				lu, ld, la, um := divergenceCounts(verifySiteForced)
				t.Fatalf("a TRUE %s loss published %d (update/delete/add/uid = %d/%d/%d/%d), want exactly 1 of its class",
					c.class, got, lu, ld, la, um)
			}
			if got := r872Published(); got != 1 {
				t.Errorf("published %d divergences in total, want 1", got)
			}
			if d := storeRepairQueueDepth(); d != 1 {
				t.Fatalf("store_repairs_pending = %d after a TRUE loss, want 1 — the re-check must not stop the repair", d)
			}
			// Bound: a candidate that stays divergent is read twice, never more.
			// A literal, not recheckReads: a bound read from the constant under
			// test would loosen with it.
			if g := cli.gets.Load(); g != 2 {
				t.Errorf("re-check issued %d GETs for 1 divergent candidate, want 2", g)
			}
			if got := storeRecheckClearedTotal.Load(); got != 0 {
				t.Errorf("store_divergence_recheck_cleared_total = %d for a true loss, want 0", got)
			}
		})
	}
}

// TestForcedPass872_ObjectMovedDuringRecheck_IsNotALoss — why the re-check
// reads twice. The first quorum read answers rv 2; by the time the store is
// compared it already holds rv 3, because the object was updated again in
// between. One read would call that a lost UPDATE; the second read sees rv 3
// and clears it.
func TestForcedPass872_ObjectMovedDuringRecheck_IsNotALoss(t *testing.T) {
	a3 := r872Object{"panel-a", "3", "uid-a"}
	_, cli := r872Setup(t, []r872Object{a3}, []r872Object{a3}, []r872Object{a1}, nil)
	cli.firstGetRV = "2"
	forcedVerify(context.Background(), repairGVR)
	if got := r872Published(); got != 0 {
		t.Fatalf("#872 RED: an object that moved during the re-check was published as %d divergence(s)", got)
	}
	if d := storeRepairQueueDepth(); d != 0 {
		t.Fatalf("#872 RED: %d repair(s) queued for an object that only moved", d)
	}
	if g := cli.gets.Load(); g != 2 {
		t.Errorf("re-check issued %d GETs, want 2 (the first read stale, the second current)", g)
	}
}

// TestForcedPass872_FailedRecheck_IsInconclusive — a re-check read that fails
// must neither repair (a transient apiserver error would become the very false
// repair #872 removes) nor be recorded as a clean verification (a real loss
// would then hide for a whole maxVerificationAge). The fixture is a TRUE loss,
// so "it was dropped quietly" and "it was repaired anyway" are both visible.
func TestForcedPass872_FailedRecheck_IsInconclusive(t *testing.T) {
	_, cli := r872Setup(t, []r872Object{a1}, []r872Object{a2}, []r872Object{a2},
		errors.New("etcdserver: request timed out"))
	before := VerifySkippedByReasonSnapshot()[verifySkipRecheckError]
	forcedVerify(context.Background(), repairGVR)

	if cli.gets.Load() == 0 {
		t.Fatalf("precondition: the re-check never ran, so this arm tests nothing")
	}
	if got := r872Published(); got != 0 {
		t.Errorf("#872 RED: an unconfirmed candidate was published as %d divergence(s) after the re-check failed", got)
	}
	if d := storeRepairQueueDepth(); d != 0 {
		t.Errorf("#872 RED: %d repair(s) queued on a re-check error; an error must not become a repair", d)
	}
	if got := VerifySkippedByReasonSnapshot()[verifySkipRecheckError] - before; got != 1 {
		t.Errorf("store_verification_skipped{recheck_error} advanced by %d, want 1", got)
	}
	if row, ok := verificationRowFor(repairGVR); !ok || row.verified {
		t.Errorf("an inconclusive pass must leave the GVR unverified so the deadline returns to it (row=%+v ok=%v)", row, ok)
	}
}
