package cache

// store_swap_helpers_test.go — #870 test seams shared by the swap arms, the
// rewritten repair arms and the #854 comparator arms.

import (
	"context"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	clientcache "k8s.io/client-go/tools/cache"
)

// storeRepairsRequestedForTest is how many repair builds the forced pass has
// started (store_repairs_fired_total): #870 deleted the repair queue whose
// depth these arms used to read. A request that joins a running build, or is
// refused, does not count.
func storeRepairsRequestedForTest() int64 { return int64(storeRepairsFiredTotal.Load()) }

// noteDeliveredForTest records obj as delivered to the dep handler of gvr's
// live instance — what the adapter does after a real delivery — for an arm
// that writes the indexer directly and must model "the dep handler ran"
// (#854's comparator arms). It writes the comparator record itself (F5).
func noteDeliveredForTest(gvr schema.GroupVersionResource, obj any, deleted bool) {
	v, ok := comparatorRecord.Load(gvr)
	if !ok {
		return
	}
	a := v.(*handlerAdapter)
	key, err := clientcache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		return
	}
	a.mu.Lock()
	if !a.freed {
		a.recordSeenLocked(key, obj, !deleted)
	}
	a.mu.Unlock()
}

// typeMarksAnyCause sums every TYPE-path dirty-mark bucket of d, whatever the
// cause (#870 K2: a swap must not move any of them).
func typeMarksAnyCause(d *DepTracker) uint64 {
	var n uint64
	for b, c := range d.dmAttr.byBucket {
		if b.Path == dmPathType {
			n += c.Load()
		}
	}
	return n
}

// partialMetaFor is partialMeta for any GVR.
func partialMetaFor(gvr schema.GroupVersionResource, ns, name, rv, uid string) *metav1.PartialObjectMetadata {
	pm := partialMeta(ns, name, rv, uid)
	pm.APIVersion = gvr.GroupVersion().String()
	return pm
}

// indexerRVFor is indexerRV for any GVR.
func indexerRVFor(t *testing.T, rw *ResourceWatcher, gvr schema.GroupVersionResource, ns, name string) (string, bool) {
	t.Helper()
	st := rw.StoreObjectState(gvr, ns, name)
	return st.ResourceVersion, st.Found
}

// holdListDynamic holds LISTs of one GVR OUTSIDE the fake client's reactor
// lock (a sleeping reactor holds that lock and stalls every other action of the
// fake, writes included). hold is asked for the n-th LIST (1-based) of gvr and
// returns the channel to wait on, or nil for no hold. The wait also ends when
// the LIST's own context is cancelled (C4: a stopped copy's LIST returns).
type holdListDynamic struct {
	dynamic.Interface
	gvr   schema.GroupVersionResource
	hold  func(n int64) <-chan struct{}
	lists *atomic.Int64
}

func newHoldListDynamic(inner dynamic.Interface, gvr schema.GroupVersionResource, hold func(n int64) <-chan struct{}) holdListDynamic {
	return holdListDynamic{Interface: inner, gvr: gvr, hold: hold, lists: &atomic.Int64{}}
}

func (c holdListDynamic) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	r := c.Interface.Resource(gvr)
	if gvr != c.gvr {
		return r
	}
	return holdListResource{NamespaceableResourceInterface: r, c: c}
}

func (c holdListDynamic) wait(ctx context.Context) error {
	n := c.lists.Add(1)
	if ch := c.hold(n); ch != nil {
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type holdListResource struct {
	dynamic.NamespaceableResourceInterface
	c holdListDynamic
}

func (r holdListResource) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	if err := r.c.wait(ctx); err != nil {
		return nil, err
	}
	return r.NamespaceableResourceInterface.List(ctx, opts)
}

func (r holdListResource) Namespace(ns string) dynamic.ResourceInterface {
	return holdListNSResource{ResourceInterface: r.NamespaceableResourceInterface.Namespace(ns), c: r.c}
}

type holdListNSResource struct {
	dynamic.ResourceInterface
	c holdListDynamic
}

func (r holdListNSResource) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	if err := r.c.wait(ctx); err != nil {
		return nil, err
	}
	return r.ResourceInterface.List(ctx, opts)
}
