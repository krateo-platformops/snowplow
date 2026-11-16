package cache

// store_swap_computem_bench_test.go — #928 F5: the cost of computeM (step 5)
// over a 50K store, on the swap goroutine, for the store shapes a swap meets:
//
//   - bytes/shared   production (H5 *bytesObject) after a divergence repair:
//     reuse made every unchanged object the SAME pointer as acted's
//   - bytes/widen    *bytesObject after a schema widen that changed every
//     object: distinct pointers, a bytes compare per key
//   - unstructured/widen  a non-streaming *Unstructured store, every object
//     distinct: storeItemJSON per object per side
//
// Two adapters (the dep handler and one extension), as every owned GVR has.

import (
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientcache "k8s.io/client-go/tools/cache"
)

const benchM = 50000

func benchObj(gvr schema.GroupVersionResource, i int, v string) *unstructured.Unstructured {
	o := swapObj(gvr, fmt.Sprintf("o%05d", i), "1", v)
	o.Object["spec"] = map[string]any{"v": v, "pad": fmt.Sprintf("%0256d", i)} // ~0.5 KB per object
	return o
}

func benchComputeM(b *testing.B, mk func(i int, side string) any) {
	gvr := schema.GroupVersionResource{Group: "bench870.krateo.io", Version: "v1", Resource: "panels"}
	idx := clientcache.NewIndexer(clientcache.MetaNamespaceKeyFunc, clientcache.Indexers{})
	inst := newStoreInstance(gvr, stubGenericInformer{inf: stubSharedInformer{idx: idx}}, shapeStandaloneDynamic, make(chan struct{}))
	acted := []*actedRecord{newActedRecord(), newActedRecord()}
	for i := 0; i < benchM; i++ {
		nv := mk(i, "new")
		if err := idx.Add(nv); err != nil {
			b.Fatal(err)
		}
		av := mk(i, "acted")
		k, _ := clientcache.MetaNamespaceKeyFunc(nv)
		for _, r := range acted {
			r.set(k, av, true)
		}
	}
	inst.adapt(depHandlerName, (&swapRecorder{}).handler(), acted[0])
	inst.adapt("ext/1", (&swapRecorder{}).handler(), acted[1])
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if m := computeM(inst); len(m) != 0 {
			b.Fatalf("|M| = %d, want 0 (byte-equal stores)", len(m))
		}
	}
}

func BenchmarkComputeM50K(b *testing.B) {
	gvr := schema.GroupVersionResource{Group: "bench870.krateo.io", Version: "v1", Resource: "panels"}
	b.Run("bytes/shared", func(b *testing.B) {
		shared := map[int]any{}
		benchComputeM(b, func(i int, _ string) any {
			if o, ok := shared[i]; ok {
				return o
			}
			o, err := newBytesObject(benchObj(gvr, i, "v1"))
			if err != nil {
				b.Fatal(err)
			}
			shared[i] = o
			return o
		})
	})
	b.Run("bytes/widen", func(b *testing.B) {
		benchComputeM(b, func(i int, _ string) any {
			o, err := newBytesObject(benchObj(gvr, i, "v1"))
			if err != nil {
				b.Fatal(err)
			}
			return o
		})
	})
	b.Run("unstructured/widen", func(b *testing.B) {
		benchComputeM(b, func(i int, _ string) any { return benchObj(gvr, i, "v1") })
	})
}
