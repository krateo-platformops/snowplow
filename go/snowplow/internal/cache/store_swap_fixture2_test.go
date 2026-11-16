package cache

// store_swap_fixture2_test.go — #870 fixture extensions: an incremental list
// cell over the arm's GVR, the metadata-only variant, and the CRD lifecycle
// queue saturation used by never-blocks (c).

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	metadatafake "k8s.io/client-go/metadata/fake"
	k8stesting "k8s.io/client-go/testing"
	clientcache "k8s.io/client-go/tools/cache"
)

func swapCellFuncs() ListCellFuncs {
	return ListCellFuncs{
		Decode: func(raw []byte) (*unstructured.Unstructured, error) {
			var m map[string]any
			if err := json.Unmarshal(raw, &m); err != nil {
				return nil, err
			}
			return &unstructured.Unstructured{Object: m}, nil
		},
		Match: func([]byte) (bool, bool) { return true, true },
		Marshal: func(raws [][]byte) ([]byte, error) {
			out := []byte(`{"items":[`)
			for i, r := range raws {
				if i > 0 {
					out = append(out, ',')
				}
				out = append(out, r...)
			}
			return append(out, `]}`...), nil
		},
	}
}

// anchorCell anchors an incremental list cell over the GVR's namespace.
func (f *swapFix) anchorCell() *ListCell {
	f.t.Helper()
	keyed, ok := f.rw.listRawKeyedServable(f.gvr, swapNS)
	if !ok {
		f.t.Fatal("anchor: the GVR is not servable")
	}
	fn := swapCellFuncs()
	raws := make([][]byte, len(keyed))
	items := make([]*unstructured.Unstructured, len(keyed))
	for i, kr := range keyed {
		raws[i] = kr.raw
		items[i], _ = fn.Decode(kr.raw)
	}
	env, _ := fn.Marshal(raws)
	c := AnchorListCell(ListCellAnchor{Key: "swap-cell-" + f.gvr.Resource, GVR: f.gvr, Namespace: swapNS,
		Envelope: env, Items: items, APIVersion: f.gvr.GroupVersion().String(), Kind: "PanelList", Funcs: fn})
	if c == nil {
		f.t.Fatal("precondition: the cell did not anchor")
	}
	return c
}

// readCell reads the cell as a customer read would (drains pending up to now).
func (f *swapFix) readCell(c *ListCell) string {
	v, ok := c.Snapshot(depEventSeq.Load())
	if !ok {
		return "<unanchored>"
	}
	return string(v.Raw())
}

// fromScratch is the full fill a cell must equal: the servable listing.
func (f *swapFix) fromScratch() string {
	keyed, ok := f.rw.listRawKeyedServable(f.gvr, swapNS)
	if !ok {
		return "<not servable>"
	}
	raws := make([][]byte, len(keyed))
	for i, kr := range keyed {
		raws[i] = kr.raw
	}
	b, _ := swapCellFuncs().Marshal(raws)
	return string(b)
}

// newSwapFixMetadata is the fixture for a METADATA-ONLY GVR: the informer is
// the standalone metadata informer (owned) and the store holds
// *PartialObjectMetadata. The cluster's truth is the metadata fake's tracker.
func newSwapFixMetadata(t *testing.T, gvr schema.GroupVersionResource, n int) *swapFix {
	t.Helper()
	// The dynamic fixture provides the watcher, the recorder and the
	// registry; the GVR is then registered through the metadata path.
	base := newSwapFix(t, schema.GroupVersionResource{Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource + "dyn"}, nil)
	sch := k8sruntime.NewScheme()
	_ = metav1.AddMetaToScheme(sch)
	mfake := metadatafake.NewSimpleMetadataClient(sch)
	// Seeded under the arm's GVR explicitly: the constructor would guess the
	// resource from the kind.
	for i := 0; i < n; i++ {
		if err := mfake.Tracker().Create(gvr, &metav1.PartialObjectMetadata{
			TypeMeta:   metav1.TypeMeta{APIVersion: gvr.GroupVersion().String(), Kind: "Panel"},
			ObjectMeta: metav1.ObjectMeta{Namespace: swapNS, Name: "m" + itoaForTest(i), ResourceVersion: "1", UID: "u"},
		}, swapNS); err != nil {
			t.Fatalf("metadata seed: %v", err)
		}
	}
	f := &swapFix{t: t, gvr: gvr, rec: base.rec, holds: map[int64]chan struct{}{}, rw: base.rw, fake: base.fake}
	mfake.PrependWatchReactor("*", func(a k8stesting.Action) (bool, watch.Interface, error) {
		w := watch.NewFakeWithChanSize(256, false)
		t.Cleanup(w.Stop)
		f.mu.Lock()
		f.watchers = append(f.watchers, w)
		f.mu.Unlock()
		return true, w, nil
	})
	f.meta = mfake
	// The recorder must match this GVR too.
	RegisterHandlerExtension(HandlerExtension{
		Name:      "swap-recorder-meta-" + t.Name(),
		Predicate: func(g schema.GroupVersionResource) bool { return g == gvr },
		Handlers: func(*ResourceWatcher, schema.GroupVersionResource) clientcache.ResourceEventHandler {
			return f.rec.handler()
		},
	})
	f.rw.SetMetadataClient(mfake)
	AddNavigationDiscoveredGroup(gvr.Group)
	_, syncCh := f.rw.EnsureResourceTypeMetadataOnly(gvr)
	select {
	case <-syncCh:
	case <-time.After(30 * time.Second):
		t.Fatal("metadata informer never synced")
	}
	f.waitFor("metadata GVR servable", func() bool { return f.rw.IsServable(gvr) })
	if in := f.live(); in == nil || in.shape != shapeStandaloneMetadata {
		t.Fatalf("precondition: a standalone metadata instance, got %+v", in)
	}
	return f
}

// saturateCRDLifecycleQueueForTest fills the CRD lifecycle queue with no worker
// draining it, and drains it at cleanup.
func saturateCRDLifecycleQueueForTest(t *testing.T) {
	t.Helper()
	c := crdDiscoverySingleton()
	c.startOnce.Do(func() {})
	for i := 0; i < crdDiscoveryQueueDepth; i++ {
		c.queue <- crdDiscoveryEvent{obj: crdLifecycleObj("fill-" + itoaForTest(i)), kind: crdLifecycleUpdate}
	}
	t.Cleanup(func() {
		for {
			select {
			case <-c.queue:
			case <-time.After(100 * time.Millisecond):
				return
			}
		}
	})
}

var _ = context.Background
