// issue579_deterministic_list_order_test.go — #579 falsifier.
//
// THE PROPERTY: reading one unchanged indexer partition twice must produce
// identical bytes. Before the fix both read paths enumerate a Go map, whose
// iteration order is randomised per read, so a rebuild of UNCHANGED data
// differs every time and a no-op refresh cannot be distinguished from a real
// one.
//
// WHY THESE ARMS ARE RELIABLY RED PRE-FIX, which matters for a randomised
// defect: a single two-read comparison passes by luck whenever the map happens
// to iterate the same way twice. #579's own evidence shows that — it flaked in
// 3 of 6 runs, and NEVER on a 1-item partition.
//
// The per-read odds are NOT 1/n!, and assuming they were cost this file a
// flaky arm before the mutation run caught it. Go randomises a small map's
// iteration START OFFSET and then walks its buckets in order, so a partition
// that fits one bucket yields a random ROTATION — roughly n distinct orders,
// not n!. Measured: with the fix mutated out, an arm asserting sorted order
// ONCE passed in 1 of 3 runs, consistent with ~1/8 for this 8-object
// partition and nowhere near 1/40320.
//
// So every arm here repeats the read `issue579Reads` (20) times and requires
// EVERY rendering to agree, which puts a false green at ~(1/8)^19. The 1-item
// case is deliberately avoided for the same reason it never flaked in #579.
//
// ARM-3 pins the CHOICE of order, not merely its stability, so that a future
// change to "stable but arbitrary" fails here. (namespace, name) is the order
// the apiserver itself returns a LIST in, which is what makes the cached path
// agree with the live path rather than only with itself.

package cache_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	dynamicfake "k8s.io/client-go/dynamic/fake"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// issue579Reads is how many times each arm re-reads one unchanged partition.
const issue579Reads = 20

// issue579Objects seeds names whose INSERTION order is deliberately not their
// sorted order, so a path that preserved insertion order would still fail ARM-3.
var issue579Objects = []struct{ ns, name string }{
	{"ns-a", "zulu"}, {"ns-a", "alpha"}, {"ns-a", "mike"}, {"ns-a", "bravo"},
	{"ns-a", "yankee"}, {"ns-a", "charlie"}, {"ns-a", "oscar"}, {"ns-a", "delta"},
	{"ns-b", "sierra"}, {"ns-b", "echo"},
}

func issue579Seed() []k8sruntime.Object {
	out := make([]k8sruntime.Object, 0, len(issue579Objects))
	for i, o := range issue579Objects {
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      o.name,
				"namespace": o.ns,
			},
			// A payload field so renderings differ per object and an
			// order change is visible in the bytes.
			"data": map[string]any{"ordinal": fmt.Sprintf("%d", i)},
		}}
		out = append(out, u)
	}
	return out
}

func issue579Watcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), servableListKinds(), issue579Seed()...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	registered, syncCh := rw.EnsureResourceType(servableTestGVR)
	if !registered {
		t.Fatalf("EnsureResourceType: want registered=true")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("informer did not sync within 5s")
	}
	return rw
}

// wantOrder is the (namespace, name) order for a partition.
func issue579WantOrder(ns string) []string {
	var keys []string
	for _, o := range issue579Objects {
		if ns != "" && o.ns != ns {
			continue
		}
		keys = append(keys, o.ns+"/"+o.name)
	}
	sort.Strings(keys)
	return keys
}

func issue579DecodedOrder(items []*unstructured.Unstructured) []string {
	out := make([]string, 0, len(items))
	for _, u := range items {
		out = append(out, u.GetNamespace()+"/"+u.GetName())
	}
	return out
}

// ARM-1: the RAW path (#578) renders identical bytes across repeated reads of
// one unchanged partition.
func TestIssue579_ARM1_RawRenderingIsByteStableAcrossReads(t *testing.T) {
	rw := issue579Watcher(t)

	first, ok := rw.ListRawServable(servableTestGVR, "ns-a")
	if !ok {
		t.Fatalf("ListRawServable: want servable=true")
	}
	if len(first) < 2 {
		t.Fatalf("precondition: need a multi-item partition to detect order; got %d", len(first))
	}
	join := func(bs [][]byte) string {
		s := ""
		for _, b := range bs {
			s += string(b) + "\x00"
		}
		return s
	}
	want := join(first)

	for i := 1; i < issue579Reads; i++ {
		got, ok := rw.ListRawServable(servableTestGVR, "ns-a")
		if !ok {
			t.Fatalf("read %d: want servable=true", i)
		}
		if join(got) != want {
			t.Fatalf("read %d of %d differs from read 0 over UNCHANGED data — "+
				"the raw envelope is not deterministically ordered, so a no-op "+
				"refresh cannot be detected (#579)", i, issue579Reads)
		}
	}
}

// ARM-2: the DECODED path is stable across repeated reads.
func TestIssue579_ARM2_DecodedRenderingIsStableAcrossReads(t *testing.T) {
	rw := issue579Watcher(t)

	want := issue579DecodedOrder(rw.ListObjects(servableTestGVR, "ns-a"))
	if len(want) < 2 {
		t.Fatalf("precondition: need a multi-item partition; got %d", len(want))
	}
	for i := 1; i < issue579Reads; i++ {
		got := issue579DecodedOrder(rw.ListObjects(servableTestGVR, "ns-a"))
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("read %d of %d differs from read 0 over UNCHANGED data:\n got %v\nwant %v",
				i, issue579Reads, got, want)
		}
	}
}

// ARM-3: the order is specifically (namespace, name) — not merely stable.
//
// This is what makes the cached path agree with the LIVE apiserver path, whose
// LIST is returned in etcd key order. A future change to some other stable
// order would keep ARM-1/ARM-2 green and must fail here.
func TestIssue579_ARM3_OrderIsNamespaceThenName(t *testing.T) {
	rw := issue579Watcher(t)

	want := issue579WantOrder("ns-a")
	// Repeated, not single-shot: a lone read is red only ~7/8 of the time
	// pre-fix (random rotation, ~n orders for a one-bucket partition), which
	// is a flaky arm. See the file header.
	for i := 0; i < issue579Reads; i++ {
		got := issue579DecodedOrder(rw.ListObjects(servableTestGVR, "ns-a"))
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("read %d: partition order is not (namespace, name):\n got %v\nwant %v",
				i, got, want)
		}
	}

	// Insertion order must NOT be what we got, or the arm proves nothing:
	// the seed is deliberately unsorted.
	var insertion []string
	for _, o := range issue579Objects {
		if o.ns == "ns-a" {
			insertion = append(insertion, o.ns+"/"+o.name)
		}
	}
	if fmt.Sprint(insertion) == fmt.Sprint(want) {
		t.Fatalf("test fixture is degenerate: seed insertion order equals sorted order, "+
			"so this arm cannot distinguish them (%v)", insertion)
	}
}

// ARM-4: the cluster-wide read ("" namespace) is also deterministic and sorted
// across namespaces — it uses store.List() rather than ByIndex, a different
// branch of the same selection.
func TestIssue579_ARM4_ClusterWideReadIsOrderedAcrossNamespaces(t *testing.T) {
	rw := issue579Watcher(t)

	want := issue579WantOrder("")
	for i := 0; i < issue579Reads; i++ {
		got := issue579DecodedOrder(rw.ListObjects(servableTestGVR, ""))
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("cluster-wide read %d is not (namespace, name) ordered:\n got %v\nwant %v",
				i, got, want)
		}
	}
}

// ARM-5: the raw and decoded paths agree on ORDER.
//
// #579 notes the defect is a property of the indexer read shared by both, so a
// fix applied to only one would leave the two envelopes describing the same
// data in different orders. That is the regression this arm exists for.
func TestIssue579_ARM5_RawAndDecodedPathsAgreeOnOrder(t *testing.T) {
	rw := issue579Watcher(t)

	// Repeated for the same reason as ARM-3: one raw read and one decoded
	// read can coincide by chance (~1/8 here) when ordering is a random
	// rotation, which would make this arm a latent flake rather than a test.
	for i := 0; i < issue579Reads; i++ {
		raw, ok := rw.ListRawServable(servableTestGVR, "ns-a")
		if !ok {
			t.Fatalf("read %d: ListRawServable: want servable=true", i)
		}
		decoded := rw.ListObjects(servableTestGVR, "ns-a")
		if len(raw) != len(decoded) {
			t.Fatalf("read %d: raw and decoded disagree on LENGTH: %d vs %d", i, len(raw), len(decoded))
		}

		rawOrder := make([]string, 0, len(raw))
		for _, b := range raw {
			var u unstructured.Unstructured
			if err := u.UnmarshalJSON(b); err != nil {
				t.Fatalf("read %d: raw item is not valid JSON: %v", i, err)
			}
			rawOrder = append(rawOrder, u.GetNamespace()+"/"+u.GetName())
		}
		if fmt.Sprint(rawOrder) != fmt.Sprint(issue579DecodedOrder(decoded)) {
			t.Fatalf("read %d: raw and decoded paths disagree on ORDER:\n raw %v\n dec %v",
				i, rawOrder, issue579DecodedOrder(decoded))
		}
	}
}
