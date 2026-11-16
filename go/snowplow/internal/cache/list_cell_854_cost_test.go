package cache

// list_cell_854_cost_test.go — #854 (#865 review): what a read of a 50K-object
// incremental cell costs, per kind of work it can trigger. MEASUREMENT, not an
// arm: it runs only with LISTCELL854_COST=1 and logs, it asserts nothing.
//
//   - a comparator pass (the work maybePass starts in the background);
//   - a read with nothing pending (a restamp) and its Items view;
//   - a read that applies k pending changes (k = 1, 10, 100) and its Items view.
//
// Objects carry a ~1.2 KB annotation, so each is ~1.4 KB of JSON, as in the
// paired arm. Allocation is runtime TotalAlloc around the call, time is wall.

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestListCell854_CostAt50K(t *testing.T) {
	if os.Getenv("LISTCELL854_COST") == "" {
		t.Skip("measurement: set LISTCELL854_COST=1")
	}
	const n = 50000
	pad := strings.Repeat("x", 1200)
	objs := make([]*unstructured.Unstructured, n)
	for i := range objs {
		objs[i] = c854Obj(i)
		objs[i].SetAnnotations(map[string]string{"pad": pad})
	}
	rw, c, _ := c854AnchorObjs(t, objs, c854Funcs())
	c.passDue.Store(true) // no pass is made due (or enqueued) while a read is measured
	idx := rw.c854Indexer(t)

	measure := func(name string, reps int, prep, fn func()) {
		var alloc uint64
		var wall time.Duration
		var ms0, ms1 runtime.MemStats
		for r := 0; r < reps; r++ {
			if prep != nil {
				prep()
			}
			runtime.GC()
			runtime.ReadMemStats(&ms0)
			t0 := time.Now()
			fn()
			wall += time.Since(t0)
			runtime.ReadMemStats(&ms1)
			alloc += ms1.TotalAlloc - ms0.TotalAlloc
		}
		t.Logf("COST %-34s mean alloc %8.3f MB   mean wall %8.3f ms   (n=%d objects, %d reps)",
			name, float64(alloc)/float64(reps)/1e6, float64(wall.Microseconds())/float64(reps)/1000, n, reps)
	}
	read := func() {
		v, ok := c.Snapshot(DepGenEpochNow())
		if !ok {
			t.Fatal("the cell unanchored during the measurement")
		}
		_ = v.Items()
	}
	gen := 0
	change := func(k int) func() {
		return func() {
			for j := 0; j < k; j++ {
				gen++
				o := c854Obj((gen * 7919) % n)
				o.SetAnnotations(map[string]string{"pad": pad})
				o.Object["spec"] = map[string]any{"i": int64(gen)}
				bo, err := newBytesObject(o)
				if err != nil {
					t.Fatal(err)
				}
				if err := idx.Update(bo); err != nil {
					t.Fatal(err)
				}
				noteListCellsPending(c854GVR, "c854", o.GetName())
			}
			depEventSeq.Add(1)
		}
	}

	measure("comparator pass", 5, nil, func() { c.ComparePass() })
	measure("read, nothing pending", 20, func() { depEventSeq.Add(1) }, read)
	for _, k := range []int{1, 10, 100} {
		measure(fmt.Sprintf("read applying %d change(s)", k), 10, change(k), read)
	}
}
