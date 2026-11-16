//go:build unit || integration

package cache

import (
	"context"
	"testing"
)

// TestIssue890_DeclinedExternalMarkHoldsItsKeysEdges — a #643 declined-external
// mark judges whether its facts still hold by its key's dep edges, so a resolve
// end does not strip a MARKED key; once the mark goes (GONE, config redrive),
// the non-resident key's edges go with it.
func TestIssue890_DeclinedExternalMarkHoldsItsKeysEdges(t *testing.T) {
	d, store, _ := setup890(t)
	set := EngineDeclinedExternalSet()
	set.Reset()
	t.Cleanup(set.Reset)
	widget := DepKey{GVR: gvr883Calm, Namespace: "ns-890", Name: "w-ext"}
	dep := DepKey{GVR: gvr883Calm, Namespace: "ns-890", Name: "ext-dep"}

	t.Run("refusal_keeps_a_held_key", func(t *testing.T) {
		k := "L1_890_marked_refused"
		store.put(k, &ResolvedEntry{RawJSON: []byte(`{}`)})
		gen := store.CaptureGen(k)
		old, _ := store.GetNoTouch(k)
		store.EvictUnrefreshable(k, old)
		ctx := WithL1KeyContext(context.Background(), k)
		d.Record(ctx, k, dep.GVR, dep.Namespace, dep.Name)
		set.MarkWith(k, &DeclinedExternalMark{Widget: widget, Deps: []DepKey{dep}})
		t.Cleanup(func() { set.Unmark(k) })
		if store.putIfGen(ctx, k, &ResolvedEntry{RawJSON: []byte(`{"x":1}`)}, gen) {
			t.Fatal("harness: the write-back of the evicted cell must be refused")
		}
		if !d.hasEdge(k, dep) {
			t.Fatal("#890 RED: the refusal strip removed the edges of a key a registered holder holds")
		}
	})

	for _, retire := range []struct {
		name string
		do   func()
	}{
		{"gone", func() { set.ForgetCoordinate(widget.GVR, widget.Namespace, widget.Name) }},
		{"config_redrive", func() { set.Reset() }},
	} {
		t.Run(retire.name, func(t *testing.T) {
			k := "L1_890_marked_" + retire.name
			ctx := WithL1KeyContext(context.Background(), k)
			d.Record(ctx, k, dep.GVR, dep.Namespace, dep.Name)
			set.MarkWith(k, &DeclinedExternalMark{Widget: widget, Deps: []DepKey{dep}})
			d.ReleaseUnstored(ctx, k) // the declining resolve ends
			if !d.hasEdge(k, dep) {
				t.Fatal("#890: a resolve end stripped the edges of a key its declined-external mark holds")
			}
			retire.do()
			if d.hasEdges(k) {
				t.Fatalf("#890/#893 RED (%s): the mark retired and the non-resident key kept its edges", retire.name)
			}
		})
	}
}
