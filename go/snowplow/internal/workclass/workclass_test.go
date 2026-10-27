package workclass_test

// workclass_test.go — #871: the label mechanism itself. The per-wrapper arms
// live next to each wrapper (cache, dispatchers, api).

import (
	"context"
	"encoding/json"
	"expvar"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/testsupport/workclassprobe"
	"github.com/krateo-platformops/snowplow/internal/workclass"
)

// TestDo_EveryClass_OnCPUSamples_SelfAndChild is the property the bench reads:
// a CPU sample on the unit's goroutine, and on a goroutine the unit starts,
// carries the unit's class — for every class.
func TestDo_EveryClass_OnCPUSamples_SelfAndChild(t *testing.T) {
	for _, c := range workclass.Classes {
		t.Run(c.String(), func(t *testing.T) {
			var p *workclassprobe.Probe
			got := workclassprobe.CPUClassesUntilSampled(t, func(spin time.Duration) {
				p = &workclassprobe.Probe{Child: true, Spin: spin}
				workclass.Do(context.Background(), c, func(ctx context.Context) { p.Observe(ctx) })
			})
			if got[c.String()] == 0 {
				t.Fatalf("no CPU sample of the spin carried snowplow_work=%s: %v", c, got)
			}
			for k, n := range got {
				if k != c.String() {
					t.Fatalf("%d spin sample(s) carried %q inside a %s unit: %v", n, k, c, got)
				}
			}
			workclassprobe.RequireAll(t, "unit goroutine", p.Self(), c.String())
			workclassprobe.RequireAll(t, "goroutine started inside the unit", p.Children(), c.String())
			workclassprobe.RequireAll(t, "unit ctx", p.Ctx(), c.String())
		})
	}
}

// TestProbe_NegativeControl: the instruments read "<none>" off unlabelled
// work, so a pass above is not an instrument that always says yes.
func TestProbe_NegativeControl(t *testing.T) {
	var p *workclassprobe.Probe
	got := workclassprobe.CPUClassesUntilSampled(t, func(spin time.Duration) {
		p = &workclassprobe.Probe{Child: true, Spin: spin}
		p.Observe(context.Background())
	})
	if got[workclassprobe.None] == 0 || len(got) != 1 {
		t.Fatalf("unlabelled spin samples: %v, want only %q", got, workclassprobe.None)
	}
	workclassprobe.RequireAll(t, "unlabelled goroutine", p.Self(), workclassprobe.None)
	workclassprobe.RequireAll(t, "unlabelled child", p.Children(), workclassprobe.None)
}

// TestDo_Nested_RestoresOuterClass: a comparator pass inside a refresh unit
// labels the pass, and the refresh label is back when it returns.
func TestDo_Nested_RestoresOuterClass(t *testing.T) {
	var inner, after string
	workclass.Do(context.Background(), workclass.Refresh, func(ctx context.Context) {
		workclass.Do(ctx, workclass.Comparator, func(context.Context) { inner = workclassprobe.GoroutineClass() })
		after = workclassprobe.GoroutineClass()
	})
	if inner != "comparator" || after != "refresh" {
		t.Fatalf("inner=%q after=%q, want comparator then refresh", inner, after)
	}
	if got := workclassprobe.GoroutineClass(); got != workclassprobe.None {
		t.Fatalf("after the unit the goroutine still carries %q", got)
	}
}

// TestPprofDo_DetachedContextHazard pins the Go semantics the package works
// around: pprof.Do on a context WITHOUT the work label resets the goroutine to
// that context's labels when it returns. Background(c) is the fix.
func TestPprofDo_DetachedContextHazard(t *testing.T) {
	var dropped, kept string
	workclass.Do(context.Background(), workclass.FillBG, func(context.Context) {
		pprof.Do(context.Background(), pprof.Labels("other", "x"), func(context.Context) {})
		dropped = workclassprobe.GoroutineClass()
	})
	workclass.Do(context.Background(), workclass.FillBG, func(context.Context) {
		pprof.Do(workclass.Background(workclass.FillBG), pprof.Labels("other", "x"), func(context.Context) {})
		kept = workclassprobe.GoroutineClass()
	})
	if dropped != workclassprobe.None {
		t.Fatalf("pprof.Do(context.Background()) left %q; the hazard this package documents no longer exists", dropped)
	}
	if kept != "fill_bg" {
		t.Fatalf("pprof.Do(workclass.Background(FillBG)) left %q, want fill_bg", kept)
	}
}

func counts(t *testing.T) map[string]workclass.Counts {
	t.Helper()
	v := expvar.Get("snowplow_work_class")
	if v == nil {
		t.Fatal("expvar snowplow_work_class is not published")
	}
	var m map[string]workclass.Counts
	if err := json.Unmarshal([]byte(v.String()), &m); err != nil {
		t.Fatalf("snowplow_work_class is not JSON: %v (%s)", err, v.String())
	}
	return m
}

// TestExpvar_CountersMove: every class is always present, and a unit moves
// exactly its own class's started and completed by one.
func TestExpvar_CountersMove(t *testing.T) {
	for _, c := range workclass.Classes {
		before := counts(t)
		if len(before) != len(workclass.Classes) {
			t.Fatalf("snowplow_work_class has %d classes, want %d: %v", len(before), len(workclass.Classes), before)
		}
		var mid workclass.Counts
		workclass.Do(context.Background(), c, func(context.Context) { mid = counts(t)[c.String()] })
		after := counts(t)
		for _, o := range workclass.Classes {
			b, a := before[o.String()], after[o.String()]
			want := workclass.Counts{Started: b.Started, Completed: b.Completed}
			if o == c {
				want = workclass.Counts{Started: b.Started + 1, Completed: b.Completed + 1}
			}
			if a != want {
				t.Fatalf("after one %s unit, %s = %+v, want %+v", c, o, a, want)
			}
		}
		if mid.Started != before[c.String()].Started+1 || mid.Completed != before[c.String()].Completed {
			t.Fatalf("inside a %s unit: %+v, want started+1 and completed unchanged from %+v", c, mid, before[c.String()])
		}
	}
}

// TestContinue_LabelsWithoutCounting and Detach's class rule.
func TestDetach_ClassRule(t *testing.T) {
	cases := []struct {
		name    string
		parent  context.Context
		want    string
		counted bool
	}{
		{"background caller continues its class", workclass.Background(workclass.Prewarm), "prewarm", false},
		{"customer caller starts a fill_bg unit", workclass.Background(workclass.Customer), "fill_bg", true},
		{"unlabelled caller starts a fill_bg unit", context.Background(), "fill_bg", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := counts(t)
			done := make(chan [2]string)
			go func() {
				ctx, end := workclass.Detach(tc.parent)
				defer end()
				done <- [2]string{workclassprobe.GoroutineClass(), workclass.Of(ctx).String()}
			}()
			got := <-done
			if got[0] != tc.want || got[1] != tc.want {
				t.Fatalf("goroutine=%q ctx=%q, want %q", got[0], got[1], tc.want)
			}
			after := counts(t)
			d := after["fill_bg"].Started - before["fill_bg"].Started
			if tc.counted != (d == 1) {
				t.Fatalf("fill_bg started moved by %d; counted=%v", d, tc.counted)
			}
		})
	}
}

// TestClearGoroutine: a long-lived goroutine started inside a unit sheds the
// unit's label.
func TestClearGoroutine(t *testing.T) {
	done := make(chan [2]string)
	workclass.Do(context.Background(), workclass.Customer, func(context.Context) {
		go func() {
			inherited := workclassprobe.GoroutineClass()
			workclass.ClearGoroutine()
			done <- [2]string{inherited, workclassprobe.GoroutineClass()}
		}()
	})
	got := <-done
	if got[0] != "customer" || got[1] != workclassprobe.None {
		t.Fatalf("inherited=%q cleared=%q, want customer then none", got[0], got[1])
	}
}

func TestParse_RoundTrip(t *testing.T) {
	for _, c := range workclass.Classes {
		if got, ok := workclass.Parse(c.String()); !ok || got != c {
			t.Fatalf("Parse(%q) = %v, %v", c, got, ok)
		}
	}
	for _, s := range []string{"", "none", "Customer", "refresh "} {
		if _, ok := workclass.Parse(s); ok {
			t.Fatalf("Parse(%q) accepted", s)
		}
	}
	if workclass.Customer.Background() || workclass.None.Background() || !workclass.FillBG.Background() {
		t.Fatal("Background() is wrong")
	}
}

// --- overhead ------------------------------------------------------------------

//go:noinline
func unitBody(ctx context.Context) {
	_ = ctx
}

// BenchmarkUnit_NoLabel / _Do / _Begin: the cost one unit pays for its label
// and counters (the body is empty, so this is the whole overhead).
func BenchmarkUnit_NoLabel(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		unitBody(ctx)
	}
}

func BenchmarkUnit_Do(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		workclass.Do(ctx, workclass.Refresh, unitBody)
	}
}

func BenchmarkUnit_Begin(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		c, end := workclass.Begin(ctx, workclass.Refresh)
		unitBody(c)
		end()
	}
}

// BenchmarkUnit_Begin_OnRequestContext: the customer form — the parent context
// already carries values (a request context does).
func BenchmarkUnit_Begin_OnRequestContext(b *testing.B) {
	type k struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), k{}, 1))
	defer cancel()
	b.ReportAllocs()
	for b.Loop() {
		c, end := workclass.Begin(ctx, workclass.Customer)
		unitBody(c)
		end()
	}
}
