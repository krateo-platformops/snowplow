package api

// work_class_871_test.go — #871 (measurement only): the api package's work
// classes, at the two escapes the pre-build check named (E1, E2) and at the
// detached fill_bg work a customer's stale serve starts.
//
//	E1  the cluster-list async populate (a goroutine detached to
//	    context.Background()) carries a background caller's class, or is a new
//	    fill_bg unit when a customer started it — and its ctx carries the label,
//	    so a pprof.Do on it (the content fill's) does not drop it
//	E2  a self-loopback stage carries a background caller's class in
//	    workclass.Header (the ingest half is dispatchers' LB arm)
//	D2  a stale serve's debt refill and its dependent replace are fill_bg,
//	    although the customer's goroutine that started them is customer

import (
	"context"
	"io"
	"log/slog"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	templatesapi "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/testsupport/workclassprobe"
	"github.com/krateo-platformops/snowplow/internal/workclass"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestWorkClass871_E1_ClusterListAsyncPopulate(t *testing.T) {
	cases := []struct {
		name    string
		caller  workclass.Class
		want    string
		counted bool
	}{
		{"started by a customer: a new fill_bg unit", workclass.Customer, "fill_bg", true},
		{"started by a prewarm walk: continues prewarm", workclass.Prewarm, "prewarm", false},
		{"started by a refresh: continues refresh", workclass.Refresh, "refresh", false},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ResetClusterListAsyncStateForTest()
			t.Cleanup(ResetClusterListAsyncStateForTest)
			type seen struct{ goroutine, ctx, afterPprofDo string }
			got := make(chan seen, 1)
			prev := populateClusterListCellSyncFn
			populateClusterListCellSyncFn = func(ctx context.Context, _ *slog.Logger, _ *templatesapi.API, _ schema.GroupVersionResource, _ string, _ httpcall.RequestOptions, _ *cache.ResolvedCacheStore) bool {
				s := seen{goroutine: workclassprobe.GoroutineClass(), ctx: workclass.Of(ctx).String()}
				// The content fill wraps its dispatch in pprof.Do(ctx, …), which
				// resets the goroutine to ctx's labels on return.
				pprof.Do(ctx, pprof.Labels("apistage_fill_reason", "probe"), func(context.Context) {})
				s.afterPprofDo = workclassprobe.GoroutineClass()
				got <- s
				return true
			}
			t.Cleanup(func() { populateClusterListCellSyncFn = prev })
			before := workclass.Snapshot()["fill_bg"].Started

			workclass.Do(context.Background(), tc.caller, func(ctx context.Context) {
				populateClusterListCellAsync(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)),
					&templatesapi.API{Name: "wc871"}, endpoints.Endpoint{},
					schema.GroupVersionResource{Group: "g", Version: "v1", Resource: "r"},
					"wc871-cell-"+string(rune('a'+i)), httpcall.RequestOptions{}, cache.ResolvedCache())
			})
			var s seen
			select {
			case s = <-got:
			case <-time.After(5 * time.Second):
				t.Fatal("the async populate never ran")
			}
			if s.goroutine != tc.want || s.ctx != tc.want || s.afterPprofDo != tc.want {
				t.Fatalf("populate goroutine=%q ctx=%q after-pprof.Do=%q, want %q", s.goroutine, s.ctx, s.afterPprofDo, tc.want)
			}
			// The fill_bg unit completes when the goroutine returns.
			if !fill710WaitFor(5*time.Second, func() bool {
				c := workclass.Snapshot()["fill_bg"]
				return !tc.counted || c.Completed >= before+1
			}) {
				t.Fatal("the fill_bg unit never completed")
			}
			if d := workclass.Snapshot()["fill_bg"].Started - before; tc.counted != (d == 1) {
				t.Fatalf("fill_bg started moved by %d; counted=%v", d, tc.counted)
			}
		})
	}
}

func TestWorkClass871_E2_SelfLoopbackHeaderCarriesABackgroundClass(t *testing.T) {
	for _, c := range workclass.Classes {
		h, ok := selfLoopbackWorkClassHeader(workclass.Background(c))
		if c.Background() != ok {
			t.Fatalf("%s: header emitted=%v, want %v (only a background class rides the hop)", c, ok, c.Background())
		}
		if ok && h != workclass.Header+": "+c.String() {
			t.Fatalf("%s: header %q", c, h)
		}
	}
	if _, ok := selfLoopbackWorkClassHeader(context.Background()); ok {
		t.Fatal("an unlabelled caller emitted a work-class header")
	}
}

func TestWorkClass871_D2_DebtRefillAndReplaceAreFillBG(t *testing.T) {
	f := newD2Fixture(t)
	var (
		mu       sync.Mutex
		dispatch []string
		replace  []string
	)
	orig := dispatchViaInformerFn
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		mu.Lock()
		dispatch = append(dispatch, workclassprobe.GoroutineClass()+"/"+workclass.Of(ctx).String())
		mu.Unlock()
		return orig(ctx, call)
	}
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	cache.RegisterRefreshFunc("restactions", func(ctx context.Context, key string, in cache.ResolvedKeyInputs) error {
		mu.Lock()
		replace = append(replace, workclassprobe.GoroutineClass()+"/"+workclass.Of(ctx).String())
		mu.Unlock()
		f.store.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: []byte(`{"fresh":true}`), Inputs: &in})
		cache.PublishRefresh(key)
		return nil
	})
	release := HoldStaleDebtRefillsForTest()
	t.Cleanup(release)
	before := workclass.Snapshot()["fill_bg"].Started
	// The stale serve runs inside a customer unit: the debt goroutines it
	// starts inherit "customer" and must relabel themselves.
	workclass.Do(context.Background(), workclass.Customer, func(context.Context) {
		f.customer(t, "dep-871")
	})
	f.putDependent("dep-871")
	release()
	d2WaitDebtsClosed(t)
	if !fill710WaitFor(10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(replace) > 0 }) {
		t.Fatal("the dependent replace never ran")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dispatch) == 0 {
		t.Fatal("PRECONDITION: the debt refill never dispatched")
	}
	for _, s := range append(append([]string(nil), dispatch...), replace...) {
		if s != "fill_bg/fill_bg" {
			t.Fatalf("debt work carried goroutine/ctx %q, want fill_bg/fill_bg (dispatches %v, replaces %v)", s, dispatch, replace)
		}
	}
	if d := workclass.Snapshot()["fill_bg"].Started - before; d < 2 {
		t.Fatalf("fill_bg started moved by %d, want at least 2 (one refill, one replace)", d)
	}
}
