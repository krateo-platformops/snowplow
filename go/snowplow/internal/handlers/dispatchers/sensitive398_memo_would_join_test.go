package dispatchers

// sensitive398_memo_would_join_test.go — #1284 review follow-up: a leader
// whose body read a Secret saves no joiner. The memo never stores a body
// produced from a sensitive read (#398), so a join could not have handed it
// over either: the measurement-only would-join count must not count the
// produces that attached to it. Driven through the REAL resolve path (seed
// widget → widgets.Resolve → apiref.Resolve) with a RESTAction whose step
// reads a Secret; a joiner attaches while the first widget's produce is in
// flight (cache.SetBeginProduceHookForTest).

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

func TestS398_SensitiveLeaderSavesNoJoiner(t *testing.T) {
	k423Env(t)
	a := s398Arm()
	ra := psRACR(a)
	w1 := s398MemoWidget("w-sensitive-leader")
	extra := append(k423WidgetExtras(a), runtime.Object(ra), runtime.Object(w1))
	psBuildWatcher(t, a, extra...)
	if _, ch := cache.Global().EnsureResourceType(h1RAGVR); ch != nil {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("RA informer did not sync")
		}
	}
	srv := psFakeAPIServer(t, a, k423Counters())
	psSeedClientconfigs(t, srv.URL)

	memo := cache.NewSeedResolveMemo()
	base := cache.WithSeedResolveMemo(context.Background(), memo)
	cohort := withCohortSeedContext(base, seedTarget{Username: psAlice, Groups: psGroups(a)},
		endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-sa"}, &rest.Config{Host: srv.URL, BearerToken: "tok-sa"})

	origWR := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = origWR })
	widgetsResolveFn = func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
		out, _ := widgets.Resolve(ctx, opts) // REAL resolve; tail CRD-status validate error benign (a3Serve pattern)
		if out == nil {
			out = &unstructured.Unstructured{Object: map[string]any{}}
		}
		return out, nil
	}

	// A joiner attaches the moment the widget's produce registers.
	var joiner *cache.MemoProduce
	attached := false
	t.Cleanup(cache.SetBeginProduceHookForTest(func(key string) {
		if attached {
			return // the joiner's own BeginProduce re-enters here
		}
		attached = true
		joiner = memo.BeginProduce(key)
	}))

	memoSkippedBefore := sensitiveMemoSkipped398()
	_ = seedOneWidget(cohort, navWidgetEntry{W: w1, GVR: h1WidgetGVR, Pagination: unpaginatedPrewarm}, psAuthnNS, seedModeBoot)
	if joiner == nil {
		t.Fatal("SETUP: the widget's resolve never registered a memo produce")
	}
	joiner.End()
	if sensitiveMemoSkipped398() == memoSkippedBefore {
		t.Fatal("SETUP: the leader's Store was not skipped for a sensitive read — the arm did not exercise #398")
	}
	if got := memo.WouldJoin(); got != 0 {
		t.Fatalf("a leader whose body read a Secret (never stored, #398) was counted as saving its joiner: WouldJoin=%d, want 0", got)
	}
}
