// refilter_review_unavailable_1362_test.go — #1362 review finding 1.
//
// #1362 made a cache=off SelfSubjectAccessReview that cannot be asked or
// answered an ERROR (rbac.ErrReviewUnavailable) rather than a bare
// (false, nil). Evaluator errors are not memoized in the refilter (AC-6), so a
// collection-verb UAF over N items in one namespace made N reviews instead of
// the one it made before #1362, multiplying the load on an apiserver that is
// already failing. These arms pin the fix: the unavailable outcome is
// remembered as a deny for the one refilterSlice invocation. That gives one
// review per (resource, namespace, name), the verdict stays fail-closed, and
// the next invocation asks again. Any other evaluator error stays un-memoized
// (AC-6).

package api

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// i1362FailingSAR installs a SelfSubjectAccessReview fake that fails every
// Create, as an apiserver in distress does, and counts the attempts.
func i1362FailingSAR(t *testing.T) *atomic.Int64 {
	t.Helper()
	var calls atomic.Int64
	t.Cleanup(rbac.SetSARClientsetForTest(func(context.Context, endpoints.Endpoint) (kubernetes.Interface, error) {
		cs := k8sfake.NewSimpleClientset()
		cs.PrependReactor("create", "selfsubjectaccessreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
			calls.Add(1)
			return true, nil, errors.New("etcdserver: request timed out")
		})
		return cs, nil
	}))
	return &calls
}

func i1362Items(namespaces []string, perNS int) []any {
	var items []any
	for _, ns := range namespaces {
		for i := 0; i < perNS; i++ {
			items = append(items, memoNSItem(ns, ns+"-"+itoaMemo(i)))
		}
	}
	return items
}

func TestIssue1362_RefilterReviewUnavailable_OneReviewPerTupleAndFailClosed(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	calls := i1362FailingSAR(t)
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: narrowUser, Groups: []string{"devs"}}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: "https://sar.test", Token: narrowUser}),
	)

	namespaces := []string{"team-a", "team-b"}
	const perNS = 25 // 50 items
	items := i1362Items(namespaces, perNS)

	kept, dropped, _ := refilterSlice(ctx, discardLogger(), narrowUser, []string{"devs"},
		memoUAFList("widgets"), []string{"widgets"}, items)

	if got, want := calls.Load(), int64(len(namespaces)); got != want {
		t.Fatalf("%d items over %d namespaces made %d SelfSubjectAccessReviews against a failing apiserver; "+
			"want %d (one per (resource, namespace, name)): the unavailable outcome is not remembered "+
			"within the invocation", len(items), len(namespaces), got, want)
	}
	if len(kept) != 0 || dropped != len(items) {
		t.Fatalf("an unanswerable review must fail closed: kept %d, dropped %d of %d", len(kept), dropped, len(items))
	}

	// The memo dies with the invocation: the next one asks again.
	_, _, _ = refilterSlice(ctx, discardLogger(), narrowUser, []string{"devs"},
		memoUAFList("widgets"), []string{"widgets"}, items)
	if got, want := calls.Load(), int64(2*len(namespaces)); got != want {
		t.Fatalf("a second invocation made %d reviews in total, want %d: the unavailable outcome leaked across invocations",
			got, want)
	}
}

// AC-6 is kept for every other error: a cache=on evaluator that cannot decide
// (here, no RBAC snapshot yet) is retried for each item, not remembered.
func TestIssue1362_RefilterOtherEvaluatorErrors_StayUnmemoized(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	prevW, prevS := cache.Global(), cache.RBACSnapshotForTest()
	cache.SetGlobal(&cache.ResourceWatcher{})
	cache.PublishRBACSnapshotForTest(nil)
	t.Cleanup(func() { cache.SetGlobal(prevW); cache.PublishRBACSnapshotForTest(prevS) })

	items := i1362Items([]string{"team-a"}, 10)
	rbac.ResetEvaluateRBACCallCount()
	kept, _, _ := refilterSlice(ctxWithUser(narrowUser, "devs"), discardLogger(), narrowUser, []string{"devs"},
		memoUAFList("widgets"), []string{"widgets"}, items)
	if got := rbac.EvaluateRBACCallCount(); got != uint64(len(items)) {
		t.Fatalf("a snapshot-not-built error was evaluated %d times over %d items; want %d (AC-6: not memoized)",
			got, len(items), len(items))
	}
	if len(kept) != 0 {
		t.Fatalf("kept %d items with no RBAC snapshot; must fail closed", len(kept))
	}
}
