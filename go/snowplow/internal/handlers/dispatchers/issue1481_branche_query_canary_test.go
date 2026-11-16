package dispatchers

// issue1481_branche_query_canary_test.go — #1481 review B1(b): a branch-E
// SA-endpoint stage that the re-gate refuses, whose path carries a query,
// driven through the REAL refresher path on the #574 harness. The refusal's
// message is built from the stage path, and it travels: the branch-E WARN, the
// stage-error sink (the DEBUG "per-item errors" join, the refresher's WARN
// decline with its stage_err_sample, the seed's skip line). No log line, at
// any level, may carry the query's value.
//
// It also pins the retry half of the #1481 class decision: a 502 is
// retryable and seeds as operational whatever its class.

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestIssue1481_BranchERegateLogsCarryNoQueryValue(t *testing.T) {
	const canary = "BRANCHECANARY1481"
	stage := map[string]any{"name": "podstatus", "path": sar574PodPath + "?pretty=" + canary, "verb": "GET", "filter": ".podstatus"}
	ra := sar574RA("ns1", "i1481-pod", stage)
	sar574Watcher(t, []*unstructured.Unstructured{ra}, true, []rbacv1.PolicyRule{sar574CreateSAR})
	srv := newSAR574Server(t, map[string]any{
		sar574PodPath: map[string]any{"apiVersion": "v1", "kind": "Pod", "status": map[string]any{"phase": sar574Marker}},
	})
	logs := sar574CaptureLogs(t)
	got := sar574Refresh(t, logs.ctx(context.Background()), srv, "ns1", "i1481-pod", http.MethodGet, sar574PodPath)
	if got.dialed {
		t.Fatal("PRE: the re-gated stage must not dial")
	}
	sar574RequireRegateFired(t, "1481", got)
	if recs := logs.lines(t, sar574BranchEMsg); len(recs) == 0 {
		t.Fatal("PRE: the branch-E WARN did not fire")
	}
	logs.mu.Lock()
	all := logs.buf.String()
	logs.mu.Unlock()
	if !strings.Contains(all, "pretty=") {
		t.Fatalf("PRE: the refusal no longer names the path's query keys:\n%s", all)
	}
	for _, ln := range strings.Split(all, "\n") {
		if strings.Contains(ln, canary) {
			t.Errorf("RED: a log line carries the stage path's query value:\n%s", strings.ReplaceAll(ln, canary, "<CANARY>"))
		}
	}
}

func TestIssue1481_A502IsRetriedWhateverItsClass(t *testing.T) {
	mk := func(reason string) error {
		return cache.WrapResolveStage(cache.ResolveStageApiRef, "p",
			&apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: 502, Reason: metav1.StatusReason(reason), Message: "x"}})
	}
	for _, c := range []struct {
		reason, class string
	}{
		{"", cache.RefreshErrBadGateway},
		{string(metav1.StatusReasonInternalError), cache.RefreshErrBadGateway}, // main's client-go shape
		{credhttp.RedirectRefusedReason, cache.RefreshErrRedirectRefused},
	} {
		err := mk(c.reason)
		if cache.IsNonRetryableRefreshError(err) {
			t.Errorf("RED: a 502 with reason %q is non-retryable", c.reason)
		}
		if cl, _ := cache.ClassifyRefreshError(err); cl != c.class {
			t.Errorf("RED: a 502 with reason %q is classed %q, want %q", c.reason, cl, c.class)
		}
		if v := classifySeedErr(err); v != seedFailOperational {
			t.Errorf("RED: a 502 with reason %q seeds as %v, want operational", c.reason, v)
		}
	}
}
