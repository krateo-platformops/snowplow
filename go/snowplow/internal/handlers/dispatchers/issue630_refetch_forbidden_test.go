// issue630_refetch_forbidden_test.go — #630 falsifier: a 403 on the widget CR
// RE-FETCH (objects.Get of the entry's own object) during a refresh must be
// classified like every other 403 on the refresh path (#608/#619).
//
// THE DEFECT. resolveOnceProd flattened the re-fetch error to a string
// (`fmt.Errorf("re-fetch %s/%s: %s", …, got.Err.Message)`), so the
// *apierrors.StatusError never reached the refresher. ClassifyRefreshError
// read it as err_class=unclassified stage=none, IsNonRetryableRefreshError
// said false, the key burned the whole requeue budget (6 attempts) and the
// drop point EVICTED it — a cold navigation. #619's "forbidden never evicts"
// rule held for the validate/apiref hops (they keep their StatusError) but
// not for this one.
//
// THE ARM drives the REAL refresher pool -> resolveAndPopulateL1 ->
// resolveOnceProd -> objects.Get (informer registered, misses, falls through)
// -> an httptest apiserver that answers 403 Forbidden on the object GET on
// EVERY attempt (a persistent 403, not a blip). It asserts:
//
//   - refresher.refresh_failed carries err_class=forbidden stage=refetch;
//   - exactly ONE attempt (non-retryable, #608 Q2);
//   - exactly one refresher.refresh_dropped, non_retryable=true evicted=false;
//   - the cell is RESIDENT with its last body afterwards;
//   - snowplow_refresh_failed{forbidden,refetch} == 1 and no unclassified.
//
// RED on 7fc85b60 (origin/main before the fix): unclassified/none, 6
// attempts, evicted=true, resident=false.
package dispatchers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// i630StageRefetch is spelled as a literal, not cache.ResolveStageRefetch, so
// the arm COMPILES on the pre-fix tree and its RED is an assertion, not a
// build failure. The cache package's classifier test pins the constant's
// value.
const i630StageRefetch = "refetch"

func TestIssue630_PersistentRefetchForbiddenIsClassifiedAndNeverEvicts(t *testing.T) {
	i187RefresherEnv(t)
	srv := newI187APIServer(t, http.StatusForbidden)
	store, inputs, key, saEP, saRC := i187Fixture(t, srv, true)
	before, ok := store.Get(key)
	if !ok {
		t.Fatalf("premise: the fixture cell is not resident")
	}
	lastBody := string(before.RawJSON)

	// The classification, at the site that produces it: the StatusError must
	// survive the re-fetch wrap, and the stage must name the hop.
	err := resolveAndPopulateL1(context.Background(), inputs, saEP, saRC)
	if err == nil {
		t.Fatalf("premise: want an error from a forbidden re-fetch, got nil")
	}
	if n := srv.objectGets.Load(); n == 0 {
		t.Fatalf("premise: the re-fetch never reached the apiserver, so the 403 under test is not the wire one")
	}
	var se *apierrors.StatusError
	t.Logf("#630 direct: err=%q statusError=%v", err.Error(), errors.As(err, &se))
	if !errors.As(err, &se) || !apierrors.IsForbidden(err) {
		t.Errorf("#630 RED: the re-fetch 403 was flattened — no *StatusError/Forbidden in the chain: %v", err)
	}
	if errors.Is(err, cache.ErrSelfObjectGone) {
		t.Fatalf("#630: a 403 was classified as a deletion — only a definite 404 may take the self-gone route")
	}
	if c, s := cache.ClassifyRefreshError(err); c != cache.RefreshErrForbidden || s != i630StageRefetch {
		t.Errorf("#630 RED: ClassifyRefreshError = (%s, %s), want (forbidden, refetch)", c, s)
	}

	// The whole loop.
	cache.ResetRefreshFailedByClassForTest()
	t.Cleanup(cache.ResetRefreshFailedByClassForTest)
	capt := &f608LogCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(capt))
	t.Cleanup(func() { slog.SetDefault(prev) })

	invocations, stop := i1126Loop(t, "widgets", saEP, saRC)
	cache.EnqueueRefresh(key)
	// Settled = the drop point ran (either outcome). On the defect that takes
	// the full budget; on the fix, one attempt. Then give a stray requeue a
	// window to show itself before the count is read.
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) && len(capt.find("refresher.refresh_dropped")) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	n := invocations.Load()
	stop()
	slog.SetDefault(prev)

	failed := capt.find("refresher.refresh_failed")
	dropped := capt.find("refresher.refresh_dropped")
	after, resident := store.Get(key)
	served := resident && string(after.RawJSON) == lastBody
	t.Logf("#630 loop: invocations=%d refresh_failed=%v refresh_dropped=%v resident=%v served_last_body=%v",
		n, failed, dropped, resident, served)

	if len(failed) == 0 {
		t.Fatalf("#630: no refresher.refresh_failed record")
	}
	for i, r := range failed {
		if r["err_class"] != cache.RefreshErrForbidden || r["stage"] != i630StageRefetch {
			t.Errorf("#630 RED: refresh_failed[%d] err_class=%q stage=%q, want forbidden/refetch",
				i, r["err_class"], r["stage"])
		}
	}
	if n != 1 || len(failed) != 1 {
		t.Errorf("#630 RED: a persistent re-fetch 403 was attempted %d times (%d refresh_failed), want 1 "+
			"(non-retryable, #608 Q2)", n, len(failed))
	}
	if len(dropped) != 1 || dropped[0]["non_retryable"] != "true" || dropped[0]["evicted"] != "false" {
		t.Errorf("#630 RED: want exactly one refresh_dropped non_retryable=true evicted=false, got %v", dropped)
	}
	if !served {
		t.Errorf("#630 RED: a forbidden re-fetch removed the cell (resident=%v) — the next navigation is cold", resident)
	}

	// Detector: snowplow_refresh_failed{err_class="forbidden",stage="refetch"},
	// and nothing in unclassified (the alert #608 recommends).
	byClass := map[string]uint64{}
	for _, c := range cache.RefreshFailedByClassSnapshot() {
		byClass[c.Class+"/"+c.Stage] = c.N
	}
	if byClass["forbidden/refetch"] != 1 || len(byClass) != 1 {
		t.Errorf("#630 RED: refresh_failed by class = %v, want exactly {forbidden/refetch: 1}", byClass)
	}
	if got := cache.RefresherSelfNotFoundEvictTotal(); got != 0 {
		t.Errorf("#630: self_notfound_evict_total = %d, want 0 — a 403 must never take the 404 route", got)
	}
}
