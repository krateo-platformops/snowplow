package dispatchers

// issue1080_definite_notfound_test.go — #1080: a named object's NotFound under
// continueOnError is a definite answer, cached on the #681 denial-only path
// with the exact-object dependency edge the resolver records for every named
// GET, so the object's creation invalidates the cell.
//
// CUSTOMER /call (the #681 c681 harness: REAL restActionHandler.ServeHTTP, REAL
// key derivation and L1, REAL ResourceWatcher with configmaps servable, REAL
// RBAC; the requester's own apiserver is an httptest server that authorizes
// every dial with EvaluateRBAC and counts it):
//
//	C1  a 404 answer is cached; the second /call does no apiserver request.
//	C2  creating the object dirty-marks the cell through its exact-object edge
//	    (the informer ADD reaches the dep worker).
//	C3  a 5xx is still declined: every /call dials.
//	C4  a 404 in a stage WITHOUT continueOnError behaves as before: declined.
//	C5  identity: on x/ghost, B (may get configmaps in x) is cached a 404; A
//	    (may not) is answered the 403 and cached a denial — another key, never
//	    B's 404; C (A's binding set) is served A's cell.
//
// REFRESHER (the #681 i677 harness: the REAL refresher pool ->
// resolveAndPopulateL1 -> branch C against an httptest apiserver, REAL
// informers, REAL RBAC snapshot), with configmaps watched:
//
//	R1  an authorized 404 refresh is Put (denial_only_put_total{refresher}).
//	R2  creating the object makes the refresher re-resolve the cell BY ITSELF
//	    (no manual mark) and the cell shows the object; deleting it again
//	    goes back to a cached 404.

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const n1080Msg = `configmaps \"ghost\" not found`

// n1080Marks captures every dirty mark the dep tracker hands the refresher.
type n1080Marks struct {
	mu   sync.Mutex
	keys map[string]int
}

func n1080CaptureMarks(t *testing.T) *n1080Marks {
	t.Helper()
	m := &n1080Marks{keys: map[string]int{}}
	cache.Deps().SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
		m.mu.Lock()
		m.keys[k]++
		m.mu.Unlock()
	})
	t.Cleanup(func() { cache.Deps().SetRefreshHook(nil) })
	return m
}

func (m *n1080Marks) has(k string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.keys[k] > 0
}

func n1080CM(ns, name, v string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"namespace": ns, "name": name},
		"data":     map[string]any{"v": v},
	}}
}

func TestIssue1080_C1_NotFoundCachedAndServedWarm(t *testing.T) {
	e := c681Setup(t)
	e.ra = c681RAObj("/api/v1/namespaces/pub/configmaps/ghost")
	body1, d1 := e.call(t, c681A)
	if d1 != 1 {
		t.Fatalf("PRE: the first /call made %d own dials, want 1 (the 404)", d1)
	}
	if !bytes.Contains(body1, []byte(n1080Msg)) {
		t.Fatalf("PRE: the body must carry the canonical NotFound: %s", s425Trunc(body1))
	}
	if _, ok := e.cell(c681A, t); !ok {
		t.Fatal("C1 RED: a named object's NotFound under continueOnError was not cached — every visit dials the apiserver")
	}
	if got := c681DenialPuts(cache.DenialOnlyPutSiteCallRA); got != 1 {
		t.Fatalf("denial_only_put_total{call_ra} = %d, want 1", got)
	}
	body2, d2 := e.call(t, c681A)
	if d2 != 0 {
		t.Fatalf("C1 RED: the second /call dialled %d times — not served warm", d2)
	}
	if !bytes.Equal(body1, body2) {
		t.Fatal("C1: the warm body differs from the cold one")
	}
}

func TestIssue1080_C2_CreatingTheObjectDirtyMarksTheCell(t *testing.T) {
	e := c681Setup(t)
	e.ra = c681RAObj("/api/v1/namespaces/pub/configmaps/ghost")
	e.call(t, c681A)
	if _, ok := e.cell(c681A, t); !ok {
		t.Fatal("PRE: the 404 cell is not cached")
	}
	marks := n1080CaptureMarks(t)
	key := e.key(t, c681A)
	if _, err := e.dyn.Resource(c681CMs).Namespace("pub").Create(context.Background(),
		n1080CM("pub", "ghost", "CREATED-1080"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	i1126WaitFor(t, 10*time.Second, "the creation dirty-marks the 404 cell (C2 RED: no edge invalidates it)", func() bool {
		return marks.has(key)
	})
}

func TestIssue1080_C3_5xxStillDeclined(t *testing.T) {
	e := c681Setup(t)
	e.ra = c681RAObj("/api/v1/namespaces/pub/configmaps/boom")
	e.call(t, c681A)
	if _, ok := e.cell(c681A, t); ok {
		t.Fatal("C3 RED: a 5xx result was cached")
	}
	if _, d := e.call(t, c681A); d != 1 {
		t.Fatalf("C3: the second /call dialled %d, want 1", d)
	}
	if got := c681DenialPuts(cache.DenialOnlyPutSiteCallRA); got != 0 {
		t.Fatalf("denial_only_put_total{call_ra} = %d, want 0", got)
	}
}

func TestIssue1080_C4_NoContinueOnError_BehavesAsBefore(t *testing.T) {
	e := c681Setup(t)
	e.ra = c681RAObj("/api/v1/namespaces/pub/configmaps/ghost")
	api := e.ra.Object["spec"].(map[string]any)["api"].([]any)
	delete(api[0].(map[string]any), "continueOnError")
	e.call(t, c681A)
	if _, ok := e.cell(c681A, t); ok {
		t.Fatal("C4 RED: a 404 in a stage without continueOnError was cached")
	}
	if _, d := e.call(t, c681A); d != 1 {
		t.Fatalf("C4: the second /call dialled %d, want 1", d)
	}
}

func TestIssue1080_C5_Identity_404IsOnlyTheAuthorizedClassAnswer(t *testing.T) {
	e := c681Setup(t)
	e.ra = c681RAObj("/api/v1/namespaces/x/configmaps/ghost")
	if e.key(t, c681A) == e.key(t, c681B) {
		t.Fatal("PRE: A and B must derive different keys")
	}
	bodyB, dB := e.call(t, c681B)
	if dB != 1 || !bytes.Contains(bodyB, []byte(n1080Msg)) {
		t.Fatalf("PRE: B (may get configmaps in x) must be answered the 404 (dials %d): %s", dB, s425Trunc(bodyB))
	}
	if _, ok := e.cell(c681B, t); !ok {
		t.Fatal("C5: B's 404 was not cached")
	}
	bodyA, dA := e.call(t, c681A)
	if dA != 1 {
		t.Fatalf("C5 RED: A dialled %d times — A must not be served B's cell", dA)
	}
	if bytes.Contains(bodyA, []byte("not found")) || !bytes.Contains(bodyA, []byte("Forbidden")) {
		t.Fatalf("C5 RED: A (may not get x/ghost) was not answered the 403: %s", s425Trunc(bodyA))
	}
	c681NoIdentity(t, "A's body", bodyA)
	c681NoIdentity(t, "B's body", bodyB)
	cellA, ok := e.cell(c681A, t)
	if !ok || bytes.Equal(cellA, bodyB) {
		t.Fatal("C5: A's cell must be A's own denial-only cell")
	}
	bodyC, dC := e.call(t, c681C)
	if dC != 0 || !bytes.Equal(bodyC, bodyA) {
		t.Fatalf("C5: C (A's binding set) must be served A's cell (dials %d)", dC)
	}
}

// --- refresher ---------------------------------------------------------------

const n1080Ghost = "/api/v1/namespaces/team-a/configmaps/ghost"

func n1080RefresherSetup(t *testing.T) *i677Env {
	t.Helper()
	e := i677Setup(t, i677RAObject(i677Stage("ghost", n1080Ghost)))
	rw := cache.Global()
	if _, ch := rw.EnsureResourceType(i677CMGVR); ch != nil {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatal("configmaps informer did not sync")
		}
	}
	if !rw.IsServable(i677CMGVR) {
		t.Fatal("PRE: configmaps must be servable")
	}
	return e
}

func TestIssue1080_R1_AuthorizedNotFoundRefresh_IsPut(t *testing.T) {
	e := n1080RefresherSetup(t)
	e.cycle(t)
	if d := e.pathDials(n1080Ghost); d != 1 {
		t.Fatalf("PRE: the 404 stage dialled %d, want 1", d)
	}
	if b := e.body(t); b == `{"prior":true}` || !strings.Contains(b, n1080Msg) {
		t.Fatalf("R1 RED: an authorized NotFound refresh was not Put: %s", b)
	}
	if _, _, stale := cache.KnownStale(e.key); stale {
		t.Fatal("R1: the accepted re-Put left the window KNOWN-STALE")
	}
	if got := i681DenialPuts(cache.DenialOnlyPutSiteRefresher); got != 1 {
		t.Fatalf("denial_only_put_total{refresher} = %d, want 1", got)
	}
}

func TestIssue1080_R2_CreateShowsTheObject_DeleteReturnsToCached404(t *testing.T) {
	e := n1080RefresherSetup(t)
	e.cycle(t)
	if !strings.Contains(e.body(t), n1080Msg) {
		t.Fatalf("PRE: the cell must hold the 404: %s", e.body(t))
	}
	cms := e.dyn.Resource(i677CMGVR).Namespace("team-a")
	if _, err := cms.Create(context.Background(), n1080CM("team-a", "ghost", "CREATED-1080"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// No manual mark: the informer ADD must reach the cell through its edge
	// and the refresher must re-resolve it.
	i1126WaitFor(t, 15*time.Second, "R2 RED: the created object never reached the 404 cell", func() bool {
		got, ok := e.store.GetNoTouch(e.key)
		return ok && strings.Contains(string(got.RawJSON), "CREATED-1080")
	})
	if strings.Contains(e.body(t), "not found") {
		t.Fatalf("R2: the refreshed cell still carries the 404: %s", e.body(t))
	}
	puts := i681DenialPuts(cache.DenialOnlyPutSiteRefresher)
	if err := cms.Delete(context.Background(), "ghost", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	i1126WaitFor(t, 15*time.Second, "R2 RED: the deletion never brought the cell back to the 404", func() bool {
		got, ok := e.store.GetNoTouch(e.key)
		return ok && strings.Contains(string(got.RawJSON), n1080Msg)
	})
	if got := i681DenialPuts(cache.DenialOnlyPutSiteRefresher); got != puts+1 {
		t.Fatalf("denial_only_put_total{refresher} = %d, want %d (the 404 is Put again)", got, puts+1)
	}
}
