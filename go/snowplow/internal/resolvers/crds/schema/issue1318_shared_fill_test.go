package schema

// issue1318_shared_fill_test.go — #1318: a CRD-schema memo miss is refilled by
// one shared, detached fill (schema_fill.go), never under the page budget of
// the request that missed.
//
// The arms drive the real ValidateObjectStatus against an httptest apiserver
// (discovery + the CRD GET), so the CRD read goes through the same clients and
// the same credhttp budget enforcement production uses:
//   - SpentBudget: the issue's arm. The memo is reset (a CRD lifecycle event)
//     while the caller's budget is spent; the miss still refills the memo, the
//     call succeeds, and the next spent-budget call is a memo hit.
//   - Coalesced: N concurrent misses of one slot send ONE CRD GET.
//   - JoinerOwnWait: a joiner's wait ends with its own ctx; the flight goes on
//     and stores the memo.
//   - GenerationFolded: a miss after a reset never joins a flight that started
//     before it (it may have read the old CRD).
//   - TransportFolded: without the process SA config the CRD is read with the
//     caller's config; a caller never takes another identity's answer.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

const (
	fill1318Group   = "fill1318.example.io"
	fill1318CRDPath = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/probes." + fill1318Group
)

// fill1318Server is an apiserver serving discovery and the probes CRD. A CRD
// GET blocks until release when the server is gated; deny names an
// Impersonate-User whose CRD GET is answered 403.
type fill1318Server struct {
	rc      *rest.Config
	crdGETs atomic.Int32
	gate    chan struct{} // nil: never blocks
	once    sync.Once
	deny    string
}

func newFill1318Server(t *testing.T, gated bool, deny string) *fill1318Server {
	t.Helper()
	s := &fill1318Server{deny: deny}
	if gated {
		s.gate = make(chan struct{})
	}
	crd := tinyCRD("v1")
	crd["apiVersion"] = "apiextensions.k8s.io/v1"
	crd["kind"] = "CustomResourceDefinition"
	crd["metadata"] = map[string]any{"name": "probes." + fill1318Group}
	crdJSON, err := json.Marshal(crd)
	if err != nil {
		t.Fatal(err)
	}
	docs := map[string]string{
		"/api":    `{"kind":"APIVersions","versions":["v1"]}`,
		"/api/v1": `{"kind":"APIResourceList","groupVersion":"v1","resources":[]}`,
		"/apis": `{"kind":"APIGroupList","apiVersion":"v1","groups":[` +
			`{"name":"` + fill1318Group + `","versions":[{"groupVersion":"` + fill1318Group + `/v1","version":"v1"}],"preferredVersion":{"groupVersion":"` + fill1318Group + `/v1","version":"v1"}},` +
			`{"name":"apiextensions.k8s.io","versions":[{"groupVersion":"apiextensions.k8s.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"apiextensions.k8s.io/v1","version":"v1"}}]}`,
		"/apis/" + fill1318Group + "/v1": `{"kind":"APIResourceList","groupVersion":"` + fill1318Group + `/v1","resources":[{"name":"probes","singularName":"probe","namespaced":true,"kind":"Probe","verbs":["get","list"]}]}`,
		"/apis/apiextensions.k8s.io/v1":  `{"kind":"APIResourceList","groupVersion":"apiextensions.k8s.io/v1","resources":[{"name":"customresourcedefinitions","singularName":"customresourcedefinition","namespaced":false,"kind":"CustomResourceDefinition","verbs":["get","list"]}]}`,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == fill1318CRDPath {
			s.crdGETs.Add(1)
			if s.gate != nil {
				select {
				case <-s.gate:
				case <-r.Context().Done():
					return
				}
			}
			if s.deny != "" && r.Header.Get("Impersonate-User") == s.deny {
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":403,"reason":"Forbidden"}`)
				return
			}
			_, _ = w.Write(crdJSON)
			return
		}
		if d, ok := docs[r.URL.Path]; ok {
			_, _ = io.WriteString(w, d)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":404,"reason":"NotFound"}`)
	}))
	t.Cleanup(srv.Close)
	// Cleanups run last-in first-out: a failed arm releases a held GET before
	// srv.Close waits for it, so it fails instead of hanging.
	if s.gate != nil {
		t.Cleanup(s.release)
	}
	s.rc = &rest.Config{Host: srv.URL}
	return s
}

func (s *fill1318Server) release() { s.once.Do(func() { close(s.gate) }) }

// waitCRDGETs waits until the server has seen n CRD GETs.
func (s *fill1318Server) waitCRDGETs(t *testing.T, n int32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for s.crdGETs.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("server saw %d CRD GET(s); want %d", s.crdGETs.Load(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func fill1318Obj() map[string]any {
	return map[string]any{
		"apiVersion": fill1318Group + "/v1",
		"kind":       "Probe",
		"metadata":   map[string]any{"name": "p", "namespace": "ns"},
		"status":     map[string]any{widgetDataKey: map[string]any{"title": "t"}},
	}
}

// setup1318 isolates an arm: cache on, empty plurals store, memo and flights.
// It warms the plural with a background call (the permanent plurals store a
// running process has), so the arm's misses are memo misses only.
func setup1318(t *testing.T, warm *fill1318Server) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetPluralsStoreForTest()
	t.Cleanup(cache.ResetPluralsStoreForTest)
	resetCRDSchemaMemoForTest()
	t.Cleanup(resetCRDSchemaMemoForTest)
	resetCRDSchemaFlightsForTest()
	t.Cleanup(resetCRDSchemaFlightsForTest)
	if warm != nil {
		if _, err := cache.GVRFor(context.Background(), runtimeschema.GroupVersionKind{Group: fill1318Group, Version: "v1", Kind: "Probe"}, warm.rc); err != nil {
			t.Fatalf("warm plural: %v", err)
		}
	}
}

func spentBudget() context.Context {
	return pagebudget.WithCustomer(context.Background(), time.Now().Add(-2*pagebudget.PageMaxGoal))
}

// TestIssue1318_SpentBudgetMissStillRefills is the issue's arm: an exhausted
// customer budget plus a memo miss — the memo still refills and the next
// request succeeds.
func TestIssue1318_SpentBudgetMissStillRefills(t *testing.T) {
	s := newFill1318Server(t, false, "")
	setup1318(t, nil)

	// Control: a background call fills the memo (and the plural).
	if err := ValidateObjectStatus(context.Background(), s.rc, fill1318Obj()); err != nil {
		t.Fatalf("control: %v", err)
	}
	// A CRD lifecycle event resets the memo.
	InvalidateCRDSchemaMemo()
	before := s.crdGETs.Load()

	// Every caller's budget is spent (a slow stage ate it).
	if err := ValidateObjectStatus(spentBudget(), s.rc, fill1318Obj()); err != nil {
		t.Fatalf("SPENT budget, memo miss: %v; want the schema refilled outside the request's budget", err)
	}
	if got := s.crdGETs.Load() - before; got != 1 {
		t.Fatalf("SPENT budget, memo miss: %d CRD GET(s) sent; want 1 (the shared fill)", got)
	}
	// The next spent-budget request is a memo hit: nothing sent, success.
	if err := ValidateObjectStatus(spentBudget(), s.rc, fill1318Obj()); err != nil {
		t.Fatalf("next SPENT-budget request: %v; want success from the refilled memo", err)
	}
	if got := s.crdGETs.Load() - before; got != 1 {
		t.Fatalf("next request sent %d more CRD GET(s); want a memo hit", got-1)
	}
}

// TestIssue1318_ConcurrentMissesShareOneFill: N requests that miss one slot
// together send one CRD GET; every one gets the schema.
func TestIssue1318_ConcurrentMissesShareOneFill(t *testing.T) {
	s := newFill1318Server(t, true, "")
	setup1318(t, s)

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); errs[0] = ValidateObjectStatus(spentBudget(), s.rc, fill1318Obj()) }()
	s.waitCRDGETs(t, 1)
	for i := 1; i < n; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errs[i] = ValidateObjectStatus(spentBudget(), s.rc, fill1318Obj()) }(i)
	}
	// Every other miss is waiting on the flight (or, without one, has sent
	// its own GET) before the leader's GET is answered.
	deadline := time.Now().Add(10 * time.Second)
	for crdSchemaFillJoins.Load() < n-1 && s.crdGETs.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("joins %d, CRD GETs %d: the misses never reached the fill", crdSchemaFillJoins.Load(), s.crdGETs.Load())
		}
		time.Sleep(time.Millisecond)
	}
	s.release()
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: %v", i, err)
		}
	}
	if got := s.crdGETs.Load(); got != 1 {
		t.Fatalf("%d concurrent misses sent %d CRD GETs; want 1 shared fill", n, got)
	}
}

// TestIssue1318_JoinerWaitEndsWithItsOwnCtx: a joiner whose ctx ends stops
// waiting at once; the flight goes on and stores the memo.
func TestIssue1318_JoinerWaitEndsWithItsOwnCtx(t *testing.T) {
	s := newFill1318Server(t, true, "")
	setup1318(t, s)

	leaderErr := make(chan error, 1)
	go func() { leaderErr <- ValidateObjectStatus(spentBudget(), s.rc, fill1318Obj()) }()
	s.waitCRDGETs(t, 1)

	jctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	joinerErr := make(chan error, 1)
	go func() { joinerErr <- ValidateObjectStatus(jctx, s.rc, fill1318Obj()) }()
	select {
	case err := <-joinerErr:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("joiner with an ending ctx: err %v; want its own context.DeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("joiner still waiting 5 s past its own 50 ms ctx")
	}
	if crdSchemaFillJoins.Load() != 1 {
		t.Fatalf("joins = %d; want the second miss to join the flight", crdSchemaFillJoins.Load())
	}

	s.release()
	if err := <-leaderErr; err != nil {
		t.Fatalf("leader: %v", err)
	}
	if _, hit := lookupCRDSchema(gvrOf1318()); !hit {
		t.Fatalf("memo not refilled after the flight ended")
	}
}

// TestIssue1318_MissAfterResetDoesNotJoinAnOlderFlight: a reset during a
// flight means the flight may have read the old CRD; a miss after the reset
// leads its own fill, which the memo keeps.
func TestIssue1318_MissAfterResetDoesNotJoinAnOlderFlight(t *testing.T) {
	s := newFill1318Server(t, true, "")
	setup1318(t, s)

	errs := make(chan error, 2)
	go func() { errs <- ValidateObjectStatus(context.Background(), s.rc, fill1318Obj()) }()
	s.waitCRDGETs(t, 1)
	InvalidateCRDSchemaMemo() // a CRD UPDATE lands during the first fill's GET
	go func() { errs <- ValidateObjectStatus(context.Background(), s.rc, fill1318Obj()) }()
	s.waitCRDGETs(t, 2)
	s.release()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("caller: %v", err)
		}
	}
	if got := s.crdGETs.Load(); got != 2 {
		t.Fatalf("CRD GETs = %d; want 2 (the post-reset miss reads the CRD itself)", got)
	}
	st := crdSchemaMemoStats()
	if st.StaleDropped != 1 {
		t.Fatalf("stale-dropped stores = %d; want 1 (the pre-reset flight's)", st.StaleDropped)
	}
	if _, hit := lookupCRDSchema(gvrOf1318()); !hit {
		t.Fatalf("memo not filled by the post-reset flight")
	}
}

// TestIssue1318_FlightIsPerTransport: without the process SA config the CRD is
// read with the caller's config. A caller never joins a flight reading with
// another identity's config, so another identity's 403 is never its answer.
func TestIssue1318_FlightIsPerTransport(t *testing.T) {
	s := newFill1318Server(t, true, "denied-1318")
	setup1318(t, s)

	denied := &rest.Config{Host: s.rc.Host, Impersonate: rest.ImpersonationConfig{UserName: "denied-1318"}}
	allowed := &rest.Config{Host: s.rc.Host, Impersonate: rest.ImpersonationConfig{UserName: "allowed-1318"}}

	deniedErr := make(chan error, 1)
	go func() { deniedErr <- ValidateObjectStatus(context.Background(), denied, fill1318Obj()) }()
	s.waitCRDGETs(t, 1)

	allowedErr := make(chan error, 1)
	go func() { allowedErr <- ValidateObjectStatus(context.Background(), allowed, fill1318Obj()) }()
	s.waitCRDGETs(t, 2) // the allowed caller reads the CRD itself
	s.release()
	if err := <-allowedErr; err != nil {
		t.Fatalf("allowed identity: %v; want its own answer, never the denied identity's", err)
	}
	if err := <-deniedErr; !apierrors.IsForbidden(err) {
		t.Fatalf("denied identity: %v; want its own 403", err)
	}
}

func gvrOf1318() runtimeschema.GroupVersionResource {
	return runtimeschema.GroupVersionResource{Group: fill1318Group, Version: "v1", Resource: "probes"}
}
