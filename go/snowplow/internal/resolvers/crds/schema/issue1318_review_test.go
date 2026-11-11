package schema

// issue1318_review_test.go — #1318 review F1-F3: the flight's own bounds.
//   - F1: the per-call fallback client's mapper discovery (sent by client-go on
//     context.TODO()) ends at the fill deadline, so a hanging discovery releases
//     the flight and its joiners.
//   - F2: the fill deadline bounds the CRD read itself.
//   - F3: a leader that exits without a result (a panic) gives its joiners an
//     error, never (nil, nil).
// The fill deadline is the server write deadline (cache.DemandRevalidationDeadline,
// the ResolvedCache tombstone TTL); the arms shorten it through the store.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/client-go/rest"
)

const review1318Deadline = 300 * time.Millisecond

// shortFillDeadline sets the fill deadline to d for the arm.
func shortFillDeadline(t *testing.T, d time.Duration) {
	t.Helper()
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("no ResolvedCache (CACHE_ENABLED must be set before shortFillDeadline)")
	}
	store.SetTombstoneTTL(d)
	if got := cache.DemandRevalidationDeadline(); got != d {
		t.Fatalf("fill deadline = %v; want %v", got, d)
	}
}

func crdSchemaFlightsInFlight() int {
	crdSchemaFlightsMu.Lock()
	defer crdSchemaFlightsMu.Unlock()
	return len(crdSchemaFlights)
}

// awaitBoth waits for a leader's and a joiner's results; both must arrive
// within bound.
func awaitBoth(t *testing.T, leader, joiner <-chan error, bound time.Duration) (error, error) {
	t.Helper()
	var le, je error
	timeout := time.After(bound)
	for got := 0; got < 2; got++ {
		select {
		case le = <-leader:
			leader = nil
		case je = <-joiner:
			joiner = nil
		case <-timeout:
			t.Fatalf("the flight was not released %v after it began (fill deadline %v)", bound, review1318Deadline)
		}
	}
	return le, je
}

// TestIssue1318_F1_HangingFallbackDiscoveryEndsAtTheFillDeadline: without the
// SA singleton the CRD is read with a per-call client whose mapper discovers on
// context.TODO(). A discovery that never answers ends at the fill deadline:
// the leader and its joiner both get an error and the flight is gone.
func TestIssue1318_F1_HangingFallbackDiscoveryEndsAtTheFillDeadline(t *testing.T) {
	s := newFill1318Server(t, true, "")
	setup1318(t, s) // the plural is warm: only the fill's own sends remain
	shortFillDeadline(t, review1318Deadline)
	s.hangDiscovery.Store(true)

	// Impersonating: the SA singleton refuses it, so the fallback runs.
	imp := &rest.Config{Host: s.rc.Host, Impersonate: rest.ImpersonationConfig{UserName: "f1-1318"}}
	leader := make(chan error, 1)
	t0 := time.Now()
	go func() { leader <- ValidateObjectStatus(context.Background(), imp, fill1318Obj()) }()
	waitFlight(t)
	joiner := make(chan error, 1)
	go func() { joiner <- ValidateObjectStatus(context.Background(), imp, fill1318Obj()) }()

	le, je := awaitBoth(t, leader, joiner, 5*time.Second)
	if le == nil || je == nil {
		t.Fatalf("leader %v, joiner %v; want both to fail at the fill deadline", le, je)
	}
	if d := time.Since(t0); d < review1318Deadline {
		t.Fatalf("released after %v, before the %v fill deadline: not the deadline that ended it", d, review1318Deadline)
	}
	if s.crdGETs.Load() != 0 {
		t.Fatalf("CRD GETs = %d; want 0 (the discovery in front of it never answered)", s.crdGETs.Load())
	}
	if n := crdSchemaFlightsInFlight(); n != 0 {
		t.Fatalf("%d flight(s) left in flight after the deadline", n)
	}
}

// TestIssue1318_F2_HangingCRDReadEndsAtTheFillDeadline: the CRD GET itself is
// bounded by the fill deadline (the SA singleton path).
func TestIssue1318_F2_HangingCRDReadEndsAtTheFillDeadline(t *testing.T) {
	s := newFill1318Server(t, true, "") // the CRD GET is held until cleanup
	setup1318(t, s)
	shortFillDeadline(t, review1318Deadline)

	leader := make(chan error, 1)
	t0 := time.Now()
	go func() { leader <- ValidateObjectStatus(context.Background(), s.rc, fill1318Obj()) }()
	s.waitCRDGETs(t, 1)
	joiner := make(chan error, 1)
	go func() { joiner <- ValidateObjectStatus(context.Background(), s.rc, fill1318Obj()) }()

	le, je := awaitBoth(t, leader, joiner, 5*time.Second)
	if !errors.Is(le, context.DeadlineExceeded) || !errors.Is(je, context.DeadlineExceeded) {
		t.Fatalf("leader %v, joiner %v; want both ended by the fill deadline", le, je)
	}
	if d := time.Since(t0); d < review1318Deadline {
		t.Fatalf("released after %v, before the %v fill deadline", d, review1318Deadline)
	}
	if s.crdGETs.Load() != 1 {
		t.Fatalf("CRD GETs = %d; want 1 (the joiner shared the flight)", s.crdGETs.Load())
	}
	if n := crdSchemaFlightsInFlight(); n != 0 {
		t.Fatalf("%d flight(s) left in flight after the deadline", n)
	}
}

// TestIssue1318_F3_LeaderPanicGivesJoinersAnError: a leader that exits without
// a result fails its joiners with errCRDSchemaFillAbandoned and ends the
// flight; the panic itself stays on the leader's goroutine.
func TestIssue1318_F3_LeaderPanicGivesJoinersAnError(t *testing.T) {
	s := newFill1318Server(t, true, "")
	setup1318(t, s)
	prev := compileCRDSchemaFn
	compileCRDSchemaFn = func(map[string]any, string) (*apiextensions.CustomResourceValidation, error) {
		panic("compile panicked (#1318 F3)")
	}
	t.Cleanup(func() { compileCRDSchemaFn = prev })

	gvr := gvrOf1318()
	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		_, _ = fillCRDSchema(context.Background(), s.rc, gvr)
	}()
	s.waitCRDGETs(t, 1)

	type result struct {
		crv *apiextensions.CustomResourceValidation
		err error
	}
	joined := make(chan result, 1)
	go func() {
		crv, err := fillCRDSchema(context.Background(), s.rc, gvr)
		joined <- result{crv, err}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for crdSchemaFillJoins.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("the second caller never joined the flight")
		}
		time.Sleep(time.Millisecond)
	}
	s.release()

	select {
	case r := <-joined:
		if r.crv != nil || !errors.Is(r.err, errCRDSchemaFillAbandoned) {
			t.Fatalf("joiner got (%v, %v); want (nil, errCRDSchemaFillAbandoned)", r.crv, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("joiner still waiting after its leader panicked")
	}
	if p := <-panicked; p == nil {
		t.Fatalf("the leader did not panic: the arm did not exercise the abandoned branch")
	}
	if n := crdSchemaFlightsInFlight(); n != 0 {
		t.Fatalf("%d flight(s) left in flight after the leader panicked", n)
	}
}

// waitFlight waits until a flight is in flight.
func waitFlight(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for crdSchemaFlightsInFlight() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("no flight started")
		}
		time.Sleep(time.Millisecond)
	}
}
