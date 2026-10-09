//go:build unit || integration

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// TestIssue560_SubscribeArrivalIsCountedBeforeEveryBranch is the load-bearing
// arm of the #560 detector.
//
// The defect is that an 18 KB `GET /refreshes?sub=` URL is answered 431 by the
// INGRESS and never reaches this process, so snowplow could not tell "nobody
// subscribed" from "every subscriber was refused upstream". The counter only
// answers that question if it is incremented on ARRIVAL — ahead of every branch
// that can return early. If it ever moves below the cache-off branch or below
// validation, a whole class of arrival stops being counted and the detector
// silently narrows to "arrivals that happened to get past the gates", which is
// not the question.
//
// This drives the cache-off path specifically because that is the EARLIEST
// return in the handler and it needs no authenticated identity, so it pins the
// ordering claim directly rather than by inspection.
func TestIssue560_SubscribeArrivalIsCountedBeforeEveryBranch(t *testing.T) {
	// Cache off => RefreshSSEEnabled() is false => the handler takes its first
	// branch (serveIdleSSE) and returns before auth and before validation.
	t.Setenv("CACHE_ENABLED", "false")
	if cache.RefreshSSEEnabled() {
		t.Fatalf("precondition: RefreshSSEEnabled must be false with CACHE_ENABLED=false; " +
			"this test is not exercising the early-return branch")
	}
	cache.ResetLiveRefreshArmingStatsForTest()

	// The handler serves an SSE STREAM, so it only returns when the client
	// disconnects. An httptest recorder never disconnects, so calling it
	// directly parks the goroutine and times the whole package out at 600s —
	// which is exactly what the first version of this test did. Hand it an
	// already-cancelled context (serveIdleSSE selects on req.Context().Done())
	// and run it behind a timeout, so a future hang fails THIS test instead of
	// the package.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/refreshes?sub=", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		Refreshes()(rec, req)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Refreshes() did not return on a cancelled context within 5s — a streaming " +
			"handler under test must terminate, or it parks the package until the global timeout")
	}

	got := cache.LiveRefreshArmingStatsSnapshot()
	if got.SubscribeArrived != 1 {
		t.Fatalf("an arrival on the cache-off path must still be counted: "+
			"SubscribeArrived=%d want 1. If this is 0 the counter sits BELOW an "+
			"early return and no longer measures arrival.", got.SubscribeArrived)
	}
	// The cache-off path neither rejects nor opens a subscription stream.
	if got.SubscribeRejected != 0 {
		t.Fatalf("cache-off is not a rejection: SubscribeRejected=%d want 0", got.SubscribeRejected)
	}
	if got.StreamsOpen != 0 {
		t.Fatalf("cache-off serves an idle stream and must not move the subscribe gauge: "+
			"StreamsOpen=%d want 0", got.StreamsOpen)
	}
}

// TestIssue560_TheDetectorSignatureIsPositiveNotAnAbsence states the reading
// rule as an executable claim.
//
// A detector whose only evidence is a zero is not a detector
// (feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector). The #560
// signature must therefore be a counter that CLIMBS while the defect holds:
// widgets keep arming (ArmStamped rising) while nothing arrives and no stream is
// open. This asserts that combination is distinguishable from an idle product,
// which is the case the counters would otherwise be confused with.
func TestIssue560_TheDetectorSignatureIsPositiveNotAnAbsence(t *testing.T) {
	cache.ResetLiveRefreshArmingStatsForTest()

	idle := cache.LiveRefreshArmingStatsSnapshot()
	if idle.ArmStamped != 0 || idle.SubscribeArrived != 0 || idle.StreamsOpen != 0 {
		t.Fatalf("reset did not zero the quartet: %+v", idle)
	}

	// Simulate the measured 057 shape: 58 widgets armed, zero subscriptions
	// arrived (all 431'd upstream), no stream ever opened.
	for i := 0; i < 58; i++ {
		cache.BumpRefreshArmStamped()
	}

	s := cache.LiveRefreshArmingStatsSnapshot()
	defect := s.ArmStamped > 0 && s.SubscribeArrived == 0 && s.StreamsOpen == 0
	if !defect {
		t.Fatalf("the #560 signature must be reachable from these counters; got %+v", s)
	}
	if s.ArmStamped != 58 {
		t.Fatalf("ArmStamped=%d want 58", s.ArmStamped)
	}

	// And it must be DISTINGUISHABLE from an idle product, which is the whole
	// reason the arm counter exists alongside the arrival counter.
	cache.ResetLiveRefreshArmingStatsForTest()
	quiet := cache.LiveRefreshArmingStatsSnapshot()
	quietLooksLikeDefect := quiet.ArmStamped > 0 && quiet.SubscribeArrived == 0 && quiet.StreamsOpen == 0
	if quietLooksLikeDefect {
		t.Fatalf("an idle product must NOT read as the defect; got %+v", quiet)
	}
}

// TestIssue560_StreamGaugePairs — the gauge must return to its starting value
// when a stream ends, or a long-lived pod's reading drifts upward and
// "streams_open > 0" stops meaning "a subscriber is connected", which is the one
// thing it is read for.
func TestIssue560_StreamGaugePairs(t *testing.T) {
	cache.ResetLiveRefreshArmingStatsForTest()

	cache.RefreshStreamOpened()
	cache.RefreshStreamOpened()
	if got := cache.LiveRefreshArmingStatsSnapshot().StreamsOpen; got != 2 {
		t.Fatalf("StreamsOpen=%d want 2", got)
	}
	cache.RefreshStreamClosed()
	cache.RefreshStreamClosed()
	if got := cache.LiveRefreshArmingStatsSnapshot().StreamsOpen; got != 0 {
		t.Fatalf("StreamsOpen=%d want 0 after both closed — an unpaired close or open "+
			"makes the gauge drift and stop meaning what it is read for", got)
	}
}
