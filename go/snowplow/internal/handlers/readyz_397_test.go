// readyz_397_test.go — #397: the /readyz warming body says WHERE phase 1 is
// (`reason`) and for how long (`elapsed_s`); the ready body says HOW readiness
// was released (`outcome`). The pre-#397 fields and status codes are unchanged.
// RED without #397: the warming body carries no reason / elapsed_s and the ready
// body no outcome.

package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

func readyzRaw(t *testing.T) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	ReadyCheck()(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	var m map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&m); err != nil {
		t.Fatalf("decode /readyz body: %v", err)
	}
	return rec.Code, m
}

func TestIssue397_Readyz_WarmingBodyCarriesReasonAndElapsed_ReadyCarriesOutcome(t *testing.T) {
	cache.ResetPhase1DoneForTest()
	dispatchers.ResetPhase1ReadinessExitForTest()
	t.Cleanup(func() {
		cache.ResetPhase1DoneForTest()
		dispatchers.ResetPhase1ReadinessExitForTest()
	})

	// Before Phase1Warmup reaches its first step.
	code, body := readyzRaw(t)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("warming /readyz returned %d, want 503 (status code must be unchanged)", code)
	}
	if body["status"] != "warming" || body["phase1Done"] != false {
		t.Fatalf("pre-#397 fields changed: %v", body)
	}
	if body["reason"] != "phase1_not_started" {
		t.Fatalf("#397 RED: warming body reason = %v, want phase1_not_started; body=%v", body["reason"], body)
	}
	el, ok := body["elapsed_s"].(float64)
	if !ok || el < 0 {
		t.Fatalf("#397 RED: warming body must carry a non-negative elapsed_s; body=%v", body)
	}
	if _, has := body["outcome"]; has {
		t.Fatalf("warming body must not carry an outcome yet; body=%v", body)
	}

	// During the boot seed.
	dispatchers.SetPhase1StageForTest("boot_seed_in_progress")
	code, body = readyzRaw(t)
	if code != http.StatusServiceUnavailable || body["reason"] != "boot_seed_in_progress" {
		t.Fatalf("#397: during the seed want 503 + reason=boot_seed_in_progress; got %d %v", code, body)
	}

	// Ready: the exit is recorded first, then the flip (the production order).
	dispatchers.RecordPhase1ReadinessExitNoSeed()
	cache.MarkPhase1Done()
	code, body = readyzRaw(t)
	if code != http.StatusOK {
		t.Fatalf("ready /readyz returned %d, want 200", code)
	}
	if body["status"] != "ready" || body["phase1Done"] != true {
		t.Fatalf("pre-#397 ready fields changed: %v", body)
	}
	if body["outcome"] != "none-configured" {
		t.Fatalf("#397: ready body outcome = %v, want none-configured; body=%v", body["outcome"], body)
	}
	if _, has := body["reason"]; has {
		t.Fatalf("ready body must not carry a warming reason; body=%v", body)
	}
	if _, has := body["elapsed_s"]; has {
		t.Fatalf("ready body must not carry elapsed_s; body=%v", body)
	}
}

// TestIssue407_Readyz_ReadyBodyCarriesStepTimings — #407: the ready body carries
// elapsed_ms and the per-step *_ms values from the readiness-exit record
// (exactly the values recorded); the warming body carries none of them, and the
// status codes / pre-#407 fields are unchanged. RED without #407: the ready body
// has no *_ms field.
func TestIssue407_Readyz_ReadyBodyCarriesStepTimings(t *testing.T) {
	cache.ResetPhase1DoneForTest()
	dispatchers.ResetPhase1ReadinessExitForTest()
	t.Cleanup(func() {
		cache.ResetPhase1DoneForTest()
		dispatchers.ResetPhase1ReadinessExitForTest()
	})
	timingKeys := []string{"elapsed_ms", "walk_ms", "sync_wait_ms", "content_prewarm_ms", "cluster_list_prewarm_ms", "seed_ms"}

	code, body := readyzRaw(t)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("warming /readyz returned %d, want 503", code)
	}
	for _, k := range timingKeys {
		if _, has := body[k]; has {
			t.Fatalf("warming body must not carry %s; body=%v", k, body)
		}
	}

	dispatchers.RecordPhase1ReadinessExitForTest("latch",
		9500*time.Millisecond, 1100*time.Millisecond, 2200*time.Millisecond,
		3300*time.Millisecond, 440*time.Millisecond, 2460*time.Millisecond)
	cache.MarkPhase1Done()
	code, body = readyzRaw(t)
	if code != http.StatusOK || body["status"] != "ready" || body["phase1Done"] != true || body["outcome"] != "latch" {
		t.Fatalf("ready body/status changed: %d %v", code, body)
	}
	for k, want := range map[string]float64{
		"elapsed_ms": 9500, "walk_ms": 1100, "sync_wait_ms": 2200,
		"content_prewarm_ms": 3300, "cluster_list_prewarm_ms": 440, "seed_ms": 2460,
	} {
		if got, ok := body[k].(float64); !ok || got != want {
			t.Errorf("#407 RED: ready body %s = %v, want %v; body=%v", k, body[k], want, body)
		}
	}
	if _, has := body["reason"]; has {
		t.Fatalf("ready body must not carry a warming reason; body=%v", body)
	}

	// -1 arm: a step that did not run (the -1ns sentinel) reads -1 on the ready
	// body, never 0 ((-1ns).Milliseconds() truncates to 0 without stepMs).
	cache.ResetPhase1DoneForTest()
	dispatchers.ResetPhase1ReadinessExitForTest()
	dispatchers.RecordPhase1ReadinessExitForTest("latch",
		9500*time.Millisecond, 1100*time.Millisecond, 2200*time.Millisecond,
		time.Duration(-1), 440*time.Millisecond, 2460*time.Millisecond)
	cache.MarkPhase1Done()
	code, body = readyzRaw(t)
	if code != http.StatusOK {
		t.Fatalf("ready /readyz returned %d, want 200", code)
	}
	if got, ok := body["content_prewarm_ms"].(float64); !ok || got != -1 {
		t.Errorf("not-run step: ready body content_prewarm_ms = %v, want -1; body=%v", body["content_prewarm_ms"], body)
	}
	if got, ok := body["walk_ms"].(float64); !ok || got != 1100 {
		t.Errorf("-1 arm: walk_ms = %v, want 1100", body["walk_ms"])
	}
}
