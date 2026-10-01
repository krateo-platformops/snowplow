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
