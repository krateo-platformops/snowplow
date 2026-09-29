// debug_shadow_parity_test.go — #272 (1.12.17): hermetic unit tests for the
// two shadow-parity toggle handlers. These ARE acceptance criteria 1a/1b as
// tests: they drive the handler constructors directly (no gate, no mux) and
// assert the POST/GET semantics and the 400 contract.
//
// The toggle is a PROCESS-LOCAL atomic in package rbac. Every test resets it to
// the default-off state in cleanup so no test leaks its write to another (Go
// runs tests in a package sequentially by default, but a leaked global would
// still make a later test order-dependent).

package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// resetShadowParity restores the default-off state after a test.
func resetShadowParity(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
}

// callShadowParity drives one request through h and returns the status code and
// the decoded {"enabled": ...} body (Enabled is only meaningful on a 200).
func callShadowParity(t *testing.T, h http.HandlerFunc, method, target string) (int, bool) {
	t.Helper()
	req := httptest.NewRequest(method, "http://snowplow.example"+target, nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	res := rec.Result()
	var body struct {
		Enabled bool `json:"enabled"`
	}
	if res.StatusCode == http.StatusOK {
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			t.Fatalf("%s %s: decode 200 body: %v", method, target, err)
		}
	}
	return res.StatusCode, body.Enabled
}

// GET reports the live committed state and mutates nothing (criterion 1b).
func TestDebugShadowParityGet_ReadsLiveStateWithoutMutating(t *testing.T) {
	resetShadowParity(t)
	get := DebugShadowParityGet()

	// Default-off.
	if code, enabled := callShadowParity(t, get, http.MethodGet, "/debug/shadow-parity"); code != http.StatusOK || enabled {
		t.Errorf("GET default = (%d, %v), want (200, false)", code, enabled)
	}

	// Reflects a state set out-of-band, and reading it twice does not flip it.
	rbac.SetShadowParityEnabled(true)
	if code, enabled := callShadowParity(t, get, http.MethodGet, "/debug/shadow-parity"); code != http.StatusOK || !enabled {
		t.Errorf("GET after enable = (%d, %v), want (200, true)", code, enabled)
	}
	if code, enabled := callShadowParity(t, get, http.MethodGet, "/debug/shadow-parity"); code != http.StatusOK || !enabled {
		t.Errorf("GET again = (%d, %v), want (200, true) — GET must not mutate", code, enabled)
	}
	if !rbac.ShadowParityEnabled() {
		t.Error("live toggle flipped by a GET — GET must be a pure read")
	}
}

// POST ?enabled=true|false flips the toggle and reports the committed live
// state (criterion 1a).
func TestDebugShadowParitySet_TrueThenFalse(t *testing.T) {
	resetShadowParity(t)
	set := DebugShadowParitySet()

	if code, enabled := callShadowParity(t, set, http.MethodPost, "/debug/shadow-parity?enabled=true"); code != http.StatusOK || !enabled {
		t.Errorf("POST enabled=true = (%d, %v), want (200, true)", code, enabled)
	}
	if !rbac.ShadowParityEnabled() {
		t.Error("POST enabled=true did not flip the live toggle on")
	}

	if code, enabled := callShadowParity(t, set, http.MethodPost, "/debug/shadow-parity?enabled=false"); code != http.StatusOK || enabled {
		t.Errorf("POST enabled=false = (%d, %v), want (200, false)", code, enabled)
	}
	if rbac.ShadowParityEnabled() {
		t.Error("POST enabled=false did not flip the live toggle off")
	}
}

// A missing or unparseable `enabled` is a 400 — the /debug "don't silently
// guess" convention (debug_store.go's parseGVRParam). The live toggle must not
// move on a rejected request.
func TestDebugShadowParitySet_AbsentOrUnparseableIs400(t *testing.T) {
	resetShadowParity(t)
	set := DebugShadowParitySet()

	rbac.SetShadowParityEnabled(true) // a known non-default state to prove it is untouched
	for _, target := range []string{
		"/debug/shadow-parity",             // absent
		"/debug/shadow-parity?enabled=",    // empty
		"/debug/shadow-parity?enabled=yes", // unparseable (ParseBool rejects "yes")
		"/debug/shadow-parity?enabled=banana",
	} {
		if code, _ := callShadowParity(t, set, http.MethodPost, target); code != http.StatusBadRequest {
			t.Errorf("POST %q = %d, want 400", target, code)
		}
	}
	if !rbac.ShadowParityEnabled() {
		t.Error("a rejected (400) POST moved the live toggle — it must be untouched")
	}
}

// Double-POST of the same value is idempotent: state == last write, no
// flip-flop.
func TestDebugShadowParitySet_Idempotent(t *testing.T) {
	resetShadowParity(t)
	set := DebugShadowParitySet()

	for i := 0; i < 2; i++ {
		if code, enabled := callShadowParity(t, set, http.MethodPost, "/debug/shadow-parity?enabled=true"); code != http.StatusOK || !enabled {
			t.Errorf("POST enabled=true #%d = (%d, %v), want (200, true)", i+1, code, enabled)
		}
	}
	if !rbac.ShadowParityEnabled() {
		t.Error("state after two identical POSTs is not the written value")
	}
}
