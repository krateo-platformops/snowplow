// debug_reconcile_dryrun_test.go — #238: the /debug/reconcile ?dryRun handler
// contract. Hermetic (httptest; the cache is off in a bare unit test, so
// ReconcileFullDryRun returns ok=false and the handler answers 200 with
// cacheEnabled=false — enough to pin the PARSE + MODE-ECHO, which is the new
// handler logic; the evict-vs-preserve behaviour is proven at the cache layer
// in internal/cache/issue238_dryrun_falsifier_test.go).

package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func callDebugReconcile(t *testing.T, target string) (int, debugReconcileBody) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://snowplow.example"+target, nil)
	rec := httptest.NewRecorder()
	DebugReconcile()(rec, req)
	res := rec.Result()
	var body debugReconcileBody
	if res.StatusCode == http.StatusOK {
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			t.Fatalf("%s: decode 200 body: %v", target, err)
		}
	}
	return res.StatusCode, body
}

// An unparseable dryRun is a 400 — never a silent fall to the DESTRUCTIVE
// reconcile default (#238).
func TestDebugReconcile_UnparseableDryRunIs400(t *testing.T) {
	for _, target := range []string{
		"/debug/reconcile?dryRun=banana",
		"/debug/reconcile?dryRun=",
		"/debug/reconcile?dryRun=maybe",
	} {
		if code, _ := callDebugReconcile(t, target); code != http.StatusBadRequest {
			t.Errorf("GET %q = %d, want 400 (unparseable dryRun must not fall to the reconcile default)", target, code)
		}
	}
}

// The response echoes the mode so a captured reconcile.json is self-describing,
// in BOTH modes (absent → reconcile default → dryRun:false).
func TestDebugReconcile_EchoesMode(t *testing.T) {
	if code, body := callDebugReconcile(t, "/debug/reconcile?dryRun=1"); code != http.StatusOK || !body.DryRun {
		t.Errorf("GET ?dryRun=1 = (%d, dryRun=%v), want (200, true)", code, body.DryRun)
	}
	if code, body := callDebugReconcile(t, "/debug/reconcile"); code != http.StatusOK || body.DryRun {
		t.Errorf("GET (no dryRun) = (%d, dryRun=%v), want (200, false) — the reconcile default", code, body.DryRun)
	}
	if code, body := callDebugReconcile(t, "/debug/reconcile?dryRun=false"); code != http.StatusOK || body.DryRun {
		t.Errorf("GET ?dryRun=false = (%d, dryRun=%v), want (200, false)", code, body.DryRun)
	}
}
