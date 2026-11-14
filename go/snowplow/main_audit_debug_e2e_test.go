package main

// main_audit_debug_e2e_test.go — #909 and audit v0.13.0 (rbac_not_allowed),
// through the REAL debug lane: registerDebugRoutes on a routeTable (so POST
// /debug/shadow-parity is wrapped in audit.OpenRecord), newRequestChain,
// RefreshAuth with the test key and DebugAuthz over a real RBAC snapshot.
//
//   A  a caller the RBAC grants flips the toggle: exactly one row, success;
//   D  a verified caller snowplow's own RBAC evaluation refuses: exactly one
//      row, denied rbac_not_allowed, no status code (an authority's 403 would
//      be a status code; this one is snowplow's);
//   U  a granted caller whose authorization cannot be evaluated (the RBAC
//      snapshot is not built): 503, exactly one row, failure, no status code,
//      the fixed body MessageAuthorizationUnavailable (nothing ran, so no
//      authority answered);
//   N  GET /debug/shadow-parity refused (a read, not audited) and a request
//      with no token: zero rows;
//   C  every row conforms; the design-error counter never moves.
//
// url.path targets: the writer is the authority, so a success records its own
// answer (A's 200); a denied or failed record carries none.

import (
	"log/slog"
	"net/http"
	"testing"
	"time"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

func debugE2EToken(t *testing.T, id i1362Identity) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: id.user, Groups: id.groups, Duration: time.Hour,
		KeyID: debugAuthTestKeyID, PrivateKey: debugAuthTestPrivateKey, Principal: jwtutil.PrincipalHuman,
	})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	return tok
}

func TestAudit_DebugE2E_A_D_U_N_C_TheToggleThroughTheRealDebugLane(t *testing.T) {
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	i1362Watcher(t, i1362World(t)...)
	audit.SetNamespaceReader(e2eNamespaces{})

	for _, c := range []struct {
		name        string
		method      string
		id          *i1362Identity
		undecidable bool // the RBAC snapshot is not built: EvaluateRBAC errors
		code        int
		rows        int
		outcome     string
		reason      string
		status      string // the row's http.response.status_code; "" none
		body        string // the row's body; "" not checked
	}{
		{"A: a granted caller flips it", http.MethodPost, &i1362Operator, false, http.StatusOK, 1,
			krateoaudit.OutcomeSuccess, "", "200", ""},
		{"D: snowplow's RBAC refuses a verified caller", http.MethodPost, &i1362Visitor, false, http.StatusForbidden, 1,
			krateoaudit.OutcomeDenied, audit.DenialRBACNotAllowed, "", audit.MessageRefused},
		{"U: the caller's authorization cannot be evaluated", http.MethodPost, &i1362Operator, true, http.StatusServiceUnavailable, 1,
			krateoaudit.OutcomeFailure, "", "", audit.MessageAuthorizationUnavailable},
		{"N: a refused read is not audited", http.MethodGet, &i1362Visitor, false, http.StatusForbidden, 0, "", "", "", ""},
		{"N: no token", http.MethodPost, nil, false, http.StatusUnauthorized, 0, "", "", "", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.undecidable {
				prevW, prevS := cache.Global(), cache.RBACSnapshotForTest()
				cache.SetGlobal(&cache.ResourceWatcher{})
				cache.PublishRBACSnapshotForTest(nil)
				t.Cleanup(func() { cache.SetGlobal(prevW); cache.PublishRBACSnapshotForTest(prevS) })
			}
			rec := audittest.NewRecorder(t)
			audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
			t.Cleanup(func() { audit.SetDefault(nil) })
			mux := http.NewServeMux()
			registerDebugRoutes(newRouteTable(mux), newRequestChain(slog.New(slog.DiscardHandler)), debugAuthTestKeys)

			bearer := ""
			if c.id != nil {
				bearer = debugE2EToken(t, *c.id)
			}
			before := audit.DesignErrorCount("answered")
			w := i1362Drive(t, mux, c.method, handlers.ShadowParityPath+"?enabled=true", bearer)
			if w.Code != c.code {
				t.Fatalf("answered %d, want %d: %s", w.Code, c.code, w.Body.String())
			}
			if d := audit.DesignErrorCount("answered") - before; d != 0 {
				t.Errorf("the design-error counter moved by %d", d)
			}
			recs := rec.Records()
			if len(recs) != c.rows {
				t.Fatalf("%d rows, want %d", len(recs), c.rows)
			}
			if c.rows == 0 {
				return
			}
			a := map[string]string{}
			for _, kv := range audittest.Attributes(recs[0]) {
				a[string(kv.Key)] = kv.Value.Emit()
			}
			for k, want := range map[string]string{
				krateoaudit.KeyAction:       "debug.shadowParity",
				krateoaudit.KeyURLPath:      handlers.ShadowParityPath,
				krateoaudit.KeyOutcome:      c.outcome,
				krateoaudit.KeyDenialReason: c.reason,
				krateoaudit.KeyUserName:     c.id.user,
			} {
				if a[k] != want {
					t.Errorf("%s = %q, want %q", k, a[k], want)
				}
			}
			if v, ok := a[krateoaudit.KeyHTTPStatusCode]; v != c.status || (c.status == "" && ok) {
				t.Errorf("http.response.status_code = %q (present %t), want %q", v, ok, c.status)
			}
			if got := recs[0].Body().AsString(); c.body != "" && got != c.body {
				t.Errorf("body %q, want %q", got, c.body)
			}
			audittest.AssertConformant(t, audit.Writer, recs[0])
			audittest.AssertRequiredKeysDetected(t, audit.Writer, recs[0])
			audittest.AssertEnumsDetected(t, audit.Writer, recs[0])
		})
	}
}
