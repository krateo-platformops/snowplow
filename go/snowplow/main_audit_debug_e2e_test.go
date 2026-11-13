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
//   N  GET /debug/shadow-parity refused (a read, not audited) and a request
//      with no token: zero rows;
//   C  every row conforms; the design-error counter never moves.

import (
	"log/slog"
	"net/http"
	"testing"
	"time"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"github.com/krateo-platformops/plumbing/jwtutil"
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

func TestAudit_DebugE2E_A_D_N_C_TheToggleThroughTheRealDebugLane(t *testing.T) {
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	i1362Watcher(t, i1362World(t)...)
	audit.SetNamespaceReader(e2eNamespaces{})

	for _, c := range []struct {
		name    string
		method  string
		id      *i1362Identity
		code    int
		rows    int
		outcome string
		reason  string
	}{
		{"A: a granted caller flips it", http.MethodPost, &i1362Operator, http.StatusOK, 1, krateoaudit.OutcomeSuccess, ""},
		{"D: snowplow's RBAC refuses a verified caller", http.MethodPost, &i1362Visitor, http.StatusForbidden, 1, krateoaudit.OutcomeDenied, audit.DenialRBACNotAllowed},
		{"N: a refused read is not audited", http.MethodGet, &i1362Visitor, http.StatusForbidden, 0, "", ""},
		{"N: no token", http.MethodPost, nil, http.StatusUnauthorized, 0, "", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
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
			if c.outcome == krateoaudit.OutcomeDenied {
				if v, ok := a[krateoaudit.KeyHTTPStatusCode]; ok {
					t.Errorf("a denied row carries http.response.status_code=%s", v)
				}
			}
			audittest.AssertConformant(t, audit.Writer, recs[0])
			audittest.AssertRequiredKeysDetected(t, audit.Writer, recs[0])
			audittest.AssertEnumsDetected(t, audit.Writer, recs[0])
		})
	}
}
