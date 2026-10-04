// call_443_nearmiss_test.go — #443 reviewer-415 hardening item H1.
package handlers

import (
	"net/http"
	"strings"
	"testing"
)

// TestS443_NearMissSpelling — a query key that is a case, underscore or hyphen
// variant of dryRun or fieldValidation (dryrun, DryRun, dry_run,
// FieldValidation, field_validation, …) is a 400 that names the correct
// spelling, with ZERO outbound requests. That holds on every verb of plain
// /call, on /call/dry-run, and through the dispatcher for a GVR that has a
// resolve handler. Before the fix, `POST /call?dryrun=All` was a REAL write
// with no echo: the misspelled key was silently dropped.
func TestS443_NearMissSpelling(t *testing.T) {
	f, ep := newFakeAPI443(t)
	keys := []struct{ key, want string }{
		{"dryrun", "dryRun"}, {"DryRun", "dryRun"}, {"DRYRUN", "dryRun"},
		{"dry_run", "dryRun"}, {"dry-run", "dryRun"}, {"Dry_Run", "dryRun"},
		{"FieldValidation", "fieldValidation"}, {"fieldvalidation", "fieldValidation"},
		{"field_validation", "fieldValidation"}, {"field-validation", "fieldValidation"},
	}
	ra, widget, m := dispatchHandlers()
	for _, k := range keys {
		param := "&" + k.key + "=All"
		if k.want == "fieldValidation" {
			param = "&" + k.key + "=Strict"
		}
		targets := []struct {
			name, method, target string
			h                    http.Handler
		}{
			{"POST /call", http.MethodPost, "/call?" + cmQuery443 + param, Call()},
			{"PUT /call", http.MethodPut, "/call?" + cmQuery443 + param, Call()},
			{"PATCH /call", http.MethodPatch, "/call?" + cmQuery443 + param, Call()},
			{"DELETE /call", http.MethodDelete, "/call?" + cmQuery443 + param, Call()},
			{"GET /call", http.MethodGet, "/call?" + cmQuery443 + param, Call()},
			{"GET /call RA via dispatcher", http.MethodGet, "/call?" + raQuery443 + param, Dispatcher(m)(Call())},
			{"POST /call/read RA via dispatcher", http.MethodPost, "/call/read?" + raQuery443 + param, ReadDispatcher(m)(CallRead())},
			{"POST /call/dry-run", http.MethodPost, "/call/dry-run?" + cmQuery443 + param, CallDryRun()},
		}
		for _, tc := range targets {
			f.reset()
			rec := serve443(tc.h, ep, tc.method, tc.target, cmCreate443)
			name := tc.name + " ?" + k.key
			if rec.Code != http.StatusBadRequest {
				t.Errorf("%s: code=%d, want 400", name, rec.Code)
			}
			if got := f.requests(); len(got) != 0 {
				t.Errorf("%s: %d outbound requests (first: %s ?%s), want 0 — a misspelled key must never be silently dropped",
					name, len(got), got[0].method, got[0].query.Encode())
			}
			if !strings.Contains(rec.Body.String(), k.want) {
				t.Errorf("%s: body %q does not name the correct spelling %q", name, rec.Body.String(), k.want)
			}
			assertNoEcho443(t, rec, name)
		}
	}
	if ra.called || widget.called {
		t.Error("a misspelled dryRun/fieldValidation reached a resolve handler")
	}
}
