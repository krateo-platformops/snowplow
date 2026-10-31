package handlers

// #1025 through an export: GET and POST /export resolve every page through
// the /call lane, so a stored RESTAction whose stage names ANOTHER user's
// per-user credential Secret by a literal endpointRef is refused there too.
// The victim's step-up credential points at its own recorder: a resolve that
// selected it would send the read there with the victim's bearer and export
// the victim's rows.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
)

const victimRAName1025 = "team-rows-victim-1025"

// victimSecret1025 is another user's step-up credential Secret name.
var victimSecret1025 = credsecret.StepUpName("victim1025")

func TestExport1025_PerUserEndpointRefRefusedOnEveryExportLane(t *testing.T) {
	for _, l := range exportLanes970(victimRAName1025) {
		t.Run(l.name, func(t *testing.T) {
			local := newClusterRecorder970(t, "local")
			victim := newClusterRecorder970(t, "victim")
			seedExtrasHarnessWithSecrets(t, local.srv.URL, clientConfigSecret970(victimSecret1025, victim.srv.URL, "token-victim-stepup"))
			rec := serveAs970(l.h(), l.method, l.target, l.body, local.srv.URL)
			oc := rec.Header().Get("X-Snowplow-Stage-Outcomes")
			got := victim.take()
			t.Logf("%d outcomes=%s victim=%v local=%v", rec.Code, oc, got, local.take())
			if len(got) != 0 {
				t.Fatalf("#1025 VIOLATED: the export sent the stage with the victim's credential: %v", got)
			}
			if strings.Contains(rec.Body.String(), "victim-0") {
				t.Fatalf("#1025 VIOLATED: the export carries the victim's rows: %s", rec.Body.String())
			}
			if rec.Code != http.StatusOK || !strings.Contains(oc, `"name":"cms","ok":false,"reason":"Error"`) {
				t.Errorf("export %d with outcomes %q: the refused stage is not reported", rec.Code, oc)
			}
		})
	}
}
