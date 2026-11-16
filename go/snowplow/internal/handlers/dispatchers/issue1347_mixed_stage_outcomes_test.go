package dispatchers

// issue1347_mixed_stage_outcomes_test.go — #1347 review blocker, lead's
// ruling, end to end: a stage whose items fail with reasons of two groups
// reports the higher group's reason (cut / failed > denied > by design), on a
// widget /call and on the RESTAction /call alike, from the one collector both
// headers read.
//
// Through the REAL widgetsHandler / restActionHandler and the #681 harness: one
// iterator stage (continueOnError) reads x/y, which A may not read (A's own
// apiserver dial answers 403), and pub/boom, which is in no informer, so its
// dial answers 500. Before the ruling the stage read Forbidden ("denied"): the
// portal showed no notice although the 500 item's data is missing.
//
//   M1 the widget header lists the stage with Error, a "cut / failed" code;
//      the cell is not stored (a non-denial stage error, the shared Put gate);
//   M2 the RESTAction /call header lists the same stage with Error.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/objects"
)

// m1347RA is the widget's apiRef RESTAction: sb reads pub/p; sa iterates
// (after sb) over the denied x/y and the failing pub/boom.
func m1347RA() *unstructured.Unstructured {
	ra := c681RAObj("/api/v1/namespaces/x/configmaps/y", "/api/v1/namespaces/pub/configmaps/p")
	api := ra.Object["spec"].(map[string]any)["api"].([]any)
	sa := api[0].(map[string]any)
	sa["path"] = `${ "/api/v1/namespaces/" + .ns + "/configmaps/" + .name }`
	sa["dependsOn"] = map[string]any{"name": "sb",
		"iterator": `[ {ns: "x", name: "y"}, {ns: "pub", name: "boom"} ]`}
	return ra
}

// m1347WantError asserts the header lists exactly stage sa, with Error.
func m1347WantError(t *testing.T, what string, rows map[string]w1319Row) {
	t.Helper()
	if r, ok := rows["sa"]; !ok || r.Reason != "Error" || len(rows) != 1 {
		t.Fatalf("%s RED: rows %v, want exactly sa with Error (cut / failed): a denied item must not hide the failed one", what, rows)
	}
}

func TestIssue1347_M1_AWidgetStageWithADenialAndAFailureReadsTheFailure(t *testing.T) {
	e := w1319SetupRA(t, m1347RA())
	rec, dials := e.call(t, e.ctx())
	if dials < 2 {
		t.Fatalf("SETUP: A's cold /call made %d dials, want the 403 and the 500 at least", dials)
	}
	if !strings.Contains(rec.Body.String(), "PUBDATA-681") {
		t.Fatalf("SETUP: the widget body must carry sb's data: %s", s425Trunc(rec.Body.Bytes()))
	}
	m1347WantError(t, "M1 widget", w1319Rows(t, "M1 widget", rec))
	if _, ok := e.cell(t, e.ctx()); ok {
		t.Fatal("M1 RED: a result with a failed (non-denial) item was stored")
	}
}

func TestIssue1347_M2_TheRESTActionHeaderReadsTheSameReason(t *testing.T) {
	e := w1319SetupRA(t, m1347RA())
	ra := m1347RA()
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: ra.DeepCopy()}
	})
	defer restore()
	h := &restActionHandler{authnNS: c681AuthnNS, saRC: &rest.Config{Host: e.user.URL}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(c681A.ctx()))
	if rec.Code != http.StatusOK {
		t.Fatalf("SETUP: RESTAction /call status %d body=%s", rec.Code, rec.Body.String())
	}
	// PRE: the stage is mixed: its errorKey holds the 403 and the 500.
	if b := rec.Body.String(); !strings.Contains(b, `"code":403`) || !strings.Contains(b, `"code":500`) {
		t.Fatalf("PRE: sa's errorKey must hold the 403 and the 500: %s", s425Trunc(rec.Body.Bytes()))
	}
	if rec.Header().Get(util.HeaderStageOutcomes) == "" {
		t.Fatal("M2 RED: a RESTAction /call carries no stage-outcomes header")
	}
	m1347WantError(t, "M2 RESTAction", w1319Rows(t, "M2 RESTAction", rec))
}
