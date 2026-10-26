package dispatchers

// issue904_stage_outcomes_header_test.go — #904 on the customer /call path,
// through the REAL restActionHandler.ServeHTTP and the REAL L1 (the #681
// harness: a per-user httptest apiserver that authorizes every dial with the
// real EvaluateRBAC and counts it).
//
// The RESTAction reads x/y (A may NOT: an apiserver 403), reads pub/p, and
// POSTs to x (dependsOn the x/y read, no continueOnError).
//
// ARMS
//   W1  A: the POST is NOT sent (the only dial is the 403), the status stays
//       200, and X-Snowplow-Stage-Outcomes lists the Forbidden read and the
//       skipped write with the stage it depended on — no identity in it.
//   W2  A's result is denial-only (#681), so it is cached; the L1 hit (0
//       dials) answers with the SAME header as the cold resolve.
//   W3  B (may read x/y): the POST is sent exactly once.
//   W4  a RESTAction whose stages all succeed answers [] cold and warm.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

func w904RA() *unstructured.Unstructured {
	ra := c681RAObj("/api/v1/namespaces/x/configmaps/y", "/api/v1/namespaces/pub/configmaps/p")
	stages := ra.Object["spec"].(map[string]any)["api"].([]any)
	stages = append(stages, map[string]any{
		"name": "create", "verb": "POST", "errorKey": "createErr",
		"path":      "/api/v1/namespaces/x/configmaps",
		"payload":   `{"metadata":{"name":"z"}}`,
		"dependsOn": map[string]any{"name": "sa"},
	})
	ra.Object["spec"].(map[string]any)["api"] = stages
	return ra
}

// callRec serves one /call for id and returns the recorder and its dials.
func (e *c681Env) callRec(t *testing.T, id ident681) (*httptest.ResponseRecorder, int64) {
	t.Helper()
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: e.ra.DeepCopy()}
	})
	defer restore()
	d0 := e.dials.Load()
	h := &restActionHandler{authnNS: c681AuthnNS, saRC: &rest.Config{Host: e.user.URL}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(id.ctx()))
	if rec.Code != http.StatusOK {
		t.Fatalf("/call for %s: status %d body=%s", id.user, rec.Code, rec.Body.String())
	}
	return rec, e.dials.Load() - d0
}

type w904Row struct {
	Name      string `json:"name"`
	OK        bool   `json:"ok"`
	Reason    string `json:"reason"`
	DependsOn string `json:"dependsOn"`
}

func w904Rows(t *testing.T, rec *httptest.ResponseRecorder) (string, map[string]w904Row) {
	t.Helper()
	v := rec.Header().Get(util.HeaderStageOutcomes)
	if v == "" {
		t.Fatalf("%s missing on a RESTAction /call answer", util.HeaderStageOutcomes)
	}
	var rows []w904Row
	if err := json.Unmarshal([]byte(v), &rows); err != nil {
		t.Fatalf("%s is not a JSON array: %v (%s)", util.HeaderStageOutcomes, err, v)
	}
	out := map[string]w904Row{}
	for _, r := range rows {
		out[r.Name] = r
	}
	return v, out
}

func TestIssue904_W1_W2_SkippedWriteHeaderColdAndWarm(t *testing.T) {
	e := c681Setup(t)
	e.ra = w904RA()

	rec1, d1 := e.callRec(t, c681A)
	if d1 != 1 {
		t.Fatalf("#904 RED: A's /call made %d dial(s), want 1 (the 403 read; the dependent POST must not be sent)", d1)
	}
	v1, rows := w904Rows(t, rec1)
	if w := (w904Row{Name: "sa", Reason: api.StageReasonForbidden}); rows["sa"] != w {
		t.Errorf("stage sa outcome = %+v, want %+v (header=%s)", rows["sa"], w, v1)
	}
	if w := (w904Row{Name: "create", Reason: api.StageReasonSkipped, DependsOn: "sa"}); rows["create"] != w {
		t.Errorf("stage create outcome = %+v, want %+v (header=%s)", rows["create"], w, v1)
	}
	if _, listed := rows["sb"]; listed {
		t.Errorf("stage sb succeeded and must not be listed: %s", v1)
	}
	low := strings.ToLower(v1)
	for _, s := range []string{c681Sentinel, "alice", c681All, "/api/", "forbidden:"} {
		if strings.Contains(low, strings.ToLower(s)) {
			t.Errorf("the header carries %q: %s", s, v1)
		}
	}

	if _, ok := e.cell(c681A, t); !ok {
		t.Fatal("PRE: A's denial-only result must be cached (#681)")
	}
	rec2, d2 := e.callRec(t, c681A)
	if d2 != 0 {
		t.Fatalf("PRE: A's second /call must be an L1 hit (0 dials), got %d", d2)
	}
	if v2 := rec2.Header().Get(util.HeaderStageOutcomes); v2 != v1 {
		t.Fatalf("#904 RED: the L1 hit answered %s=%q, the cold resolve %q", util.HeaderStageOutcomes, v2, v1)
	}
}

func TestIssue904_W3_DependencySucceededWriteSentOnce(t *testing.T) {
	e := c681Setup(t)
	e.ra = w904RA()
	rec, d := e.callRec(t, c681B)
	// B reads x/y and pub/p in-process (informer), so the only dial is the POST.
	if d != 1 {
		t.Fatalf("B's dependent POST must be sent exactly once: dials=%d", d)
	}
	_, rows := w904Rows(t, rec)
	if r, listed := rows["sa"]; listed {
		t.Errorf("B's read of x/y succeeded and must not be listed: %+v", r)
	}
	if r := rows["create"]; r.Reason == api.StageReasonSkipped {
		t.Errorf("B's write was skipped: %+v", r)
	}
}

func TestIssue904_W4_AllSucceededHeaderIsEmptyArrayColdAndWarm(t *testing.T) {
	e := c681Setup(t)
	e.ra = c681RAObj("/api/v1/namespaces/pub/configmaps/p")
	rec1, _ := e.callRec(t, c681A)
	if v := rec1.Header().Get(util.HeaderStageOutcomes); v != "[]" {
		t.Fatalf("cold: every stage succeeded, want [] got %q", v)
	}
	rec2, d2 := e.callRec(t, c681A)
	if d2 != 0 {
		t.Fatalf("PRE: the second /call must be an L1 hit, dials=%d", d2)
	}
	if v := rec2.Header().Get(util.HeaderStageOutcomes); v != "[]" {
		t.Fatalf("warm: want [] got %q", v)
	}
}
