package api

// stage_audit_957_test.go — #957 (krateo-acmp#59 Option B) at the RESTAction
// stage emit site, driven through the REAL api.Resolve against a fake
// apiserver whose answers carry a uid, generation and resourceVersion that all
// DIFFER from the stage payload's, so a record built from the request is told
// apart from one built from the response.
//
//   W  create, update, patch: the record carries the RESPONSE object's
//      metadata.uid / generation / resourceVersion.
//   Wd delete: the uid only (returned object, or a Status's details.uid).
//   F  a refused write records none of them.
//   Xe a stage to a named endpoint (endpointRef) records none, though the
//      endpoint answers with an apiserver-shaped object.
//   Fb the site handed a failure answer with an object body records none.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

const (
	base957 = "/apis/composition.krateo.io/v1alpha1/namespaces/team-a/fireworksapps"
	// The stage payload: what the stage sends. Never what the record carries.
	payload957 = `{"apiVersion":"composition.krateo.io/v1alpha1","kind":"FireworksApp",` +
		`"metadata":{"name":"demo","uid":"uid-req-957","generation":9,"resourceVersion":"100"},"spec":{"replicas":1}}`
)

// object957 is the apiserver's answer for a stored object.
func object957(gen, rv string, extra string) string {
	return `{"apiVersion":"composition.krateo.io/v1alpha1","kind":"FireworksApp","metadata":{"name":"demo",` +
		`"namespace":"team-a","uid":"uid-resp-957","generation":` + gen + `,"resourceVersion":"` + rv + `"` + extra + `},` +
		`"spec":{"replicas":2}}`
}

// apiserver957 answers per method like the apiserver does, and records the
// body of every request it receives.
func apiserver957(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, r.Method+" "+string(b))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/team-b/"):
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","reason":"Forbidden",`+
				`"code":403,"details":{"name":"demo","kind":"fireworksapps","uid":"uid-resp-957"}}`)
		case r.Method == http.MethodPost:
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, object957("1", "501", ""))
		case r.Method == http.MethodPut:
			_, _ = io.WriteString(w, object957("4", "502", ""))
		case r.Method == http.MethodPatch:
			_, _ = io.WriteString(w, object957("5", "503", ""))
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/gone"):
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Success",`+
				`"details":{"name":"gone","group":"composition.krateo.io","kind":"fireworksapps","uid":"uid-gone-957"}}`)
		case r.Method == http.MethodDelete:
			_, _ = io.WriteString(w, object957("6", "504", `,"deletionTimestamp":"2026-10-28T10:00:00Z","finalizers":["x.io/f"]`))
		default:
			_, _ = io.WriteString(w, object957("4", "502", ""))
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), bodies...)
	}
}

var objectKeys957 = []string{"k8s.object.uid", "k8s.object.generation", "k8s.object.resource_version"}

// wantObject957 asserts the record's k8s.object.* attributes are exactly want.
func wantObject957(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	for _, k := range objectKeys957 {
		w, wantIt := want[k]
		g, has := got[k]
		switch {
		case wantIt && (!has || g != w):
			t.Errorf("%s = %q (present=%t), want %q (record %v)", k, g, has, w, got)
		case !wantIt && has:
			t.Errorf("%s = %q, want it absent (record %v)", k, g, got)
		}
	}
}

func withPayload957(s *templates.API) *templates.API {
	s.Payload = ptr.To(payload957)
	return s
}

func TestStageAudit957_W_WriteRecordsTheResponseObject(t *testing.T) {
	api, bodies := apiserver957(t)
	env410(t, api.URL, api.URL)
	rec := records410(t)

	resolve410(ctxWithUser(user410, group410), api.URL,
		withPayload957(stage410("create", http.MethodPost, base957)),
		withPayload957(stage410("update", http.MethodPut, base957+"/demo")),
		withPayload957(stage410("patch", http.MethodPatch, base957+"/demo")),
		withPayload957(stage410("status", http.MethodPut, base957+"/demo/status")),
		stage410("remove", http.MethodDelete, base957+"/demo"),
		stage410("removed", http.MethodDelete, base957+"/gone"))

	sent := bodies()
	if len(sent) != 6 {
		t.Fatalf("precondition: the apiserver saw %d requests, want 6: %v", len(sent), sent)
	}
	for _, b := range sent {
		if strings.HasPrefix(b, "P") && !strings.Contains(b, "uid-req-957") {
			t.Fatalf("precondition: a write was sent without the stage payload: %q", b)
		}
	}
	byStage := map[string]map[string]string{}
	for _, r := range attrs410(rec) {
		byStage[r["krateo.restaction.stage"]] = r
	}
	if len(byStage) != 6 {
		t.Fatalf("records = %d stages, want 6: %v", len(byStage), byStage)
	}
	want := map[string]map[string]string{
		"create":  {"k8s.object.uid": "uid-resp-957", "k8s.object.generation": "1", "k8s.object.resource_version": "501"},
		"update":  {"k8s.object.uid": "uid-resp-957", "k8s.object.generation": "4", "k8s.object.resource_version": "502"},
		"patch":   {"k8s.object.uid": "uid-resp-957", "k8s.object.generation": "5", "k8s.object.resource_version": "503"},
		"status":  {"k8s.object.uid": "uid-resp-957", "k8s.object.generation": "4", "k8s.object.resource_version": "502"},
		"remove":  {"k8s.object.uid": "uid-resp-957"},
		"removed": {"k8s.object.uid": "uid-gone-957"},
	}
	for stage, w := range want {
		r, ok := byStage[stage]
		if !ok {
			t.Errorf("no record for stage %q", stage)
			continue
		}
		if r["outcome"] != "success" || r["k8s.resource.resource"] != "fireworksapps" {
			t.Errorf("stage %s: precondition: record %v is not a successful apiserver write", stage, r)
		}
		wantObject957(t, r, w)
	}
}

func TestStageAudit957_F_RefusedWriteRecordsNone(t *testing.T) {
	api, _ := apiserver957(t)
	env410(t, api.URL, api.URL)
	rec := records410(t)

	resolve410(ctxWithUser(user410, group410), api.URL,
		withPayload957(stage410("create", http.MethodPost, denyPath410)),
		stage410("remove", http.MethodDelete, denyPath410+"/demo"))

	got := attrs410(rec)
	if len(got) != 2 {
		t.Fatalf("records = %d, want 2: %v", len(got), got)
	}
	for _, r := range got {
		if r["outcome"] != "failure" || r["http.response.status_code"] != "403" {
			t.Fatalf("precondition: record %v is not the refused write's", r)
		}
		wantObject957(t, r, nil)
	}
}

func TestStageAudit957_Xe_EndpointRefStageRecordsNone(t *testing.T) {
	api, _ := apiserver957(t)
	ext, extBodies := apiserver957(t) // answers with apiserver-shaped objects
	env410(t, api.URL, ext.URL)
	rec := records410(t)

	resolve410(ctxWithUser(user410, group410), api.URL,
		withPayload957(extStage410("create", http.MethodPost, base957)),
		withPayload957(extStage410("update", http.MethodPut, base957+"/demo")))

	if n := len(extBodies()); n != 2 {
		t.Fatalf("precondition: the named endpoint saw %d requests, want 2", n)
	}
	got := attrs410(rec)
	if len(got) != 2 {
		t.Fatalf("records = %d, want 2: %v", len(got), got)
	}
	for _, r := range got {
		if r["outcome"] != "success" || r["server.address"] != strings.TrimPrefix(ext.URL, "http://") {
			t.Fatalf("precondition: record %v is not the endpoint write's", r)
		}
		wantObject957(t, r, nil)
	}
}

// Fb — the site itself: handed a failure answer together with an object body,
// it records none of the three. The control (a 2xx answer, the same body)
// proves the site reads the body at all.
func TestStageAudit957_Fb_StageSite_FailureWithBody_RecordsNone(t *testing.T) {
	rec := records410(t)
	call := httpcallOpts410(http.MethodPut, base957+"/demo", "https://api.example")
	call.Payload = ptr.To(payload957)
	apiCall := stage410("update", http.MethodPut, base957+"/demo")
	body := []byte(object957("4", "502", ""))

	for _, a := range []*dialAnswer{{code: http.StatusConflict}, {code: http.StatusInternalServerError}, {}} {
		auditStageWrite(ctxWithUser(user410, group410), raNS410, raName410, apiCall, call, a, body)
	}
	got := attrs410(rec)
	if len(got) != 3 {
		t.Fatalf("records = %d, want 3", len(got))
	}
	for _, r := range got {
		if r["outcome"] != "failure" {
			t.Fatalf("precondition: %v is not a failure", r)
		}
		wantObject957(t, r, nil)
	}

	rec2 := records410(t)
	auditStageWrite(ctxWithUser(user410, group410), raNS410, raName410, apiCall, call, &dialAnswer{code: http.StatusOK}, body)
	got2 := attrs410(rec2)
	if len(got2) != 1 {
		t.Fatalf("control: records = %d, want 1", len(got2))
	}
	wantObject957(t, got2[0], map[string]string{
		"k8s.object.uid": "uid-resp-957", "k8s.object.generation": "4", "k8s.object.resource_version": "502",
	})
}

// Dr — review F1 (#962): a stage whose apiserver path asks for a dry run is
// marked restaction.dryRun and records none of the three fields, though the
// apiserver answers with the object (a real dry-run update carries the real
// uid and the generation the change would reach). Controls: a key in another
// case is not a dry run to the apiserver, so it is a real write and records
// the object; a named endpoint's "dryRun" query means nothing to the trail.
func TestStageAudit957_Dr_DryRunStageRecordsNoObject(t *testing.T) {
	api, _ := apiserver957(t)
	ext, _ := apiserver957(t)
	env410(t, api.URL, ext.URL)
	rec := records410(t)

	resolve410(ctxWithUser(user410, group410), api.URL,
		withPayload957(stage410("probe", http.MethodPatch, base957+"/demo?dryRun=All")),
		withPayload957(stage410("create", http.MethodPost, base957+"?fieldManager=x&dryRun=All")),
		withPayload957(stage410("update", http.MethodPut, base957+"/demo?dryRun=All")),
		stage410("remove", http.MethodDelete, base957+"/demo?dryRun=All"),
		withPayload957(stage410("othercase", http.MethodPatch, base957+"/demo?DryRun=All")),
		withPayload957(extStage410("endpoint", http.MethodPatch, base957+"/demo?dryRun=All")))

	byStage := map[string]map[string]string{}
	for _, r := range attrs410(rec) {
		byStage[r["krateo.restaction.stage"]] = r
	}
	if len(byStage) != 6 {
		t.Fatalf("records = %d stages, want 6: %v", len(byStage), byStage)
	}
	for _, stage := range []string{"probe", "create", "update", "remove"} {
		r := byStage[stage]
		if r["outcome"] != "success" || r["k8s.resource.resource"] != "fireworksapps" {
			t.Fatalf("stage %s: precondition: record %v is not a successful apiserver write", stage, r)
		}
		if r["krateo.action"] != "restaction.dryRun" {
			t.Errorf("stage %s: krateo.action = %q, want restaction.dryRun", stage, r["krateo.action"])
		}
		wantObject957(t, r, nil)
	}
	if r := byStage["othercase"]; r["krateo.action"] != "restaction" {
		t.Errorf("othercase: krateo.action = %q, want restaction (the apiserver's key is case-sensitive)", r["krateo.action"])
	} else {
		wantObject957(t, r, map[string]string{
			"k8s.object.uid": "uid-resp-957", "k8s.object.generation": "5", "k8s.object.resource_version": "503",
		})
	}
	if r := byStage["endpoint"]; r["krateo.action"] != "restaction" {
		t.Errorf("endpoint: krateo.action = %q, want restaction (a named endpoint is not the apiserver)", r["krateo.action"])
	} else {
		wantObject957(t, r, nil)
	}
}

// apiServerDryRun decides as the apiserver does: the case-sensitive key
// "dryRun" present with at least one value.
func TestStageAudit957_APIServerDryRun(t *testing.T) {
	cases := map[string]bool{
		"":                          false,
		"dryRun=All":                true,
		"fieldManager=x&dryRun=All": true,
		"dryRun=All&dryRun=All":     true,
		"dryRun=":                   true, // a dry run to the apiserver (then refused 400)
		"dryRun":                    true,
		"dryRun=Bogus":              true, // ditto
		"DryRun=All":                false,
		"dryrun=All":                false,
		"fieldManager=x":            false,
		"%zz&dryRun=All":            true, // a pair that does not parse is skipped, not fatal
	}
	for q, want := range cases {
		if got := apiServerDryRun(q); got != want {
			t.Errorf("apiServerDryRun(%q) = %t, want %t", q, got, want)
		}
	}
}
