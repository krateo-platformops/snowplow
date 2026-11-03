package handlers

// export_audit_1055_test.go — #1055: the export's audit record names the
// method the export was SENT with and the target it resolved, through the
// helper every audited request builds its record with (requestAuditEvent).
//
//   M  POST /export records http.request.method=POST, GET /export records GET,
//      both through the real route compositions (ExportPOST, Export(Call())).
//   T  both carry k8s.resource.group and k8s.resource.version.
//   P  parity: the export record's attribute set is the /call write record's
//      minus the call-specific fields, for the same target; both sets are
//      derived from requestAuditEvent, never from a list in this file.
//   X  every exit of the export that audits still does, for both methods,
//      and each record carries the method and the target.

import (
	"context"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"go.opentelemetry.io/otel/log/logtest"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// A non-core group, so k8s.resource.group is a real value (the core group
// is "" and its attribute is omitted by design).
const (
	group1055   = "composition.krateo.io"
	version1055 = "v1alpha1"
	target1055  = "apiVersion=" + group1055 + "/" + version1055 + "&resource=fireworksapps&namespace=team-a&name=demo"
	// One body serves both requests of the parity arm: a list for the export
	// (items) that is also an object for the write (metadata), so the /call
	// record carries its written-object identity.
	body1055 = `{"apiVersion":"` + group1055 + `/` + version1055 + `","kind":"FireworksApp",` +
		`"metadata":{"name":"demo","namespace":"team-a","uid":"uid-1055","generation":2,"resourceVersion":"77"},` +
		`"items":[{"id":"a"},{"id":"b"}]}`
)

// helperRecord emits ev the way the handlers do (the same caller on ctx as
// withCaller443 attaches) and returns its attributes.
func helperRecord(t *testing.T, ev audit.Event) map[string]string {
	t.Helper()
	rec := logtest.NewRecorder()
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}}))
	audit.New(rec.Logger("helper-1055")).Emit(ctx, ev)
	got := records957(rec)
	if len(got) != 1 {
		t.Fatalf("the helper's record emitted %d records, want 1", len(got))
	}
	return got[0]
}

func keySet(m map[string]string) []string { return slices.Sorted(maps.Keys(m)) }

func TestExport1055_M_T_MethodIsTheRequestsAndTheTargetHasGroupAndVersion(t *testing.T) {
	for _, c := range []struct {
		method string
		h      func() http.Handler
		body   string
	}{
		{http.MethodGet, func() http.Handler { return Export(Call()) }, ""},
		{http.MethodPost, ExportPOST, `{"extras":{}}`},
	} {
		t.Run(c.method, func(t *testing.T) {
			api, ep := newFakeAPI443(t)
			api.respond(http.StatusOK, body1055)
			rec := recorder957(t)

			w := serve443(c.h(), ep, c.method, "/export?format=json&"+target1055, c.body)
			if w.Code != http.StatusOK {
				t.Fatalf("%s /export → %d: %s", c.method, w.Code, w.Body.String())
			}
			if reqs := api.requests(); len(reqs) == 0 || reqs[0].method != http.MethodGet {
				t.Fatalf("precondition: the apiserver got %+v, want the export's GET pages", reqs)
			}
			got := one957(t, rec)
			want := map[string]string{
				"krateo.action":         "export",
				"http.request.method":   c.method,
				"k8s.resource.group":    group1055,
				"k8s.resource.version":  version1055,
				"k8s.resource.resource": "fireworksapps",
				"k8s.resource.name":     "demo",
				"k8s.namespace.name":    "team-a",
				"outcome":               "success",
			}
			for k, v := range want {
				if got[k] != v {
					t.Errorf("%s = %q, want %q (record %v)", k, got[k], v, got)
				}
			}
			if m, ok := got["krateo.audit.incomplete"]; ok {
				t.Errorf("the export record is marked incomplete=%q", m)
			}
		})
	}
}

func TestExport1055_P_ExportRecordIsTheCallRecordMinusTheCallsOwnFields(t *testing.T) {
	const sub = "status"
	q := target1055 + "&subresource=" + sub
	target := auditTarget{
		gvr:         schema.GroupVersionResource{Group: group1055, Version: version1055, Resource: "fireworksapps"},
		subresource: sub,
		nsn:         types.NamespacedName{Namespace: "team-a", Name: "demo"},
	}

	// The /call write record (PUT: a by-name verb, so the subresource rides it).
	api, ep := newFakeAPI443(t)
	api.respond(http.StatusOK, body1055)
	rec := recorder957(t)
	if w := serve443(Call(), ep, http.MethodPut, "/call?"+q, body1055); w.Code != http.StatusOK {
		t.Fatalf("PUT /call → %d: %s", w.Code, w.Body.String())
	}
	call := one957(t, rec)

	// The export record, same target.
	rec = recorder957(t)
	if w := serve443(Export(Call()), ep, http.MethodGet, "/export?format=json&"+q, ""); w.Code != http.StatusOK {
		t.Fatalf("GET /export → %d: %s", w.Code, w.Body.String())
	}
	export := one957(t, rec)

	// H: what the helper alone records for this target. The export adds no
	// attribute of its own, so its record is exactly H.
	h := helperRecord(t, requestAuditEvent("export", http.MethodGet, target, "success", http.StatusOK))
	if got, want := keySet(export), keySet(h); !slices.Equal(got, want) {
		t.Errorf("export attributes %v, want the helper's %v", got, want)
	}

	// The call-specific fields are what the /call record carries beyond the
	// helper's set. The /call record must carry all of H, and the export's
	// set must be the /call's minus those fields.
	var callOwn []string
	for _, k := range keySet(call) {
		if _, ok := h[k]; !ok {
			callOwn = append(callOwn, k)
		}
	}
	for _, k := range keySet(h) {
		if _, ok := call[k]; !ok {
			t.Errorf("the /call record lacks %s, which the helper records (call %v)", k, call)
		}
	}
	if len(callOwn) == 0 {
		t.Fatalf("precondition: the /call write recorded nothing of its own (%v): the written object (#957) is missing, so the arm cannot tell its fields from the helper's", call)
	}
	var callMinusOwn []string
	for _, k := range keySet(call) {
		if !slices.Contains(callOwn, k) {
			callMinusOwn = append(callMinusOwn, k)
		}
	}
	if got := keySet(export); !slices.Equal(got, callMinusOwn) {
		t.Errorf("export attributes %v, want the /call record's minus its own fields %v = %v", got, callOwn, callMinusOwn)
	}

	// The target attributes are the keys the target contributes to the
	// helper's record; both records give them the same values.
	zero := helperRecord(t, requestAuditEvent("export", http.MethodGet, auditTarget{}, "success", http.StatusOK))
	var targetKeys []string
	for _, k := range keySet(h) {
		if _, ok := zero[k]; !ok {
			targetKeys = append(targetKeys, k)
		}
	}
	for _, k := range []string{"k8s.resource.group", "k8s.resource.version", "k8s.resource.subresource"} {
		if !slices.Contains(targetKeys, k) {
			t.Fatalf("precondition: the helper's target attributes %v lack %s", targetKeys, k)
		}
	}
	for _, k := range targetKeys {
		if export[k] != call[k] {
			t.Errorf("%s: export %q, /call %q — the same target named two ways", k, export[k], call[k])
		}
	}
	if call["http.request.method"] != http.MethodPut || export["http.request.method"] != http.MethodGet {
		t.Errorf("methods: /call %q export %q, want each request's own (PUT, GET)", call["http.request.method"], export["http.request.method"])
	}
	t.Logf("call-specific fields: %v; target attributes: %v", callOwn, targetKeys)
}

// TestExport1055_X_EveryAuditedExitStillAudits drives each exit of the export
// that audits, with each method, and wants one record per export carrying
// the method and the target.
func TestExport1055_X_EveryAuditedExitStillAudits(t *testing.T) {
	const fullTarget = "/export?" + target1055 + "&"
	type exit struct {
		name    string
		query   string
		inner   func(t *testing.T) http.Handler
		writer  func() http.ResponseWriter
		setup   func(t *testing.T)
		outcome string
		code    string
	}
	small := func(*testing.T) http.Handler { var q []url.Values; return recordingCall(itemsEnvelope(3), &q) }
	paged := func(t *testing.T) http.Handler { var c [][2]int; return pagedCall(t, 100, &c) }
	recorder := func() http.ResponseWriter { return httptest.NewRecorder() }
	exits := []exit{
		{"csv", "format=csv", small, recorder, nil, "success", "200"},
		{"json", "format=json", small, recorder, nil, "success", "200"},
		{"pdf", "format=pdf", small, recorder, nil, "success", "200"},
		{"csv truncated", "format=csv", paged, recorder, func(t *testing.T) { setExportPaging(t, 3, 2) }, "success", "200"},
		{"pdf at the bound", "format=pdf", paged, recorder, func(t *testing.T) { setExportPaging(t, 3, 2) }, "failure", "422"},
		{"pdf render failure", "format=pdf&fields=id", small, recorder, failRender, "failure", "500"},
		{"pdf send failure", "format=pdf", small, func() http.ResponseWriter { return &failingWriter{header: http.Header{}} }, nil, "failure", "200"},
	}
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		for _, e := range exits {
			t.Run(method+" "+e.name, func(t *testing.T) {
				if e.setup != nil {
					e.setup(t)
				}
				rec := recorder957(t)
				req := httptest.NewRequest(method, fullTarget+e.query, nil)
				req = req.WithContext(xcontext.BuildContext(req.Context(),
					xcontext.WithLogger(slog.New(slog.DiscardHandler)),
					xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice"})))
				Export(e.inner(t)).ServeHTTP(e.writer(), req)

				got := records957(rec)
				if len(got) != 1 {
					t.Fatalf("records = %d, want 1: %v", len(got), got)
				}
				r := got[0]
				for k, v := range map[string]string{
					"http.request.method":       method,
					"k8s.resource.group":        group1055,
					"k8s.resource.version":      version1055,
					"k8s.resource.resource":     "fireworksapps",
					"outcome":                   e.outcome,
					"http.response.status_code": e.code,
				} {
					if r[k] != v {
						t.Errorf("%s = %q, want %q (record %v)", k, r[k], v, r)
					}
				}
				if m, ok := r["krateo.audit.incomplete"]; ok {
					t.Errorf("record marked incomplete=%q", m)
				}
			})
		}
	}
}
