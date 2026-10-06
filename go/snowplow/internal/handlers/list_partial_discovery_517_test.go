// list_partial_discovery_517_test.go — the #517 end-to-end falsifier.
//
// The CONSEQUENCE in the issue is an HTTP one ("/list answers 500"), so it is
// asserted over HTTP, through the real handlers.List() and the real
// client-go discovery stack, against a fake apiserver with ONE STALE
// AGGREGATED APISERVICE: /apis/stale.example.io/v1 answers 503, exactly as a
// stale aggregated API does. Nothing is stubbed between the handler and
// discovery — the arm drives the boundary, it does not install the end state.
//
// RED before the fix: discovery returns the healthy lists AND
// ErrGroupDiscoveryFailed; Discover discarded the lists, so List() answered
// 500 and the two healthy categories vanished.
//
// The second arm is the other direction: an apiserver whose /apis is
// unreachable (a transport failure, not a group failure) must STILL answer 500,
// never a 200 with an empty list.
package handlers_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/handlers"
)

const (
	cat517 = "krateo517"
	ns517  = "demo517"
)

// fakeAPI517 is a fake apiserver whose aggregated group stale.example.io/v1 is
// STALE: its discovery endpoint answers 503. Every other group is healthy.
type fakeAPI517 struct {
	mu   sync.Mutex
	hits []string
	// staleGroup, when false, serves stale.example.io/v1 normally (used to pin
	// that the arm's RED really comes from the stale group).
	staleGroup bool
	// breakAPIs makes /apis itself fail with a non-group transport-class error.
	breakAPIs bool
}

func (f *fakeAPI517) record(p string) {
	f.mu.Lock()
	f.hits = append(f.hits, p)
	f.mu.Unlock()
}

func (f *fakeAPI517) handler(w http.ResponseWriter, r *http.Request) {
	f.record(r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	write := func(body string) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}

	switch r.URL.Path {
	case "/api":
		write(`{"kind":"APIVersions","versions":["v1"],"serverAddressByClientCIDRs":[]}`)

	case "/api/v1":
		write(`{"kind":"APIResourceList","groupVersion":"v1","resources":[
			{"name":"configmaps","singularName":"configmap","namespaced":true,"kind":"ConfigMap",
			 "verbs":["get","list"],"categories":["` + cat517 + `"]}]}`)

	case "/apis":
		if f.breakAPIs {
			// Not a group-discovery failure: /apis itself is broken, so NOTHING
			// is discovered. client-go surfaces this as a plain error.
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`<html>bad gateway</html>`))
			return
		}
		write(`{"kind":"APIGroupList","groups":[
			{"name":"good.example.io",
			 "versions":[{"groupVersion":"good.example.io/v1","version":"v1"}],
			 "preferredVersion":{"groupVersion":"good.example.io/v1","version":"v1"}},
			{"name":"stale.example.io",
			 "versions":[{"groupVersion":"stale.example.io/v1","version":"v1"}],
			 "preferredVersion":{"groupVersion":"stale.example.io/v1","version":"v1"}}]}`)

	case "/apis/good.example.io/v1":
		write(`{"kind":"APIResourceList","groupVersion":"good.example.io/v1","resources":[
			{"name":"goodthings","singularName":"goodthing","namespaced":true,"kind":"GoodThing",
			 "verbs":["get","list"],"categories":["` + cat517 + `"]}]}`)

	case "/apis/stale.example.io/v1":
		if f.staleGroup {
			// THE DEFECT TRIGGER: a stale aggregated APIService. This is what
			// the apiserver answers when the backing service is gone.
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure",` +
				`"message":"the server is currently unable to handle the request","code":503}`))
			return
		}
		write(`{"kind":"APIResourceList","groupVersion":"stale.example.io/v1","resources":[
			{"name":"stalethings","singularName":"stalething","namespaced":true,"kind":"StaleThing",
			 "verbs":["get","list"],"categories":["` + cat517 + `"]}]}`)

	case "/api/v1/namespaces/" + ns517 + "/configmaps":
		write(`{"apiVersion":"v1","kind":"ConfigMapList","metadata":{"resourceVersion":"1"},
			"items":[{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-517","namespace":"` + ns517 + `"}}]}`)

	case "/apis/good.example.io/v1/namespaces/" + ns517 + "/goodthings":
		write(`{"apiVersion":"good.example.io/v1","kind":"GoodThingList","metadata":{"resourceVersion":"1"},
			"items":[{"apiVersion":"good.example.io/v1","kind":"GoodThing","metadata":{"name":"gt-517","namespace":"` + ns517 + `"}}]}`)

	case "/apis/stale.example.io/v1/namespaces/" + ns517 + "/stalethings":
		write(`{"apiVersion":"stale.example.io/v1","kind":"StaleThingList","metadata":{"resourceVersion":"1"},
			"items":[{"apiVersion":"stale.example.io/v1","kind":"StaleThing","metadata":{"name":"st-517","namespace":"` + ns517 + `"}}]}`)

	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"kind":"Status","status":"Failure","code":404}`))
	}
}

// serve517 drives the real GET /list handler against the fake apiserver and
// returns the recorder plus everything the handler logged.
func serve517(t *testing.T, f *fakeAPI517) (*httptest.ResponseRecorder, string) {
	t.Helper()

	prev := xenv.TestMode()
	xenv.SetTestMode(true) // keep the injected ServerURL (plumbing rewrites it otherwise)
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(func() {
		srv.Close()
		xenv.SetTestMode(prev)
	})

	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	req := httptest.NewRequest("GET", "/list?category="+cat517+"&ns="+ns517, nil)
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithLogger(log),
		xcontext.WithAccessToken("test-token-517"),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL}),
	))

	rec := httptest.NewRecorder()
	handlers.List().ServeHTTP(rec, req)
	return rec, logs.String()
}

// names517 reads the object names out of /list's JSON array body.
func names517(t *testing.T, body string) []string {
	t.Helper()
	var items []struct {
		Metadata struct{ Name string } `json:"metadata"`
	}
	if err := json.Unmarshal([]byte(body), &items); err != nil {
		t.Fatalf("#517: /list body is not a JSON array (%v): %s", err, body)
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.Metadata.Name)
	}
	return out
}

// TestIssue517_List_OneStaleAPIService_DoesNotBreakEveryCategory is the
// headline arm. One stale aggregated APIService must not take /list down.
func TestIssue517_List_OneStaleAPIService_DoesNotBreakEveryCategory(t *testing.T) {
	rec, logs := serve517(t, &fakeAPI517{staleGroup: true})

	if rec.Code != http.StatusOK {
		t.Fatalf("#517 RED: GET /list answered %d (want 200) with ONE stale aggregated APIService — "+
			"the healthy groups' resources were discarded, so every category broke. Body: %s",
			rec.Code, rec.Body.String())
	}

	got := names517(t, rec.Body.String())
	want := map[string]bool{"cm-517": false, "gt-517": false}
	for _, n := range got {
		if _, ok := want[n]; ok {
			want[n] = true
		}
	}
	for n, seen := range want {
		if !seen {
			t.Errorf("#517: /list omitted %q — a HEALTHY group's objects must still be served when a "+
				"sibling group's discovery fails; got %v", n, got)
		}
	}

	// The degradation must be REPORTED, not left as a silently shorter list.
	if !strings.Contains(logs, "DEGRADED") {
		t.Errorf("#517: nothing in the handler's logs reports the degradation — a 200 that looks "+
			"complete while silently dropping kinds is not an acceptable degraded mode. Logs:\n%s", logs)
	}
	if !strings.Contains(logs, "stale.example.io/v1") {
		t.Errorf("#517: the logs do not NAME the failed group/version, which is the datum that "+
			"identifies the stale APIService. Logs:\n%s", logs)
	}
	if !strings.Contains(logs, `"level":"WARN"`) {
		t.Errorf("#517: the degradation was not logged at WARN — some group/versions genuinely "+
			"failed. Logs:\n%s", logs)
	}
}

// TestIssue517_List_AllGroupsHealthy_ServesEveryGroup is the must-still-work
// arm: with no stale group, /list serves ALL THREE categories and reports no
// degradation. Without it, "200 with the healthy groups" could be satisfied by
// a handler that always drops a group.
func TestIssue517_List_AllGroupsHealthy_ServesEveryGroup(t *testing.T) {
	rec, logs := serve517(t, &fakeAPI517{staleGroup: false})

	if rec.Code != http.StatusOK {
		t.Fatalf("#517: GET /list answered %d (want 200) with a fully healthy cluster. Body: %s",
			rec.Code, rec.Body.String())
	}
	got := names517(t, rec.Body.String())
	for _, n := range []string{"cm-517", "gt-517", "st-517"} {
		found := false
		for _, g := range got {
			if g == n {
				found = true
			}
		}
		if !found {
			t.Errorf("#517: /list omitted %q on a HEALTHY cluster; got %v", n, got)
		}
	}
	if strings.Contains(logs, "DEGRADED") {
		t.Errorf("#517: a healthy discovery reported a degradation — a detector that fires when "+
			"nothing is wrong is not a detector. Logs:\n%s", logs)
	}
}

// TestIssue517_List_TransportFailure_Still500 is the other direction. /apis
// itself fails, so NOTHING was discovered: answering 200 with an empty array
// would be the same under-report the fix exists to avoid.
func TestIssue517_List_TransportFailure_Still500(t *testing.T) {
	rec, logs := serve517(t, &fakeAPI517{breakAPIs: true})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("#517: GET /list answered %d with discovery wholly broken; want 500 — a genuine "+
			"failure must NOT be laundered into a 200 with an empty list. Body: %s",
			rec.Code, rec.Body.String())
	}
	if strings.Contains(logs, "DEGRADED") {
		t.Errorf("#517: a total discovery failure was reported as a DEGRADED partial. Logs:\n%s", logs)
	}
}
