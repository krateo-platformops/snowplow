//go:build falsifier_268_267

// sa_serve_leak_falsifier_268_267_test.go — RED-first falsifier suite for the
// SA-serve identity-leak bundle:
//
//   - #268  branch-E SA-serve leak: a per-user /call whose api step leaves the
//           #256 branch-C re-gate (subresource GET / non-GET / RC1 fall-through /
//           unparseable) dials snowplow's OWN ServiceAccount endpoint UNGATED at
//           the external fetch, serving cross-user data. Masked in production only
//           by #267's stale token.
//   - #269  site-3 SA-serve leak: a per-user /call resolving a widget apiRef
//           TARGET the caller may not read fetches it under the SA *rest.Config
//           (objects.getFromAPIServer → cache.ClientConfigFor), credential A,
//           which is NEVER stale → leaks TODAY.
//   - #267  stale SA token: dynamic.ServiceAccountEndpoint reads the projected
//           token once and caches it (arm lives in the dynamic package).
//
// THE REAL BOUNDARY (feedback_falsifier_must_drive_real_boundary_not_install_
// crossed_state, feedback_seamed_dispatch_cannot_falsify_a_deep_frame): every
// leak arm here drives the REAL dispatcher ServeHTTP with the CROSSED STATE
// produced by the PRODUCTION attach (restactions.go:260-263 / widgets.go:271-273),
// NOT a hand-installed cache.WithInternalEndpoint / cache.WithInternalRESTConfig
// seam. The attach fires because the handler struct is constructed with non-nil
// saEP/saRC (the same fields RESTAction()/Widgets() populate from snowplowSACtx()
// in-cluster) — so Part 1 (deleting the attach) is exactly what flips each arm
// GREEN. The resolver is the REAL restactions.Resolve / widgets.Resolve
// (restactionsResolveFn / widgetsResolveFn are NOT swapped); only fetchObjectFn
// (the CR fetch, not the leak boundary) and checkDispatchRBACFn (the top-level CR
// dispatch gate) are faked, exactly as servehttp_orchestration_test.go does.
//
// THE VALID SA TOKEN (TL dispatch): in a hermetic test the SA credential is
// whatever the harness supplies, so a VALID token is provisioned (saEP.Token /
// saRC.BearerToken = a live value) → branch E authenticates → the arm goes RED.
// This mirrors the real "fresh pod" trigger #267 masks.
//
// PROOF DISCIPLINE (feedback_arm_that_cannot_fail_is_not_coverage): each leak arm
// records the credential presented ON THE WIRE to the fake apiserver and compares
// the served-body sha256 to the SA-read body. The no-attach CONTROL arm
// (TestLeak268_Control_NoAttach_NoSADial) proves the assertion CAN fail: with the
// attach absent the SA endpoint is never dialed and the secret never serves.
//
// Gate tag: falsifier_268_267. Run:
//   go test -tags falsifier_268_267 -run TestLeak26 -v ./internal/handlers/dispatchers/
package dispatchers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/objects"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// ── shared harness ──────────────────────────────────────────────────────────

// The single fresh, VALID SA bearer the harness provisions (the "fresh pod"
// trigger #267 masks in production). Every leak arm asserts this exact value on
// the wire — a per-user dial would present the caller's own token, never this.
const saLeakToken = "sa-fresh-valid-token-268"

// leakServer is a hermetic apiserver stand-in that (1) RECORDS the method, path
// and Authorization header of every request (the wire-credential proof) and
// (2) serves a fixed set of JSON/text objects keyed by path. Both the SA
// Endpoint (branch E, plumbing httpcall over plain HTTP) and the SA *rest.Config
// (branch C / objects.getFromAPIServer, client-go dynamic) reach it.
type leakServer struct {
	srv       *httptest.Server
	mu        sync.Mutex
	reqs      []wireReq
	objects   map[string]servedObj // path -> served body
	discovery bool                 // serve apiserver discovery docs (client-go dynamic RESTMapper needs them)
}

type wireReq struct {
	method string
	path   string
	auth   string
}

type servedObj struct {
	contentType string
	// value is the Go value served as JSON when json==true; when json==false,
	// text is served verbatim under contentType.
	json  bool
	value map[string]any
	text  string
}

func (l *leakServer) URL() string { return l.srv.URL }

// sawBearer reports whether ANY recorded request for a path with the given
// method carried exactly `Bearer <token>`.
func (l *leakServer) sawBearer(method, pathSuffix, token string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	want := "Bearer " + token
	for _, r := range l.reqs {
		if r.method == method && strings.HasSuffix(r.path, pathSuffix) && r.auth == want {
			return true
		}
	}
	return false
}

func (l *leakServer) wireDump() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var b strings.Builder
	for _, r := range l.reqs {
		fmt.Fprintf(&b, "\n  %s %s auth=%q", r.method, r.path, r.auth)
	}
	if b.Len() == 0 {
		return " (no requests received)"
	}
	return b.String()
}

func newLeakServer(t *testing.T, objs map[string]servedObj) *leakServer {
	t.Helper()
	l := &leakServer{objects: objs}
	l.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.mu.Lock()
		l.reqs = append(l.reqs, wireReq{method: r.Method, path: r.URL.Path, auth: r.Header.Get("Authorization")})
		l.mu.Unlock()
		if l.discovery {
			if doc, ok := discoveryDoc(r.URL.Path); ok {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, doc)
				return
			}
		}
		if o, ok := l.objects[r.URL.Path]; ok {
			if o.json {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(o.value)
				return
			}
			ct := o.contentType
			if ct == "" {
				ct = "text/plain"
			}
			w.Header().Set("Content-Type", ct)
			_, _ = io.WriteString(w, o.text)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`)
	}))
	t.Cleanup(l.srv.Close)
	return l
}

// newLeakServerWithDiscovery is newLeakServer plus canned apiserver discovery for
// the templates.krateo.io group, so the client-go dynamic RESTMapper (used by
// objects.getFromAPIServer on the #269 path) can resolve the restactions GVR.
func newLeakServerWithDiscovery(t *testing.T, objs map[string]servedObj) *leakServer {
	l := newLeakServer(t, objs)
	l.discovery = true
	return l
}

// discoveryDoc returns the canned discovery document for the paths client-go's
// memcache discovery client fetches while resolving a templates.krateo.io GVR.
func discoveryDoc(path string) (string, bool) {
	switch path {
	case "/api":
		return `{"kind":"APIVersions","versions":["v1"],"serverAddressByClientCIDRs":[]}`, true
	case "/api/v1":
		return `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[` +
			`{"name":"configmaps","singularName":"configmap","namespaced":true,"kind":"ConfigMap","verbs":["get","list"]},` +
			`{"name":"pods","singularName":"pod","namespaced":true,"kind":"Pod","verbs":["get","list"]}` +
			`]}`, true
	case "/apis":
		return `{"kind":"APIGroupList","apiVersion":"v1","groups":[` +
			`{"name":"templates.krateo.io","versions":[{"groupVersion":"templates.krateo.io/v1","version":"v1"}],` +
			`"preferredVersion":{"groupVersion":"templates.krateo.io/v1","version":"v1"}}` +
			`]}`, true
	case "/apis/templates.krateo.io/v1":
		return `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"templates.krateo.io/v1","resources":[` +
			`{"name":"restactions","singularName":"restaction","namespaced":true,"kind":"RestAction","verbs":["get","list"]}` +
			`]}`, true
	}
	return "", false
}

// saLeakRC is the SA *rest.Config the harness sets on the handler (the SAME field
// production populates via snowplowSARC in-cluster), pointed at the hermetic server
// and carrying a VALID token.
//
// PRE Part 1 these arms were RED: the dispatcher's attach put the SA endpoint +
// this rc on the per-user ctx (WithInternalEndpoint/WithInternalRESTConfig), which
// branch E and objects.getFromAPIServer dialed as the SA → the leak. POST Part 1
// there is NO attach — saRC rides ResolveOptions.SArc only (clientconfig-Secret
// read), never the ctx — so a no-endpointRef step dials as the user and the arms
// flip GREEN. They remain permanent regression guards: driving the REAL ServeHTTP,
// they go RED again if any change re-attaches the SA transport to the per-user ctx.
func saLeakRC(url string) *rest.Config {
	return &rest.Config{Host: url, BearerToken: saLeakToken}
}

// newRALeakHandler / newWidgetLeakHandler construct the REAL handler as production
// does in-cluster (saRC set from the SA rc). No attach exists post Part 1; the
// handler drives the REAL ServeHTTP so the arms observe the live dispatch path.
func newRALeakHandler(url string) *restActionHandler {
	return &restActionHandler{authnNS: "krateo-system", saRC: saLeakRC(url)}
}

func newWidgetLeakHandler(url string) *widgetsHandler {
	return &widgetsHandler{authnNS: "krateo-system", saRC: saLeakRC(url)}
}

// installLeakFetch swaps ONLY the CR-fetch and the top-level dispatch-RBAC gate
// (never the resolver) for the duration of a test. fetchObjectFn returns the
// given CR (as if it existed in the cluster); checkDispatchRBACFn ALLOWS the
// top-level dispatch (the RA/Widget CR the caller IS entitled to /call) — the
// leak is in the STEP the resolver then dispatches, not in this gate.
func installLeakFetch(t *testing.T, gvr schema.GroupVersionResource, cr *unstructured.Unstructured) {
	t.Helper()
	r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: gvr, Unstructured: cr}
	})
	r2 := setCheckDispatchRBACForTest(func(context.Context, schema.GroupVersionResource, string) bool { return true })
	t.Cleanup(func() { r2(); r1() })
}

var raLeakGVR = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}

var widgetLeakGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}

// raWithStage builds a RESTAction CR whose single api stage is `stage`.
// topFilter is the RESTAction-level jq projection (surfaces the stage output at
// status).
func raWithStage(ns, name string, stage map[string]any, topFilter string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"namespace": ns, "name": name},
		"spec": map[string]any{
			"api":    []any{stage},
			"filter": topFilter,
		},
	}}
}

// leakReqCtx builds a per-user request ctx carrying the caller identity (a real
// end-user — non-SA, non-empty UserInfo). withUserCfg additionally installs a
// per-user UserConfig endpoint (needed by objects.getFromAPIServer's
// xcontext.UserConfig read on the #269 path) — a plausible per-user endpoint the
// attach's SA rest.Config then OVERRIDES (ClientConfigFor), which IS the leak.
func leakReqCtx(user string, withUserCfg bool) context.Context {
	opts := []xcontext.WithContextFunc{
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: []string{"tenant-a"}}),
	}
	if withUserCfg {
		opts = append(opts, xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: "https://per-user.invalid", Token: "user-token"}))
	}
	return xcontext.BuildContext(context.Background(), opts...)
}

func leakSHA(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("canonical marshal: %v", err)
	}
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum)
}

// responseStatus decodes a dispatcher RESTAction/Widget response body and
// returns its status subtree (the resolved payload). Nil when absent.
func responseStatus(t *testing.T, body []byte) any {
	t.Helper()
	var env map[string]any
	if err := json.Unmarshal(body, &env); err != nil {
		return nil
	}
	return env["status"]
}

func bodyContains(body []byte, marker string) bool { return strings.Contains(string(body), marker) }

func sortedStrs(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ── PRIMARY DEMONSTRATOR (#268 case 2 — subresource GET, JSON round-trip) ─────

// TestLeak268_Primary_SubresourceStatus_ServedUnderSA — a user WITHOUT read on
// pods/status in ns1 issues a RESTAction whose step reads a pod's /status
// subresource with NO endpointRef, on a fresh-token pod. Gate 3 (subresource)
// excludes it from branch C → it falls through to the external fetch, which dials
// the SA Endpoint UNGATED.
//
// RED (a6b9d348): the /status body (secretField) is SERVED in the response, its
// sha256 equals the SA-read body, and the wire shows the SA bearer dialed the
// denied subresource. GREEN (Part 1): no attach → resolveOne(nil) → the per-user
// clientconfig path → the secret is not served under the SA identity.
func TestLeak268_Primary_SubresourceStatus_ServedUnderSA(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")

	const path = "/api/v1/namespaces/ns1/pods/p1/status"
	saBody := map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"namespace": "ns1", "name": "p1"},
		"status":   map[string]any{"phase": "Running", "secretField": "SA-ONLY-STATUS-268"},
	}
	srv := newLeakServer(t, map[string]servedObj{path: {json: true, value: saBody}})

	cr := raWithStage("ns1", "leak-ra-status",
		map[string]any{"name": "podstatus", "path": path, "verb": "GET", "filter": ".podstatus"},
		".podstatus")
	installLeakFetch(t, raLeakGVR, cr)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(leakReqCtx("denied-user", false))
	newRALeakHandler(srv.URL()).ServeHTTP(rec, req)

	served := responseStatus(t, rec.Body.Bytes())
	servedSHA := leakSHA(t, served)
	saSHA := leakSHA(t, saBody)
	wire := srv.sawBearer("GET", "/pods/p1/status", saLeakToken)
	// COND-4 (debug-surface-never-dumps-bodies): observe the leak via the served
	// sha256 + a boolean marker probe, NEVER by logging the raw cross-identity body.
	leaked := bodyContains(rec.Body.Bytes(), "SA-ONLY-STATUS-268")

	t.Logf("PRIMARY code=%d served_sha256=%s sa_body_sha256=%s wire_sa_bearer=%v secret_marker_in_body=%v", rec.Code, servedSHA, saSHA, wire, leaked)
	t.Logf("PRIMARY wire:%s", srv.wireDump())

	// FIXED EXPECTATION (asserted): a denied per-user subresource read must NOT be
	// served under the SA identity. RED pre-fix (a6b9d348): the attach routes the
	// subresource to branch E, which dials the SA endpoint ungated → this fires.
	// GREEN post-Part-1: no attach → per-user clientconfig → the secret is absent.
	if wire {
		t.Fatalf("LEAK (#268 branch-E subresource) — RED: the denied pods/status was dialed under the SA bearer and served cross-user.\n"+
			"  served status sha256 = %s\n  SA-read body sha256   = %s (EQUAL → the served body IS the SA body)\n  secret_marker_in_body=%v\n  wire:%s",
			servedSHA, saSHA, leaked, srv.wireDump())
	}
	if leaked {
		t.Fatalf("LEAK (#268 branch-E subresource) — RED: the SA-read subresource secret was served in the /call response (secret_marker_in_body=true, served_sha256=%s == sa_body_sha256=%s).", servedSHA, saSHA)
	}
	if servedSHA == saSHA {
		t.Fatalf("LEAK (#268 branch-E subresource) — RED: served status sha256 == SA-read body sha256 (%s) — the served body is the SA body.", servedSHA)
	}
	t.Logf("PRIMARY GREEN: the denied subresource was not served under the SA identity.")
}

// TestLeak268_Control_NoAttach_NoSADial — the can-fail CONTROL for the primary
// (feedback_arm_that_cannot_fail_is_not_coverage). IDENTICAL setup, but the
// handler is constructed WITHOUT the attach (saEP/saRC nil — the out-of-cluster /
// post-Part-1 shape). The SA endpoint is never dialed and the secret never
// serves. This arm is GREEN now and MUST STAY GREEN; it proves the primary's
// assertions can fail and pinpoints the attach as the sole cause.
func TestLeak268_Control_NoAttach_NoSADial(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")

	const path = "/api/v1/namespaces/ns1/pods/p1/status"
	saBody := map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"namespace": "ns1", "name": "p1"},
		"status":   map[string]any{"phase": "Running", "secretField": "SA-ONLY-STATUS-268"},
	}
	srv := newLeakServer(t, map[string]servedObj{path: {json: true, value: saBody}})

	cr := raWithStage("ns1", "leak-ra-status",
		map[string]any{"name": "podstatus", "path": path, "verb": "GET", "filter": ".podstatus"},
		".podstatus")
	installLeakFetch(t, raLeakGVR, cr)

	// No attach: the handler struct leaves saEP/saRC nil (post-Part-1 shape).
	h := &restActionHandler{authnNS: "krateo-system"}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(leakReqCtx("denied-user", false))
	h.ServeHTTP(rec, req)

	wire := srv.sawBearer("GET", "/pods/p1/status", saLeakToken)
	leaked := bodyContains(rec.Body.Bytes(), "SA-ONLY-STATUS-268")
	t.Logf("CONTROL code=%d wire_sa_bearer=%v secret_marker_in_body=%v", rec.Code, wire, leaked)
	if wire {
		t.Fatalf("CONTROL: with NO attach the SA endpoint must NOT be dialed; wire:%s", srv.wireDump())
	}
	if leaked {
		t.Fatalf("CONTROL: with NO attach the SA-read secret must NOT be served (secret_marker_in_body=true).")
	}
	t.Logf("CONTROL GREEN: no attach → no SA dial → no leak. The primary's assertions can fail; the attach is the cause.")
}

// ── ARM B (#268 subresource GET — pods/log, text body) ───────────────────────

// TestLeak268_ArmB_SubresourceLog_DialedUnderSA — the pods/log subresource. A pod
// log is served text/plain, which branch E's toJSONBytes rejects (it accepts only
// a JSON object/array), so the log TEXT cannot round-trip into the response the
// way a /status object does. The LEAK is nonetheless real at the WIRE: the denied
// pods/log path is dialed under the SA bearer. This arm asserts the SA dial (the
// leak at the boundary); the served-body sha256 parity is covered by the /status
// primary.
//
// FLAGGED DEVIATION FROM THE ARM SPEC: the TL/design primary names pods/log with
// "log body served, sha256 == SA-read body". Verbatim that is not reproducible —
// external_fetch.go toJSONBytes gates a 2xx body to a JSON object/array, so a
// text log surfaces as a StatusFailure, not served bytes. The /status subresource
// (also a #268 case-2 subresource) carries the sha256==SA-body parity; pods/log
// carries the wire-credential proof. Both are RED against a6b9d348.
func TestLeak268_ArmB_SubresourceLog_DialedUnderSA(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")

	const path = "/api/v1/namespaces/ns1/pods/p1/log"
	srv := newLeakServer(t, map[string]servedObj{path: {contentType: "text/plain", text: "SA-ONLY-LOG-268\n"}})

	cr := raWithStage("ns1", "leak-ra-log",
		map[string]any{"name": "podlog", "path": path, "verb": "GET", "filter": ".podlog"},
		".podlog")
	installLeakFetch(t, raLeakGVR, cr)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(leakReqCtx("denied-user", false))
	newRALeakHandler(srv.URL()).ServeHTTP(rec, req)

	wire := srv.sawBearer("GET", "/pods/p1/log", saLeakToken)
	t.Logf("ARM-B code=%d wire_sa_bearer=%v wire:%s", rec.Code, wire, srv.wireDump())
	// FIXED EXPECTATION: a denied per-user subresource read must not be dialed
	// under the SA credential. RED pre-fix: branch E dials the SA endpoint.
	if wire {
		t.Fatalf("LEAK (#268 branch-E subresource pods/log) — RED: the denied pods/log path was dialed under the SA bearer at branch E (ungated).\n  wire:%s", srv.wireDump())
	}
	t.Logf("ARM-B GREEN: the denied pods/log subresource was not dialed under the SA identity.")
}

// ── ARM C (#268 non-GET verb — the user's write attempted as the SA) ──────────

// TestLeak268_ArmC_NonGETVerb_AttemptedAsSA — a write step (POST) the user IS
// entitled to make is excluded from branch C by Gate 2 (verb!=GET) and falls
// through to the external fetch, which dials the SA Endpoint. This does not
// escalate a read (the SA holds no write verbs) but it means the caller's write
// is attempted under the WRONG identity (the SA), so a write the user is entitled
// to is refused / mis-attributed.
//
// RED (a6b9d348): the POST reaches the SA endpoint bearing the SA token (the
// dispatch host/credential is the SA endpoint, not the caller's). GREEN (Part 1):
// no attach → the write dials the per-user clientconfig.
func TestLeak268_ArmC_NonGETVerb_AttemptedAsSA(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")

	const path = "/apis/apps/v1/namespaces/ns1/deployments/d1"
	srv := newLeakServer(t, map[string]servedObj{path: {json: true, value: map[string]any{"ok": true}}})

	cr := raWithStage("ns1", "leak-ra-write",
		map[string]any{"name": "writestep", "path": path, "verb": "PUT", "payload": `{"spec":{"replicas":3}}`, "filter": ".writestep"},
		".writestep")
	installLeakFetch(t, raLeakGVR, cr)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(leakReqCtx("entitled-writer", false))
	newRALeakHandler(srv.URL()).ServeHTTP(rec, req)

	wire := srv.sawBearer("PUT", "/deployments/d1", saLeakToken)
	t.Logf("ARM-C code=%d wire_sa_bearer=%v wire:%s", rec.Code, wire, srv.wireDump())
	// FIXED EXPECTATION: the caller's write must NOT be attempted under the SA
	// identity. RED pre-fix: Gate 2 excludes the non-GET from branch C → branch E
	// dispatches it to the SA endpoint under the SA bearer.
	if wire {
		t.Fatalf("LEAK (#268 branch-E non-GET) — RED: the caller's PUT was dispatched to the SA endpoint under the SA bearer (wrong identity — the SA, not the caller).\n  wire:%s", srv.wireDump())
	}
	t.Logf("ARM-C GREEN: the non-GET step was not attempted under the SA identity.")
}
