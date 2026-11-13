//go:build js && wasm

package fakeapiserver

// The fake apiserver against testdata/ (the agentiko-sandbox test fixture), checked against
// kube-apiserver's documented behaviour; each test names it. These were the JS fake env's 16 tests
// (agentiko-sandbox snowplow-wasm/test/fake-apiserver.test.mjs), which this package replaces.
// Run: scripts/check_js_wasm.sh (GOOS=js GOARCH=wasm go test with go_js_wasm_exec).

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
)

const (
	incident = "sre-volume-attach-failure-20261012-140005"
	inc      = "/apis/observability.krateo.io/v1alpha1/namespaces/krateo-system/incidents"
	merge    = "application/merge-patch+json"
)

type fakeTimer struct {
	at   time.Time
	f    func()
	done bool
}

type harness struct {
	t      *testing.T
	s      *Server
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	logs   []map[string]any
}

type opts struct {
	edit     func(snap map[string]any)
	pageEdit func(objects []map[string]any)
	extra    []map[string]any
}

func readFixture(t *testing.T, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", rel))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newHarness(t *testing.T, o opts) *harness {
	t.Helper()
	h := &harness{t: t, now: time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)}
	snap := readFixture(t, "snapshot.json")
	if o.edit != nil {
		var m map[string]any
		if err := json.Unmarshal(snap, &m); err != nil {
			t.Fatal(err)
		}
		o.edit(m)
		snap, _ = json.Marshal(m)
	}
	page0 := readFixture(t, "objects/000.json")
	if o.pageEdit != nil {
		var pg struct {
			Items []map[string]any `json:"items"`
		}
		if err := json.Unmarshal(page0, &pg); err != nil {
			t.Fatal(err)
		}
		o.pageEdit(pg.Items)
		page0, _ = json.Marshal(pg)
	}
	pages := [][]byte{page0}
	if len(o.extra) > 0 {
		b, _ := json.Marshal(map[string]any{"items": o.extra})
		pages = append(pages, b)
	}
	n := 0
	s, err := New(Options{
		Snapshot: snap, Pages: pages, SAToken: "sa-token", VisitorToken: "visitor-token",
		Clock: func() time.Time { h.mu.Lock(); defer h.mu.Unlock(); return h.now },
		AfterFunc: func(d time.Duration, f func()) func() bool {
			h.mu.Lock()
			defer h.mu.Unlock()
			ft := &fakeTimer{at: h.now.Add(d), f: f}
			h.timers = append(h.timers, ft)
			return func() bool { h.mu.Lock(); defer h.mu.Unlock(); was := !ft.done; ft.done = true; return was }
		},
		Log:    func(l map[string]any) { h.mu.Lock(); defer h.mu.Unlock(); h.logs = append(h.logs, l) },
		NewUID: func() string { n++; return fmt.Sprintf("uid-%05d", n) },
	})
	if err != nil {
		t.Fatal(err)
	}
	h.s = s
	return h
}

// advance moves the clock, running due timers in order (outside the harness lock).
func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	until := h.now.Add(d)
	h.mu.Unlock()
	for {
		h.mu.Lock()
		var due *fakeTimer
		for _, ft := range h.timers {
			if !ft.done && !ft.at.After(until) && (due == nil || ft.at.Before(due.at)) {
				due = ft
			}
		}
		if due == nil {
			h.now = until
			h.mu.Unlock()
			return
		}
		h.now, due.done = due.at, true
		h.mu.Unlock()
		due.f()
	}
}

func (h *harness) log() []map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]map[string]any(nil), h.logs...)
}

type req struct {
	as      string // sa | visitor | "" (no token) | anything else (a bad token)
	body    any
	ct      string
	accept  string
	headers map[string]string
}

func (h *harness) request(method, path string, r req) *http.Request {
	var body io.Reader
	switch b := r.body.(type) {
	case nil:
	case []byte:
		body = bytes.NewReader(b)
	case string:
		body = strings.NewReader(b)
	default:
		j, _ := json.Marshal(b)
		body = bytes.NewReader(j)
	}
	hr := httptest.NewRequest(method, path, body)
	if r.as != "" {
		hr.Header.Set("Authorization", "Bearer "+r.as+"-token")
	}
	if r.ct != "" {
		hr.Header.Set("Content-Type", r.ct)
	}
	if r.accept != "" {
		hr.Header.Set("Accept", r.accept)
	}
	for k, v := range r.headers {
		hr.Header.Add(k, v)
	}
	return hr
}

type resp struct {
	code int
	body map[string]any
	ct   string
	hdr  http.Header
}

func (h *harness) do(method, path string, r req) resp {
	h.t.Helper()
	rec := httptest.NewRecorder()
	h.s.ServeHTTP(rec, h.request(method, path, r))
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil && rec.Body.Len() > 0 {
		h.t.Fatalf("%s %s: not JSON: %q", method, path, rec.Body.String())
	}
	return resp{rec.Code, m, rec.Header().Get("Content-Type"), rec.Header()}
}

// stream is a watch: events arrive as the server writes them.
type stream struct {
	t      *testing.T
	code   chan int
	lines  chan map[string]any
	cancel context.CancelFunc
}

type streamWriter struct {
	h    http.Header
	code chan int
	pw   *io.PipeWriter
	once sync.Once
}

func (w *streamWriter) Header() http.Header         { return w.h }
func (w *streamWriter) WriteHeader(c int)           { w.once.Do(func() { w.code <- c }) }
func (w *streamWriter) Write(b []byte) (int, error) { w.WriteHeader(200); return w.pw.Write(b) }
func (w *streamWriter) Flush()                      {}

func (h *harness) watch(path string, r req) *stream {
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	st := &stream{t: h.t, code: make(chan int, 1), lines: make(chan map[string]any, 4096), cancel: cancel}
	w := &streamWriter{h: http.Header{}, code: st.code, pw: pw}
	go func() {
		h.s.ServeHTTP(w, h.request(http.MethodGet, path, r).WithContext(ctx))
		w.WriteHeader(200)
		pw.Close()
	}()
	go func() {
		sc := bufio.NewScanner(pr)
		sc.Buffer(make([]byte, 1<<20), 1<<24)
		for sc.Scan() {
			var m map[string]any
			_ = json.Unmarshal(sc.Bytes(), &m)
			st.lines <- m
		}
		close(st.lines)
	}()
	return st
}

// next is the next event, or nil when the stream ended.
func (st *stream) next() map[string]any {
	st.t.Helper()
	select {
	case ev, ok := <-st.lines:
		if !ok {
			return nil
		}
		return ev
	case <-time.After(10 * time.Second):
		st.t.Fatal("no watch event within 10s")
		return nil
	}
}

func (st *stream) quiet() {
	st.t.Helper()
	select {
	case ev, ok := <-st.lines:
		if ok {
			st.t.Fatalf("unexpected event %v", ev)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

func get(m any, path ...string) any {
	for _, p := range path {
		mm, _ := m.(map[string]any)
		m = mm[p]
	}
	return m
}

func eq(t *testing.T, got, want any, what string) {
	t.Helper()
	g, _ := json.Marshal(got)
	w, _ := json.Marshal(want)
	if !bytes.Equal(g, w) {
		t.Fatalf("%s: got %s, want %s", what, g, w)
	}
}

func items(r resp) []any { l, _ := r.body["items"].([]any); return l }

// ------------------------------------------------------------------ tests

func TestDiscovery(t *testing.T) {
	h := newHarness(t, opts{})
	eq(t, h.do("GET", "/api", req{as: "visitor"}).body["kind"], "APIVersions", "/api")
	groups, _ := h.do("GET", "/apis", req{as: "visitor"}).body["groups"].([]any)
	found := false
	for _, g := range groups {
		found = found || get(g, "name") == "observability.krateo.io"
	}
	if !found {
		t.Fatal("/apis has no observability.krateo.io")
	}
	rl := h.do("GET", "/apis/observability.krateo.io/v1alpha1", req{as: "visitor"})
	if !strings.Contains(fmt.Sprint(rl.body["resources"]), "incidents/status") {
		t.Fatalf("no incidents/status: %v", rl.body)
	}
	eq(t, h.do("GET", "/version", req{as: "visitor"}).body["gitVersion"], "v1.36.4", "/version")
	eq(t, h.do("GET", "/apis/nope.example/v1", req{as: "visitor"}).code, 404, "an unknown group")
}

func TestAuthentication401(t *testing.T) {
	h := newHarness(t, opts{})
	r := h.do("GET", inc, req{as: "nope"})
	eq(t, r.code, 401, "code")
	eq(t, r.body, map[string]any{"kind": "Status", "apiVersion": "v1", "metadata": map[string]any{}, "status": "Failure", "message": "Unauthorized", "reason": "Unauthorized", "code": 401}, "body")
}

func TestGetAndNotFound(t *testing.T) {
	h := newHarness(t, opts{})
	ok := h.do("GET", inc+"/"+incident, req{as: "visitor"})
	eq(t, ok.code, 200, "code")
	eq(t, get(ok.body, "metadata", "name"), incident, "name")
	nf := h.do("GET", inc+"/missing", req{as: "visitor"})
	eq(t, nf.body, map[string]any{"kind": "Status", "apiVersion": "v1", "metadata": map[string]any{}, "status": "Failure",
		"message": `incidents.observability.krateo.io "missing" not found`, "reason": "NotFound",
		"details": map[string]any{"name": "missing", "group": "observability.krateo.io", "kind": "incidents"}, "code": 404}, "404")
}

func TestRBACDenials(t *testing.T) {
	h := newHarness(t, opts{})
	sec := h.do("GET", "/api/v1/namespaces/krateo-system/secrets/sandbox-visitor-clientconfig", req{as: "visitor"})
	eq(t, sec.body, map[string]any{"kind": "Status", "apiVersion": "v1", "metadata": map[string]any{}, "status": "Failure",
		"message": `secrets "sandbox-visitor-clientconfig" is forbidden: User "sandbox-visitor" cannot get resource "secrets" in API group "" in the namespace "krateo-system"`,
		"reason":  "Forbidden", "details": map[string]any{"name": "sandbox-visitor-clientconfig", "kind": "secrets"}, "code": 403}, "visitor Secret")
	st := h.do("PATCH", inc+"/"+incident+"/status", req{as: "visitor", ct: merge, body: map[string]any{"status": map[string]any{"state": "Resolved"}}})
	eq(t, st.code, 403, "status patch")
	if !strings.Contains(fmt.Sprint(st.body["message"]), `cannot patch resource "incidents/status" in API group "observability.krateo.io" in the namespace "krateo-system"`) {
		t.Fatal(st.body["message"])
	}
	anon := h.do("GET", "/api/v1/namespaces", req{})
	eq(t, anon.code, 403, "anonymous")
	if !strings.Contains(fmt.Sprint(anon.body["message"]), `User "system:anonymous" cannot list resource "namespaces" in API group "" at the cluster scope`) {
		t.Fatal(anon.body["message"])
	}
	// The ServiceAccount reads the synthesised clientconfig; it is the only Secret.
	eq(t, h.do("GET", "/api/v1/namespaces/krateo-system/secrets/sandbox-visitor-clientconfig", req{as: "sa"}).code, 200, "sa get")
	eq(t, len(items(h.do("GET", "/api/v1/namespaces/krateo-system/secrets", req{as: "sa"}))), 1, "sa list")
	// Authorization precedes routing: an unknown resource the identity may not read is 403, not 404.
	eq(t, h.do("GET", "/apis/observability.krateo.io/v1alpha1/namespaces/krateo-system/nothings", req{}).code, 403, "403 before 404")
}

func TestListPagination(t *testing.T) {
	h := newHarness(t, opts{})
	const crs = "/apis/rbac.authorization.k8s.io/v1/clusterroles"
	all := h.do("GET", crs+"?resourceVersion=0&limit=1", req{as: "sa"})
	if len(items(all)) < 2 || get(all.body, "metadata", "continue") != nil {
		t.Fatalf("resourceVersion=0 must ignore limit: %v", all.body["metadata"])
	}
	p1 := h.do("GET", crs+"?limit=1", req{as: "sa"})
	eq(t, len(items(p1)), 1, "page 1")
	cont, _ := get(p1.body, "metadata", "continue").(string)
	if cont == "" {
		t.Fatal("no continue")
	}
	eq(t, get(p1.body, "metadata", "remainingItemCount"), float64(len(items(all))-1), "remainingItemCount")
	p2 := h.do("GET", crs+"?limit=1&continue="+cont, req{as: "sa"})
	eq(t, len(items(p2)), 1, "page 2")
	if get(items(p2)[0], "metadata", "name") == get(items(p1)[0], "metadata", "name") {
		t.Fatal("page 2 repeats page 1")
	}
	eq(t, get(p2.body, "metadata", "resourceVersion"), get(p1.body, "metadata", "resourceVersion"), "a consistent RV")
	// Built-in list items carry no apiVersion/kind; a custom resource's do.
	eq(t, get(items(p1)[0], "kind"), nil, "built-in item kind")
	eq(t, get(items(h.do("GET", inc, req{as: "visitor"}))[0], "kind"), "Incident", "CR item kind")
	h.advance(6 * time.Minute)
	eq(t, h.do("GET", crs+"?limit=1&continue="+cont, req{as: "sa"}).code, 410, "an expired continue")
}

func TestFieldSelector(t *testing.T) {
	h := newHarness(t, opts{})
	eq(t, len(items(h.do("GET", inc+"?fieldSelector=metadata.name%3D"+incident, req{as: "visitor"}))), 1, "by name")
	bad := h.do("GET", inc+"?fieldSelector=spec.applied%3Dtrue", req{as: "visitor"})
	eq(t, bad.code, 400, "code")
	eq(t, bad.body["reason"], "BadRequest", "reason")
	if !strings.Contains(fmt.Sprint(bad.body["message"]), "field label not supported: spec.applied") {
		t.Fatal(bad.body["message"])
	}
}

func TestPartialObjectMetadata(t *testing.T) {
	h := newHarness(t, opts{})
	r := h.do("GET", inc, req{as: "sa", accept: "application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1,application/json"})
	eq(t, r.body["kind"], "PartialObjectMetadataList", "kind")
	eq(t, get(items(r)[0], "kind"), "PartialObjectMetadata", "item kind")
	eq(t, get(items(r)[0], "spec"), nil, "no spec")
	if !strings.Contains(r.ct, "as=PartialObjectMetadataList") {
		t.Fatal(r.ct)
	}
	eq(t, h.do("GET", inc, req{as: "visitor", accept: "application/vnd.kubernetes.protobuf"}).code, 406, "protobuf only")
}

func TestWatchList(t *testing.T) {
	h := newHarness(t, opts{})
	w := h.watch(inc+"?watch=1&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true", req{as: "sa"})
	defer w.cancel()
	eq(t, w.next()["type"], "ADDED", "initial")
	bm := w.next()
	eq(t, bm["type"], "BOOKMARK", "bookmark")
	eq(t, get(bm, "object", "metadata", "annotations", "k8s.io/initial-events-end"), "true", "initial-events-end")
	h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"note": "x"}}})
	mod := w.next()
	eq(t, mod["type"], "MODIFIED", "live")
	eq(t, get(mod, "object", "spec", "note"), "x", "the change")
	brv, _ := strconv.Atoi(fmt.Sprint(get(bm, "object", "metadata", "resourceVersion")))
	mrv, _ := strconv.Atoi(fmt.Sprint(get(mod, "object", "metadata", "resourceVersion")))
	eq(t, mrv, brv+1, "RV+1")
	eq(t, h.do("GET", inc+"?watch=1&sendInitialEvents=true&allowWatchBookmarks=true", req{as: "sa"}).code, 422, "no NotOlderThan")
	eq(t, h.do("GET", inc+"?watch=1&sendInitialEvents=true&resourceVersionMatch=NotOlderThan", req{as: "sa"}).code, 422, "no bookmarks")
}

func TestWatchBookmarksAndTimeout(t *testing.T) {
	h := newHarness(t, opts{})
	w := h.watch(inc+"?watch=1&resourceVersion=100&allowWatchBookmarks=true&timeoutSeconds=150", req{as: "sa"})
	defer w.cancel()
	time.Sleep(50 * time.Millisecond) // let the watch register its timers
	h.advance(time.Minute)
	eq(t, w.next()["type"], "BOOKMARK", "1 min")
	h.advance(time.Minute)
	eq(t, w.next()["type"], "BOOKMARK", "2 min")
	h.advance(30 * time.Second)
	eq(t, w.next()["type"], "BOOKMARK", "at the deadline")
	if ev := w.next(); ev != nil {
		t.Fatalf("the stream did not end: %v", ev)
	}
}

func TestWatchHistory410(t *testing.T) {
	h := newHarness(t, opts{})
	for i := 0; i < 100; i++ {
		h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"n": i}}})
	}
	h.advance(80 * time.Second) // older than eventFreshDuration (75 s): the cache evicts rather than grows
	h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"n": "last"}}})
	w := h.watch(inc+"?watch=1&resourceVersion=100", req{as: "sa"})
	e := w.next()
	eq(t, e["type"], "ERROR", "type")
	eq(t, get(e, "object", "code"), float64(410), "code")
	eq(t, get(e, "object", "reason"), "Expired", "reason")
	if !regexp.MustCompile(`^too old resource version: 100 \(\d+\)$`).MatchString(fmt.Sprint(get(e, "object", "message"))) {
		t.Fatal(get(e, "object", "message"))
	}
	if w.next() != nil {
		t.Fatal("the stream did not end after the ERROR")
	}
	latest := h.do("GET", inc+"/"+incident, req{as: "sa"})
	rv, _ := strconv.Atoi(fmt.Sprint(get(latest.body, "metadata", "resourceVersion")))
	recent := h.watch(inc+"?watch=1&resourceVersion="+strconv.Itoa(rv-1), req{as: "sa"})
	defer recent.cancel()
	eq(t, recent.next()["type"], "MODIFIED", "inside the window")
}

func TestPatchMediaTypes(t *testing.T) {
	h := newHarness(t, opts{})
	eq(t, h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"applied": false}}}).code, 200, "merge")
	eq(t, h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: "application/json-patch+json", body: []any{map[string]any{"op": "replace", "path": "/spec/applied", "value": false}}}).code, 200, "json patch")
	eq(t, h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: "application/json-patch+json", body: []any{map[string]any{"op": "test", "path": "/spec/applied", "value": true}}}).code, 422, "a failed test op")
	for _, ct := range []string{"application/json", "application/strategic-merge-patch+json", "application/apply-patch+cbor", "text/plain"} {
		r := h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: ct, body: map[string]any{}})
		eq(t, r.code, 415, ct)
		eq(t, r.body["message"], "the body of the request was in an unknown format - accepted media types include: application/json-patch+json, application/merge-patch+json, application/apply-patch+yaml", ct)
	}
	apply := h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: "application/apply-patch+yaml", body: "{}"})
	eq(t, apply.code, 422, "apply without fieldManager")
	eq(t, apply.body["message"], `PatchOptions.meta.k8s.io "" is invalid: fieldManager: Required value: is required for apply patch`, "message")
	eq(t, h.do("PATCH", inc+"/"+incident+"?fieldManager=x", req{as: "visitor", ct: "application/apply-patch+yaml", body: "{}"}).code, 500, "SSA")
	if !logged(h, func(l map[string]any) bool {
		return l["event"] == "fake.unimplemented" && strings.Contains(fmt.Sprint(l["what"]), "server-side apply")
	}) {
		t.Fatal("no fake.unimplemented line for SSA")
	}
	before := h.do("GET", inc+"/"+incident, req{as: "visitor"})
	noop := h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"status": map[string]any{"state": "Resolved"}}})
	eq(t, get(noop.body, "status", "state"), "Open", "the main resource never writes status")
	eq(t, get(noop.body, "metadata", "resourceVersion"), get(before.body, "metadata", "resourceVersion"), "a no-op keeps the RV")
}

var nsPatchGrant = []map[string]any{
	{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": map[string]any{"name": "t-ns-patch"}, "rules": []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"namespaces"}, "verbs": []any{"patch"}}}},
	{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": map[string]any{"name": "t-ns-patch"}, "roleRef": map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "t-ns-patch"}, "subjects": []any{map[string]any{"kind": "User", "name": "sandbox-visitor"}}},
}

func TestStrategicMergePatchIsLoud(t *testing.T) {
	h := newHarness(t, opts{extra: nsPatchGrant})
	r := h.do("PATCH", "/api/v1/namespaces/krateo-system", req{as: "visitor", ct: "application/strategic-merge-patch+json", body: map[string]any{"metadata": map[string]any{"labels": map[string]any{"a": "b"}}}})
	eq(t, r.code, 500, "code")
	if !logged(h, func(l map[string]any) bool {
		return l["event"] == "fake.unimplemented" && strings.Contains(fmt.Sprint(l["what"]), "strategic merge patch on /namespaces")
	}) {
		t.Fatal("no fake.unimplemented line for SMP")
	}
}

// statusWrite is the request line of the scripted /status write: the ServiceAccount's patch.
func statusWrite(l map[string]any) bool {
	return l["verb"] == "patch" && l["resource"] == "incidents/status" && l["identity"] == "sa"
}

func logged(h *harness, f func(map[string]any) bool) bool {
	for _, l := range h.log() {
		if f(l) {
			return true
		}
	}
	return false
}

func TestApplyStatusV1(t *testing.T) {
	h := newHarness(t, opts{})
	w := h.watch(inc+"?watch=1&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true", req{as: "sa"})
	defer w.cancel()
	for w.next()["type"] != "BOOKMARK" {
	}
	wr := h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"applied": true}}})
	eq(t, wr.code, 200, "the visitor's write")
	eq(t, w.next()["type"], "MODIFIED", "the visitor's event")
	gen := get(wr.body, "metadata", "generation")
	h.advance(1499 * time.Millisecond)
	if logged(h, statusWrite) {
		t.Fatal("applyStatus fired before afterMs")
	}
	h.advance(time.Millisecond)
	mod := w.next()
	eq(t, mod["type"], "MODIFIED", "the controller's event")
	eq(t, get(mod, "object", "status"), map[string]any{"state": "Resolved", "resolution": map[string]any{"by": "verify", "at": "2026-11-12T10:00:01Z"}}, "stamped status")
	eq(t, get(mod, "object", "metadata", "generation"), gen, "a status write keeps generation")
	n := 0
	for _, l := range h.log() {
		if statusWrite(l) {
			n++
		}
	}
	eq(t, n, 1, "one status write by the ServiceAccount")
	w.quiet()

	g := newHarness(t, opts{})
	g.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"other": 1}}})
	g.advance(5 * time.Second)
	if logged(g, statusWrite) {
		t.Fatal("fired without the match")
	}
	r := newHarness(t, opts{})
	r.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: "application/json", body: map[string]any{"spec": map[string]any{"applied": true}}})
	r.advance(5 * time.Second)
	if logged(r, statusWrite) {
		t.Fatal("a refused write fired a rule")
	}
}

func TestApplyStatusRuntimeFailure(t *testing.T) {
	h := newHarness(t, opts{edit: func(s map[string]any) {
		rule := get(s, "scripted").(map[string]any)["applyStatus"].([]any)[0].(map[string]any)
		rule["status"] = map[string]any{"resolution": nil}
	}})
	before := h.do("GET", inc+"/"+incident, req{as: "sa"})
	h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"applied": true}}})
	h.advance(1500 * time.Millisecond)
	var fail map[string]any
	for _, l := range h.log() {
		if l["event"] == "applyStatus.failed" {
			fail = l
		}
	}
	eq(t, fail, map[string]any{"event": "applyStatus.failed", "id": "resolve-on-applied", "pointer": "/status/resolution/at", "reason": "parent is not an object after the status merge"}, "failure line")
	eq(t, get(h.do("GET", inc+"/"+incident, req{as: "sa"}).body, "status"), get(before.body, "status"), "nothing written")
}

var impersonateGrant = []map[string]any{
	{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": map[string]any{"name": "t-imp"}, "rules": []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"users", "groups"}, "verbs": []any{"impersonate"}}}},
	{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": map[string]any{"name": "t-imp"}, "roleRef": map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "t-imp"}, "subjects": []any{map[string]any{"kind": "ServiceAccount", "name": "snowplow", "namespace": "krateo-system"}}},
}

func TestImpersonation(t *testing.T) {
	as := map[string]string{"Impersonate-User": "sandbox-visitor", "Impersonate-Group": "sandbox-viewers"}
	h := newHarness(t, opts{})
	eq(t, h.do("GET", inc+"/"+incident, req{as: "sa", headers: as}).code, 403, "without the impersonate verb")
	// A user alone needs the verb too: as any authenticated user the request would succeed (system:discovery).
	eq(t, h.do("GET", "/api", req{as: "sa", headers: map[string]string{"Impersonate-User": "someone"}}).code, 403, "a user alone needs the verb too")
	g := newHarness(t, opts{extra: impersonateGrant})
	eq(t, g.do("GET", inc+"/"+incident, req{as: "sa", headers: as}).code, 200, "with it")
	eq(t, g.do("GET", "/api/v1/namespaces/krateo-system/secrets/sandbox-visitor-clientconfig", req{as: "sa", headers: map[string]string{"Impersonate-User": "sandbox-visitor"}}).code, 403, "acts as the visitor")
	l := g.log()
	eq(t, l[len(l)-1]["identity"], "visitor", "logged identity")
}

func TestContains(t *testing.T) {
	j := func(s string) any { var v any; _ = json.Unmarshal([]byte(s), &v); return v }
	for _, c := range []struct {
		o, m string
		want bool
	}{
		{`{"spec":{"applied":true,"x":1}}`, `{"spec":{"applied":true}}`, true},
		{`{"spec":{"applied":"true"}}`, `{"spec":{"applied":true}}`, false},
		{`{"spec":{"tags":["a","b"]}}`, `{"spec":{"tags":["a"]}}`, false},
		{`{"spec":{}}`, `{"spec":{"applied":null}}`, true},
		{`{"spec":{"applied":null}}`, `{"spec":{"applied":null}}`, false},
		{`{}`, `{"spec":{"applied":true}}`, false},
		{`{"spec":{"n":1}}`, `{"spec":{"n":1.0}}`, true},
	} {
		eq(t, contains(j(c.o), j(c.m)), c.want, c.o+" ⊇ "+c.m)
	}
}

// ------------------------------------------------------------------ beyond the JS suite

// A typed clientset posts reviews as protobuf (the #574 probe does); the fake decodes them with
// client-go's scheme, as the apiserver does.
func TestProtobufSelfSubjectAccessReview(t *testing.T) {
	h := newHarness(t, opts{})
	ssar := &authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authorizationv1.ResourceAttributes{
		Verb: "get", Group: "observability.krateo.io", Resource: "incidents", Namespace: "krateo-system", Name: incident}}}
	ssar.SetGroupVersionKind(authorizationv1.SchemeGroupVersion.WithKind("SelfSubjectAccessReview"))
	info, ok := runtime.SerializerInfoForMediaType(scheme.Codecs.SupportedMediaTypes(), "application/vnd.kubernetes.protobuf")
	if !ok {
		t.Fatal("no protobuf serializer")
	}
	var buf bytes.Buffer
	if err := scheme.Codecs.EncoderForVersion(info.Serializer, authorizationv1.SchemeGroupVersion).Encode(ssar, &buf); err != nil {
		t.Fatal(err)
	}
	r := h.do("POST", "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", req{as: "visitor", ct: "application/vnd.kubernetes.protobuf", body: buf.Bytes(), accept: "application/vnd.kubernetes.protobuf, application/json"})
	eq(t, r.code, 201, "code")
	eq(t, r.body["kind"], "SelfSubjectAccessReview", "kind")
	eq(t, get(r.body, "status", "allowed"), true, "allowed")
	ssar.Spec.ResourceAttributes = &authorizationv1.ResourceAttributes{Verb: "get", Resource: "secrets", Namespace: "krateo-system"}
	buf.Reset()
	_ = scheme.Codecs.EncoderForVersion(info.Serializer, authorizationv1.SchemeGroupVersion).Encode(ssar, &buf)
	eq(t, get(h.do("POST", "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", req{as: "visitor", ct: "application/vnd.kubernetes.protobuf", body: buf.Bytes()}).body, "status", "allowed"), false, "denied")
}

// §4: a rules review answers RBAC's rules, complete or as the reference cluster answered.
func TestSelfSubjectRulesReview(t *testing.T) {
	body := map[string]any{"apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectRulesReview", "spec": map[string]any{"namespace": "krateo-system"}}
	h := newHarness(t, opts{})
	r := h.do("POST", "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews", req{as: "visitor", ct: "application/json", body: body})
	eq(t, r.code, 201, "code")
	eq(t, get(r.body, "status", "incomplete"), false, "complete without a record")
	var verbs []string
	for _, rr := range get(r.body, "status", "resourceRules").([]any) {
		for _, res := range get(rr, "resources").([]any) {
			if res == "incidents" {
				for _, v := range get(rr, "verbs").([]any) {
					verbs = append(verbs, v.(string))
				}
			}
		}
	}
	sort.Strings(verbs)
	if !strings.Contains(strings.Join(verbs, ","), "patch") {
		t.Fatalf("the visitor's patch on incidents is not in its rules: %v", verbs)
	}
	g := newHarness(t, opts{edit: func(s map[string]any) {
		s["capturedFrom"].(map[string]any)["selfSubjectRulesReview"] = map[string]any{"incomplete": true, "evaluationError": "webhook authorizer does not support user rule resolution"}
	}})
	w := g.do("POST", "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews", req{as: "visitor", ct: "application/json", body: body})
	eq(t, get(w.body, "status", "incomplete"), true, "incomplete, as captured")
	eq(t, get(w.body, "status", "evaluationError"), "webhook authorizer does not support user rule resolution", "the webhook message")
}

// A watch from the snapshot's resourceVersion, inside the window, gets every change since.
func TestWatchFromTheSnapshotRV(t *testing.T) {
	h := newHarness(t, opts{})
	for i := 0; i < 3; i++ {
		h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: merge, body: map[string]any{"spec": map[string]any{"n": i}}})
	}
	w := h.watch(inc+"?watch=1&resourceVersion=100", req{as: "sa"})
	defer w.cancel()
	for i := 0; i < 3; i++ {
		ev := w.next()
		eq(t, ev["type"], "MODIFIED", "event")
		eq(t, get(ev, "object", "spec", "n"), float64(i), "in order")
	}
	w.quiet()
}

// Q7: a watch from "0" without sendInitialEvents starts with the current state (legacy watch).
func TestLegacyWatchFromZero(t *testing.T) {
	h := newHarness(t, opts{})
	w := h.watch(inc+"?watch=1&resourceVersion=0", req{as: "sa"})
	defer w.cancel()
	eq(t, w.next()["type"], "ADDED", "initial state")
	w.quiet()
}

// Every request is one log line in §4's format.
func TestRequestLog(t *testing.T) {
	h := newHarness(t, opts{})
	h.do("GET", inc+"?labelSelector=a%3Db", req{as: "visitor"})
	l := h.log()
	last := l[len(l)-1]
	for k, want := range map[string]any{"verb": "list", "group": "observability.krateo.io", "version": "v1alpha1", "resource": "incidents", "namespace": "krateo-system", "name": "*", "status": 200, "identity": "visitor", "items": 0, "query": "labelSelector=a%3Db"} {
		eq(t, last[k], want, k)
	}
}
