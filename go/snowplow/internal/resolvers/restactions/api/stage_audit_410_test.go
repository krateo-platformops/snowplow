package api

// stage_audit_410_test.go — AUDIT-4 (krateo-platformops/installer#410), the
// RESTAction stage audit record, driven through the REAL api.Resolve.
//
//   C1  every write-verb stage of a customer resolve emits one record, with
//       enduser.id / user.name from the REQUEST context, the stage's target and
//       the RESTAction + stage that performed it; a read stage emits none.
//   C1e a stage with an endpointRef records where it went (server.address),
//       never a Kubernetes resource it cannot know it addressed.
//   C2  a write the answering authority refuses emits outcome=failure with that
//       authority's code and a reason class — the shape of the /call record.
//   Cr  a credential in the stage PATH (a webhook) or in the upstream's error
//       BODY never reaches the record (review item C).
//   E   the outcome is what the authority answered: the real 2xx code, an
//       accepted write whose body is unusable is a success with a note, a dial
//       with no response is a failure with no code (review item E).
//   C3  the same write under a BACKGROUND driver (no requester: a reconcile,
//       not a request) is never sent (#903) and emits nothing.
//   C5  is the audit package's (audit_incomplete_410_test.go); here the stage
//       emit site handed a write with no actor emits nothing.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"go.opentelemetry.io/otel/log/logtest"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

const (
	user410     = "u-alice-410"
	group410    = "g-team-410"
	extRef410   = "render-endpoint"
	raNS410     = "krateo-system"
	raName410   = "ra410"
	denyPath410 = "/apis/composition.krateo.io/v1alpha1/namespaces/team-b/fireworksapps"
	// Canaries standing for a credential a stage carries in its PATH and one an
	// upstream echoes in its error BODY. Neither may ever reach the 365-day
	// trail. Deliberately low-entropy, so no secret scanner mistakes the test
	// for a leak.
	pathCanary410 = "PATHCANARY410"
	bodyCanary410 = "BODYCANARY410"
)

// dial410 is one request a fake upstream received.
type dial410 struct{ method, path string }

// upstream410 is an HTTP API that records every request. It refuses any request
// under team-b with an apiserver-style 403 and accepts the rest.
func upstream410(t *testing.T) (*httptest.Server, func() []dial410) {
	t.Helper()
	var mu sync.Mutex
	var got []dial410
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		got = append(got, dial410{r.Method, r.URL.Path})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/created"):
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"ok":true}`)
			return
		case strings.HasSuffix(r.URL.Path, "/nocontent"):
			w.WriteHeader(http.StatusNoContent)
			return
		case strings.HasSuffix(r.URL.Path, "/html"):
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, `<html><body>accepted</body></html>`)
			return
		case strings.HasPrefix(r.URL.Path, "/services/"):
			// A webhook that refuses, echoing a credential in its error body.
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":"invalid_token","token":"`+bodyCanary410+`"}`)
			return
		}
		if strings.Contains(r.URL.Path, "/team-b/") {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,`+
				`"message":"fireworksapps.composition.krateo.io is forbidden: User \"`+user410+`\" cannot create resource"}`)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []dial410 {
		mu.Lock()
		defer mu.Unlock()
		return append([]dial410(nil), got...)
	}
}

// records410 installs a recording audit emitter for the test.
func records410(t *testing.T) *logtest.Recorder {
	t.Helper()
	rec := logtest.NewRecorder()
	audit.SetDefault(audit.New(rec.Logger("test-410")))
	t.Cleanup(func() { audit.SetDefault(nil) })
	return rec
}

// attrs410 flattens every emitted record to its attribute map, in emit order
// within the scope.
func attrs410(rec *logtest.Recorder) []map[string]string {
	var out []map[string]string
	for _, recs := range rec.Result() {
		for _, r := range recs {
			m := map[string]string{}
			for _, kv := range r.Attributes {
				m[string(kv.Key)] = kv.Value.String()
			}
			out = append(out, m)
		}
	}
	return out
}

// env410 publishes the requester's own clientconfig (apiURL) and a named
// endpoint Secret (extURL) the way a /call resolve finds them.
func env410(t *testing.T, apiURL, extURL string) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.SetGlobal(nil)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	own := kubeutil.MakeDNS1123Compatible(user410) + "-clientconfig"
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		own: {ObjectMeta: metav1.ObjectMeta{Namespace: raNS410, Name: own},
			Data: map[string][]byte{"server-url": []byte(apiURL), "token": []byte("tok")}},
		extRef410: {ObjectMeta: metav1.ObjectMeta{Namespace: raNS410, Name: extRef410},
			Data: map[string][]byte{"server-url": []byte(extURL), "token": []byte("ext-tok")}},
	}})
	cache.ForceSecretsCacheReadyForTest(raNS410)
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
}

func stage410(id, verb, path string) *templates.API {
	return &templates.API{Name: id, Path: path, Verb: ptr.To(verb), ContinueOnError: ptr.To(true), ErrorKey: ptr.To(id + "Err")}
}

func extStage410(id, verb, path string) *templates.API {
	s := stage410(id, verb, path)
	s.EndpointRef = &templates.Reference{Name: extRef410, Namespace: raNS410}
	return s
}

func resolve410(ctx context.Context, apiURL string, stages ...*templates.API) {
	_ = Resolve(ctx, ResolveOptions{RC: &rest.Config{Host: apiURL}, AuthnNS: raNS410, Items: stages,
		RESTActionNamespace: raNS410, RESTActionName: raName410})
}

func wantAttrs410(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q (record %v)", k, got[k], v, got)
		}
	}
}

// C1 — every write verb on the RESTAction surface emits, with the actor from
// the request context; the read stage beside them emits nothing.
func TestAudit410_C1_EveryWriteVerbStageEmits_WithRequestIdentity(t *testing.T) {
	api, dials := upstream410(t)
	env410(t, api.URL, api.URL)
	rec := records410(t)

	const base = "/apis/composition.krateo.io/v1alpha1/namespaces/team-a/fireworksapps"
	resolve410(ctxWithUser(user410, group410), api.URL,
		stage410("read", http.MethodGet, base+"/demo"),
		stage410("create", http.MethodPost, base),
		stage410("patch", http.MethodPatch, base+"/demo"),
		stage410("status", http.MethodPut, base+"/demo/status"),
		stage410("remove", http.MethodDelete, base+"/demo"))

	if n := len(dials()); n != 5 {
		t.Fatalf("precondition: upstream saw %d requests, want 5 (%v)", n, dials())
	}
	got := attrs410(rec)
	if len(got) != 4 {
		t.Fatalf("records = %d, want 4 (one per write stage, none for the GET): %v", len(got), got)
	}
	byStage := map[string]map[string]string{}
	for _, r := range got {
		byStage[r["krateo.restaction.stage"]] = r
	}
	if _, ok := byStage["read"]; ok {
		t.Fatal("the GET stage emitted a record; a read is not a write")
	}
	common := map[string]string{
		"event.name": "audit", "krateo.action": "restaction",
		"enduser.id": user410, "user.name": user410,
		"krateo.restaction.namespace": raNS410, "krateo.restaction.name": raName410,
		"k8s.resource.group": "composition.krateo.io", "k8s.resource.version": "v1alpha1",
		"k8s.resource.resource": "fireworksapps", "k8s.namespace.name": "team-a",
		"outcome": "success", "http.response.status_code": "200",
	}
	for stage, method := range map[string]string{"create": "POST", "patch": "PATCH", "status": "PUT", "remove": "DELETE"} {
		r, ok := byStage[stage]
		if !ok {
			t.Errorf("no record for write stage %q", stage)
			continue
		}
		wantAttrs410(t, r, common)
		wantAttrs410(t, r, map[string]string{"http.request.method": method})
	}
	wantAttrs410(t, byStage["create"], map[string]string{"k8s.resource.name": ""})
	for stage, r := range byStage {
		if v, ok := r["url.path"]; ok {
			t.Errorf("stage %s: the record carries url.path=%q; a path is never recorded", stage, v)
		}
	}
	wantAttrs410(t, byStage["patch"], map[string]string{"k8s.resource.name": "demo"})
	wantAttrs410(t, byStage["status"], map[string]string{"k8s.resource.name": "demo", "k8s.resource.subresource": "status"})
	if roles := byStage["create"]["user.roles"]; !strings.Contains(roles, group410) {
		t.Errorf("user.roles = %q, want it to carry %q from the request", roles, group410)
	}
}

// C1e — an endpointRef stage records the host it wrote to, and neither a path
// nor a Kubernetes resource: a named endpoint may be any HTTP API.
func TestAudit410_C1e_EndpointRefStage_RecordsHostOnly(t *testing.T) {
	api, _ := upstream410(t)
	ext, extDials := upstream410(t)
	env410(t, api.URL, ext.URL)
	rec := records410(t)

	// The path is apiserver-SHAPED on purpose: the endpoint, not the path's
	// shape, decides whether the record may name a Kubernetes resource.
	const extPath = "/apis/render.example.io/v1/namespaces/team-a/renders"
	resolve410(ctxWithUser(user410, group410), api.URL,
		extStage410("render", http.MethodPost, extPath+"?token=SECRET410"))

	if n := len(extDials()); n != 1 {
		t.Fatalf("precondition: the named endpoint saw %d requests, want 1", n)
	}
	got := attrs410(rec)
	if len(got) != 1 {
		t.Fatalf("records = %d, want 1: %v", len(got), got)
	}
	host := strings.TrimPrefix(ext.URL, "http://")
	wantAttrs410(t, got[0], map[string]string{
		"server.address": host, "http.request.method": "POST",
		"enduser.id": user410, "krateo.restaction.stage": "render", "outcome": "success",
	})
	for _, k := range []string{"k8s.resource.resource", "url.path"} {
		if v, ok := got[0][k]; ok {
			t.Errorf("an endpointRef stage record must not carry %s, got %q", k, v)
		}
	}
	for k, v := range got[0] {
		if strings.Contains(v, "SECRET410") {
			t.Errorf("%s carries the request query: %q", k, v)
		}
	}
}

// bodies410 returns every emitted record's body.
func bodies410(rec *logtest.Recorder) []string {
	var out []string
	for _, recs := range rec.Result() {
		for _, r := range recs {
			out = append(out, r.Body.AsString())
		}
	}
	return out
}

// C2 — a refused write emits outcome=failure with the refusing authority's
// code and a reason class from a closed set — never the upstream's own text.
func TestAudit410_C2_RefusedWrite_EmitsFailureWithAuthorityCode(t *testing.T) {
	api, _ := upstream410(t)
	env410(t, api.URL, api.URL)
	rec := records410(t)

	resolve410(ctxWithUser(user410, group410), api.URL, stage410("create", http.MethodPost, denyPath410))

	got := attrs410(rec)
	if len(got) != 1 {
		t.Fatalf("records = %d, want 1: %v", len(got), got)
	}
	wantAttrs410(t, got[0], map[string]string{
		"outcome": "failure", "http.response.status_code": "403", "http.request.method": "POST",
		"enduser.id": user410, "k8s.namespace.name": "team-b", "k8s.resource.resource": "fireworksapps",
	})
	if b := bodies410(rec); len(b) != 1 || b[0] != "upstream answered 403 Forbidden" {
		t.Errorf("record body = %q, want the reason class %q", b, "upstream answered 403 Forbidden")
	}
}

// Cr — review item C: a credential carried in the stage PATH (a webhook) or
// echoed in the upstream's error BODY never reaches any attribute or the body
// of the record. The record still says who, which RESTAction/stage, which host,
// and the authority's code.
func TestAudit410_Cr_CredentialsInPathOrBody_NeverReachTheRecord(t *testing.T) {
	api, _ := upstream410(t)
	ext, extDials := upstream410(t)
	env410(t, api.URL, ext.URL)
	rec := records410(t)

	resolve410(ctxWithUser(user410, group410), api.URL,
		extStage410("notify", http.MethodPost, "/services/T000/B000/"+pathCanary410))

	if d := extDials(); len(d) != 1 || !strings.Contains(d[0].path, pathCanary410) {
		t.Fatalf("precondition: the webhook was not dialled with its secret path: %v", d)
	}
	got := attrs410(rec)
	if len(got) != 1 {
		t.Fatalf("records = %d, want 1: %v", len(got), got)
	}
	wantAttrs410(t, got[0], map[string]string{
		"outcome": "failure", "http.response.status_code": "403", "enduser.id": user410,
		"krateo.restaction.stage": "notify", "server.address": strings.TrimPrefix(ext.URL, "http://"),
	})
	for _, secret := range []string{pathCanary410, bodyCanary410, "invalid_token", "/services/"} {
		for k, v := range got[0] {
			if strings.Contains(v, secret) {
				t.Errorf("attribute %s carries %q: %q", k, secret, v)
			}
		}
		for _, b := range bodies410(rec) {
			if strings.Contains(b, secret) {
				t.Errorf("record body carries %q: %q", secret, b)
			}
		}
	}
}

// E — review item E: the outcome is what the authority answered.
func TestAudit410_E_OutcomeIsWhatTheAuthorityAnswered(t *testing.T) {
	cases := []struct {
		name, path, outcome, code, body string
	}{
		{"201 created", "/things/created", "success", "201", ""},
		{"204 no content", "/things/nocontent", "success", "204", ""},
		{"2xx body not convertible", "/things/html", "success", "200", dialNoteBodyUnconvertible},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			api, _ := upstream410(t)
			ext, extDials := upstream410(t)
			env410(t, api.URL, ext.URL)
			rec := records410(t)

			resolve410(ctxWithUser(user410, group410), api.URL, extStage410("w", http.MethodPost, c.path))

			if n := len(extDials()); n != 1 {
				t.Fatalf("precondition: %d dials, want 1", n)
			}
			got := attrs410(rec)
			if len(got) != 1 {
				t.Fatalf("records = %d, want 1: %v", len(got), got)
			}
			wantAttrs410(t, got[0], map[string]string{"outcome": c.outcome, "http.response.status_code": c.code})
			if b := bodies410(rec); len(b) != 1 || b[0] != c.body {
				t.Errorf("record body = %q, want %q", b, c.body)
			}
		})
	}

	t.Run("no response", func(t *testing.T) {
		api, _ := upstream410(t)
		gone := httptest.NewServer(http.NotFoundHandler())
		goneURL := gone.URL
		gone.Close() // nothing listens: the dial gets no response
		env410(t, api.URL, goneURL)
		rec := records410(t)

		resolve410(ctxWithUser(user410, group410), api.URL, extStage410("w", http.MethodPost, "/things"))

		got := attrs410(rec)
		if len(got) != 1 {
			t.Fatalf("records = %d, want 1: %v", len(got), got)
		}
		wantAttrs410(t, got[0], map[string]string{"outcome": "failure"})
		if v, ok := got[0]["http.response.status_code"]; ok {
			t.Errorf("a dial with no response must carry no status code, got %q", v)
		}
		if b := bodies410(rec); len(b) != 1 || b[0] != "no response from the endpoint" {
			t.Errorf("record body = %q", b)
		}
	})
}

// C3 — a write under a background driver (a reconcile: no requester) is never
// sent (#903: background work only reads) and so emits no record. 0 sends AND 0
// records: the trail has nothing to say about a write that never happened. The
// control (the same stage on a customer resolve) proves both counts can move.
func TestAudit410_C3_BackgroundDriverWrite_EmitsNoRecord(t *testing.T) {
	run := func(t *testing.T, ctx context.Context, wantSends, wantRecords int) {
		t.Helper()
		api, _ := upstream410(t)
		ext, extDials := upstream410(t)
		env410(t, api.URL, ext.URL)
		rec := records410(t)

		resolve410(ctx, api.URL, extStage410("render", http.MethodPost, "/render"))

		if n := len(extDials()); n != wantSends {
			t.Fatalf("the resolve dialled %d writes, want %d", n, wantSends)
		}
		if got := attrs410(rec); len(got) != wantRecords {
			t.Fatalf("the resolve emitted %d records, want %d: %v", len(got), wantRecords, got)
		}
	}
	for _, origin := range []string{cache.BackgroundOriginRefresher, cache.BackgroundOriginCohortSeed, cache.BackgroundOriginPrewarmEngineBoot} {
		t.Run(origin, func(t *testing.T) {
			run(t, cache.WithBackgroundResolveOrigin(ctxWithUser(user410, group410), origin), 0, 0)
		})
	}
	t.Run("prewarm-path", func(t *testing.T) {
		run(t, cache.WithPrewarmPath(ctxWithUser(user410, group410)), 0, 0)
	})
	t.Run("control-customer", func(t *testing.T) {
		run(t, ctxWithUser(user410, group410), 1, 1)
	})
}

// C5 (emit-site arm) — the stage site handed a customer write with no actor on
// its context emits no record and counts the refusal. (A real actorless /call
// resolve never reaches the dial: the caller's endpoint is found by username.)
// The control with an actor proves the site emits at all.
func TestAudit410_C5_StageSite_NoActor_NoRecord(t *testing.T) {
	rec := records410(t)
	call := httpcallOpts410(http.MethodPost, "/render", "https://render.example")
	res := &dialAnswer{code: http.StatusOK}
	apiCall := extStage410("render", http.MethodPost, "/render")

	before := audit.IncompleteCount(audit.IncompleteActor)
	auditStageWrite(context.Background(), raNS410, raName410, apiCall, call, res, nil)
	if got := attrs410(rec); len(got) != 0 {
		t.Fatalf("an actorless write emitted %d records, want 0: %v", len(got), got)
	}
	if d := audit.IncompleteCount(audit.IncompleteActor) - before; d != 1 {
		t.Fatalf("incomplete{actor} moved by %d, want 1", d)
	}

	auditStageWrite(ctxWithUser(user410, group410), raNS410, raName410, apiCall, call, res, nil)
	if got := attrs410(rec); len(got) != 1 || got[0]["enduser.id"] != user410 {
		t.Fatalf("control: the same write with an actor emitted %v, want one record for %q", got, user410)
	}
}

// C3e (#912 re-review N3, emit-site arm) — the stage site itself, handed a
// write dialed under a background driver, emits no record. Since #903 such a
// write is refused before the dial (C3), so this guard is defence in depth;
// this arm keeps it honest. The control (the same write on a customer resolve)
// proves the site emits at all.
func TestAudit410_C3e_StageSite_BackgroundDriver_NoRecord(t *testing.T) {
	rec := records410(t)
	call := httpcallOpts410(http.MethodPost, "/render", "https://render.example")
	res := &dialAnswer{code: http.StatusOK}
	apiCall := extStage410("render", http.MethodPost, "/render")

	bg := map[string]context.Context{
		cache.BackgroundOriginRefresher:         cache.WithBackgroundResolveOrigin(ctxWithUser(user410, group410), cache.BackgroundOriginRefresher),
		cache.BackgroundOriginCohortSeed:        cache.WithBackgroundResolveOrigin(ctxWithUser(user410, group410), cache.BackgroundOriginCohortSeed),
		cache.BackgroundOriginPrewarmEngineBoot: cache.WithBackgroundResolveOrigin(ctxWithUser(user410, group410), cache.BackgroundOriginPrewarmEngineBoot),
		"prewarm-path":                          cache.WithPrewarmPath(ctxWithUser(user410, group410)),
	}
	for name, ctx := range bg {
		auditStageWrite(ctx, raNS410, raName410, apiCall, call, res, nil)
		if got := attrs410(rec); len(got) != 0 {
			t.Fatalf("#912 N3 RED: a write handed to the stage site under %s emitted %d record(s), want 0: %v", name, len(got), got)
		}
	}

	auditStageWrite(ctxWithUser(user410, group410), raNS410, raName410, apiCall, call, res, nil)
	if got := attrs410(rec); len(got) != 1 {
		t.Fatalf("control: the same write on a customer resolve emitted %d record(s), want 1: %v", len(got), got)
	}
}

func httpcallOpts410(verb, path, server string) httpcall.RequestOptions {
	return httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(verb)},
		Endpoint:    &endpoints.Endpoint{ServerURL: server},
	}
}

// The subresource is read from the parser's path shapes, including a namespace
// whose name equals the object's.
func TestAudit410_APIServerSubresource(t *testing.T) {
	cases := map[string]string{
		"/api/v1/namespaces/a/configmaps/a/status":                        "status",
		"/api/v1/namespaces/a/configmaps/a":                               "",
		"/api/v1/namespaces/a/configmaps":                                 "",
		"/api/v1/nodes/n1/proxy/metrics":                                  "proxy/metrics",
		"/api/v1/nodes/n1":                                                "",
		"/apis/apps/v1/namespaces/x/deployments/x/scale":                  "scale",
		"/apis/composition.krateo.io/v1alpha1/fireworksapps/demo/status/": "status",
		"/apis/composition.krateo.io/v1alpha1/fireworksapps":              "",
		"/render": "",
	}
	for path, want := range cases {
		if got := apiServerSubresource(path); got != want {
			t.Errorf("apiServerSubresource(%q) = %q, want %q", path, got, want)
		}
	}
}
