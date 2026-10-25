package api

// stage_audit_410_test.go — AUDIT-4 (krateo-platformops/installer#410), the
// RESTAction stage audit record, driven through the REAL api.Resolve.
//
//   C1  every write-verb stage of a customer resolve emits one record, with
//       enduser.id / user.name from the REQUEST context, the stage's target and
//       the RESTAction + stage that performed it; a read stage emits none.
//   C1e a stage with an endpointRef records where it went (server.address +
//       url.path), never a Kubernetes resource it cannot know it addressed.
//   C2  a write the answering authority refuses emits outcome=failure with that
//       authority's code and message — the shape of the /call write record.
//   C3  the same write dialled by a BACKGROUND driver (no requester: a reconcile,
//       not a request) emits nothing, though the write went out.
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
	"github.com/krateo-platformops/plumbing/http/response"
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
	wantAttrs410(t, byStage["create"], map[string]string{"k8s.resource.name": "", "url.path": base})
	wantAttrs410(t, byStage["patch"], map[string]string{"k8s.resource.name": "demo"})
	wantAttrs410(t, byStage["status"], map[string]string{"k8s.resource.name": "demo", "k8s.resource.subresource": "status"})
	if roles := byStage["create"]["user.roles"]; !strings.Contains(roles, group410) {
		t.Errorf("user.roles = %q, want it to carry %q from the request", roles, group410)
	}
}

// C1e — an endpointRef stage records the host and path it wrote to, and no
// Kubernetes resource: a named endpoint may be any HTTP API.
func TestAudit410_C1e_EndpointRefStage_RecordsHostAndPath(t *testing.T) {
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
		"server.address": host, "url.path": extPath, "http.request.method": "POST",
		"enduser.id": user410, "krateo.restaction.stage": "render", "outcome": "success",
	})
	if r, ok := got[0]["k8s.resource.resource"]; ok {
		t.Errorf("an endpointRef stage must not claim a Kubernetes resource, got %q", r)
	}
	for k, v := range got[0] {
		if strings.Contains(v, "SECRET410") {
			t.Errorf("%s carries the request query: %q", k, v)
		}
	}
}

// C2 — a refused write emits outcome=failure with the refusing authority's
// code and message.
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
	var body string
	for _, recs := range rec.Result() {
		for _, r := range recs {
			body = r.Body.AsString()
		}
	}
	if !strings.Contains(body, "is forbidden") {
		t.Errorf("record body = %q, want the authority's refusal message", body)
	}
}

// C3 — a write a background driver dials (a reconcile: no requester) emits
// nothing. The precondition proves the write went out, so the absence is the
// emit site's decision and not a write that never happened.
func TestAudit410_C3_BackgroundDriverWrite_EmitsNoRecord(t *testing.T) {
	for _, origin := range []string{cache.BackgroundOriginRefresher, cache.BackgroundOriginCohortSeed, cache.BackgroundOriginPrewarmEngineBoot} {
		t.Run(origin, func(t *testing.T) {
			api, _ := upstream410(t)
			ext, extDials := upstream410(t)
			env410(t, api.URL, ext.URL)
			rec := records410(t)

			ctx := cache.WithBackgroundResolveOrigin(ctxWithUser(user410, group410), origin)
			resolve410(ctx, api.URL, extStage410("render", http.MethodPost, "/render"))

			if n := len(extDials()); n != 1 {
				t.Fatalf("precondition: the background resolve dialled %d writes, want 1", n)
			}
			if got := attrs410(rec); len(got) != 0 {
				t.Fatalf("a background write emitted %d records, want 0: %v", len(got), got)
			}
		})
	}
	t.Run("prewarm-path", func(t *testing.T) {
		api, _ := upstream410(t)
		ext, extDials := upstream410(t)
		env410(t, api.URL, ext.URL)
		rec := records410(t)

		resolve410(cache.WithPrewarmPath(ctxWithUser(user410, group410)), api.URL, extStage410("render", http.MethodPost, "/render"))

		if n := len(extDials()); n != 1 {
			t.Fatalf("precondition: the prewarm resolve dialled %d writes, want 1", n)
		}
		if got := attrs410(rec); len(got) != 0 {
			t.Fatalf("a prewarm write emitted %d records, want 0: %v", len(got), got)
		}
	})
}

// C5 (emit-site arm) — the stage site handed a customer write with no actor on
// its context emits no record and counts the refusal. (A real actorless /call
// resolve never reaches the dial: the caller's endpoint is found by username.)
// The control with an actor proves the site emits at all.
func TestAudit410_C5_StageSite_NoActor_NoRecord(t *testing.T) {
	rec := records410(t)
	call := httpcallOpts410(http.MethodPost, "/render", "https://render.example")
	res := responseOK410()
	apiCall := extStage410("render", http.MethodPost, "/render")

	before := audit.IncompleteCount(audit.IncompleteActor)
	auditStageWrite(context.Background(), raNS410, raName410, apiCall, call, res)
	if got := attrs410(rec); len(got) != 0 {
		t.Fatalf("an actorless write emitted %d records, want 0: %v", len(got), got)
	}
	if d := audit.IncompleteCount(audit.IncompleteActor) - before; d != 1 {
		t.Fatalf("incomplete{actor} moved by %d, want 1", d)
	}

	auditStageWrite(ctxWithUser(user410, group410), raNS410, raName410, apiCall, call, res)
	if got := attrs410(rec); len(got) != 1 || got[0]["enduser.id"] != user410 {
		t.Fatalf("control: the same write with an actor emitted %v, want one record for %q", got, user410)
	}
}

func httpcallOpts410(verb, path, server string) httpcall.RequestOptions {
	return httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(verb)},
		Endpoint:    &endpoints.Endpoint{ServerURL: server},
	}
}

func responseOK410() *response.Status { return response.New(http.StatusOK, nil) }

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
