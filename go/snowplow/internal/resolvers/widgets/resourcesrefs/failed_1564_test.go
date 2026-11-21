// failed_1564_test.go — #1564: a resourcesRef that fails to resolve is a
// failure entry in status, a log line and a count, never an absence.
//
// Driven through the REAL Resolve: the caller's endpoint goes through
// cache.ClientConfigFor, and cache.KindForGVR runs real discovery against a
// fake apiserver (httptest) that serves, refuses or 404s each group/version's
// discovery document. Each arm resolves a widget's refs:
//
//	(a) one ref fails, of each class (apiVersion that does not parse; a type
//	    discovery says is not served, by a 404 document or a document that
//	    lacks the resource; discovery refused 403 or unavailable 503): that
//	    ref is a failure entry (id, verb, allowed:false, NO path, the
//	    dependency-status Status as `error`), a WARN carrying the class and
//	    the redacted error, one count and one stage-error bump; the other
//	    refs resolve exactly as they do without it;
//	(b) the caller's discovery fails outright: every ref whose kind needs
//	    discovery is a failure entry, the builtin one is unaffected, and once
//	    discovery answers again the same refs resolve (nothing remembered the
//	    failure);
//	a background driver's failure is the same entry and count, at Debug.
package resourcesrefs

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/statusdetails"
)

const f1564Msg = "resource ref failed; it is a failure entry in status.resourcesRefs"

type f1564Rec struct {
	level slog.Level
	attrs map[string]string
}

type f1564Handler struct {
	mu   *sync.Mutex
	recs *[]f1564Rec
}

func (h f1564Handler) Enabled(context.Context, slog.Level) bool { return true }
func (h f1564Handler) Handle(_ context.Context, r slog.Record) error {
	if r.Message != f1564Msg {
		return nil
	}
	attrs := map[string]string{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.recs = append(*h.recs, f1564Rec{level: r.Level, attrs: attrs})
	return nil
}
func (h f1564Handler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h f1564Handler) WithGroup(string) slog.Handler      { return h }

// f1564API is a fake apiserver's discovery. served maps a group/version path
// ("/apis/<g>/<v>") to the resources it lists; fail maps one to the status it
// answers; anything else is a 404. down answers 503 to every discovery GET.
type f1564API struct {
	srv    *httptest.Server
	mu     sync.Mutex
	served map[string][]string
	fail   map[string]int
	down   atomic.Bool
	gets   atomic.Int64
}

func newF1564API(t *testing.T) *f1564API {
	t.Helper()
	a := &f1564API{served: map[string][]string{}, fail: map[string]int{}}
	a.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.gets.Add(1)
		w.Header().Set("Content-Type", "application/json")
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"ServiceUnavailable","code":503,"message":"the server is currently unable to handle the request"}`))
			return
		}
		if code, ok := a.fail[r.URL.Path]; ok {
			w.WriteHeader(code)
			// A message naming a user, as the apiserver's Forbidden does: the
			// log must carry it redacted, and the status entry never.
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,"message":"User \"f1564-alice\" cannot get path \"` + r.URL.Path + `\""}`))
			return
		}
		res, ok := a.served[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,"message":"the server could not find the requested resource"}`))
			return
		}
		gv := strings.TrimPrefix(r.URL.Path, "/apis/")
		list := map[string]any{"kind": "APIResourceList", "apiVersion": "v1", "groupVersion": gv}
		var rs []map[string]any
		for _, name := range res {
			rs = append(rs, map[string]any{"name": name, "kind": "K" + name, "namespaced": true, "singularName": name, "verbs": []string{"get", "list"}})
		}
		list["resources"] = rs
		_ = json.NewEncoder(w).Encode(list)
	}))
	t.Cleanup(a.srv.Close)
	return a
}

// f1564Ctx is a customer's request context (wrap adds a background marker),
// dialling api with its own endpoint, with a stage-error sink installed as the
// widget dispatcher installs one.
func f1564Ctx(api *f1564API, wrap func(context.Context) context.Context) (context.Context, *[]f1564Rec, *cache.StageErrorSink) {
	recs := &[]f1564Rec{}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(slog.New(f1564Handler{mu: &sync.Mutex{}, recs: recs})),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "f1564-alice", Groups: []string{"f1564-devs"}}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: api.srv.URL, Token: "f1564-token"}),
	)
	if wrap != nil {
		ctx = wrap(ctx)
	}
	ctx, sink := cache.WithStageErrorSink(ctx)
	return ctx, recs, sink
}

// f1564Good is a ref of a builtin type: KindForGVR answers it in-process, so
// it never depends on discovery.
func f1564Good() templatesv1.ResourceRef {
	return templatesv1.ResourceRef{ID: "good", APIVersion: "v1", Resource: "configmaps", Name: "cm", Namespace: "ns", Verb: "GET"}
}

func f1564ByID(out []templatesv1.ResourceRefResult, id string) []templatesv1.ResourceRefResult {
	var got []templatesv1.ResourceRefResult
	for _, r := range out {
		if r.ID == id {
			got = append(got, r)
		}
	}
	return got
}

// assertGoodUnaffected: the builtin ref resolves as it does alone — one GET
// entry with its /call path and no error.
func assertGoodUnaffected(t *testing.T, out []templatesv1.ResourceRefResult) {
	t.Helper()
	good := f1564ByID(out, "good")
	if len(good) != 1 || good[0].Error != nil || good[0].Verb != http.MethodGet ||
		!strings.HasPrefix(good[0].Path, "/call?") || !strings.Contains(good[0].Path, "resource=configmaps") {
		t.Fatalf("the other ref must resolve unaffected; got %+v", good)
	}
}

// assertFailureEntry: the failed ref is ONE entry per declared verb, with no
// path, allowed:false and the given error code and reason; and it encodes with
// no "path" key at all, so nothing can fetch it.
func assertFailureEntry(t *testing.T, out []templatesv1.ResourceRefResult, id string, code int, reason string) templatesv1.ResourceRefResult {
	t.Helper()
	got := f1564ByID(out, id)
	if len(got) != 1 {
		t.Fatalf("#1564: ref %q must be exactly one failure entry (a GET ref), never absent; got %d entries in %+v", id, len(got), out)
	}
	e := got[0]
	if e.Error == nil || e.Path != "" || e.Allowed || e.Verb != http.MethodGet || e.Payload != nil {
		t.Fatalf("failure entry for %q: want error set, no path, allowed:false, verb GET; got %+v", id, e)
	}
	if e.Error.Code != code || string(e.Error.Reason) != reason || e.Error.Status != "Failure" || e.Error.Kind != "Status" || e.Error.Message == "" {
		t.Fatalf("failure entry for %q: error = %+v, want code %d reason %q", id, e.Error, code, reason)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"path"`) || !strings.Contains(string(b), `"allowed":false`) || !strings.Contains(string(b), `"error":{`) {
		t.Fatalf("failure entry JSON must carry error and allowed:false and no path: %s", b)
	}
	for _, leak := range []string{"f1564-alice", "f1564-devs", "f1564-token"} {
		if strings.Contains(string(b), leak) {
			t.Fatalf("failure entry carries identity %q: %s", leak, b)
		}
	}
	return e
}

func TestIssue1564_A_EachFailureClassIsAFailureEntryAWarnAndACount(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cases := []struct {
		name   string
		ref    templatesv1.ResourceRef
		setup  func(a *f1564API)
		class  string
		code   int
		reason string
	}{
		{
			name:  "apiVersion does not parse",
			ref:   templatesv1.ResourceRef{ID: "bad", APIVersion: "a/b/c", Resource: "things", Name: "x", Namespace: "ns", Verb: "GET"},
			class: FailedRefAPIVersionInvalid, code: 500, reason: "InternalError",
		},
		{
			name:  "type not installed: the group/version document is a 404",
			ref:   templatesv1.ResourceRef{ID: "bad", APIVersion: "gone1564.example.io/v1", Resource: "things", Name: "x", Namespace: "ns", Verb: "GET"},
			class: FailedRefTypeNotInstalled, code: 424, reason: "FailedDependency",
		},
		{
			name: "type not installed: the document lists no such resource",
			ref:  templatesv1.ResourceRef{ID: "bad", APIVersion: "half1564.example.io/v1", Resource: "things", Name: "x", Namespace: "ns", Verb: "GET"},
			setup: func(a *f1564API) {
				a.served["/apis/half1564.example.io/v1"] = []string{"otherthings"}
			},
			class: FailedRefTypeNotInstalled, code: 424, reason: "FailedDependency",
		},
		{
			name: "discovery refused for the caller (403)",
			ref:  templatesv1.ResourceRef{ID: "bad", APIVersion: "denied1564.example.io/v1", Resource: "things", Name: "x", Namespace: "ns", Verb: "GET"},
			setup: func(a *f1564API) {
				a.fail["/apis/denied1564.example.io/v1"] = http.StatusForbidden
			},
			class: FailedRefDiscoveryFailed, code: 403, reason: "Forbidden",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			api := newF1564API(t)
			if c.setup != nil {
				c.setup(api)
			}
			ctx, recs, sink := f1564Ctx(api, nil)
			before := FailedRefSnapshot()

			out, err := Resolve(ctx, []templatesv1.ResourceRef{f1564Good(), c.ref})
			if err != nil {
				t.Fatalf("Resolve errored: %v", err)
			}
			assertGoodUnaffected(t, out)
			e := assertFailureEntry(t, out, "bad", c.code, c.reason)

			if c.class == FailedRefTypeNotInstalled {
				gv := c.ref.APIVersion
				d := e.Error.Details
				if d == nil || d.Kind != "things" || len(d.Causes) != 1 || string(d.Causes[0].Type) != statusdetails.CauseTypeNotInstalled ||
					d.Causes[0].Field != "apiVersion" || d.Causes[0].Message != gv {
					t.Fatalf("a type that is not installed carries the TypeNotInstalled details (#1429); got %+v", d)
				}
			} else if e.Error.Details != nil {
				t.Fatalf("only a type that is not installed carries details; got %+v", e.Error.Details)
			}
			if strings.Contains(e.Error.Message, "f1564-alice") || strings.Contains(e.Error.Message, "cannot get") {
				t.Fatalf("the entry's message must be built from coordinates, never the apiserver's text: %q", e.Error.Message)
			}

			if len(*recs) != 1 {
				t.Fatalf("want exactly one %q line, got %d", f1564Msg, len(*recs))
			}
			rec := (*recs)[0]
			if rec.level != slog.LevelWarn || rec.attrs["reason"] != c.class || rec.attrs["id"] != "bad" || rec.attrs["err"] == "" {
				t.Fatalf("want a WARN naming id and class %q with the error; got %v %v", c.class, rec.level, rec.attrs)
			}
			if strings.Contains(rec.attrs["err"], "f1564-alice") {
				t.Fatalf("the logged error must be redacted: %q", rec.attrs["err"])
			}

			after := FailedRefSnapshot()
			for _, cl := range FailedRefClasses {
				want := before[cl]
				if cl == c.class {
					want++
				}
				if after[cl] != want {
					t.Fatalf("snowplow_resourcesrefs_failed_total{%s} went %d → %d, want %d", cl, before[cl], after[cl], want)
				}
			}
			if sink.Count() != 1 {
				t.Fatalf("the stage-error sink must be bumped once (served, never persisted); count=%d", sink.Count())
			}
		})
	}
}

// A ref that declares no verb resolves to every verb; failed, it is a failure
// entry for every verb, none with a path.
func TestIssue1564_A_FailedRefWithoutVerbIsAnEntryPerVerb(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	api := newF1564API(t)
	ctx, _, _ := f1564Ctx(api, nil)
	out, err := Resolve(ctx, []templatesv1.ResourceRef{{ID: "bad", APIVersion: "noverb1564.example.io/v1", Resource: "things"}})
	if err != nil {
		t.Fatal(err)
	}
	verbs := map[string]bool{}
	for _, e := range f1564ByID(out, "bad") {
		if e.Error == nil || e.Path != "" || e.Allowed {
			t.Fatalf("every entry of a failed ref is a failure entry: %+v", e)
		}
		verbs[e.Verb] = true
	}
	if len(verbs) != len(kubeToREST) {
		t.Fatalf("want one failure entry per verb (%d), got %v", len(kubeToREST), verbs)
	}
}

// (b) The caller's discovery itself fails: every ref whose kind needs
// discovery is a failure entry, the builtin one is unaffected. When discovery
// answers again, the same refs resolve: KindForGVR keeps only successes, so
// no failure is remembered for this identity.
func TestIssue1564_B_DiscoveryDownFailsEveryAffectedRefAndRecovers(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	api := newF1564API(t)
	for _, g := range []string{"one1564", "two1564", "three1564"} {
		api.served["/apis/"+g+".example.io/v1"] = []string{"things"}
	}
	refs := []templatesv1.ResourceRef{f1564Good()}
	for _, g := range []string{"one1564", "two1564", "three1564"} {
		refs = append(refs, templatesv1.ResourceRef{ID: g, APIVersion: g + ".example.io/v1", Resource: "things", Name: "x", Namespace: "ns", Verb: "GET"})
	}

	api.down.Store(true)
	ctx, recs, sink := f1564Ctx(api, nil)
	out, err := Resolve(ctx, refs)
	if err != nil {
		t.Fatal(err)
	}
	assertGoodUnaffected(t, out)
	for _, g := range []string{"one1564", "two1564", "three1564"} {
		assertFailureEntry(t, out, g, http.StatusServiceUnavailable, "ServiceUnavailable")
	}
	if len(*recs) != 3 || sink.Count() != 3 {
		t.Fatalf("want one WARN and one bump per failed ref (3); got %d lines, %d bumps", len(*recs), sink.Count())
	}

	api.down.Store(false)
	ctx2, recs2, sink2 := f1564Ctx(api, nil)
	out2, err := Resolve(ctx2, refs)
	if err != nil {
		t.Fatal(err)
	}
	assertGoodUnaffected(t, out2)
	for _, g := range []string{"one1564", "two1564", "three1564"} {
		got := f1564ByID(out2, g)
		if len(got) != 1 || got[0].Error != nil || !strings.HasPrefix(got[0].Path, "/call?") {
			t.Fatalf("once discovery answers, ref %q must resolve (no remembered failure); got %+v", g, got)
		}
	}
	if len(*recs2) != 0 || sink2.Count() != 0 {
		t.Fatalf("a healthy resolve logs no failure and bumps nothing; got %d lines, %d bumps", len(*recs2), sink2.Count())
	}
}

// A background driver's failure is the same entry and the same count, logged
// at Debug with its origin (the #563 rule): a broken ref cannot flood the log
// from every refresh.
func TestIssue1564_BackgroundFailureIsCountedAtDebug(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	api := newF1564API(t)
	ctx, recs, _ := f1564Ctx(api, func(ctx context.Context) context.Context {
		return cache.WithBackgroundResolveOrigin(ctx, cache.BackgroundOriginRefresher)
	})
	before := FailedRefSnapshot()
	out, err := Resolve(ctx, []templatesv1.ResourceRef{f1564Good(),
		{ID: "bad", APIVersion: "bg1564.example.io/v1", Resource: "things", Name: "x", Namespace: "ns", Verb: "GET"}})
	if err != nil {
		t.Fatal(err)
	}
	assertFailureEntry(t, out, "bad", 424, "FailedDependency")
	if len(*recs) != 1 || (*recs)[0].level != slog.LevelDebug || (*recs)[0].attrs["origin"] != cache.BackgroundOriginRefresher {
		t.Fatalf("a background failure logs at Debug with its origin; got %+v", *recs)
	}
	if after := FailedRefSnapshot(); after[FailedRefTypeNotInstalled] != before[FailedRefTypeNotInstalled]+1 {
		t.Fatalf("a background failure is still counted: %v → %v", before, after)
	}
}
