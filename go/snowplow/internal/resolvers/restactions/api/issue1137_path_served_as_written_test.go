package api

// issue1137_path_served_as_written_test.go — #1137 (SECURITY, the #1136 residual).
//
// THE DEFECT. #1124 checks an object of a checked resource on that resource
// alone, and reads the resource from the stage's rendered path. Two path shapes
// were served as one resource and parsed as another, or as none:
//   - no leading "/": `apis/g/v/p2s` parses as nothing (servedOK false), but
//     the dial puts the "/" back and the apiserver serves p2s;
//   - "%2F" for "/": `/apis/g/v/namespaces%2Fns-a%2Fp2s` parses as one
//     resource named with escapes, but the apiserver decodes the path and
//     serves p2s in ns-a.
// Narrowing was skipped, the check fell back to OR across the set, and a
// caller who may list p1s in ns-a read p2 objects there. Cache on and off.
//
// THE RULE (servedAsWritten, in the #288 dial guard). An own-cluster path is
// planned only when the apiserver reads it exactly as it is written: the
// decoded path of the URL the dial builds (credhttp.RequestURL) equals the
// written path. Every reader of a planned call reads its path as written, so
// that is the only way all of them decide about the request served.
//
// THE APISERVER. i37APIServer reads every request with k8s.io/apiserver's own
// RequestInfoFactory (configured as kube-apiserver configures it), on the
// request Go's net/http server decoded, and answers with the fixture objects of
// the resource THAT names. So "what the apiserver serves" is the apiserver's
// code, not snowplow's parser. The RBAC oracle is #1124's i24Allows.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/rest"
)

// i37Coord is a request's coordinates: group, version, resource, namespace,
// name. ok=false: not a resource request.
type i37Coord struct {
	group, version, resource, ns, name string
	ok                                 bool
}

// i37Read is the apiserver's reading of a request: k8s.io/apiserver's
// RequestInfoFactory over the request as net/http received it.
func i37Read(r *http.Request) i37Coord {
	f := request.RequestInfoFactory{
		APIPrefixes:          sets.NewString("api", "apis"),
		GrouplessAPIPrefixes: sets.NewString("api"),
	}
	info, err := f.NewRequestInfo(r)
	if err != nil || !info.IsResourceRequest {
		return i37Coord{}
	}
	return i37Coord{info.APIGroup, info.APIVersion, info.Resource, info.Namespace, info.Name, true}
}

// i37Parse is snowplow's reading of a path as written.
func i37Parse(path string) i37Coord {
	gvr, ns, name, ok := cache.ParseAPIServerPathToDep(path)
	if !ok {
		return i37Coord{}
	}
	return i37Coord{gvr.Group, gvr.Version, gvr.Resource, ns, name, true}
}

type i37Log struct {
	mu   sync.Mutex
	seen []i37Coord
}

func (l *i37Log) add(c i37Coord) { l.mu.Lock(); l.seen = append(l.seen, c); l.mu.Unlock() }

func (l *i37Log) reset() { l.mu.Lock(); l.seen = nil; l.mu.Unlock() }

// compositions counts the requests the apiserver read as a composition.krateo.io
// resource: the ones that could serve the objects the arms check.
func (l *i37Log) compositions() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, c := range l.seen {
		if c.ok && c.group == i24Comp {
			n++
		}
	}
	return n
}

// i37APIServer serves the i24 fixture by the apiserver's own reading of each
// request: a LIST (cluster-wide or in the namespace) or a GET by name.
func i37APIServer(t *testing.T, log *i37Log) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := i37Read(r)
		log.add(c)
		if !c.ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		items := []any{}
		for _, o := range i24Objects() {
			if o.gvr.Group != c.group || o.gvr.Version != c.version || o.gvr.Resource != c.resource || (c.ns != "" && o.ns != c.ns) {
				continue
			}
			if c.name != "" {
				if o.name == c.name {
					_ = json.NewEncoder(w).Encode(o.object())
					return
				}
				continue
			}
			items = append(items, o.object())
		}
		if c.name != "" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
			return
		}
		gv := c.version
		if c.group != "" {
			gv = c.group + "/" + c.version
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": gv, "kind": "List",
			"metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// i37Shape is a path the apiserver serves as served while snowplow, reading it
// as written, does not.
type i37Shape struct {
	name, path string
	served     i37Coord // what the apiserver reads (asserted, not assumed)
	canonical  string   // the same request written as the apiserver reads it
}

var (
	i37P2      = i37Coord{i24Comp, i24Ver, "p2s", "", "", true}
	i37P2InA   = i37Coord{i24Comp, i24Ver, "p2s", "ns-a", "", true}
	i37P2Named = i37Coord{i24Comp, i24Ver, "p2s", "ns-a", "b-p2-in-ns-a", true}
)

var i37Shapes = []i37Shape{
	{"no leading slash, LIST", "apis/composition.krateo.io/v1-0-0/p2s", i37P2,
		"/apis/composition.krateo.io/v1-0-0/p2s"},
	{"no leading slash, namespaced LIST", "apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s", i37P2InA,
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s"},
	{"no leading slash, GET by name", "apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s/b-p2-in-ns-a", i37P2Named,
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s/b-p2-in-ns-a"},
	{"%2F for the separators", "/apis/composition.krateo.io/v1-0-0/namespaces%2Fns-a%2Fp2s", i37P2InA,
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s"},
	{"%2f, lower case", "/apis/composition.krateo.io/v1-0-0/namespaces%2fns-a%2fp2s", i37P2InA,
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s"},
	{"%2F before the name", "/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s%2Fb-p2-in-ns-a", i37P2Named,
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s/b-p2-in-ns-a"},
	{"an escaped letter", "/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p%32s", i37P2InA,
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s"},
	{"no leading slash and %2F", "apis/composition.krateo.io/v1-0-0/namespaces%2Fns-a%2Fp2s", i37P2InA,
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s"},
}

// i37Dial sends path through the resolver's own fetch (the dial an
// own-cluster stage uses) and returns what the apiserver read.
func i37Dial(t *testing.T, srv *httptest.Server, log *i37Log, path string) i37Coord {
	t.Helper()
	log.reset()
	opts := httpcall.RequestOptions{Endpoint: &endpoints.Endpoint{ServerURL: srv.URL, Token: "SA"}}
	opts.Path = path
	opts.Verb = ptr.To(http.MethodGet)
	_, _, _, _ = httpFetchAllowingNonJSON(context.Background(), opts)
	log.mu.Lock()
	defer log.mu.Unlock()
	if len(log.seen) != 1 {
		t.Fatalf("dial %q: the apiserver saw %d requests, want 1", path, len(log.seen))
	}
	return log.seen[0]
}

// TestIssue1137_TheApiserverReadsEachShapeAsAnotherPath measures the premise:
// dialled through snowplow's own fetch, the apiserver's RequestInfoFactory
// reads each shape as the composition resource it serves, while snowplow's
// parser reads it as something else or nothing. The canonical form of each is
// read the same way by both.
func TestIssue1137_TheApiserverReadsEachShapeAsAnotherPath(t *testing.T) {
	log := &i37Log{}
	srv := i37APIServer(t, log)
	for _, sh := range i37Shapes {
		t.Run(sh.name, func(t *testing.T) {
			got := i37Dial(t, srv, log, sh.path)
			if got != sh.served {
				t.Fatalf("the apiserver read %q as %+v, want %+v", sh.path, got, sh.served)
			}
			if p := i37Parse(sh.path); p == got {
				t.Fatalf("snowplow's parser reads %q as the apiserver does (%+v): not a divergent shape", sh.path, p)
			}
			if c := i37Dial(t, srv, log, sh.canonical); c != sh.served || i37Parse(sh.canonical) != c {
				t.Fatalf("canonical %q: apiserver %+v, parser %+v, want both %+v", sh.canonical, c, i37Parse(sh.canonical), sh.served)
			}
		})
	}
}

// i37Paths is the differential corpus: the shapes, their canonical forms, and
// the shapes the guard must keep (a query value may be escaped; a fragment is
// never sent; a trailing slash, a subresource).
func i37Paths() []string {
	out := []string{
		"/apis/composition.krateo.io/v1-0-0/p2s?limit=500",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s?labelSelector=a%3Db%2Cc",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s#frag",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s/",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s/b-p2-in-ns-a/status",
		"/api/v1/namespaces/ns-a/configmaps/cm",
		"/api/v1/namespaces/ns-a/configmaps?limit=1",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s%3Fx=",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s/b-p2-in-ns-a%2Fstatus",
		"api/v1/namespaces/ns-a/secrets/s",
		"/api/v1/namespaces/ns-a/secret%73/s",
	}
	for _, sh := range i37Shapes {
		out = append(out, sh.path, sh.canonical)
	}
	return out
}

// TestIssue1137_ServedAsWrittenKeepsOnlyWhatTheApiserverReadsAsWritten — the
// guard against the apiserver's own reading, both directions: every path the
// guard keeps is read by the apiserver with the coordinates snowplow's parser
// reads from it, and every path it refuses is read by the apiserver as
// different coordinates.
func TestIssue1137_ServedAsWrittenKeepsOnlyWhatTheApiserverReadsAsWritten(t *testing.T) {
	log := &i37Log{}
	srv := i37APIServer(t, log)
	kept, refused := 0, 0
	for _, p := range i37Paths() {
		read := i37Dial(t, srv, log, p)
		parsed := i37Parse(p)
		if servedAsWritten(p) {
			kept++
			if read != parsed {
				t.Errorf("kept %q: the apiserver reads %+v, snowplow %+v", p, read, parsed)
			}
			continue
		}
		refused++
		if read == parsed {
			t.Errorf("refused %q, yet the apiserver reads it as snowplow does (%+v)", p, read)
		}
	}
	if kept == 0 || refused == 0 {
		t.Fatalf("the corpus must hold both kept and refused paths: kept=%d refused=%d", kept, refused)
	}
	// The rule's own edges: an empty path is sent as the server URL itself, a
	// leading "//" is sent as written, an invalid escape cannot be sent.
	for p, want := range map[string]bool{"": true, "/": true, "//apis/x": true, "/apis/x%zz": false, "https://elsewhere/apis/x": false} {
		if got := servedAsWritten(p); got != want {
			t.Errorf("servedAsWritten(%q) = %v, want %v", p, got, want)
		}
	}
}

// i37Out is one resolve of the shape arms.
type i37Out struct {
	dict     map[string]any
	outcomes string
}

func i37Resolve(e *i24Env, arm i24Arm, user string, ra *templates.RESTAction, extras map[string]any, stages ...*templates.API) i37Out {
	t := e.t
	t.Helper()
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.SetGlobal(e.rw)
	if arm == i24Off {
		t.Setenv("CACHE_ENABLED", "false")
		cache.SetGlobal(nil)
	}
	defer cache.SetGlobal(e.rw)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	defer env.SetTestMode(prevTM)
	ctx := xcontext.BuildContext(ctxWithUser(user, "g-1124"),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: e.sa.URL, Token: "tok:" + user}))
	ctx, _ = withSinks681(ctx)
	ctx, oc := WithStageOutcomes(ctx)
	cp := make([]*templates.API, len(stages))
	for i, s := range stages {
		c := *s
		cp[i] = &c
	}
	dict, _ := ResolveWithFailure(ctx, ResolveOptions{
		RC:      &rest.Config{Host: e.sa.URL, BearerToken: "SA-RC-TOKEN"},
		AuthnNS: "krateo-system", Items: cp, Extras: extras,
		RESTActionNamespace: ra.Namespace, RESTActionName: ra.Name,
	})
	return i37Out{dict: dict, outcomes: oc.HeaderValue()}
}

// i37RA owns the shape arms' stage.
var i37RA = &templates.RESTAction{}

func init() { i37RA.Namespace, i37RA.Name = "krateo-system", "shape-1137" }

// i37UAFStage is an object stage under the two-plural set #1124 narrows.
func i37UAFStage(path string) *templates.API {
	return &templates.API{Name: "st", Path: path,
		UserAccessFilter: &templates.UserAccessFilterSpec{Verb: "list", Group: i24Comp,
			ResourcesFrom: `["p1s","p2s"]`, NamespaceFrom: ".metadata.namespace"}}
}

// i37Want is Kubernetes' answer for the objects the apiserver serves at c: the
// ones whose own plural the caller may list in their own namespace.
func i37Want(user string, c i37Coord) []string {
	rows := []string{}
	for _, o := range i24Compositions() {
		if o.gvr.Resource != c.resource || (c.ns != "" && o.ns != c.ns) || (c.name != "" && o.name != c.name) {
			continue
		}
		if i24Allows(user, "list", o.gvr.Group, o.gvr.Resource, o.ns) {
			rows = append(rows, o.name)
		}
	}
	sort.Strings(rows)
	return rows
}

func i37Norm(e *i24Env, dict map[string]any) string {
	b, _ := json.Marshal(dict)
	return strings.ReplaceAll(string(b), e.sa.URL, "SRV")
}

// TestIssue1137_EachShapeIsRefusedNeverCheckedOnTheWholeSet — for each shape,
// every caller, cache on and off: the call is refused when it is planned (no
// request reaches the apiserver, the stage fails InvalidPath, one
// path_not_as_served count per resolve), no composition object is returned,
// and cache on and off answer the same. The canonical form of the same request
// is narrowed: every caller's rows equal Kubernetes' answer, so A2 (p1s in
// ns-a) reads no p2 object, which the OR across the set would have kept.
func TestIssue1137_EachShapeIsRefusedNeverCheckedOnTheWholeSet(t *testing.T) {
	// Were a shape dialled (the rule absent: the mutation arms), the cache-on
	// path would register an informer for the resource it parses; the fake
	// client panics on an unregistered LIST, so register each one.
	var extra []schema.GroupVersionResource
	for _, sh := range i37Shapes {
		if g, _, _, ok := cache.ParseAPIServerPathToDep(sh.path); ok {
			extra = append(extra, g)
		}
	}
	e := newI24Env(t, extra...)
	log := &i37Log{}
	e.sa = i37APIServer(t, log)
	for _, sh := range i37Shapes {
		t.Run(sh.name, func(t *testing.T) {
			for _, u := range i24Users {
				outs := map[i24Arm]string{}
				for _, arm := range i24Arms {
					log.reset()
					before := MalformedDialSkippedByReason(reasonPathNotAsServed)
					o := i37Resolve(e, arm, u, i37RA, nil, i37UAFStage(sh.path))
					if n := log.compositions(); n != 0 {
						t.Errorf("%s %s: %d composition requests reached the apiserver, want 0 (refused when planned)", arm, u, n)
					}
					if d := MalformedDialSkippedByReason(reasonPathNotAsServed) - before; d != 1 {
						t.Errorf("%s %s: path_not_as_served moved by %d, want 1", arm, u, d)
					}
					if !strings.Contains(o.outcomes, `"name":"st"`) || !strings.Contains(o.outcomes, `"reason":"`+StageReasonInvalidPath+`"`) {
						t.Errorf("%s %s: stage outcomes %s, want st InvalidPath", arm, u, o.outcomes)
					}
					if rows := i24Rows(i37Norm(e, o.dict)); len(rows) != 0 {
						t.Errorf("%s %s: returned %v from a refused call", arm, u, rows)
					}
					outs[arm] = i37Norm(e, o.dict)
				}
				if outs[i24Content] != outs[i24Off] {
					t.Errorf("%s: cache on and off disagree:\n on  %s\n off %s", u, outs[i24Content], outs[i24Off])
				}

				// The same request, written as the apiserver reads it.
				want := i37Want(u, sh.served)
				for _, arm := range i24Arms {
					o := i37Resolve(e, arm, u, i37RA, nil, i37UAFStage(sh.canonical))
					if rows := i24Rows(i37Norm(e, o.dict)); strings.Join(rows, ",") != strings.Join(want, ",") {
						t.Errorf("%s %s canonical %q: rows %v, want %v (Kubernetes' answer)", arm, u, sh.canonical, rows, want)
					}
				}
			}
		})
	}
}

// TestIssue1137_PortalRESTActionsPlanNoRefusal — the vendored portal
// RESTActions (testdata/issue1124_portal_uaf_ras.yaml) write every path as the
// apiserver reads it. Run the way the #1124 arms run them, for every caller,
// cache on and off: compositions-list and projects end to end,
// composition-detail end to end for every composition, and the namespaces
// stages of blueprint-formdef and agent-formdef. The rule refuses nothing and
// no stage reports InvalidPath. The rule only refuses, so a run in which it
// refuses nothing is the run of before; the #1124 arms pin those outputs to the
// RBAC oracle. The shape arm shows this counter moves when the rule fires.
func TestIssue1137_PortalRESTActionsPlanNoRefusal(t *testing.T) {
	e := newI24Env(t)
	type run struct {
		ra     *templates.RESTAction
		extras map[string]any
		stages []*templates.API
	}
	var runs []run
	for _, name := range []string{"compositions-list", "projects"} {
		ra := i24RA(t, name)
		runs = append(runs, run{ra, nil, ra.Spec.API})
	}
	detail := i24RA(t, "composition-detail")
	for _, o := range i24Compositions() {
		runs = append(runs, run{detail, map[string]any{"namespace": o.ns, "name": o.name}, detail.Spec.API})
	}
	bp := i24RA(t, "blueprint-formdef")
	bpNS := *i24Stage(t, bp, "namespaces")
	bpNS.DependsOn = nil
	runs = append(runs, run{bp, map[string]any{"compdef": map[string]any{"status": map[string]any{"resource": "p1s"}}}, []*templates.API{&bpNS}})
	ag := i24RA(t, "agent-formdef")
	agNS := *i24Stage(t, ag, "namespaces")
	agNS.DependsOn = nil
	runs = append(runs, run{ag, nil, []*templates.API{&agNS}})

	resolves := 0
	for _, r := range runs {
		for _, arm := range i24Arms {
			for _, u := range i24Users {
				before := MalformedDialSkippedByReason(reasonPathNotAsServed)
				o := i37Resolve(e, arm, u, r.ra, r.extras, r.stages...)
				resolves++
				if d := MalformedDialSkippedByReason(reasonPathNotAsServed) - before; d != 0 {
					t.Errorf("%s %s %s extras=%v: the rule refused %d planned calls of a portal RESTAction", r.ra.Name, arm, u, r.extras, d)
				}
				if strings.Contains(o.outcomes, StageReasonInvalidPath) {
					t.Errorf("%s %s %s extras=%v: a stage reported InvalidPath: %s", r.ra.Name, arm, u, r.extras, o.outcomes)
				}
			}
		}
	}
	if resolves == 0 {
		t.Fatal("no portal resolve ran")
	}
}
