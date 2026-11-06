package api

// issue1137_path_served_as_written_test.go — #1137 (SECURITY, the #1136 residual).
//
// THE DEFECT. #1124 checks an object of a checked resource on that resource
// alone, and reads the resource from the stage's rendered path. Path shapes
// were served as one resource and parsed as another, or as none:
//   - no leading "/": `apis/g/v/p2s` parses as nothing (servedOK false), but
//     the dial puts the "/" back and the apiserver serves p2s;
//   - "%2F" for "/": `/apis/g/v/namespaces%2Fns-a%2Fp2s` parses as one
//     resource named with escapes, but the apiserver decodes the path and
//     serves p2s in ns-a;
//   - an empty or "." segment: `/apis/g/v/namespaces/ns-a/./p2s` parses as
//     resource ".", but the refresher's internal-rest-config dial asks
//     client-go for it, and client-go's path.Join cleans it into a LIST of p2s;
//   - a leading "//": `//api/v1/namespaces/ns-a/secrets` parses as nothing (so
//     #398 does not mark it), while the apiserver's RequestInfo reads secrets;
//   - a verb written in the path: `/apis/g/v/watch/namespaces/ns-a/p2s` parses
//     as resource "watch", but the apiserver strips the verb and streams a
//     watch of p2s in ns-a.
// Narrowing was skipped, the check fell back to OR across the set, and a
// caller who may list p1s in ns-a read p2 objects there. Cache on and off.
//
// THE RULE (servedAsWritten, in the #288 dial guard). An own-cluster path is
// planned only when nothing rewrites it on the way to a resource: the decoded
// path of the URL the HTTP dial builds (credhttp.RequestURL) equals the written
// path, path cleaning leaves it as it is (no empty, "." or ".." segment; trailing
// "/"s are not segments), and the apiserver has no verb to strip after the
// version (RequestInfoFactory's specialVerbs). Every reader of a planned call
// reads its path as written, so that is the only way all of them decide about
// the request served.
//
// THE APISERVER. i37APIServer reads every request with k8s.io/apiserver's own
// RequestInfoFactory (configured as kube-apiserver configures it), on the
// request Go's net/http server decoded, and answers with the fixture objects of
// the resource THAT names. It routes as kube-apiserver v0.36.5 routes (the
// handler director and PathRecorderMux match the decoded path against "/api/"
// and "/apis/"): any other path is answered 404, though it is still read and
// logged. A watch is answered as a watch, a stream of events, and `proxy/` has
// no route (apiserver's installer.go), so it is read and answered 404. So "what
// the apiserver serves" is the apiserver's code, not snowplow's parser. The RBAC
// oracle is #1124's i24Allows.

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
	"github.com/krateo-platformops/snowplow/internal/credhttp"
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

// i37Info is k8s.io/apiserver's RequestInfoFactory, configured as kube-apiserver
// configures it, over the request as net/http received it.
func i37Info(r *http.Request) (*request.RequestInfo, error) {
	f := request.RequestInfoFactory{
		APIPrefixes:          sets.NewString("api", "apis"),
		GrouplessAPIPrefixes: sets.NewString("api"),
	}
	return f.NewRequestInfo(r)
}

// i37Read is the apiserver's reading of a request.
func i37Read(r *http.Request) i37Coord {
	info, err := i37Info(r)
	if err != nil || !info.IsResourceRequest {
		return i37Coord{}
	}
	return i37Coord{info.APIGroup, info.APIVersion, info.Resource, info.Namespace, info.Name, true}
}

// i37Verb is the verb the apiserver reads from a request.
func i37Verb(r *http.Request) string {
	info, err := i37Info(r)
	if err != nil {
		return ""
	}
	return info.Verb
}

// i37Routed is kube-apiserver's routing of a decoded request path: only the
// "/api/" and "/apis/" prefixes reach a resource handler.
func i37Routed(p string) bool {
	return strings.HasPrefix(p, "/api/") || strings.HasPrefix(p, "/apis/")
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

func (l *i37Log) count() int { l.mu.Lock(); defer l.mu.Unlock(); return len(l.seen) }

// i37APIServer serves the i24 fixture by the apiserver's own reading of each
// routed request: a LIST (cluster-wide or in the namespace), a GET by name, or a
// WATCH of either, which streams one ADDED event per object.
func i37APIServer(t *testing.T, log *i37Log) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := i37Read(r)
		log.add(c)
		verb := i37Verb(r)
		if !c.ok || !i37Routed(r.URL.Path) || verb == "proxy" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if verb == "watch" {
			enc := json.NewEncoder(w)
			for _, o := range i24Objects() {
				if o.gvr.Group == c.group && o.gvr.Version == c.version && o.gvr.Resource == c.resource &&
					(c.ns == "" || o.ns == c.ns) && (c.name == "" || o.name == c.name) {
					_ = enc.Encode(map[string]any{"type": "ADDED", "object": o.object()})
				}
			}
			return
		}
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

// i37Via is a dial an own-cluster call can take.
type i37Via string

const (
	// i37HTTP is the resolver's own fetch (httpFetchConverting), the dial of
	// a live own-cluster stage.
	i37HTTP i37Via = "http fetch"
	// i37ClientGo is the internal-rest-config branch (the refresher's dial):
	// client-go on the parsed coordinates.
	i37ClientGo i37Via = "internal rest config"
)

// i37Shape is a path the apiserver reads, on the dial via, as served, while
// snowplow, reading it as written, does not.
type i37Shape struct {
	name, path string
	via        i37Via
	served     i37Coord // what the apiserver reads (asserted, not assumed)
	canonical  string   // the same request written as the apiserver reads it
}

var (
	i37P2      = i37Coord{i24Comp, i24Ver, "p2s", "", "", true}
	i37P2InA   = i37Coord{i24Comp, i24Ver, "p2s", "ns-a", "", true}
	i37P2Named = i37Coord{i24Comp, i24Ver, "p2s", "ns-a", "b-p2-in-ns-a", true}
	i37Secrets = i37Coord{"", "v1", "secrets", "ns-a", "", true}
)

const (
	i37LP2    = "/apis/composition.krateo.io/v1-0-0/p2s"
	i37LP2InA = "/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s"
	i37GP2    = "/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s/b-p2-in-ns-a"
)

var i37Shapes = []i37Shape{
	{"no leading slash, LIST", "apis/composition.krateo.io/v1-0-0/p2s", i37HTTP, i37P2, i37LP2},
	{"no leading slash, namespaced LIST", "apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s", i37HTTP, i37P2InA, i37LP2InA},
	{"no leading slash, GET by name", "apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s/b-p2-in-ns-a", i37HTTP, i37P2Named, i37GP2},
	{"%2F for the separators", "/apis/composition.krateo.io/v1-0-0/namespaces%2Fns-a%2Fp2s", i37HTTP, i37P2InA, i37LP2InA},
	{"%2f, lower case", "/apis/composition.krateo.io/v1-0-0/namespaces%2fns-a%2fp2s", i37HTTP, i37P2InA, i37LP2InA},
	{"%2F before the name", "/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s%2Fb-p2-in-ns-a", i37HTTP, i37P2Named, i37GP2},
	{"an escaped letter", "/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p%32s", i37HTTP, i37P2InA, i37LP2InA},
	{"no leading slash and %2F", "apis/composition.krateo.io/v1-0-0/namespaces%2Fns-a%2Fp2s", i37HTTP, i37P2InA, i37LP2InA},
	{"a leading //, LIST", "//apis/composition.krateo.io/v1-0-0/p2s", i37HTTP, i37P2, i37LP2},
	{"a leading //, Secrets", "//api/v1/namespaces/ns-a/secrets", i37HTTP, i37Secrets, "/api/v1/namespaces/ns-a/secrets"},
	{"a . segment, namespaced LIST", "/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/./p2s", i37ClientGo, i37P2InA, i37LP2InA},
	{"an empty segment, namespaced LIST", "/apis/composition.krateo.io/v1-0-0/namespaces/ns-a//p2s", i37ClientGo, i37P2InA, i37LP2InA},
	{"a . segment, LIST", "/apis/composition.krateo.io/v1-0-0/./p2s", i37ClientGo, i37P2, i37LP2},
	{"an empty segment, LIST", "/apis/composition.krateo.io/v1-0-0//p2s", i37ClientGo, i37P2, i37LP2},
	{"a . segment before a name", "/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/./p2s/b-p2-in-ns-a", i37ClientGo, i37P2InA, i37LP2InA},
	{"watch/, namespaced", "/apis/composition.krateo.io/v1-0-0/watch/namespaces/ns-a/p2s?timeoutSeconds=1", i37HTTP, i37P2InA, i37LP2InA},
	{"watch/, cluster-wide", "/apis/composition.krateo.io/v1-0-0/watch/p2s?timeoutSeconds=1", i37HTTP, i37P2, i37LP2},
	{"watch/, one object", "/apis/composition.krateo.io/v1-0-0/watch/namespaces/ns-a/p2s/b-p2-in-ns-a", i37HTTP, i37P2Named, i37GP2},
	{"watch/, Secrets", "/api/v1/watch/namespaces/ns-a/secrets", i37HTTP, i37Secrets, "/api/v1/namespaces/ns-a/secrets"},
	{"proxy/", "/apis/composition.krateo.io/v1-0-0/proxy/namespaces/ns-a/p2s", i37HTTP, i37P2InA, i37LP2InA},
}

// i37Percent is the shapes the #1123 percent check refuses first: an apiserver
// path, parsed, that holds a percent-escape. That check runs before
// servedAsWritten, so they count under path_value_refused. A path no parser
// reads ("no leading slash and %2F") is not here: it stays this rule's.
var i37Percent = map[string]bool{
	"%2F for the separators": true,
	"%2f, lower case":        true,
	"%2F before the name":    true,
	"an escaped letter":      true,
}

// class is the counter a refusal of the shape moves.
func (s i37Shape) class() string {
	if i37Percent[s.name] {
		return reasonPathValueRefused
	}
	return reasonPathNotAsServed
}

// i37Dial sends path through the dial via and returns what the apiserver read;
// sent=false when that dial sent nothing (the internal-rest-config branch
// answers only a path it parses).
func i37Dial(t *testing.T, srv *httptest.Server, log *i37Log, via i37Via, path string) (read i37Coord, sent bool) {
	t.Helper()
	log.reset()
	opts := httpcall.RequestOptions{Endpoint: &endpoints.Endpoint{ServerURL: srv.URL, Token: "SA"}}
	opts.Path = path
	opts.Verb = ptr.To(http.MethodGet)
	switch via {
	case i37HTTP:
		_, _, _, _ = httpFetchAllowingNonJSON(context.Background(), opts)
	case i37ClientGo:
		resetInternalClientCacheForTest()
		// The refresher's ctx: this cluster's ServiceAccount rest config on a
		// held-credential (own-cluster) stage, identity-free so the branch
		// serves without a re-gate.
		ctx := cache.WithInternalRESTConfig(credhttp.WithHeldCredentialValue(context.Background(), true),
			&rest.Config{Host: srv.URL, BearerToken: "SA"})
		_, _, _ = dispatchViaInternalRESTConfig(ctx, opts)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	switch len(log.seen) {
	case 0:
		return i37Coord{}, false
	case 1:
		return log.seen[0], true
	}
	t.Fatalf("%s %q: the apiserver saw %d requests, want at most 1", via, path, len(log.seen))
	return i37Coord{}, false
}

// TestIssue1137_TheApiserverReadsEachShapeAsAnotherPath measures the premise:
// on the dial each shape names, the apiserver's RequestInfoFactory reads it as
// the resource it serves, while snowplow's parser reads it as something else or
// nothing. The canonical form of each is read the same way by both, on both
// dials.
func TestIssue1137_TheApiserverReadsEachShapeAsAnotherPath(t *testing.T) {
	t.Cleanup(resetInternalClientCacheForTest)
	log := &i37Log{}
	srv := i37APIServer(t, log)
	for _, sh := range i37Shapes {
		t.Run(sh.name, func(t *testing.T) {
			got, sent := i37Dial(t, srv, log, sh.via, sh.path)
			if !sent || got != sh.served {
				t.Fatalf("%s: the apiserver read %q as %+v (sent=%v), want %+v", sh.via, sh.path, got, sent, sh.served)
			}
			if p := i37Parse(sh.path); p == got {
				t.Fatalf("snowplow's parser reads %q as the apiserver does (%+v): not a divergent shape", sh.path, p)
			}
			for _, via := range []i37Via{i37HTTP, i37ClientGo} {
				if c, sent := i37Dial(t, srv, log, via, sh.canonical); !sent || c != sh.served || i37Parse(sh.canonical) != c {
					t.Fatalf("canonical %q via %s: apiserver %+v (sent=%v), parser %+v, want both %+v", sh.canonical, via, c, sent, i37Parse(sh.canonical), sh.served)
				}
			}
		})
	}
}

// i37Paths is the differential corpus: the shapes, their canonical forms, and
// the shapes the guard must keep (a query value may be escaped; a fragment is
// never sent; a trailing slash, a subresource). It holds only paths whose
// coordinates snowplow's parser and the apiserver agree on when the path is
// served as written: a few such paths read differently for reasons that have
// nothing to do with how the path is written (`/api/v1/namespaces/ns-a/status`,
// `/apis/<g>/<v>/namespaces/ns-a`), a parser divergence #1118 owns.
func i37Paths() []string {
	out := []string{
		"/apis/composition.krateo.io/v1-0-0/p2s?limit=500",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s?labelSelector=a%3Db%2Cc",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s?labelSelector=x/../y",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s#frag",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s/",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s/b-p2-in-ns-a/status",
		"/api/v1/namespaces/ns-a/configmaps/cm",
		"/api/v1/namespaces/ns-a/configmaps?limit=1",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s%3Fx=",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s/b-p2-in-ns-a%2Fstatus",
		"api/v1/namespaces/ns-a/secrets/s",
		"/api/v1/namespaces/ns-a/secret%73/s",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s//",
		// A verb is stripped only right after the version. These are read as
		// written: a proxy subresource, a namespace or object named like a verb,
		// and a watch asked for in the query.
		"/api/v1/nodes/n/proxy/metrics",
		"/api/v1/namespaces/ns-a/pods/p/proxy/a/b",
		"/api/v1/namespaces/watch/configmaps",
		"/api/v1/namespaces/ns-a/configmaps/watch",
		"/apis/composition.krateo.io/v1-0-0/namespaces/ns-a/p2s?watch=true",
	}
	for _, sh := range i37Shapes {
		out = append(out, sh.path, sh.canonical)
	}
	return out
}

// TestIssue1137_ServedAsWrittenKeepsOnlyWhatTheApiserverReadsAsWritten — the
// guard against the apiserver's own reading over the corpus, both dials, both
// directions: every path the guard keeps is read by the apiserver with the
// coordinates snowplow's parser reads from it, on each dial that sends it; and
// every path it refuses is read differently on at least one dial.
func TestIssue1137_ServedAsWrittenKeepsOnlyWhatTheApiserverReadsAsWritten(t *testing.T) {
	t.Cleanup(resetInternalClientCacheForTest)
	log := &i37Log{}
	srv := i37APIServer(t, log)
	kept, refused := 0, 0
	for _, p := range i37Paths() {
		parsed := i37Parse(p)
		same := true
		for _, via := range []i37Via{i37HTTP, i37ClientGo} {
			if read, sent := i37Dial(t, srv, log, via, p); sent && read != parsed {
				same = false
				if servedAsWritten(p) {
					t.Errorf("kept %q: via %s the apiserver reads %+v, snowplow %+v", p, via, read, parsed)
				}
			}
		}
		if servedAsWritten(p) {
			kept++
			continue
		}
		refused++
		if same {
			t.Errorf("refused %q, yet the apiserver reads it as snowplow does on every dial (%+v)", p, parsed)
		}
	}
	if kept == 0 || refused == 0 {
		t.Fatalf("the corpus must hold both kept and refused paths: kept=%d refused=%d", kept, refused)
	}
	// The rule's own edges: an empty path is sent as the server URL itself;
	// trailing "/"s are read without them; an invalid escape or a control byte
	// cannot be sent, in the query either; a query or fragment alone is sent as
	// the server root, which is not what is written; a ".." segment is resolved
	// by path cleaning (and is never a valid name); a verb is stripped only in
	// the first segment after the version.
	for p, want := range map[string]bool{
		"": true, "/": true, "/apis/x/": true, "/apis/x//": true,
		"//apis/x": false, "///apis/x": false, "/apis//x": false, "/apis/./x": false, "/apis/x/../y": false, "/apis/x/y/..": false,
		"/apis/x%zz": false, "https://elsewhere/apis/x": false,
		"?x=1": false, "#f": false, "?": false, "#": false, "/apis/x?a=\x01": false, "/apis/x?\n": false,
		"/apis/g/v/watch/x": false, "/apis/g/v/proxy/x": false, "/api/v1/watch/x": false, "/api/v1/proxy/x": false,
		"/apis/g/v/watch": false, "/apis/g/v/proxy/": false, "/api/v1/watch": false, "/api/v1/proxy/": false,
		"/apis/g/watch": true, "/apis/g/watch/x": true, "/api/watch/x": true, "/apis/watch": true, "/watch/x": true,
		"/apis/watch/v1/x": true, "/apis/g/v/Watch/x": true, "/api/v1/PROXY/x": true,
		"/apis/g/v/x/watch": true, "/api/v1/x/proxy": true,
	} {
		if got := servedAsWritten(p); got != want {
			t.Errorf("servedAsWritten(%q) = %v, want %v", p, got, want)
		}
	}
}

// TestIssue1137_TheRuleRefusesExactlyTheVerbsTheApiserverStrips — the verb half
// of the rule against the apiserver's own code, so the list is not a guess: for
// every word Kubernetes uses as a verb, a subresource or a path keyword, in the
// first segment after the version, of both API roots, RequestInfoFactory reads
// that word as the verb exactly when the rule refuses the path. A vendored
// apiserver that strips another of these words turns this red.
func TestIssue1137_TheRuleRefusesExactlyTheVerbsTheApiserverStrips(t *testing.T) {
	words := []string{"watch", "proxy", "get", "list", "create", "update", "patch", "delete", "deletecollection",
		"log", "exec", "attach", "portforward", "status", "scale", "finalize", "binding", "eviction", "token",
		"redirect", "connect", "impersonate", "bind", "escalate", "namespaces", "resources", "p2s", "Watch", "PROXY"}
	stripped := 0
	for _, w := range words {
		for _, root := range []string{"/apis/g/v/", "/api/v1/"} {
			p := root + w + "/namespaces/ns-a/p2s"
			r, err := http.NewRequest(http.MethodGet, "http://apiserver.invalid"+p, nil)
			if err != nil {
				t.Fatal(err)
			}
			info, err := i37Info(r)
			if err != nil {
				t.Fatal(err)
			}
			strips := info.Verb == w && info.Resource == "p2s"
			if strips {
				stripped++
			}
			if got := servedAsWritten(p); got == strips {
				t.Errorf("%q: the apiserver strips the verb: %v; servedAsWritten: %v", p, strips, got)
			}
		}
	}
	if stripped != 4 {
		t.Fatalf("%d (word, root) pairs were stripped, want 4 (watch and proxy on both roots)", stripped)
	}
}

// i37Out is one resolve of the shape arms.
type i37Out struct {
	dict     map[string]any
	outcomes string
}

// i37Mode is how a resolve runs: the cache arm, and whether it is the
// refresher's (the internal rest config on the ctx, under the caller's
// identity) or a live request's.
type i37Mode struct {
	arm       i24Arm
	refresher bool
}

func (m i37Mode) String() string {
	if m.refresher {
		return string(m.arm) + "/refresher"
	}
	return string(m.arm) + "/live"
}

var i37Modes = []i37Mode{{i24Content, false}, {i24Off, false}, {i24Content, true}, {i24Off, true}}

func i37Resolve(e *i24Env, m i37Mode, user string, ra *templates.RESTAction, extras map[string]any, stages ...*templates.API) i37Out {
	t := e.t
	t.Helper()
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	resetInternalClientCacheForTest()
	cache.SetGlobal(e.rw)
	if m.arm == i24Off {
		t.Setenv("CACHE_ENABLED", "false")
		cache.SetGlobal(nil)
	}
	defer cache.SetGlobal(e.rw)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	defer env.SetTestMode(prevTM)
	ctx := xcontext.BuildContext(ctxWithUser(user, "g-1124"),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: e.sa.URL, Token: "tok:" + user}))
	if m.refresher {
		ctx = cache.WithInternalRESTConfig(ctx, &rest.Config{Host: e.sa.URL, BearerToken: "SA-IC-TOKEN"})
		ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: e.sa.URL})
	}
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
		if o.gvr.Group != c.group || o.gvr.Resource != c.resource || (c.ns != "" && o.ns != c.ns) || (c.name != "" && o.name != c.name) {
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
// every caller, cache on and off, live and on the refresher's
// internal-rest-config branch: the call is refused when it is planned (no
// request reaches the apiserver, the stage fails InvalidPath, one count per
// resolve in the shape's class), no composition object is returned,
// and every mode answers the same. The canonical form of the same request is
// narrowed in every mode: every caller's rows equal Kubernetes' answer, so A2
// (p1s in ns-a) reads no p2 object, which the OR across the set would have kept.
func TestIssue1137_EachShapeIsRefusedNeverCheckedOnTheWholeSet(t *testing.T) {
	t.Cleanup(resetInternalClientCacheForTest)
	// Were a shape dialled (the rule absent: the mutation arms), the cache-on
	// path would register an informer for the resource it parses; the fake
	// client panics on an unregistered LIST, so register each one.
	var extra []schema.GroupVersionResource
	for _, sh := range i37Shapes {
		for _, p := range []string{sh.path, sh.canonical} {
			if g, _, _, ok := cache.ParseAPIServerPathToDep(p); ok {
				extra = append(extra, g)
			}
		}
	}
	e := newI24Env(t, extra...)
	log := &i37Log{}
	e.sa = i37APIServer(t, log)
	for _, sh := range i37Shapes {
		t.Run(sh.name, func(t *testing.T) {
			for _, u := range i24Users {
				outs := map[i37Mode]string{}
				for _, m := range i37Modes {
					log.reset()
					before := MalformedDialSkippedByReason(sh.class())
					o := i37Resolve(e, m, u, i37RA, nil, i37UAFStage(sh.path))
					if n := log.count(); n != 0 {
						t.Errorf("%s %s: %d requests reached the apiserver, want 0 (refused when planned)", m, u, n)
					}
					if d := MalformedDialSkippedByReason(sh.class()) - before; d != 1 {
						t.Errorf("%s %s: %s moved by %d, want 1", m, u, sh.class(), d)
					}
					if !strings.Contains(o.outcomes, `"name":"st"`) || !strings.Contains(o.outcomes, `"reason":"`+StageReasonInvalidPath+`"`) {
						t.Errorf("%s %s: stage outcomes %s, want st InvalidPath", m, u, o.outcomes)
					}
					if rows := i24Rows(i37Norm(e, o.dict)); len(rows) != 0 {
						t.Errorf("%s %s: returned %v from a refused call", m, u, rows)
					}
					outs[m] = i37Norm(e, o.dict)
				}
				for _, m := range i37Modes[1:] {
					if outs[m] != outs[i37Modes[0]] {
						t.Errorf("%s: %s and %s disagree:\n %s\n %s", u, i37Modes[0], m, outs[i37Modes[0]], outs[m])
					}
				}

				// The same request, written as the apiserver reads it. Not in the
				// cache-off refresher mode, which production never runs (the
				// refresher needs the cache): there the internal-rest-config
				// branch re-gates a cluster-scope LIST at collection scope and
				// answers Forbidden to a caller with namespaced grants only, a
				// property of that branch, not of this rule.
				want := i37Want(u, sh.served)
				for _, m := range i37Modes {
					if m.refresher && m.arm == i24Off {
						continue
					}
					o := i37Resolve(e, m, u, i37RA, nil, i37UAFStage(sh.canonical))
					if rows := i24Rows(i37Norm(e, o.dict)); strings.Join(rows, ",") != strings.Join(want, ",") {
						t.Errorf("%s %s canonical %q: rows %v, want %v (Kubernetes' answer)", m, u, sh.canonical, rows, want)
					}
				}
			}
		})
	}
}

// TestIssue1137_NamedEndpointPathsAreStillSent — the rule is this cluster's
// apiserver's (#1112 D2 split): a named endpoint is another API, which may need
// an escaped separator in an id, and its paths are sent as before.
func TestIssue1137_NamedEndpointPathsAreStillSent(t *testing.T) {
	ep := newX1106Srv(t, false, x1106OK)
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL)})
	for p, want := range map[string]string{
		"/api/v2/items/a%2Fb": "/api/v2/items/a/b", // the server decodes it, as any server does
		"api/v2/items/a":      "/api/v2/items/a",
		"/api/v2/items//a":    "/api/v2/items//a",
		"/api/v2/items/./a":   "/api/v2/items/./a",
		"//api/v2/items/a":    "//api/v2/items/a", // only one leading "/" is the join's
		"/apis/g/v/watch/x":   "/apis/g/v/watch/x",
	} {
		before := len(ep.all())
		o := x1106Resolve(t, nil, x1106Stage("items", http.MethodGet, p))
		hits := ep.all()
		if n := len(hits) - before; n != 1 {
			t.Errorf("named endpoint %q: %d requests sent, want 1", p, n)
			continue
		}
		if got := hits[len(hits)-1].Path; got != want {
			t.Errorf("named endpoint %q: the server received %q, want %q", p, got, want)
		}
		if strings.Contains(o.outcomes, StageReasonInvalidPath) {
			t.Errorf("named endpoint %q: refused: %s", p, o.outcomes)
		}
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
		for _, m := range i37Modes {
			for _, u := range i24Users {
				before := MalformedDialSkippedByReason(reasonPathNotAsServed)
				o := i37Resolve(e, m, u, r.ra, r.extras, r.stages...)
				resolves++
				if d := MalformedDialSkippedByReason(reasonPathNotAsServed) - before; d != 0 {
					t.Errorf("%s %s %s extras=%v: the rule refused %d planned calls of a portal RESTAction", r.ra.Name, m, u, r.extras, d)
				}
				if strings.Contains(o.outcomes, StageReasonInvalidPath) {
					t.Errorf("%s %s %s extras=%v: a stage reported InvalidPath: %s", r.ra.Name, m, u, r.extras, o.outcomes)
				}
			}
		}
	}
	if resolves == 0 {
		t.Fatal("no portal resolve ran")
	}
}
