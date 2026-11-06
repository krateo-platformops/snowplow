package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/apicall"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/workclass"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/request"
)

// #1123 — the plan builder (the one path render every dial, the cluster-list
// probe and inspect go through) never plans an injected path, and the path it
// plans is the path that is sent.

// plan1123 plans a stage that dials this cluster's apiserver.
func plan1123(t *testing.T, path string, dict map[string]any) ([]httpcall.RequestOptions, []skippedOption) {
	t.Helper()
	return plan1123On(t, true, path, dict)
}

// plan1123On plans a stage that dials this cluster's apiserver (apiserver=true)
// or a named endpoint (#1112 D2: only the former gets the shape guard).
func plan1123On(t *testing.T, apiserver bool, path string, dict map[string]any) ([]httpcall.RequestOptions, []skippedOption) {
	t.Helper()
	in := &templates.API{Name: "s", Path: path}
	opts, skips, err := createRequestOptionsWithSkips(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), in, dict, apiserver)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return opts, skips
}

// Each measured payload, as caller extras on an external and on an apiserver
// path: never planned, counted under path_value_refused.
func TestIssue1123_PlanNeverDialsAnInjectedPath(t *testing.T) {
	cases := []struct {
		name      string
		apiserver bool
		path, id  string
	}{
		{"external dotdot", false, `${ "/api/v2/tickets/" + .id }`, "42/../../users/me"},
		{"external slash", false, `${ "/tickets/" + .id + "/comments" }`, "42/../../users/me"},
		{"external dot", false, `${ "/tickets/" + .id + "/comments" }`, ".."},
		{"apiserver query", true, `${ "/api/v1/namespaces/demo/configmaps/" + .id }`, "x?role=admin"},
		{"apiserver encoded slash", true, `${ "/apis/apps/v1/namespaces/demo/deployments/" + .id }`, "x%2F..%2F..%2Fsecrets"},
		{"apiserver fragment", true, `${ "/api/v1/namespaces/" + .id + "/configmaps" }`, "demo#"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := MalformedDialSkippedByReason(reasonPathValueRefused)
			var line bytes.Buffer
			opts, skips, err := createRequestOptionsWithSkips(context.Background(),
				slog.New(slog.NewTextHandler(&line, &slog.HandlerOptions{Level: slog.LevelDebug})),
				&templates.API{Name: "s", Path: c.path}, map[string]any{"id": c.id}, c.apiserver)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if len(opts) != 0 {
				t.Fatalf("planned %q", opts[0].Path)
			}
			if len(skips) != 1 || skips[0].reason.class != reasonPathValueRefused {
				t.Fatalf("skips = %+v, want one %s", skips, reasonPathValueRefused)
			}
			// The value in every form the refused path could hold it: as sent,
			// and escaped, which is how the '%' branch rendered it (#1185 item C).
			if !strings.Contains(line.String(), "reason_class="+reasonPathValueRefused) {
				t.Fatalf("no skip line to read: %s", line.String())
			}
			for _, form := range []string{c.id, url.PathEscape(c.id)} {
				if len(c.id) > 2 && (strings.Contains(skips[0].reason.detail, form) || strings.Contains(line.String(), form)) {
					t.Fatalf("the refusal echoes the caller's value as %q: %+v\n%s", form, skips[0], line.String())
				}
			}
			if got := MalformedDialSkippedByReason(reasonPathValueRefused) - before; got != 1 {
				t.Fatalf("path_value_refused moved by %d, want 1", got)
			}
		})
	}
	// On an external endpoint a '?' in a value is escaped into its segment and
	// the call goes out (one segment, the template's resource).
	opts, _ := plan1123On(t, false, `${ "/tickets/" + .id + "/comments" }`, map[string]any{"id": "42?role=admin"})
	if len(opts) != 1 || opts[0].Path != "/tickets/42%3Frole=admin/comments" {
		t.Fatalf("external escape: %+v", opts)
	}
}

// The blueprint-detail case (#1116): the as-written path is refused, the
// declared form plans the resource the refilter checks.
func TestIssue1123_BlueprintDetailPlan(t *testing.T) {
	attack := map[string]any{"apiVersion": "apps/v1/deployments?x=", "resource": "p1s"}
	if opts, _ := plan1123(t, `${ "/apis/" + .apiVersion + "/" + .resource }`, attack); len(opts) != 0 {
		t.Fatalf("as written: planned %q", opts[0].Path)
	}
	// segments() is NOT the form for an apiVersion: the escaped '?' lands in
	// an apiserver path and is refused before any dial; a third segment would
	// still be a different resource, which is why apiVersion is split.
	if opts, _ := plan1123(t, `${ "/apis/" + segments(.apiVersion) + "/" + .resource }`, attack); len(opts) != 0 {
		t.Fatalf("segments(apiVersion): planned %q", opts[0].Path)
	}
	opts, _ := plan1123(t, `${ (.apiVersion | split("/")) as [$g, $v] | "/apis/" + $g + "/" + $v + "/" + .resource }`, attack)
	if len(opts) != 1 {
		t.Fatalf("declared form planned %d", len(opts))
	}
	gvr, _, name, ok := cache.ParseAPIServerPathToDep(opts[0].Path)
	if !ok || gvr.Group != "apps" || gvr.Version != "v1" || gvr.Resource != "p1s" || name != "" {
		t.Fatalf("declared form %q parses to %v name=%q", opts[0].Path, gvr, name)
	}
}

// The cluster-list probe renders through the same function as the dial: an
// injected element is not a collapsible list, a clean one probes the GVR the
// dial would read.
func TestIssue1123_ClusterListProbeRendersLikeTheDial(t *testing.T) {
	tmpl := `${ "/apis/composition.krateo.io/v1-2-0/namespaces/" + .ns + "/fireworksapps" }`
	if _, _, ok := clusterListElementTarget(tmpl, map[string]any{"ns": "a/../../../apis/apps/v1"}); ok {
		t.Fatal("an injected element probed as a list")
	}
	// Rendered as written, this element names ANOTHER resource that passes the
	// dial guard (/namespaces/demo/secrets, the '#' cutting the template's own
	// resource off): only the one render keeps the probe off it.
	if gvr, _, ok := clusterListElementTarget(tmpl, map[string]any{"ns": "demo/secrets#"}); ok {
		t.Fatalf("an element that renames the resource probed as a list of %v", gvr)
	}
	if _, _, ok := clusterListElementTarget(tmpl, map[string]any{"ns": "a?labelSelector=x"}); ok {
		t.Fatal("an escaped element probed as a list")
	}
	gvr, _, ok := clusterListElementTarget(tmpl, map[string]any{"ns": "demo"})
	opts, _ := plan1123(t, tmpl, map[string]any{"ns": "demo"})
	want, _, _, _ := cache.ParseAPIServerPathToDep(opts[0].Path)
	if !ok || gvr != want {
		t.Fatalf("probe %v ok=%v, dial parses to %v", gvr, ok, want)
	}
}

// Checked path == sent path. For every input, the path snowplow plans (and
// that every parser reads: the #288 guard, the cache key, RBAC) is byte for
// byte the request URI the server receives; for an apiserver path, the
// apiserver's own request-info (which reads the DECODED path) names the same
// namespace, resource and name snowplow parsed.
func TestIssue1123_CheckedPathIsSentPath(t *testing.T) {
	var mu sync.Mutex
	var got []*http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, r.Clone(context.Background()))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	tmpls := []string{
		`${ "/api/v1/namespaces/" + .ns + "/configmaps/" + .id }`,
		`${ "/apis/apps/v1/namespaces/" + .ns + "/deployments/" + .id }`,
		`${ "/apis/" + segments(.gv) + "/namespaces/" + .ns + "/deployments" }`,
		`${ "/api/v1/namespaces/" + .ns + "/pods?labelSelector=app%3D" + .id }`,
		`${ "/api/v1/namespaces/" + .ns + "/configmaps?fieldSelector=metadata.name%3D" + .id }`,
		`${ "/tickets/" + .id + "/comments" }`,
		`${ "/search?q=" + .id + "&role=viewer" }`,
		`${ "/search?q=" + (.id | @uri) + "&role=viewer" }`,
	}
	ids := []string{"web", "42/../../users/me", "x?role=admin", "x#f", "%2e%2e", "x%2Fy", "a b", "x&role=admin", "system:x", ".."}
	f := request.RequestInfoFactory{APIPrefixes: sets.NewString("api", "apis"), GrouplessAPIPrefixes: sets.NewString("api")}
	sent := 0
	for _, tmpl := range tmpls {
		for _, id := range ids {
			opts, _ := plan1123(t, tmpl, map[string]any{"ns": "demo", "id": id, "gv": "apps/v1"})
			for _, o := range opts {
				o.Endpoint = &endpoints.Endpoint{ServerURL: srv.URL}
				o.Verb = ptr.To(http.MethodGet)
				o.ResponseHandler = func(rc io.ReadCloser) error { _, err := io.Copy(io.Discard, rc); return err }
				mu.Lock()
				n := len(got)
				mu.Unlock()
				if st := httpcall.Do(context.Background(), o); st != nil && st.Code >= 400 {
					t.Fatalf("dial %q: %+v", o.Path, st)
				}
				mu.Lock()
				r := got[n]
				mu.Unlock()
				sent++
				if r.RequestURI != o.Path {
					t.Errorf("checked %q, sent %q", o.Path, r.RequestURI)
				}
				gvr, ns, name, ok := cache.ParseAPIServerPathToDep(o.Path)
				if !ok {
					continue
				}
				p, _, _ := apicall.SplitURI(o.Path)
				if r.URL.Path != p {
					t.Errorf("apiserver path %q: the server decodes it to %q", p, r.URL.Path)
				}
				info, err := f.NewRequestInfo(r)
				if err != nil {
					t.Fatal(err)
				}
				// A list's Name is its field selector's (#1049, rbac.CollectionRead),
				// not a path coordinate; the path coordinates must agree.
				pathName := info.Name
				if info.Verb == "list" {
					pathName = ""
				}
				if info.Resource != gvr.Resource || info.Namespace != ns || pathName != name || info.APIGroup != gvr.Group {
					t.Errorf("%q: snowplow checks %v ns=%q name=%q; the apiserver serves %s/%s ns=%q name=%q",
						o.Path, gvr, ns, name, info.APIGroup, info.Resource, info.Namespace, info.Name)
				}
			}
		}
	}
	if sent < 15 {
		t.Fatalf("only %d calls were planned; the arm does not exercise the dial", sent)
	}
}

// A template that renders no path names nothing to call: it is refused on every
// stage, as the #288 collapse at its limit, and counted as empty_interp. The
// values that get there are null, missing and "": what a nil-extras prewarm
// renders for a path read from extras.
func TestIssue1123_TemplateThatRendersNoPathIsRefused(t *testing.T) {
	cases := []struct {
		name, path string
		dict       map[string]any
	}{
		{"segments of a missing value", `${ segments(.path) }`, map[string]any{}},
		{"segments of null", `${ segments(.path) }`, map[string]any{"path": nil}},
		{"segments of the empty string", `${ segments(.path) }`, map[string]any{"path": ""}},
		{"segments with nil extras", `${ segments(.path) }`, nil},
		{"a whole value that is empty", `${ .path // "" }`, map[string]any{}},
		{"no output", `${ empty }`, map[string]any{}},
		{"an empty literal", `${ "" }`, map[string]any{}},
	}
	for _, apiserver := range []bool{true, false} {
		for _, c := range cases {
			t.Run(fmt.Sprintf("apiserver=%v/%s", apiserver, c.name), func(t *testing.T) {
				before := MalformedDialSkippedByReason(reasonEmptyInterp)
				opts, skips := plan1123On(t, apiserver, c.path, c.dict)
				if len(opts) != 0 {
					t.Fatalf("planned %q", opts[0].Path)
				}
				if len(skips) != 1 || skips[0].reason.class != reasonEmptyInterp || !skips[0].reason.collapsed {
					t.Fatalf("skips = %+v, want one collapsed %s", skips, reasonEmptyInterp)
				}
				if got := MalformedDialSkippedByReason(reasonEmptyInterp) - before; got != 1 {
					t.Fatalf("empty_interp moved by %d, want 1", got)
				}
			})
		}
	}

	// What the rule leaves alone: a stage with no path has no template.
	if opts, skips := plan1123On(t, false, "", map[string]any{}); len(opts) != 1 || len(skips) != 0 {
		t.Fatalf("a stage with no path: planned %d, skips %+v", len(opts), skips)
	}
	if opts, skips := plan1123On(t, false, `${ segments(.p) }`, map[string]any{"p": "/x/y"}); len(opts) != 1 || opts[0].Path != "/x/y" {
		t.Fatalf("a present value: planned %+v, skips %+v", opts, skips)
	}
}

// The other segments() edges, as a stage sees them: an inner empty piece is an
// empty segment (#1185), a non-string is a template error.
func TestIssue1123_SegmentsEdgesAtThePlan(t *testing.T) {
	for _, apiserver := range []bool{true, false} {
		opts, skips := plan1123On(t, apiserver, `${ "/x/" + segments(.p) }`, map[string]any{"p": "a//b"})
		if len(opts) != 0 || len(skips) != 1 || skips[0].reason.class != reasonEmptyInterp || !skips[0].reason.collapsed {
			t.Fatalf("an inner empty piece, apiserver=%v: planned %+v, skips %+v", apiserver, opts, skips)
		}
	}
	// Both verdicts refuse a collapsed namespace, whichever way it collapsed.
	for _, apiserver := range []bool{true, false} {
		for _, p := range []any{"", nil} {
			opts, skips := plan1123On(t, apiserver, `${ "/api/v1/namespaces/" + segments(.p) + "/pods" }`, map[string]any{"p": p})
			if len(opts) != 0 || len(skips) != 1 || skips[0].reason.class != reasonEmptyInterp {
				t.Fatalf("apiserver=%v p=%v: planned %+v, skips %+v", apiserver, p, opts, skips)
			}
		}
	}
	for _, v := range []any{42, true, []any{"a"}, map[string]any{"a": "b"}} {
		opts, skips := plan1123On(t, false, `${ "/x/" + segments(.p) }`, map[string]any{"p": v})
		if len(opts) != 0 || len(skips) != 1 || skips[0].reason.class != reasonJQPathError {
			t.Fatalf("p=%v: planned %+v, skips %+v, want one %s", v, opts, skips, reasonJQPathError)
		}
	}
}

// Through a whole resolve, with no extras: nothing reaches the endpoint, and
// the stage reports why (#1116) on a customer read and on a prewarm alike.
func TestIssue1123_AllNullPathFailsItsStageAndDialsNothing(t *testing.T) {
	stage := func(coe bool) *templates.API {
		return &templates.API{Name: "obj", Verb: ptr.To(http.MethodGet), ErrorKey: ptr.To("objErr"),
			ContinueOnError: ptr.To(coe), Path: `${ segments(.path) }`}
	}
	api := newX1106Srv(t, false, x1106OK)
	x1106Env(t, api.srv.URL, map[string]map[string]string{})
	for _, class := range []workclass.Class{workclass.None, workclass.Prewarm} {
		t.Run(fmt.Sprintf("class=%v", class), func(t *testing.T) {
			o := rr16Resolve(t, class, nil, stage(false))
			if hits := api.all(); len(hits) != 0 {
				t.Fatalf("dialled: %+v", hits)
			}
			x1106WantReason(t, o.outcomes, "obj", StageReasonInvalidPath)
			rr16ErrRecord(t, o, "objErr", StageReasonInvalidPath)
			if o.failure == nil || o.failure.Stage != "obj" || o.failure.Code != http.StatusBadRequest {
				t.Fatalf("StageFailure = %+v, want the stage's 400", o.failure)
			}
			if o.sinkN == 0 {
				t.Fatal("the stage-error sink was not bumped: a Put would store a body around the missing read")
			}
			if _, set := o.dict["obj"]; set {
				t.Fatalf("the refused stage wrote its key: %v", o.dict["obj"])
			}
			o = rr16Resolve(t, class, nil, stage(true))
			x1106WantReason(t, o.outcomes, "obj", StageReasonInvalidPath)
			if o.failure != nil {
				t.Fatalf("continueOnError: StageFailure = %+v, want none", o.failure)
			}
		})
	}
}

// A named endpoint, and an iterator: one null element is one refused item, the
// other elements are sent, and an iterator that yields nothing has nothing to
// call and succeeds (the form howto/restactions.md gives for an absent value).
func TestIssue1123_AllNullPathOnANamedEndpointAndAnIterator(t *testing.T) {
	iterated := func(it string) *templates.API {
		s := u904Stage("obj", http.MethodGet, `${ segments(.path) }`)
		s.DependsOn = &templates.Dependency{Iterator: ptr.To(it)}
		return u904Continue(s)
	}
	sent := func(u *u904Upstream) int {
		u.mu.Lock()
		defer u.mu.Unlock()
		n := 0
		for _, v := range u.count {
			n += v
		}
		return n
	}

	u := u904NewUpstream(t, nil)
	r := u904Resolve(t, u, []*templates.API{u904Stage("obj", http.MethodGet, `${ segments(.path) }`)}, false)
	if n := sent(u); n != 0 {
		t.Fatalf("a named endpoint was dialled %d time(s): %v", n, u.count)
	}
	r.wantRow(t, u904Row{Name: "obj", Reason: StageReasonInvalidPath})

	u = u904NewUpstream(t, nil)
	r = u904Resolve(t, u, []*templates.API{iterated(`[{path: null}, {path: "/x"}]`)}, false)
	if n, x := sent(u), u.n(http.MethodGet, "/x"); n != 1 || x != 1 {
		t.Fatalf("sent %d, /x %d: want only /x, once", n, x)
	}
	r.wantRow(t, u904Row{Name: "obj", Reason: StageReasonInvalidPath})

	u = u904NewUpstream(t, nil)
	r = u904Resolve(t, u, []*templates.API{iterated(`[ {path: .path} | select(.path != null) ]`)}, false)
	if n := sent(u); n != 0 {
		t.Fatalf("an empty iterator dialled %d time(s)", n)
	}
	r.wantSucceeded(t, "obj")
}

// #1116 meets #1123: a value the path template refuses fails its stage as an
// InvalidPath 400, read or write, like every other refused path. The message
// names the rule, never the value.
func TestIssue1123_RefusedValueFailsItsStageAsInvalidPath(t *testing.T) {
	const payload = "42/../../users/me"
	api := newX1106Srv(t, false, x1106OK)
	x1106Env(t, api.srv.URL, map[string]map[string]string{})
	for _, verb := range []string{http.MethodGet, http.MethodPut} {
		t.Run(verb, func(t *testing.T) {
			before := MalformedDialSkippedByReason(reasonPathValueRefused)
			o := rr16Resolve(t, workclass.None, map[string]any{"id": payload}, &templates.API{Name: "s", Verb: ptr.To(verb),
				ErrorKey: ptr.To("sErr"), Path: `${ "/api/v1/namespaces/demo/configmaps/" + .id }`})
			if hits := api.all(); len(hits) != 0 {
				t.Fatalf("dialled: %+v", hits)
			}
			x1106WantReason(t, o.outcomes, "s", StageReasonInvalidPath)
			if o.failure == nil || o.failure.Code != http.StatusBadRequest {
				t.Fatalf("StageFailure = %+v, want a 400", o.failure)
			}
			errs, _ := o.dict["sErr"].([]any)
			if len(errs) != 1 {
				t.Fatalf("errorKey = %v, want one record", o.dict["sErr"])
			}
			m, _ := errs[0].(map[string]any)
			msg, _ := m["message"].(string)
			if m["reason"] != StageReasonInvalidPath || statusCodeOf(m["code"]) != http.StatusBadRequest ||
				!strings.Contains(msg, "(#1123)") || strings.Contains(msg, payload) {
				t.Fatalf("errorKey record = %v", m)
			}
			if got := MalformedDialSkippedByReason(reasonPathValueRefused) - before; got != 1 {
				t.Fatalf("path_value_refused moved by %d, want 1", got)
			}
		})
	}
}

// The total is every reason's: two PRs each added a class to this sum.
func TestIssue1123_TheTotalCountsEveryReason(t *testing.T) {
	for _, reason := range MalformedDialReasonEnum() {
		before := MalformedDialSkippedTotal()
		bumpMalformedDialSkipped(reason)
		if got := MalformedDialSkippedTotal() - before; got != 1 {
			t.Errorf("%s moved the total by %d, want 1", reason, got)
		}
	}
}
