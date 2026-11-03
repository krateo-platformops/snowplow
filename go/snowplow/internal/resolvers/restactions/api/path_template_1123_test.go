package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/apicall"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/request"
)

// #1123 — the plan builder (the one path render every dial, the cluster-list
// probe and inspect go through) never plans an injected path, and the path it
// plans is the path that is sent.

func plan1123(t *testing.T, path string, dict map[string]any) ([]httpcall.RequestOptions, []skippedOption) {
	t.Helper()
	in := &templates.API{Name: "s", Path: path}
	opts, skips, err := createRequestOptionsWithSkips(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), in, dict)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return opts, skips
}

// Each measured payload, as caller extras on an external and on an apiserver
// path: never planned, counted under path_value_refused.
func TestIssue1123_PlanNeverDialsAnInjectedPath(t *testing.T) {
	cases := []struct {
		name, path, id string
	}{
		{"external dotdot", `${ "/api/v2/tickets/" + .id }`, "42/../../users/me"},
		{"external slash", `${ "/tickets/" + .id + "/comments" }`, "42/../../users/me"},
		{"external dot", `${ "/tickets/" + .id + "/comments" }`, ".."},
		{"apiserver query", `${ "/api/v1/namespaces/demo/configmaps/" + .id }`, "x?role=admin"},
		{"apiserver encoded slash", `${ "/apis/apps/v1/namespaces/demo/deployments/" + .id }`, "x%2F..%2F..%2Fsecrets"},
		{"apiserver fragment", `${ "/api/v1/namespaces/" + .id + "/configmaps" }`, "demo#"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := MalformedDialSkippedByReason(reasonPathValueRefused)
			opts, skips := plan1123(t, c.path, map[string]any{"id": c.id})
			if len(opts) != 0 {
				t.Fatalf("planned %q", opts[0].Path)
			}
			if len(skips) != 1 || skips[0].reason.class != reasonPathValueRefused {
				t.Fatalf("skips = %+v, want one %s", skips, reasonPathValueRefused)
			}
			if len(c.id) > 2 && (strings.Contains(skips[0].path, c.id) || strings.Contains(skips[0].reason.detail, c.id)) {
				t.Fatalf("the refusal echoes the caller's value: %+v", skips[0])
			}
			if got := MalformedDialSkippedByReason(reasonPathValueRefused) - before; got != 1 {
				t.Fatalf("path_value_refused moved by %d, want 1", got)
			}
		})
	}
	// On an external endpoint a '?' in a value is escaped into its segment and
	// the call goes out (one segment, the template's resource).
	opts, _ := plan1123(t, `${ "/tickets/" + .id + "/comments" }`, map[string]any{"id": "42?role=admin"})
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
