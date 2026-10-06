package api

import (
	"context"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// inspect520 drives the /rbac boundary core-provider uses — InspectReadSet(ctx,
// ra, nil), empty dict, no extras — for one single-stage RESTAction path.
func inspect520(t *testing.T, path string) ([]Resource, []Unresolved) {
	t.Helper()
	ra := &templates.RESTAction{
		Spec: templates.RESTActionSpec{
			API: []*templates.API{{Name: "s", Path: path}},
		},
	}
	rows, unresolved, err := InspectReadSet(context.Background(), ra, nil)
	if err != nil {
		t.Fatalf("path %q: InspectReadSet errored: %v", path, err)
	}
	return rows, unresolved
}

// TestIssue520_TemplatedQueryOnALiteralPathEnumerates is THE falsifier for #520.
//
// cache.ReadSetSkeleton computed `hasTemplate := strings.Contains(path, "${")`
// over the whole raw path, so a template that landed in the QUERY counted as a
// collapsed RESOURCE segment and inspect.go:399 turned the discovery branch's
// 200 into a 422. The query is not part of the read coordinate: every other
// consumer in that function, and ParseAPIServerDiscoveryPath /
// ParseAPIServerPathToDep / validInterpolatedPath beside it, strip at '?' before
// classifying.
//
// THE SHAPES ARE THE WRAPPED IDIOM ON PURPOSE. #520 names
// `/apis/apps/v1/?labelSelector=${ .sel }` — the EMBEDDED form — but that form
// is not renderable at all: jqutil.MaybeQuery returns the contents of the first
// `${...}` and discards the literal context (plumbing v1.14.2 jqutil.go:90-114),
// so it renders to `null` here and is refused one branch further down
// (inspect.go:408, "not an enumerable in-cluster apiserver path") no matter what
// the skeleton says. The wrapped form `${ "..." + .v }` is the only idiom that
// renders to a path, so it is the only one whose ResourceTemplated verdict is
// observable at /rbac — which is why the arm drives these and not the issue's
// literal text.
//
// RED on origin/main (856bc952): all three templated shapes come back
// "renders as bare discovery but the stage's RESOURCE segment is templated".
func TestIssue520_TemplatedQueryOnALiteralPathEnumerates(t *testing.T) {
	withInspectSARESTConfig(t, discovery504(t))

	for _, tc := range []struct {
		name string
		path string
	}{
		// THE defect: a literal trailing-slash coordinate, template in the query.
		{"group discovery, trailing slash, templated query", `${ "/apis/apps/v1/?labelSelector=" + (.sel // "") }`},
		{"core discovery, trailing slash, templated query", `${ "/api/v1/?labelSelector=" + (.sel // "") }`},
		// A query VALUE shaped like a path must not smuggle the template back
		// into the coordinate: everything after the first '?' is query.
		{"templated query whose value looks like a path", `${ "/apis/apps/v1/?continue=" + (.tok // "") }`},
		// No interpolation AT ALL — a constant jq string, which carried "${" and
		// so counted as a template. Same 422, one step further out.
		{"constant wrapped string with a literal query", `${ "/apis/apps/v1/?labelSelector=app%3Dweb" }`},
		// The control: no template anywhere. Green before and after the fix — it
		// is here so a future change that breaks the query strip itself (rather
		// than the hasTemplate input) also fails.
		{"plain literal path with a literal query", `/apis/apps/v1/?labelSelector=app%3Dweb`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, unresolved := inspect520(t, tc.path)
			if len(unresolved) != 0 {
				t.Errorf("#520: a template that landed in the QUERY must not make the literal "+
					"coordinate of %q look like a collapsed RESOURCE — the query is not part of "+
					"the read coordinate; got unresolved=%+v", tc.path, unresolved)
			}
			if len(rows) != 0 {
				t.Errorf("path %q: a discovery path contributes no read-set rows; got %+v", tc.path, rows)
			}
		})
	}
}

// TestIssue520_TheQueryStripDoesNotBlindTheCollapsedResourceDetector is the
// other direction, and the reason #520 is a narrowing of hasTemplate rather than
// a deletion of it.
//
// ResourceTemplated exists to fail LOUD on a template whose RESOURCE segment
// interpolated away: that render is byte-identical to a bare discovery path, so
// swallowing it is a silent under-grant (zero rows, 200, a minted Role, then a
// 403 at the first /call). Ignoring the templates that landed in the query must
// not cost that — a stage that collapses its resource AND carries a query still
// has an undetermined coordinate.
//
// An arm that can only fail in one direction is not coverage for the other,
// which is precisely how #515 F1, and then this residual, shipped.
func TestIssue520_TheQueryStripDoesNotBlindTheCollapsedResourceDetector(t *testing.T) {
	withInspectSARESTConfig(t, discovery504(t))

	for _, tc := range []struct {
		name string
		path string
	}{
		{"collapsed resource, no query (the #504 ruling-3 shape)", `${ "/apis/apps/v1/" + (.kind // "") }`},
		{"collapsed resource with a query appended", `${ "/apis/apps/v1/" + (.kind // "") + "?labelSelector=x" }`},
		{"collapsed resource, core group, with a query appended", `${ "/api/v1/" + (.kind // "") + "?labelSelector=x" }`},
		{"collapsed resource, single-literal wrapped form", `${ "/apis/apps/v1/" + .kind }`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, unresolved := inspect520(t, tc.path)
			if len(unresolved) == 0 {
				t.Errorf("#504 ruling 3 REGRESSION: a collapsed RESOURCE must stay UNRESOLVABLE "+
					"even when the stage carries a query; path %q gave zero-rows-200 (rows=%+v)",
					tc.path, rows)
			}
		})
	}
}

// TestIssue520_TemplatedQueryOnAStaticResourcePathStillEnumerates pins the read
// that must keep WORKING, not just the one that must keep failing.
//
// A templated query on a path with a real resource segment never reaches the
// ResourceTemplated discriminator at all (the resource is static), so it is the
// shape most likely to be broken by a careless "reject anything carrying ${" fix
// — the mistake #515 F1 already made once in the other direction.
func TestIssue520_TemplatedQueryOnAStaticResourcePathStillEnumerates(t *testing.T) {
	withInspectSARESTConfig(t, discovery504(t))

	for _, tc := range []struct {
		name string
		path string
		want Resource
	}{
		{
			name: "collection read with a templated label selector",
			path: `${ "/apis/apps/v1/deployments?labelSelector=" + (.sel // "") }`,
			want: Resource{Group: "apps", Version: "v1", Resource: "deployments", Verb: "list"},
		},
		{
			name: "core collection read with a templated label selector",
			path: `${ "/api/v1/configmaps?labelSelector=" + (.sel // "") }`,
			want: Resource{Group: "", Version: "v1", Resource: "configmaps", Verb: "list"},
		},
		{
			// Ruling 1 still holds alongside a query: the namespace collapsed,
			// so the row says unknown rather than inventing cluster scope.
			name: "collapsed namespace, static resource, templated query",
			path: `${ "/apis/apps/v1/namespaces/" + (.ns // "") + "/deployments?labelSelector=" + (.sel // "") }`,
			want: Resource{Group: "apps", Version: "v1", Resource: "deployments", NamespaceUnknown: true, Verb: "list"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, unresolved := inspect520(t, tc.path)
			if len(unresolved) != 0 {
				t.Fatalf("path %q must enumerate: the query is not part of the read coordinate; "+
					"got unresolved=%+v", tc.path, unresolved)
			}
			if len(rows) != 1 || rows[0] != tc.want {
				t.Errorf("path %q:\n got  %+v\n want [%+v]", tc.path, rows, tc.want)
			}
		})
	}
}
