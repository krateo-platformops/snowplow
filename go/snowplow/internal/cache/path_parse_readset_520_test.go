// path_parse_readset_520_test.go — the first DIRECT table over
// ReadSetSkeleton's ResourceTemplated discriminator (#520).
//
// Until now every arm on this function drove it through InspectReadSet in
// resolvers/restactions/api, which is part of why two successive mistakes on ONE
// predicate shipped: the #515 F1 blocker (a literal path flagged as a collapsed
// resource) and then #520, the same requirement reading a string that still had
// the query attached.
//
// ResourceTemplated is load-bearing in exactly one direction each way:
//   - true on a path whose RESOURCE coordinate is LITERAL -> a 422 for a read
//     the apiserver answers 200 (#515 F1, #520).
//   - false on a COLLAPSED resource -> zero rows, a 200, a minted Role and a 403
//     at the first /call: the silent under-grant inspect.go forbids (#504
//     ruling 3).
//
// So every case below states BOTH what must be flagged and what must not.
//
// NOTE on idioms. Two template shapes reach this function, and only ONE of them
// can be rendered by the dispatcher: jqutil.MaybeQuery returns the contents of
// the first `${...}` and DISCARDS the literal context around it (plumbing
// v1.14.2 jqutil.go:90-114), so the EMBEDDED form `/apis/apps/v1/${ .kind }`
// renders to just the .kind value, never to a path. The WRAPPED form
// `${ "/apis/apps/v1/" + .kind }` is the only renderable author idiom, and
// therefore the only one whose verdict here is observable at the /rbac boundary.
// Both are covered: the embedded cases pin the documented contract of
// skeletonizeTemplatedPath, the wrapped cases pin what /rbac actually answers.

package cache

import "testing"

func TestReadSetSkeleton_ResourceTemplatedIgnoresTheQuery(t *testing.T) {
	cases := []struct {
		name string
		path string
		// wantResourceTemplated is the discriminator under test: "the author
		// wrote a template whose RESOURCE segment collapsed away".
		wantResourceTemplated bool
		wantOK                bool
		wantResource          string
	}{
		// ---- #520: A TEMPLATE THAT LANDED IN THE QUERY IS NOT A COLLAPSED
		// RESOURCE. The renderable (wrapped) idiom first: these are the shapes
		// that 422'd a 200 at /rbac.
		{
			name: "wrapped: templated query on a literal trailing-slash group path",
			path: `${ "/apis/apps/v1/?labelSelector=" + (.sel // "") }`,
		},
		{
			name: "wrapped: templated query on a literal trailing-slash core path",
			path: `${ "/api/v1/?labelSelector=" + (.sel // "") }`,
		},
		{
			// No interpolation AT ALL — a constant jq string. It carried "${"
			// and so counted as a template, which is the same mistake one step
			// further out.
			name: "wrapped: constant string, literal query, no interpolation",
			path: `${ "/apis/apps/v1/?labelSelector=app%3Dweb" }`,
		},
		{
			name: "embedded: templated query on a literal trailing-slash group path",
			path: `/apis/apps/v1/?labelSelector=${ .sel }`,
		},
		{
			name: "embedded: templated query on a literal trailing-slash core path",
			path: `/api/v1/?labelSelector=${ .sel }`,
		},
		{
			// A query VALUE shaped like a path must not smuggle the template
			// back into the coordinate: everything after the first '?' is query,
			// whatever it looks like.
			name: "embedded: templated query whose value looks like a path",
			path: `/apis/apps/v1/?continue=/apis/apps/v1/${ .tok }`,
		},
		{
			name: "literal path, literal query, no template anywhere",
			path: `/apis/apps/v1/?labelSelector=app%3Dweb`,
		},
		{
			// The #515 F1 shape, kept here so the two live side by side.
			name: "literal trailing slash, no query at all",
			path: `/apis/apps/v1/`,
		},

		// ---- THE OTHER DIRECTION: these MUST still be flagged. ----
		{
			name:                  "collapsed resource, no query (the #504 ruling-3 shape)",
			path:                  `${ "/apis/apps/v1/" + (.kind // "") }`,
			wantResourceTemplated: true,
		},
		{
			// The query strip must not blind the detector when the RESOURCE is
			// genuinely templated AND a query is present: the template landed
			// BEFORE the '?', which the skeleton records.
			name:                  "collapsed resource with a query appended",
			path:                  `${ "/apis/apps/v1/" + (.kind // "") + "?labelSelector=x" }`,
			wantResourceTemplated: true,
		},
		{
			name:                  "collapsed resource, core group, with a query appended",
			path:                  `${ "/api/v1/" + (.kind // "") + "?labelSelector=x" }`,
			wantResourceTemplated: true,
		},
		{
			name:                  "collapsed resource, embedded form, with a query",
			path:                  `/apis/apps/v1/${ .kind }?labelSelector=x`,
			wantResourceTemplated: true,
		},
		{
			name:                  "collapsed resource, embedded form, core group, with a query",
			path:                  `/api/v1/${ .kind }?labelSelector=x`,
			wantResourceTemplated: true,
		},
		{
			// A single-literal wrapped expression loses its trailing
			// sub-expression entirely in the skeleton (Join of one element
			// inserts no sentinel), which is why the no-query fallback must stay
			// on the RAW path.
			name:                  "collapsed resource, single-literal wrapped form, no query",
			path:                  `${ "/apis/apps/v1/" + .kind }`,
			wantResourceTemplated: true,
		},

		// ---- AND THE READS THAT MUST KEEP RESOLVING. ----
		{
			// A templated query on a path with a REAL resource segment never
			// reaches the discriminator (the resource is static) and must keep
			// enumerating: the query is not part of the read coordinate.
			name:         "wrapped: templated query on a static resource path",
			path:         `${ "/apis/apps/v1/deployments?labelSelector=" + (.sel // "") }`,
			wantOK:       true,
			wantResource: "deployments",
		},
		{
			name:         "wrapped: templated query on a static core resource path",
			path:         `${ "/api/v1/configmaps?labelSelector=" + (.sel // "") }`,
			wantOK:       true,
			wantResource: "configmaps",
		},
		{
			name:         "embedded: templated query on a static resource path",
			path:         `/apis/apps/v1/deployments?labelSelector=${ .sel }`,
			wantOK:       true,
			wantResource: "deployments",
		},
		{
			name:         "wrapped: templated namespace, static resource, templated query",
			path:         `${ "/apis/apps/v1/namespaces/" + (.ns // "") + "/deployments?labelSelector=" + (.sel // "") }`,
			wantOK:       true,
			wantResource: "deployments",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ReadSetSkeleton(tc.path)
			if got.ResourceTemplated != tc.wantResourceTemplated {
				t.Errorf("ReadSetSkeleton(%q).ResourceTemplated = %v, want %v\n"+
					"  true on a literal resource coordinate 422s a read the apiserver answers 200 (#520/#515 F1);\n"+
					"  false on a collapsed resource is a silent under-grant (#504 ruling 3).\n"+
					"  full coord: %+v", tc.path, got.ResourceTemplated, tc.wantResourceTemplated, got)
			}
			if got.OK != tc.wantOK {
				t.Errorf("ReadSetSkeleton(%q).OK = %v, want %v (coord %+v)", tc.path, got.OK, tc.wantOK, got)
			}
			if got.GVR.Resource != tc.wantResource {
				t.Errorf("ReadSetSkeleton(%q).GVR.Resource = %q, want %q", tc.path, got.GVR.Resource, tc.wantResource)
			}
		})
	}
}
