package resourcesrefs

import (
	"net/url"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestBuildPath966_DeclaredSliceIsCanonical pins the child /call URL's
// pagination to the CANONICAL shape the dispatcher's paginationInfo
// normalizes a request to (#966). The browser requests exactly this URL and
// the prewarm walk derives the pagination it resolves and keys the child with
// from it, so a declared perPage-only slice must say page=1: with page=0 the
// browser's request normalizes to (perPage, 1) while the walk read "no
// pagination" and warmed the unpaginated cell instead.
func TestBuildPath966_DeclaredSliceIsCanonical(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "tables"}
	cases := []struct {
		name            string
		slice           *templatesv1.Slice
		page, perPage   string // "" = absent
		wantPaginatedQs bool
	}{
		{"perPage only means page 1", &templatesv1.Slice{PerPage: 50}, "1", "50", true},
		{"negative page means page 1", &templatesv1.Slice{Page: -3, PerPage: 50}, "1", "50", true},
		{"full declaration is kept", &templatesv1.Slice{Page: 3, PerPage: 50}, "3", "50", true},
		{"no positive perPage windows nothing", &templatesv1.Slice{Page: 2}, "", "", false},
		{"no slice", nil, "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := buildPath(gvr, &templatesv1.ResourceRef{Name: "compositions-table", Namespace: "krateo-compositions", Slice: tc.slice})
			u, err := url.Parse(p)
			if err != nil {
				t.Fatalf("buildPath produced an unparseable path %q: %v", p, err)
			}
			q := u.Query()
			if got := q.Get("page"); got != tc.page {
				t.Fatalf("#966: page=%q in %q, want %q — the declared window must be emitted in the canonical form paginationInfo normalizes the request to", got, p, tc.page)
			}
			if got := q.Get("perPage"); got != tc.perPage {
				t.Fatalf("#966: perPage=%q in %q, want %q", got, p, tc.perPage)
			}
			if q.Has("page") != tc.wantPaginatedQs || q.Has("perPage") != tc.wantPaginatedQs {
				t.Fatalf("#966: pagination params present=%v/%v in %q, want %v", q.Has("page"), q.Has("perPage"), p, tc.wantPaginatedQs)
			}
		})
	}
}
