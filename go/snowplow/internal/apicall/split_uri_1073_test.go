package apicall

// split_uri_1073_test.go — #1073 F1: SplitURI reads a rendered call path the
// way net/url does (the HTTP request snowplow sends is built with url.Parse,
// and a fragment is never sent), so the path and query snowplow decides on are
// the ones the apiserver receives.

import (
	"net/url"
	"testing"
)

func TestSplitURI_MatchesNetURL(t *testing.T) {
	cases := []string{
		"/apis/g/v1/widgets",
		"/apis/g/v1/widgets?",
		"/apis/g/v1/widgets?fieldSelector=metadata.name%3Dx",
		"/apis/g/v1/widgets?fieldSelector=metadata.name%3Dy#,metadata.name=x",
		"/apis/g/v1/widgets?fieldSelector=metadata.name%3Dx#frag",
		"/apis/g/v1/widgets#frag?fieldSelector=metadata.name%3Dx",
		"/apis/g/v1/widgets?a=1#b?c=2",
		"/apis/g/v1/widgets#",
		"/apis/g/v1/widgets?#",
		"/apis/g/v1/namespaces/ns/widgets/name#x",
		"/apis/g/v1/widgets?a=1&b=%zz",
	}
	for _, p := range cases {
		u, err := url.Parse(p)
		if err != nil {
			t.Fatalf("url.Parse(%q): %v", p, err)
		}
		path, rq, has := SplitURI(p)
		if rq != u.RawQuery {
			t.Errorf("SplitURI(%q) query = %q, net/url = %q", p, rq, u.RawQuery)
		}
		if has != (u.RawQuery != "" || u.ForceQuery) {
			t.Errorf("SplitURI(%q) hasQuery = %v, net/url RawQuery=%q ForceQuery=%v", p, has, u.RawQuery, u.ForceQuery)
		}
		if path != u.EscapedPath() {
			t.Errorf("SplitURI(%q) path = %q, net/url = %q", p, path, u.EscapedPath())
		}
	}
}

// TestParsePathAndPlanQuery_DropTheFragment: the coordinates and the planned
// selector of a rendered path never include a fragment.
func TestParsePathAndPlanQuery_DropTheFragment(t *testing.T) {
	_, ns, name, ok := ParsePath("/apis/g/v1/namespaces/ns/widgets/w#frag")
	if !ok || ns != "ns" || name != "w" {
		t.Fatalf("ParsePath with a fragment = (ns=%q, name=%q, ok=%v), want (ns, w, true)", ns, name, ok)
	}
	q := PlanQuery("/apis/g/v1/namespaces/ns/widgets?fieldSelector=metadata.name%3Dx#frag", true, true)
	if !q.Applicable || q.FieldStr != "metadata.name=x" {
		t.Fatalf("PlanQuery with a fragment = %+v, want the selector metadata.name=x", q)
	}
	q = PlanQuery("/apis/g/v1/namespaces/ns/widgets?fieldSelector=metadata.name%3Dy#,metadata.name=x", true, true)
	if !q.Applicable || q.FieldStr != "metadata.name=y" {
		t.Fatalf("PlanQuery %+v, want metadata.name=y only (the apiserver never sees ',metadata.name=x')", q)
	}
}
