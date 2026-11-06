package redact

import (
	"strings"
	"testing"
)

// TestIssue1138_PathRedactsAReferenceAndKeepsItsShape — a stage's rendered
// path is logged by URL's rules (#1138): every query value replaced, the
// fragment dropped, nothing holding an "@". The path itself, the diagnostic, is
// kept. A string that is not origin-relative is URL's to render.
func TestIssue1138_PathRedactsAReferenceAndKeepsItsShape(t *testing.T) {
	const canary = "qval-canary-zq"
	for _, tc := range []struct{ raw, want string }{
		{"/api/v1/namespaces/team-a/pods/web-", "/api/v1/namespaces/team-a/pods/web-"},
		{"/apis/g.example.io/v1/namespaces//things/x", "/apis/g.example.io/v1/namespaces//things/x"},
		{"/api/v2/tickets?token=" + canary + "&page=2", "/api/v2/tickets?page=" + URLRedactedValue + "&token=" + URLRedactedValue},
		{"/api/v2/tickets?token=" + canary + "#" + canary, "/api/v2/tickets?token=" + URLRedactedValue},
		{"/x/u:" + canary + "@evil.example.net/y", URLUnparseable},
		{"/", "/"},
		{"", ""},
		// not origin-relative: URL's rendering, unchanged
		{"//u:" + canary + "@evil.example.net/y?k=" + canary, URL("//u:" + canary + "@evil.example.net/y?k=" + canary)},
		{"https://u:" + canary + "@h.example/x?k=" + canary, "https://h.example/x?k=" + URLRedactedValue},
		{"api/v1/x", URL("api/v1/x")},
	} {
		got := Path(tc.raw)
		if got != tc.want {
			t.Errorf("Path(%q) = %q, want %q", tc.raw, got, tc.want)
		}
		if strings.Contains(got, canary) || strings.Contains(got, "redact.invalid") {
			t.Errorf("Path(%q) = %q carries the secret or the placeholder origin", tc.raw, got)
		}
	}
}
