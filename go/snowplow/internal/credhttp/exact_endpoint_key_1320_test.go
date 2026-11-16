package credhttp

import (
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
)

// #1320 — ExactEndpointKeyOf reports whether the key folds the endpoint
// exactly, so whatever is shared by it (the external read's request on the
// wire) shares nothing when it does not.
func TestExactEndpointKeyOf_ReportsAnInexactFold(t *testing.T) {
	was := endpointKeyExact
	t.Cleanup(func() { endpointKeyExact = was })
	ep := &endpoints.Endpoint{ServerURL: "https://ext.example", Token: "t"}

	endpointKeyExact = true
	k, exact := ExactEndpointKeyOf(ep, "p")
	if !exact || k != EndpointKeyOf(ep, "p") {
		t.Fatalf("exact fold: exact=%v, key equals EndpointKeyOf=%v; want true, true", exact, k == EndpointKeyOf(ep, "p"))
	}
	endpointKeyExact = false
	if _, exact := ExactEndpointKeyOf(ep, "p"); exact {
		t.Fatal("an inexact fold was reported exact: two identities could share")
	}
}
