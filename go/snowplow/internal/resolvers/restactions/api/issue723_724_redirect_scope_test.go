package api

// issue723_724_redirect_scope_test.go: the scope of the #723/#724 policy, on the
// shapes main did not have a seam for.
//
//   - A per-user clientconfig (held credential, credhttp.WithHeldCredential)
//     with a private CA takes the OWNED client branch (buildEndpointClient →
//     client-go transport.New). It must not follow a redirect either.
//   - A named endpointRef (no mark) carries the third party's OWN credential
//     and keeps net/http's default behaviour. This arm pins that the policy
//     is keyed on provenance and did not widen.
//   - A hardened *rest.Config used by a client snowplow does NOT build
//     (plumbing's endpoints.FromSecret builds its own REST client from rc):
//     the rc-level hop refusal stops the hop.

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"k8s.io/client-go/rest"
)

func newTLSRedirector(t *testing.T, code int, to string) *httptest.Server {
	t.Helper()
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, to, code)
	}))
	t.Cleanup(s.Close)
	return s
}

func caOf(s *httptest.Server) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}))
}

func TestIssue723_HeldClientconfigOwnedCABranchNeverFollowsRedirect(t *testing.T) {
	for _, code := range redirectCodes {
		rig := newRedirectRig(t, code, true) // only rig.other is used
		api := newTLSRedirector(t, code, rig.other.URL+"/stolen")
		ep := &endpoints.Endpoint{ServerURL: api.URL, Token: "user-token-held", CertificateAuthorityData: caOf(api)}
		if !endpointNeedsOwnedCAClient(ep) {
			t.Fatal("fixture must take the owned-CA branch")
		}
		ctx := credhttp.WithHeldCredential(context.Background())
		res, body, _, _ := httpFetchAllowingNonJSON(ctx, httpcall.RequestOptions{
			RequestInfo: httpcall.RequestInfo{Path: "/api/v1/namespaces/n/configmaps/x", Verb: ptr.To(http.MethodGet)},
			Endpoint:    ep,
		})
		if hits := rig.otherHits.all(); len(hits) != 0 {
			t.Errorf("code=%d: RED — %d request(s) reached the other host (auth present=%v)", code, len(hits), hits[0].auth != "")
		}
		if res == nil || res.Status != response.StatusFailure || res.Code != code || body != nil {
			t.Errorf("code=%d: want Failure %d and no body, got %+v body=%q", code, code, res, body)
		}
	}
}

func TestIssue723_NamedEndpointRefKeepsDefaultRedirectBehaviour(t *testing.T) {
	rig := newRedirectRig(t, http.StatusFound, true)
	res, body, _, _ := httpFetchAllowingNonJSON(context.Background(), httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: "/api/x", Verb: ptr.To(http.MethodGet)},
		Endpoint:    &endpoints.Endpoint{ServerURL: rig.apiserver.URL, Token: "third-party-own-token"},
	})
	if len(rig.otherHits.all()) != 1 || res == nil || res.Status == response.StatusFailure || body == nil {
		t.Fatalf("an unmarked (named endpointRef) dial must keep following redirects as before: hits=%d res=%+v", len(rig.otherHits.all()), res)
	}
}

// The backstop for clients snowplow does not build: plumbing's FromSecret
// builds its own REST client from rc. With a hardened rc the hop never runs.
func TestIssue724_HardenedRESTConfigRefusesHopInForeignClient(t *testing.T) {
	for _, code := range redirectCodes {
		rig := newRedirectRig(t, code, true)
		rc := credhttp.Harden(&rest.Config{Host: rig.apiserver.URL, BearerToken: "sa-token-R3"})
		// rest.CopyConfig (how the #574 impersonating config is derived) keeps it.
		cp := rest.CopyConfig(rc)
		cp.Impersonate = rest.ImpersonationConfig{UserName: "cohort-user-R3"}
		_, err := endpoints.FromSecret(context.Background(), cp, "s", "n")
		if hits := rig.otherHits.all(); len(hits) != 0 {
			t.Errorf("code=%d: RED — plumbing's own client followed the redirect (auth present=%v, impersonation present=%v)", code, hits[0].auth != "", hits[0].impUser != "")
		}
		if err == nil {
			t.Errorf("code=%d: want an error, the redirect target's Secret was accepted", code)
		}
	}
}

// The wiring: a nil-ref stage on an internal driver's ctx resolves to the
// ctx-carried SA endpoint and is reported held, so runStage marks it.
func TestIssue723_NilRefInternalEndpointIsHeld(t *testing.T) {
	saEP := &endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: "sa-token"}
	r := &resolveRun{
		log:  scalarFalsifierLogger(),
		dict: map[string]any{},
		ctx:  cache.WithInternalEndpoint(context.Background(), saEP),
	}
	_, isSA, held, action := r.resolveStageEndpoint("s", &templates.API{Name: "s"}, false)
	if action != stageProceed || !isSA || !held {
		t.Fatalf("want proceed/isSA/held, got action=%v isSA=%v held=%v", action, isSA, held)
	}
}
