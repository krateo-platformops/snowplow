//go:build unit
// +build unit

package api

// loopback_token_source_sa_1530_test.go — #1531 review B1: on a prewarm context the SA endpoint is the most common
// stage, and an authn JWT appended there is #271's mechanism (it suppresses the SA's own token and reaches the
// apiserver). The #271 falsifier drives the decision with a requester's token; this drives it with a token SOURCE
// only (a prewarm context): the SA-endpoint dial presents the projected SA token, never the source's JWT, and the
// source is never even asked (mutation L1b, giving a source-carrying context's SA stage the JWT, turns this RED).
// Reuses the #271 fixture (resolve_jwt_suppression_falsifier_271_test.go).

import (
	"context"
	"net/http"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/loopbacktoken"
	"k8s.io/client-go/rest"
)

func TestLoopbackTokenSource_NeverReachesTheSAEndpoint(t *testing.T) {
	sa271FailFast(t)
	installSATokenFile(t)
	f := newSADialFixture(t)
	src := &t1530Source{}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "system:serviceaccount:krateo-system:snowplow"}))
	ctx = loopbacktoken.WithSource(ctx, src.ask)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{
		ServerURL: f.server.URL, Token: "STALE-BOOT-TOKEN-must-not-appear", CertificateAuthorityData: f.caPEM})

	_ = Resolve(ctx, ResolveOptions{
		RC: &rest.Config{}, Items: []*templates.API{sar271Stage()},
		RESTActionNamespace: "krateo-system", RESTActionName: "access-preview",
	})

	if f.count() == 0 || f.method() != http.MethodPost || f.path() != sarPath271 {
		t.Fatalf("premise: the SA dial did not reach the fake apiserver (hits=%d, method=%q, path=%q): the wire "+
			"assertion would be vacuous", f.count(), f.method(), f.path())
	}
	if got := f.auth(); got != "Bearer "+saToken271Sentinel {
		t.Fatalf("THE LEAK (#271 on the source path): the SA-endpoint dial presented %q, want the projected SA token", got)
	}
	if asked := src.asked(); len(asked) != 0 {
		t.Fatalf("the token source was asked %v for an SA-endpoint stage", asked)
	}
}
