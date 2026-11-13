package api

// issue1334_unauthenticated_own_credential_test.go — #1334, the stage
// resolver's guard (endpoints.go resolveOne, nil ref): an identity the
// apiserver does not authenticate (the public identity: krateo:public +
// system:unauthenticated) holds no clientconfig credential. Its own-credential
// stage dials only through the impersonating context the auth middleware
// installs (the in-cluster endpoint, impersonated at the dial); without that
// context the stage is refused BEFORE any Secret is read.
//
// Why it matters: the name credsecret builds for "krateo:public" is
// krateo-public-clientconfig, the Secret of a REAL user named krateo-public (a
// valid DNS-1123 name). The world below holds exactly that Secret, owned by
// that user, on the apiserver, so a resolver that synthesized the name would be
// seen asking for it.

import (
	"context"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	jwtutil "github.com/krateo-platformops/plumbing/jwtutil"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

const (
	i1334OCPublic = "krateo:public"
	i1334OCUnauth = "system:unauthenticated"
	i1334OCReal   = "krateo-public" // a real user whose Secret name credsecret would build for krateo:public
)

func i1334OCWorld(t *testing.T) (m func(user string, groups ...string) *endpointReferenceMapper, asked func() []string) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	prev := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prev) })
	// The informer is left unservable, so every Secret read reaches the
	// apiserver below, which records it.
	oc1382Informer(t, nil)
	real := oc1382Secret(t, i1334OCReal+"-clientconfig", "REAL-USER-TOKEN", i1334OCReal, "")
	rc, asked := oc1382APIServer(t, nil, real)
	return func(user string, groups ...string) *endpointReferenceMapper {
		return &endpointReferenceMapper{authnNS: oc1382NS, username: user, groups: groups, rc: rc}
	}, asked
}

func TestIssue1334_StageResolver_UnauthenticatedNeverSynthesizesAClientconfigName(t *testing.T) {
	mapper, asked := i1334OCWorld(t)

	// CONTROL (the instrument can say yes): the real user's own-credential
	// stage reads its Secret from the apiserver.
	ep, isSA, err := mapper(i1334OCReal).resolveOne(context.Background(), nil)
	if err != nil || isSA || ep.Token != "REAL-USER-TOKEN" || len(asked()) == 0 {
		t.Fatalf("CONTROL: the real user's own credential = %q (isSA=%v, err=%v, asked=%v); want its Secret, read from the apiserver",
			ep.Token, isSA, err, asked())
	}
	before := len(asked())

	// The public identity, with no impersonating context: refused before any
	// Secret is read.
	ep, isSA, err = mapper(i1334OCPublic, i1334OCUnauth).resolveOne(context.Background(), nil)
	if err == nil {
		t.Fatalf("MUST NEVER: the public identity's own-credential stage resolved an endpoint (token=%q, isSA=%v) with no impersonating context", ep.Token, isSA)
	}
	if ep.Token != "" || isSA {
		t.Fatalf("MUST NEVER: the refused resolve still carries a credential (token=%q, isSA=%v)", ep.Token, isSA)
	}
	if got := asked()[before:]; len(got) != 0 {
		t.Fatalf("MUST NEVER: the public identity's stage asked the apiserver for %v — a clientconfig name synthesized for an identity that holds none (krateo-public-clientconfig is a real user's)", got)
	}
}

func TestIssue1334_StageResolver_PublicContextDialsTheImpersonatedEndpoint(t *testing.T) {
	mapper, asked := i1334OCWorld(t)
	saEP := &endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: "sa-1334"}
	// The context shape the auth middleware installs for the public identity
	// (middleware.PublicIdentity.Context): the identity, the in-cluster
	// endpoint, and the impersonated-dial marker.
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: i1334OCPublic, Groups: []string{i1334OCUnauth}}))
	ctx = cache.WithImpersonatedDial(cache.WithInternalEndpoint(ctx, saEP))

	ep, isSA, err := mapper(i1334OCPublic, i1334OCUnauth).resolveOne(ctx, nil)
	if err != nil || !isSA || ep.Token != saEP.Token {
		t.Fatalf("the public identity's own-credential stage under its context = %q (isSA=%v, err=%v); want the in-cluster endpoint, "+
			"which the dial impersonates (branch E) or refuses", ep.Token, isSA, err)
	}
	if !cache.ImpersonatedDialFromContext(ctx) {
		t.Fatal("precondition: the context carries the impersonated-dial marker branch E reads")
	}
	if got := asked(); len(got) != 0 {
		t.Fatalf("MUST NEVER: a Secret was read for the public identity: %v", got)
	}
}
