// issue1334_public_identity_background_test.go — #1334 MN3 for the background
// drivers, and the cache identity:
//
//   - REFRESHER: a cell of an identity the apiserver does not authenticate (the
//     public identity) is refreshed ONLY impersonated. When the impersonation
//     is not permitted (or there is no ServiceAccount config to impersonate
//     with), the cell is evicted and the resolver never runs: it is never
//     refreshed under snowplow's ServiceAccount with the re-gate. Control: an
//     authenticated cohort keeps that fallback, unchanged.
//   - PREWARM: no seed target is unauthenticated, whether it comes from a
//     binding representative or from the learned registry; and a live public
//     request is never learned (it presents no credential with an expiry).
//   - CACHE KEY: the public identity is ONE cache identity (two requests derive
//     one key), and the key folds the effective group set: the same username
//     without system:unauthenticated (an authenticated user named
//     krateo:public) derives a different key.

package dispatchers

import (
	"context"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

func i1334BGCapability(t *testing.T, permitPublic bool) {
	t.Helper()
	cache.ResetImpersonationCapabilityForTest()
	t.Cleanup(cache.ResetImpersonationCapabilityForTest)
	t.Cleanup(cache.SetImpersonationAvailableForTest(true))
	cache.SetImpersonationReviewer(func(_ context.Context, resource, _, name string) (bool, error) {
		if resource == cache.ImpersonateResourceUsers {
			return true, nil // the shipped chart: impersonate users, every name
		}
		return permitPublic && resource == cache.ImpersonateResourceGroups && name == i1334Unauth, nil
	})
}

type i1334Resolved struct {
	calls int
	ctx   context.Context
}

func i1334StubResolve(t *testing.T) *i1334Resolved {
	t.Helper()
	got := &i1334Resolved{}
	prev := resolveOnceFn
	resolveOnceFn = func(ctx context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		got.calls++
		got.ctx = ctx
		return []byte(`{"fresh":true}`), nil
	}
	t.Cleanup(func() { resolveOnceFn = prev })
	return got
}

// i1334Cell mints and stores the cell user would derive for ra, exactly as a
// live dispatch keys it, with user as its representative.
func i1334Cell(t *testing.T, user string, groups []string, ra string) (string, cache.ResolvedKeyInputs) {
	t.Helper()
	key, _, in := dispatchCacheLookupKey(i1334Ctx(user, groups), "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, ra, -1, -1, nil)
	if in == nil {
		t.Fatalf("setup: no key inputs for %s", user)
	}
	in.RepresentativeUsername, in.RepresentativeGroups = user, groups
	cache.ResolvedCache().PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: []byte(`{"prior":1}`), Inputs: in})
	return key, *in
}

func i1334SA() (*endpoints.Endpoint, *rest.Config) {
	return &endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: "sa-1334"},
		&rest.Config{Host: "https://10.96.0.1:443", BearerToken: "sa-1334"}
}

func TestIssue1334_Refresher_UnauthenticatedIdentityNeverRefreshedUnderTheServiceAccount(t *testing.T) {
	unauth := []string{i1334Unauth}
	saEP, saRC := i1334SA()

	for _, tc := range []struct {
		name   string
		permit bool
		rc     *rest.Config
	}{
		{"impersonation not permitted", false, saRC},
		{"no ServiceAccount config", true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			i1334BuildWatcher(t)
			i1334BGCapability(t, tc.permit)
			res := i1334StubResolve(t)
			key, in := i1334Cell(t, i1334Public, unauth, i1334PublicRA)
			before := UnauthenticatedRefreshRefused("evicted")

			if err := resolveAndPopulateL1(cache.WithBackgroundResolve(context.Background()), in, saEP, tc.rc); err != nil {
				t.Fatalf("resolveAndPopulateL1: %v", err)
			}
			if res.calls != 0 {
				t.Fatalf("MUST NEVER: the public identity's cell was re-resolved without its impersonation (impersonated=%v)",
					res.ctx != nil && cache.ImpersonatedDialFromContext(res.ctx))
			}
			if _, ok := cache.ResolvedCache().GetNoTouch(key); ok {
				t.Fatal("the unrefreshable public cell must be evicted, not left to serve stale")
			}
			if UnauthenticatedRefreshRefused("evicted") != before+1 {
				t.Fatal("the refusal was not counted")
			}
		})
	}

	t.Run("impersonation permitted: refreshed as exactly the public identity", func(t *testing.T) {
		i1334BuildWatcher(t)
		i1334BGCapability(t, true)
		res := i1334StubResolve(t)
		_, in := i1334Cell(t, i1334Public, unauth, i1334PublicRA)
		if err := resolveAndPopulateL1(cache.WithBackgroundResolve(context.Background()), in, saEP, saRC); err != nil {
			t.Fatalf("resolveAndPopulateL1: %v", err)
		}
		if res.calls != 1 {
			t.Fatalf("the permitted public cell must be refreshed; resolver calls=%d", res.calls)
		}
		rc, ok := cache.ImpersonatingConfigFor(res.ctx)
		if !ok || rc.Impersonate.UserName != i1334Public || !slices.Equal(rc.Impersonate.Groups, unauth) {
			t.Fatalf("MUST NEVER: the public cell was refreshed with transport %+v (ok=%v); want impersonation of exactly %s %v",
				rc, ok, i1334Public, unauth)
		}
	})

	t.Run("control: an authenticated cohort keeps the re-gated ServiceAccount fallback", func(t *testing.T) {
		i1334BuildWatcher(t)
		i1334BGCapability(t, false)
		cache.SetImpersonationReviewer(func(context.Context, string, string, string) (bool, error) { return false, nil })
		res := i1334StubResolve(t)
		_, in := i1334Cell(t, i1334Alice, nil, i1334AuthOnlyRA)
		if err := resolveAndPopulateL1(cache.WithBackgroundResolve(context.Background()), in, saEP, saRC); err != nil {
			t.Fatalf("resolveAndPopulateL1: %v", err)
		}
		if res.calls != 1 || cache.ImpersonatedDialFromContext(res.ctx) {
			t.Fatalf("an authenticated cohort refused impersonation must still refresh, un-impersonated and re-gated; calls=%d", res.calls)
		}
	})
}

func TestIssue1334_Prewarm_NoUnauthenticatedSeedTarget(t *testing.T) {
	i1334BuildWatcher(t)
	cache.ResetLearnedIdentitiesForTest()
	t.Cleanup(cache.ResetLearnedIdentitiesForTest)
	unauth := []string{i1334Unauth}

	// A live public request is never learned: it carries no credential.
	req := httptest.NewRequest("GET", "/call", nil).WithContext(i1334Ctx(i1334Public, unauth))
	observeLiveCaller(req)
	for _, c := range cache.LearnedIdentitiesSnapshot() {
		if c.Username == i1334Public {
			t.Fatalf("a live public request was learned as a seed class: %+v", c)
		}
	}

	// Even if the registry held unauthenticated classes (an IdP-asserted
	// system:unauthenticated) and the bindings produced unauthenticated
	// representatives, none becomes a seed target.
	exp := time.Now().Add(time.Hour)
	cache.ObserveLiveIdentity(i1334Public, unauth, exp)
	cache.ObserveLiveIdentity(i1334Visitor, unauth, exp)
	cache.ObserveLiveIdentity(i1334Alice, nil, exp)
	// CONTROL class: the same user with no group — authenticated, and
	// authorised for the GVR by the very RoleBinding that authorises the public
	// identity (it names the USER). It must be emitted as a learned target, so
	// the public class's absence is the filter's verdict, not the authorisation
	// check's.
	cache.ObserveLiveIdentity(i1334Public, nil, exp)
	gvr := schema.GroupVersionResource{Group: h1RAGVR.Group, Version: h1RAGVR.Version, Resource: h1RAGVR.Resource}
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{gvr})
	if !cache.IdentityAuthorisedForGVR(gvr, i1334Public, unauth) {
		t.Fatal("precondition: the public identity is authorised for the GVR (its RoleBinding names the user); else the learned filter is never consulted")
	}
	prev := enumerateBasePrewarmTargetsFn
	enumerateBasePrewarmTargetsFn = func(g schema.GroupVersionResource, verb string) []cache.PrewarmTarget {
		return []cache.PrewarmTarget{
			{Subject: cache.SubjectIdentity{Username: i1334Public, Groups: unauth}, GVR: g, Verb: verb},
			{Subject: cache.SubjectIdentity{Groups: unauth}, GVR: g, Verb: verb},
			{Subject: cache.SubjectIdentity{Username: i1334Alice}, GVR: g, Verb: verb},
		}
	}
	t.Cleanup(func() { enumerateBasePrewarmTargetsFn = prev })

	targets := enumeratePrewarmTargetsWithLearned(gvr, "get")
	controlLearned := false
	for _, tg := range targets {
		if !cache.Authenticated(tg.Subject.Username, tg.Subject.Groups) {
			t.Errorf("MUST NEVER: an unauthenticated seed target %q %v (learned=%v): seeds resolve under the ServiceAccount",
				tg.Subject.Username, tg.Subject.Groups, tg.Learned)
		}
		if tg.Learned && tg.Subject.Username == i1334Public && len(tg.Subject.Groups) == 0 {
			controlLearned = true
		}
	}
	if !controlLearned {
		t.Fatalf("instrument: the authenticated control class (%s, no groups) was not emitted as a learned target, so the "+
			"public class's absence is not the filter's verdict; targets=%+v", i1334Public, targets)
	}
}

// The SA-producer census (sa_producer_census_268_267_test.go) classifies the
// public identity's ctx as the refresher's impersonating call is classified: a
// REAL identity whose ServiceAccount config is an impersonating copy, carried
// with the impersonated-dial marker. This pins each half against the REAL
// builder (PublicIdentity.Context), with the same ctx shown re-gated when the
// marker is absent so the arm can fail.
func TestIssue1334_PublicContext_IsImpersonatedNotRegated(t *testing.T) {
	i1334BuildWatcher(t)
	i1334BGCapability(t, true)
	saEP, saRC := i1334SA()
	pub := middleware.NewPublicIdentity(
		func() (*endpoints.Endpoint, error) { return saEP, nil },
		func() (*rest.Config, error) { return saRC, nil })
	ctx, err := pub.Context(context.Background())
	if err != nil {
		t.Fatalf("PublicIdentity.Context: %v", err)
	}
	if _, ok := cache.InternalRESTConfigFromContext(ctx); !ok {
		t.Fatal("non-vacuity: the public ctx must carry the ServiceAccount transport the census counts")
	}
	if rbac.ServesUnnarrowed(ctx) {
		t.Fatal("the public identity is a REAL narrowing identity; it must never be ServesUnnarrowed")
	}
	rc, ok := cache.ImpersonatingConfigFor(ctx)
	if !ok || rc.Impersonate.UserName != i1334Public || !slices.Equal(rc.Impersonate.Groups, []string{i1334Unauth}) {
		t.Fatalf("the public ctx's transport must impersonate exactly %s [%s]; got %+v (ok=%v)", i1334Public, i1334Unauth, rc, ok)
	}
	if rbac.MustRegateSADial(ctx) {
		t.Fatal("an impersonated dial reads as the identity; the re-gate stands down for it as for the refresher's")
	}
	// Control: the same shape WITHOUT the marker, under a background stamp, is
	// re-gated: the stand-down above is the marker's doing.
	plain := cache.WithInternalRESTConfig(cache.WithBackgroundResolve(i1334Ctx(i1334Public, []string{i1334Unauth})), saRC)
	if !rbac.MustRegateSADial(plain) {
		t.Fatal("CONTROL: an un-impersonated ServiceAccount transport under the public identity must be re-gated")
	}
}

func TestIssue1334_CacheKey_OnePublicIdentityFoldingItsGroupSet(t *testing.T) {
	i1334BuildWatcher(t)
	key := func(user string, groups []string) string {
		k, _, _ := dispatchCacheLookupKey(xcontext.BuildContext(i1334Ctx(user, groups)), "restactions",
			h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, i1334PublicRA, -1, -1, nil)
		return k
	}
	unauth := []string{i1334Unauth}
	a, b := key(i1334Public, unauth), key(i1334Public, []string{i1334Unauth})
	if a == "" || a != b {
		t.Fatalf("two public requests must derive ONE key; got %q and %q", a, b)
	}
	if key(i1334Public, nil) == a {
		t.Fatal("the key must fold the effective group set: krateo:public WITHOUT system:unauthenticated is authenticated and must derive another key")
	}
	if key(i1334Alice, nil) == a {
		t.Fatal("the public identity must not share an authenticated user's key")
	}
}
