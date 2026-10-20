// narrow_commit_before_put_765b_test.go — #765 B, P8a (architect-623's C1).
//
// The replay fix reads a raKey edge's LIVE interest state when a widget slices
// the raKey cell (apiref ra_full_list.go). That is sound only because a
// resolve's commit (narrow or widen) has settled before ANY Put of the bytes it
// produced — every Put of a RESTAction result happens after
// restactions.Resolve / api.Resolve returns, and the commit is deferred inside
// it. This arm pins the half that lives here: when api.Resolve RETURNS, the
// edge already reflects THAT resolve. V1 is clean (narrowed); V2 fails a stage
// (#638 filter error), and right after V2 returns the edge must be all.
// Mutation: run the commit after Resolve returns → RED.
package api

import (
	"context"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

func resolve765b(key string, st *templates.API) {
	ctx := xcontext.BuildContext(cache.WithL1KeyContext(context.Background(), key),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: u375}))
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
	_ = Resolve(ctx, ResolveOptions{
		RC: &rest.Config{}, Items: []*templates.API{st},
		RESTActionNamespace: "default", RESTActionName: "c765b",
	})
}

func TestNarrow765b_P8a_TheCommitHasSettledWhenResolveReturns(t *testing.T) {
	newWatcher375(t)
	const key = "n765b-p8a"
	resolve765b(key, stage765(`[.w.items[] | {name: .metadata.name}]`))
	if got := cache.ListInterestStateForTest(key, dk765()); !strings.HasPrefix(got, "narrowed:") {
		t.Fatalf("P8a setup: V1 (clean, informer-served) must narrow the edge, got %q", got)
	}
	// V2: the same edge, a stage filter that raises at run time (#638: the
	// stage fails, the resolve is tainted).
	resolve765b(key, stage765(`[.w.items[] | {name: .metadata.name}] | error("v2 fails")`))
	// No wait, no poll: the state must ALREADY be settled when Resolve returns,
	// because the bytes V2 produced may be Put the very next instant.
	if got := cache.ListInterestStateForTest(key, dk765()); got != "all" {
		t.Fatalf("P8a RED: right after the failing V2 resolve RETURNED, the edge is still %q. The commit must "+
			"settle inside Resolve, before any Put of V2's bytes: a widget slicing V2 would otherwise carry "+
			"V1's read set (the replay reads the live state).", got)
	}
}
