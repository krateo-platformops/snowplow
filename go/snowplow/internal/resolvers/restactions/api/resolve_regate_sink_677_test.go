package api

// resolve_regate_sink_677_test.go — #677 (2) prerequisite: the REAL
// api.Resolve bumps the RegateDenySink exactly once per re-gate-denied item, at
// the same site as the stage-error bump, and never for any other stage error.
// So RegateDenySink.Count()==StageErrorSink.Count() means "every stage error
// was a correct per-identity deny" and a mixed resolve never reads that way.

import (
	"context"
	"net/http"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

func getStage677(id, path string) *templates.API {
	return &templates.API{Name: id, Path: path, Verb: ptr.To(http.MethodGet), ContinueOnError: ptr.To(true)}
}

func runResolve677(t *testing.T, stages ...*templates.API) (stage, regate int64) {
	t.Helper()
	newWatcher677(t)
	scope677(t)
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	f, rc := newFakeAPI677(t)
	f.missing["ghost"] = true
	ctx := cache.WithInternalRESTConfig(ctxWithUser("u677"), rc)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: rc.Host})
	ctx, ss := cache.WithStageErrorSink(ctx)
	ctx, rs := cache.WithRegateDenySink(ctx)
	_ = Resolve(context.WithoutCancel(ctx), ResolveOptions{
		RC: rc, Items: stages, RESTActionNamespace: "krateo-system", RESTActionName: "ra677",
	})
	return ss.Count(), rs.Count()
}

func TestIssue677_ResolveBumpsRegateSinkOnlyForRegateDenies(t *testing.T) {
	t.Run("deny_plus_success", func(t *testing.T) {
		st, rg := runResolve677(t,
			getStage677("catalog", "/api/v1/namespaces/team-b/configmaps/catalog"), // denied
			getStage677("ok", "/api/v1/namespaces/team-a/configmaps/other"),        // allowed
		)
		if st != 1 || rg != 1 {
			t.Fatalf("stage=%d regate=%d, want 1/1 (every stage error is a re-gate deny)", st, rg)
		}
	})
	t.Run("deny_plus_other_error", func(t *testing.T) {
		st, rg := runResolve677(t,
			getStage677("catalog", "/api/v1/namespaces/team-b/configmaps/catalog"), // denied
			getStage677("ghost", "/api/v1/namespaces/team-a/configmaps/ghost"),     // allowed, 404
		)
		if st != 2 || rg != 1 {
			t.Fatalf("stage=%d regate=%d, want 2/1 (a mixed resolve must not read as all-deny)", st, rg)
		}
	})
	t.Run("other_error_only", func(t *testing.T) {
		st, rg := runResolve677(t, getStage677("ghost", "/api/v1/namespaces/team-a/configmaps/ghost"))
		if st != 1 || rg != 0 {
			t.Fatalf("stage=%d regate=%d, want 1/0", st, rg)
		}
	})
}
