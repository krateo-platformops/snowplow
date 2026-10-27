package dispatchers

// issue681_denial_only_seed_test.go — #681 G + C2.
//
//   G/H5b  the cohort SEED (F1, SA credential, snowplow's re-gate) Puts the
//          denial-only result for the all681 cohort, and that seeded body is
//          BYTE-IDENTICAL to the body A's live /call (F2, the apiserver's 403
//          naming A) produces — no identity text in either.
//   C2s    the seed's declined-external mark: a resolve whose only external
//          touch was an own-credential apiserver 403 is NOT marked, and is not
//          declined as external. The raw external count would mark it.
//   C2w    the #680 walk mark site applies the same definition.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

// c681SAServer answers ONLY the snowplow SA token and lets it read everything.
func c681SAServer(t *testing.T, dials *atomic.Int64) *httptest.Server {
	t.Helper()
	tok := s425SAToken()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") != tok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		dials.Add(1)
		body, ok := c681Objects(r.URL.Path)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		b, _ := json.Marshal(body)
		_, _ = w.Write(b)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (e *c681Env) seed(t *testing.T, cohort seedTarget, saURL string) []byte {
	t.Helper()
	prev := seedObjectsGetFn
	seedObjectsGetFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: e.ra.DeepCopy()}
	}
	t.Cleanup(func() { seedObjectsGetFn = prev })
	tok := s425SAToken()
	seedCtx := withCohortSeedContext(context.Background(), cohort,
		endpoints.Endpoint{ServerURL: saURL, Token: tok}, &rest.Config{Host: saURL, BearerToken: tok})
	ref := templatesv1.ObjectReference{Reference: templatesv1.Reference{Name: c681RA, Namespace: h1NS},
		APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version, Resource: h1RAGVR.Resource}
	if err := seedOneRestaction(seedCtx, "c681", ref, c681AuthnNS, seedModeBoot); err != nil {
		t.Fatalf("seedOneRestaction: %v", err)
	}
	got, ok := cache.ResolvedCache().GetNoTouch(e.key(t, c681A))
	if !ok {
		return nil
	}
	return got.RawJSON
}

func TestIssue681_G_SeedPutsDenialOnly_ByteIdenticalToLiveCall(t *testing.T) {
	e := c681Setup(t)
	var saDials atomic.Int64
	sa := c681SAServer(t, &saDials)

	seeded := e.seed(t, seedTarget{Groups: []string{c681All}}, sa.URL)
	if seeded == nil {
		t.Fatal("G RED: the cohort seed declined the denial-only result — the first visit stays cold")
	}
	if got := c681DenialPuts(cache.DenialOnlyPutSiteSeed); got != 1 {
		t.Fatalf("denial_only_put_total{seed} = %d, want 1", got)
	}
	c681NoIdentity(t, "the seeded body", seeded)
	if bytes.Contains(seeded, []byte(c681SecretY)) {
		t.Fatal("the seeded body carries x/y the cohort may not read")
	}

	// A's live /call (cold) produces the SAME bytes through F2.
	cache.ResetResolvedCacheForTest()
	live, d := e.call(t, c681A)
	if d == 0 {
		t.Fatal("PRE: A's /call was not cold")
	}
	if !bytes.Equal(live, seeded) {
		t.Fatalf("H5b/G RED: the seeded (F1) body differs from A's live (F2) body:\nseeded %s\n  live %s", s425Trunc(seeded), s425Trunc(live))
	}
}

// c2Sinks builds sinks where the ONLY external touch is an own-credential
// apiserver 403 (F2): stage=1 forbidden, ext=1 = forbidden.external.
func c2Sinks(ctx context.Context) (context.Context, *cache.StageErrorSink, *cache.ExternalTouchedSink, *cache.PerIdentityForbiddenSink) {
	ctx, st := cache.WithStageErrorSink(ctx)
	ctx, ext := cache.WithExternalTouchedSink(ctx)
	ctx, f := cache.WithPerIdentityForbiddenSink(ctx)
	st.Bump("catalog", "forbidden")
	ext.Bump()
	f.BumpStage()
	f.BumpExternal()
	return ctx, st, ext, f
}

func c2Env(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	if cache.SeedDeclinedExternalSetFromContext(cache.WithSeedDeclinedExternalSet(context.Background(), cache.NewSeedDeclinedExternalSet())) == nil {
		t.Fatal("PRE: the DES set is not installed — the C2 arms would be vacuous")
	}
}

func TestIssue681_C2s_SeedDoesNotMarkAnOwnDial403AsExternal(t *testing.T) {
	c2Env(t)
	cache.ResetDenialOnlyPutForTest()
	set := cache.NewSeedDeclinedExternalSet()
	ctx := cache.WithSeedDeclinedExternalSet(context.Background(), set)
	ctx, st, ext, f := c2Sinks(ctx)
	if declineSeedPutOnError(ctx, "restactions", "ns/ra", "key/c2s", cache.PutGateVerdict(cache.PutGateSinks{Stage: st, Forbidden: f, External: ext}, "restactions"), st, ext) {
		t.Fatal("C2s RED: a denial-only result whose only external touch is an own-credential 403 was declined")
	}
	// Control: the same resolve with a GENUINE external touch is marked.
	ctxG := cache.WithSeedDeclinedExternalSet(context.Background(), set)
	ctxG, stG, extG, fG := c2Sinks(ctxG)
	extG.Bump()
	_ = declineSeedPutOnError(ctxG, "restactions", "ns/ra", "key/c2s-ext", cache.PutGateVerdict(cache.PutGateSinks{Stage: stG, Forbidden: fG, External: extG}, "restactions"), stG, extG)
	if !set.Marked("key/c2s-ext") {
		t.Fatal("C2s control: a genuine external touch no longer marks")
	}
	if set.Marked("key/c2s") {
		t.Fatal("C2s RED: the own-credential apiserver 403 was DES-marked external — background passes would skip it forever")
	}
}

func TestIssue681_C2w_WalkDoesNotMarkAnOwnDial403AsExternal(t *testing.T) {
	c2Env(t)
	set := cache.NewSeedDeclinedExternalSet()
	ctx := cache.WithSeedDeclinedExternalSet(context.Background(), set)
	ctx, _, _, _ = c2Sinks(ctx)
	w := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "Panel",
		"metadata": map[string]any{"name": "w", "namespace": "ns"}}}
	walkMarkDeclinedExternal(ctx, "key/c2w", w, "uid", "1", nil, false)
	if set.Marked("key/c2w") {
		t.Fatal("C2w RED: the walk DES-marked an own-credential apiserver 403 as external")
	}
	// Control: a genuine external touch still marks.
	ctx2 := cache.WithSeedDeclinedExternalSet(context.Background(), set)
	ctx2, ext2 := cache.WithExternalTouchedSink(ctx2)
	ext2.Bump()
	walkMarkDeclinedExternal(ctx2, "key/c2w-ext", w, "uid", "1", nil, false)
	if !set.Marked("key/c2w-ext") {
		t.Fatal("C2w control: a genuine external touch no longer marks")
	}
}
