package dispatchers

// issue903_declared_write_test.go — #903 review follow-ups (B2, F1), from the
// independent review's arms.
//
//   S1  (B2) the cohort seed of a RESTAction whose only branch-E stage is a
//       write (the 057 blueprint-palette / access-preview shape) sends no write,
//       stores nothing, and MARKS the cell declined-external, so later passes
//       skip it instead of re-resolving it every pass.
//   S2  (B2) the refresher: the REAL re-resolve of a RESTAction that declares
//       a write (the write is refused in the background) suppresses the key as
//       structurally permanent with its own verdict reason, declared_write —
//       never a known-stale retry, and never counted as an external touch.
//   S2b the shared verdict's other permanent stage-error case: a stage error
//       beside a genuine external touch suppresses with reason stage_error.
//   S3  (F1) a RESTAction that DECLARES a write stage is never stored, even
//       when this /call dialed none of it (a zero-item write iterator). When
//       the list later gains an item, the next customer /call is a miss and
//       sends the write once — it is never an L1 hit that skips it, and the
//       background never sends it.
//   S4  (B2) the prewarm walk: its resolve of a RESTAction that declares a
//       write sends no write, and the walk's mark site (walkMarkDeclinedExternal,
//       over the walk's own resolve ctx) MARKS it declined-external, so later
//       walks replay the mark instead of re-resolving it.
//   S5  (#912 re-review N2) a RESTAction that declares a write but whose
//       resolve stops before running any stage (its write's dependsOn names no
//       stage; a dependsOn cycle) is not stored either: the declared-write bump
//       runs before every early exit.
//
// Mutation (logic-only): drop the declared-write bump in ResolveWithFailure →
// S1 (no mark, re-resolved every pass), S3 (stored; the later /call is a hit
// that never writes), S4 (the walk does not mark), S5 go RED. Move the bump
// back after the early exits → S5 RED. Drop the external-permanent decline in
// the refresher's stage-error branch → S2 RED.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

func s903WriteRA(iterator string) *unstructured.Unstructured {
	write := map[string]any{"name": "render", "verb": "POST", "path": "/render",
		"endpointRef":     map[string]any{"name": t903EP, "namespace": c681AuthnNS},
		"payload":         `{"n":1}`,
		"continueOnError": true, "errorKey": "renderErr"}
	if iterator != "" {
		write["dependsOn"] = map[string]any{"name": "cm", "iterator": iterator}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": c681RA, "namespace": h1NS},
		"spec": map[string]any{"api": []any{
			map[string]any{"name": "cm", "path": "/api/v1/namespaces/pub/configmaps/p"},
			write,
		}},
	}}
}

func TestIssue903_S1_SeedMarksADeclaredWriteRADeclinedExternal(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	e := c681Setup(t)
	e.ra = s903WriteRA("")
	up := newB903Counter(t)
	t903PublishSecrets(e, up.srv.URL, true)
	var saDials atomic.Int64
	sa := c681SAServer(t, &saDials)
	prev := seedObjectsGetFn
	seedObjectsGetFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: e.ra.DeepCopy()}
	}
	t.Cleanup(func() { seedObjectsGetFn = prev })
	tok := s425SAToken()
	set := cache.NewSeedDeclinedExternalSet()
	seedCtx := withCohortSeedContext(context.Background(), seedTarget{Groups: []string{c681All}},
		endpoints.Endpoint{ServerURL: sa.URL, Token: tok}, &rest.Config{Host: sa.URL, BearerToken: tok})
	seedCtx = cache.WithSeedDeclinedExternalSet(seedCtx, set)
	ref := templatesv1.ObjectReference{Reference: templatesv1.Reference{Name: c681RA, Namespace: h1NS},
		APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version, Resource: h1RAGVR.Resource}
	if err := seedOneRestaction(seedCtx, "rv", ref, c681AuthnNS, seedModeBoot); err != nil {
		t.Fatalf("seedOneRestaction: %v", err)
	}
	if n := up.writes(); n != 0 {
		t.Fatalf("#903 RED: the seed sent %d write(s)", n)
	}
	if _, stored := cache.ResolvedCache().GetNoTouch(e.key(t, c681A)); stored {
		t.Fatal("#903 RED: a RESTAction that declares a write was seeded into L1")
	}
	if set.Len() == 0 {
		t.Fatal("#903 B2 RED: the seed declined the cell without marking it declined-external — every later pass re-resolves it")
	}
}

func TestIssue903_S2_RefresherSuppressesARefusedWriteRA(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	e := c681Setup(t)
	cache.ResetRefresherForTest() // the suppression markers are package state
	t.Cleanup(cache.ResetRefresherForTest)
	e.ra = s903WriteRA("")
	up := newB903Counter(t)
	t903PublishSecrets(e, up.srv.URL, true)
	k, _, in := dispatchCacheLookupKey(c681A.ctx(), "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, c681RA, -1, -1, nil)
	if in == nil || k == "" {
		t.Fatal("PRE: no key inputs")
	}
	var cr templatesv1.RESTAction
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(e.ra.Object, &cr); err != nil {
		t.Fatal(err)
	}
	// The refresher's re-resolve, through the real RESTAction resolver under the
	// refresher origin: the write stage is refused (background work never
	// writes) and the RESTAction declares it.
	t.Cleanup(setResolveOnceForTest(func(ctx context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		rctx := refresherResolveCtx(xcontext.BuildContext(ctx,
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: c681A.user, Groups: c681A.groups})))
		res, err := restactions.Resolve(rctx, restactions.ResolveOptions{In: cr.DeepCopy(),
			SArc: &rest.Config{Host: e.user.URL}, AuthnNS: c681AuthnNS})
		if err != nil {
			return nil, err
		}
		return encodeResolvedJSON(res)
	}))
	ext0 := cache.ExternalSkippedPut()
	_ = resolveAndPopulateL1(context.Background(), *in, nil, nil)
	if n := up.writes(); n != 0 {
		t.Fatalf("#903 RED: the refresher sent %d write(s)", n)
	}
	if reason, ok := cache.RefreshSuppressedReason(k); !ok || reason != string(cache.PutReasonDeclaredWrite) {
		t.Fatalf("#903 B2 RED: a refused-write re-resolve left the key suppressed=%v reason=%q, want %q — "+
			"it would be re-resolved on every change", ok, reason, cache.PutReasonDeclaredWrite)
	}
	if d := cache.ExternalSkippedPut() - ext0; d != 0 {
		t.Fatalf("#912 RED: a declared write was counted as %d external-skip decline(s) — it is its own reason", d)
	}
}

func TestIssue903_S2b_StageErrorBesideAnExternalTouchIsPermanent(t *testing.T) {
	e := c681Setup(t)
	_ = e
	cache.ResetRefresherForTest() // the suppression markers are package state
	t.Cleanup(cache.ResetRefresherForTest)
	k, _, in := dispatchCacheLookupKey(c681A.ctx(), "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, c681RA, -1, -1, nil)
	if in == nil || k == "" {
		t.Fatal("PRE: no key inputs")
	}
	t.Cleanup(setResolveOnceForTest(func(ctx context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		cache.StageErrorSinkFromContext(ctx).Bump("ext", "upstream 500")
		cache.ExternalTouchedSinkFromContext(ctx).Bump()
		return []byte(`{"status":{}}`), nil
	}))
	_ = resolveAndPopulateL1(context.Background(), *in, nil, nil)
	if reason, ok := cache.RefreshSuppressedReason(k); !ok || reason != string(cache.PutReasonStageError) {
		t.Fatalf("#903 B2 RED: a stage error beside a genuine external touch left the key suppressed=%v reason=%q, want a "+
			"permanent %q", ok, reason, cache.PutReasonStageError)
	}
}

func TestIssue903_S3_DeclaredWriteRANeverStored_TheWriteRunsOnTheNextCall(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	e := c681Setup(t)
	up := newB903Counter(t)
	e.ra = s903WriteRA(`(.cm.data.todo // "[]") | fromjson`)
	t903PublishSecrets(e, up.srv.URL, true)

	// 1. Empty todo list: nothing to write, and the cell is NOT stored.
	e.t903Call(t, c681A)
	if n := up.writes(); n != 0 {
		t.Fatalf("PRE: an empty todo list must send 0 writes, got %d", n)
	}
	if _, stored := cache.ResolvedCache().GetNoTouch(e.key(t, c681A)); stored {
		t.Fatal("#903 F1 RED: a RESTAction that declares a write was stored although it dialed none — " +
			"a later /call would be an L1 hit that never runs the write")
	}

	// 2. The list gains an item.
	cm := c681CMObj("pub", "p", "PUBDATA-681")
	cm.Data["todo"] = `["x"]`
	cm.ObjectMeta = metav1.ObjectMeta{Namespace: "pub", Name: "p"}
	e.update(t, c681CMs, cm)
	deadline := time.Now().Add(10 * time.Second)
	for {
		u, ok := e.rw.GetObject(c681CMs, "pub", "p")
		if ok && u != nil {
			if d, _, _ := unstructured.NestedString(u.Object, "data", "todo"); d == `["x"]` {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("PRE: the informer never saw the todo item")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Content cells are refreshed by the refresher in production; this harness
	// runs none, so drop them.
	var contentKeys []string
	cache.ResolvedCache().RangeMetadata(func(m cache.ResolvedEntryMeta) bool {
		if m.CacheEntryClass == cache.CacheEntryClassApistage {
			contentKeys = append(contentKeys, m.KeyHash)
		}
		return true
	})
	for _, k := range contentKeys {
		cache.ResolvedCache().DeleteForTest(k)
	}

	// 3. The customer's next /call resolves (a miss) and sends the write once.
	e.t903Call(t, c681A)
	if n := up.writes(); n != 1 {
		t.Fatalf("#903 F1: the declared write must run exactly once on the next customer /call, got %d", n)
	}
}

func TestIssue903_S4_WalkMarksADeclaredWriteRADeclinedExternal(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	e := c681Setup(t)
	e.ra = s903WriteRA("")
	up := newB903Counter(t)
	t903PublishSecrets(e, up.srv.URL, true)
	var cr templatesv1.RESTAction
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(e.ra.Object, &cr); err != nil {
		t.Fatal(err)
	}
	// The walk's resolve ctx (phase1_walk.go): a prewarm walk, carrying the
	// sinks walkMarkDeclinedExternal reads and the run's declined-external set.
	set := cache.NewSeedDeclinedExternalSet()
	ctx := cache.WithSeedDeclinedExternalSet(cache.WithPrewarmPath(c681A.ctx()), set)
	ctx, _ = cache.WithStageErrorSink(ctx)
	ctx, _ = cache.WithExternalTouchedSink(ctx)
	ctx, _ = cache.WithPerIdentityForbiddenSink(ctx)
	if _, err := restactions.Resolve(ctx, restactions.ResolveOptions{In: cr.DeepCopy(),
		SArc: &rest.Config{Host: e.user.URL}, AuthnNS: c681AuthnNS}); err != nil {
		t.Logf("walk resolve: %v", err)
	}
	if n := up.writes(); n != 0 {
		t.Fatalf("#903 RED: the walk sent %d write(s)", n)
	}
	w := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "Panel",
		"metadata": map[string]any{"name": "w", "namespace": h1NS}}}
	walkMarkDeclinedExternal(ctx, "key/s4", w, "uid", "1", nil, false)
	if !set.Marked("key/s4") {
		t.Fatal("#903 B2 RED: the walk resolved a RESTAction that declares a write (refused in the background) " +
			"without marking it declined-external — every later walk re-resolves it")
	}
}

func TestIssue903_S5_AnAbortedResolveOfAWriteRAIsNeverStored(t *testing.T) {
	cases := map[string]func(ra *unstructured.Unstructured){
		"dependsOn names no stage": func(ra *unstructured.Unstructured) {
			api := ra.Object["spec"].(map[string]any)["api"].([]any)
			api[1].(map[string]any)["dependsOn"] = map[string]any{"name": "nosuch"}
		},
		"dependsOn cycle": func(ra *unstructured.Unstructured) {
			api := ra.Object["spec"].(map[string]any)["api"].([]any)
			api[0].(map[string]any)["dependsOn"] = map[string]any{"name": "render"}
			api[1].(map[string]any)["dependsOn"] = map[string]any{"name": "cm"}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("CLIENT_MAX_RETRIES", "0")
			e := c681Setup(t)
			up := newB903Counter(t)
			e.ra = s903WriteRA("")
			mutate(e.ra)
			t903PublishSecrets(e, up.srv.URL, true)
			e.t903Call(t, c681A)
			if n := up.writes(); n != 0 {
				t.Fatalf("PRE: an aborted resolve sent %d write(s)", n)
			}
			if _, stored := cache.ResolvedCache().GetNoTouch(e.key(t, c681A)); stored {
				t.Fatal("#912 N2 RED: an aborted resolve of a RESTAction that declares a write was stored")
			}
		})
	}
}
