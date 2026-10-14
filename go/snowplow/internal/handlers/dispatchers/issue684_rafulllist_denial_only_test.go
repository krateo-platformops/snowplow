package dispatchers

// issue684_rafulllist_denial_only_test.go — #684: the paginated full-list
// (raFullList) Put path applies the SAME #681 gate as every other Put site:
// cache.DenialOnly for the stage errors and cache.GenuineExternal for the
// external count, read from the full-list resolve's OWN sinks.
//
// Driven through the REAL apiref.Resolve (paginated, so raFullListServe runs),
// the REAL restactions resolver (F2 classification + the identity-free
// canonical body), the REAL key derivation and the REAL L1, against the #681
// per-user httptest apiserver (c681Setup): it authorizes every dial with the
// real EvaluateRBAC and answers a deny with a 403 whose message NAMES the
// requester and its groups (sentinel strings).
//
// Identities (from c681): A may NOT get x/y; B may (another binding set);
// C has A's exact binding set.
//
// ARMS
//   R1  A's denial-only full list is cached (first sight) and served warm
//       (a Go-slice hit, 0 dials); after an eviction the REPOPULATE branch
//       re-Puts it.
//   R2  a denial plus a 404 is NOT cached, on first sight AND on repopulate.
//   R3  cross-identity isolation + no identity text: B (another set) derives
//       another raKey and gets the fuller body; C (A's set) is served A's cell;
//       no cached cell or served body carries a sentinel; C's served body
//       equals C's own live body byte for byte.
//   R4  the full-list gate reads ITS OWN sinks: an unrelated stage error and an
//       external touch already on the enclosing widget's sinks do not decline
//       the cell, and every bump the full-list resolve makes still reaches the
//       enclosing sinks (the widget's own Put gate sees them).

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

const (
	r684RA = "ra684"
	// The RA filter: one array key (the paginated rows of pub's configmaps) plus
	// the x/y step's outcome — the canonical forbidden body for a denied
	// requester, the object's value for an allowed one.
	r684Filter = `(.slice.offset // 0) as $o
| (.cms.items // [] | sort_by(.metadata.name)) as $all
| (.slice.perPage // ($all | length)) as $pp
| {rows: [$all[$o:($o + $pp)][] | {name: .metadata.name, v: .data.v}],
   den: ((.denErr // .den) | if type == "array" then .[0] else . end | if .data then .data.v else . end)}`
)

type r684Env struct {
	*c681Env
	sa *rest.Config
}

// The two non-denial stage errors R2 mixes in.
var (
	// r684Ghost: a 404. pub/ghost is not in the informer, so the read falls
	// through to the requester's own dial — a GENUINE external touch (not an
	// F2), declined by GenuineExternal.
	r684Ghost = map[string]any{"name": "gho", "path": "/api/v1/namespaces/pub/configmaps/ghost", "continueOnError": true, "errorKey": "ghoErr"}
	// r684BadFilter: an in-process (informer-served) read whose stage filter
	// fails — a stage error with NO external touch, so only the stage-error
	// gate (DenialOnly) can decline it.
	r684BadFilter = map[string]any{"name": "bad", "path": "/api/v1/namespaces/pub/configmaps/p", "filter": `error("boom684")`, "continueOnError": true, "errorKey": "badErr"}
)

func r684RAObj(extra ...map[string]any) *unstructured.Unstructured {
	api := []any{
		map[string]any{"name": "cms", "path": "/api/v1/namespaces/pub/configmaps"},
		map[string]any{"name": "den", "path": "/api/v1/namespaces/x/configmaps/y", "continueOnError": true, "errorKey": "denErr"},
	}
	for _, s := range extra {
		api = append(api, s)
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": r684RA, "namespace": h1NS},
		"spec":       map[string]any{"api": api, "filter": r684Filter},
	}}
}

// r684Setup is c681Setup plus two more pub configmaps (so a page is a strict
// slice) and the RESTAction in a servable informer (objects.Get serves it).
func r684Setup(t *testing.T, extra ...map[string]any) *r684Env {
	t.Helper()
	e := &r684Env{c681Env: c681Setup(t), sa: nil}
	e.sa = &rest.Config{Host: e.user.URL}
	e.create(t, c681CMs, c681CMObj("pub", "p2", "PUBDATA2-684"))
	e.create(t, c681CMs, c681CMObj("pub", "p3", "PUBDATA3-684"))
	if _, err := e.dyn.Resource(h1RAGVR).Namespace(h1NS).Create(context.Background(), r684RAObj(extra...), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create RA: %v", err)
	}
	if _, ch := e.rw.EnsureResourceType(h1RAGVR); ch != nil {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("restactions informer did not sync")
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, okRA := e.rw.GetObject(h1RAGVR, h1NS, r684RA)
		_, okP3 := e.rw.GetObject(c681CMs, "pub", "p3")
		if okRA && okP3 && e.rw.IsServable(h1RAGVR) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("PRE: the RA / pub configmaps never reached the informer")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return e
}

// r684Sinks is the enclosing widget handler's sink set (widgets.go installs
// exactly these on the request ctx before the apiRef resolve).
type r684Sinks struct {
	stage     *cache.StageErrorSink
	forbidden *cache.PerIdentityForbiddenSink
	external  *cache.ExternalTouchedSink
}

func r684WidgetCtx(id ident681) (context.Context, r684Sinks) {
	var s r684Sinks
	ctx := id.ctx()
	ctx, s.stage = cache.WithStageErrorSink(ctx)
	ctx, s.forbidden = cache.WithPerIdentityForbiddenSink(ctx)
	ctx, s.external = cache.WithExternalTouchedSink(ctx)
	ctx, _ = cache.WithSensitiveTouchedSink(ctx)
	ctx, _ = cache.WithUAFTouchedSink(ctx)
	return cache.WithL1KeyContext(ctx, "L1_684_widget_"+id.user), s
}

// page resolves page 1 (perPage 2) of the RA for id through apiref.Resolve.
func (e *r684Env) page(t *testing.T, ctx context.Context, id ident681) ([]byte, int64) {
	t.Helper()
	d0 := e.dials.Load()
	out, err := apiref.Resolve(ctx, apiref.ResolveOptions{
		ApiRef: templatesv1.ObjectReference{
			Reference:  templatesv1.Reference{Name: r684RA, Namespace: h1NS},
			Resource:   h1RAGVR.Resource,
			APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version,
		},
		RC: e.sa, AuthnNS: c681AuthnNS, PerPage: 2, Page: 1,
	})
	if err != nil {
		t.Fatalf("apiref.Resolve for %s: %v", id.user, err)
	}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if rows, _ := out["rows"].([]any); len(rows) != 2 {
		t.Fatalf("PRE: %s's page must be a 2-row slice: %s", id.user, s425Trunc(b))
	}
	return b, e.dials.Load() - d0
}

func (e *r684Env) call(t *testing.T, id ident681) ([]byte, int64) {
	t.Helper()
	ctx, _ := r684WidgetCtx(id)
	return e.page(t, ctx, id)
}

func (e *r684Env) raKey(t *testing.T, id ident681) string {
	t.Helper()
	in, ok := apiref.SeedFullListRAKeyInputsForTest(id.ctx(), h1RAGVR, h1NS, r684RA, nil)
	if !ok {
		t.Fatalf("PRE: %s must derive a raKey", id.user)
	}
	return cache.ComputeKey(in)
}

func (e *r684Env) cell(t *testing.T, id ident681) ([]byte, bool) {
	t.Helper()
	got, ok := cache.ResolvedCache().GetNoTouch(e.raKey(t, id))
	if !ok || got == nil {
		return nil, false
	}
	return got.RawJSON, true
}

func TestIssue684_R1_DenialOnlyFullListCached_WarmAndRepopulate(t *testing.T) {
	e := r684Setup(t)
	body1, d1 := e.call(t, c681A)
	if d1 == 0 {
		t.Fatal("PRE: A's first page must dial (the 403 on x/y)")
	}
	if bytes.Contains(body1, []byte(c681SecretY)) || !bytes.Contains(body1, []byte(`"reason":"Forbidden"`)) {
		t.Fatalf("PRE: A's page must carry the forbidden body and lack x/y: %s", s425Trunc(body1))
	}
	if _, ok := e.cell(t, c681A); !ok {
		t.Fatal("R1 RED (first sight): A's denial-only full list was not cached — every page is a cold navigation")
	}
	if got := c681DenialPuts(cache.DenialOnlyPutSiteRAFullList); got != 1 {
		t.Fatalf("denial_only_put_total{ra_full_list} = %d, want 1", got)
	}

	hits0 := cache.RAFullListServeSnapshot().Hit
	body2, d2 := e.call(t, c681A)
	if d2 != 0 || cache.RAFullListServeSnapshot().Hit-hits0 != 1 {
		t.Fatalf("R1 RED: A's second page was not a Go-slice hit (dials=%d)", d2)
	}
	if !bytes.Equal(body1, body2) {
		t.Fatalf("R1: the warm page differs from the cold one:\n cold %s\n warm %s", s425Trunc(body1), s425Trunc(body2))
	}

	// Evict the cell; the verdict stays known-sliceable → the REPOPULATE branch.
	cache.ResolvedCache().DeleteForTest(e.raKey(t, c681A))
	rep0 := cache.RAFullListServeSnapshot().Repopulate
	body3, _ := e.call(t, c681A)
	if cache.RAFullListServeSnapshot().Repopulate-rep0 != 1 {
		t.Fatal("R1 RED (repopulate): the evicted denial-only cell was not re-Put on the repopulate branch")
	}
	if _, ok := e.cell(t, c681A); !ok {
		t.Fatal("R1 RED (repopulate): the cell is absent after the repopulate branch")
	}
	if !bytes.Equal(body1, body3) {
		t.Fatalf("R1: the repopulated page differs from the first one")
	}
	if got := c681DenialPuts(cache.DenialOnlyPutSiteRAFullList); got != 2 {
		t.Fatalf("denial_only_put_total{ra_full_list} = %d, want 2", got)
	}
}

// r684MixedNotCached drives a denial + one other stage error through first
// sight and then the repopulate branch; neither may Put. It returns the
// stage-error-gate and external-gate decline deltas.
func r684MixedNotCached(t *testing.T, what string, other map[string]any) (stageDeclines, extDeclines uint64) {
	t.Helper()
	e := r684Setup(t, other)
	dec0, ext0 := apiref.RAFullListStageErrorDeclinedForTest(), cache.ExternalSkippedPut()
	e.call(t, c681A)
	if _, ok := e.cell(t, c681A); ok {
		t.Fatalf("R2 RED (first sight, %s): a mixed full list was cached", what)
	}
	// The verdict is recorded on first sight, so the second page takes the
	// REPOPULATE branch, which must decline too.
	s0 := cache.RAFullListServeSnapshot()
	if _, d := e.call(t, c681A); d == 0 {
		t.Fatalf("R2 (%s): the second page was served from a cell", what)
	}
	if _, ok := e.cell(t, c681A); ok {
		t.Fatalf("R2 RED (repopulate, %s): a mixed full list was cached", what)
	}
	if s := cache.RAFullListServeSnapshot(); s.Repopulate != s0.Repopulate || s.Hit != s0.Hit || s.Fallback-s0.Fallback != 1 {
		t.Fatalf("R2 (%s): the second page must be a repopulate-branch decline (before %+v after %+v)", what, s0, s)
	}
	if got := c681DenialPuts(cache.DenialOnlyPutSiteRAFullList); got != 0 {
		t.Fatalf("denial_only_put_total{ra_full_list} = %d, want 0", got)
	}
	return apiref.RAFullListStageErrorDeclinedForTest() - dec0, cache.ExternalSkippedPut() - ext0
}

// R2a — denial + an in-process stage error (no external touch): the stage-error
// gate (DenialOnly) is the ONLY thing that can decline it.
func TestIssue684_R2a_MixedDenialAndInProcessError_NotCached_BothBranches(t *testing.T) {
	if st, _ := r684MixedNotCached(t, "denial + failing stage filter", r684BadFilter); st != 2 {
		t.Fatalf("R2a: stage-error declines = %d, want 2 (first sight + repopulate)", st)
	}
}

// R2b — denial + a 404 on the requester's own dial: a genuine external touch
// (not an F2), declined by GenuineExternal on both branches.
func TestIssue684_R2b_MixedDenialAndNotFound_NotCached_BothBranches(t *testing.T) {
	if _, ex := r684MixedNotCached(t, "denial + 404", r684Ghost); ex != 2 {
		t.Fatalf("R2b: external declines = %d, want 2 (first sight + repopulate)", ex)
	}
}

func TestIssue684_R3_CrossIdentityIsolation_AndNoIdentityText(t *testing.T) {
	e := r684Setup(t)
	if e.raKey(t, c681A) != e.raKey(t, c681C) {
		t.Fatal("PRE: C must share A's binding set (and raKey)")
	}
	// B's key distinctness is asserted AFTER the behavioural check below, so a
	// key collapse surfaces as B being SERVED A's page, not only as a key compare.
	bodyA, _ := e.call(t, c681A)
	cellA, ok := e.cell(t, c681A)
	if !ok {
		t.Fatal("PRE: A's full-list cell was not cached")
	}
	c681NoIdentity(t, "A's cached full-list cell", cellA)
	c681NoIdentity(t, "A's served page", bodyA)
	if !bytes.Contains(cellA, []byte(`"reason":"Forbidden"`)) {
		t.Fatalf("PRE: A's cell must carry the canonical forbidden body: %s", s425Trunc(cellA))
	}

	bodyC, dC := e.call(t, c681C)
	if dC != 0 {
		t.Fatalf("PRE: C should be served A's cell (0 dials); dialled %d", dC)
	}
	c681NoIdentity(t, "C's served page", bodyC)

	bodyB, _ := e.call(t, c681B)
	if !bytes.Contains(bodyB, []byte(c681SecretY)) || bytes.Equal(bodyB, bodyA) {
		t.Fatalf("R3 RED: B was served A's denial-only page (or lacks x/y): %s", s425Trunc(bodyB))
	}
	if e.raKey(t, c681A) == e.raKey(t, c681B) {
		t.Fatal("R3 RED: B derives A's raKey")
	}
	if cellB, ok := e.cell(t, c681B); !ok || bytes.Equal(cellB, cellA) {
		t.Fatalf("R3 RED: B's cell is absent or equals A's")
	}

	// C's live (cold) page equals the cached page C was served.
	cache.ResetResolvedCacheForTest()
	liveC, dLive := e.call(t, c681C)
	if dLive == 0 {
		t.Fatal("PRE: C's live page was not cold")
	}
	if !bytes.Equal(liveC, bodyC) {
		t.Fatalf("R3 RED: C's served (A-cached) page differs from C's own live page:\nserved %s\n  live %s", s425Trunc(bodyC), s425Trunc(liveC))
	}
}

func TestIssue684_R4_FullListGateReadsItsOwnSinks_AndForwardsToTheWidget(t *testing.T) {
	e := r684Setup(t)
	ctx, outer := r684WidgetCtx(c681A)
	// Another stage of the enclosing widget resolve already failed (not a
	// denial) and touched an external endpoint. Neither is part of the full
	// list, so neither may decline its cell.
	outer.stage.Bump("unrelated-widget-stage", "boom")
	outer.external.Bump()
	e.page(t, ctx, c681A)
	if _, ok := e.cell(t, c681A); !ok {
		t.Fatal("R4 RED: the full-list gate read the enclosing widget's sinks — an unrelated error declined a denial-only cell")
	}
	// The full-list resolve's bumps reach the widget's own sinks: its Put gate
	// must still see the denial (first sight resolves twice: full + page).
	if outer.stage.Count() < 2 || outer.forbidden.Stage() != outer.stage.Count()-1 ||
		outer.forbidden.External() != outer.external.Count()-1 {
		t.Fatalf("R4 RED: the full-list resolve's bumps did not reach the widget's sinks (stage=%d forbidden=%d/%d external=%d)",
			outer.stage.Count(), outer.forbidden.Stage(), outer.forbidden.External(), outer.external.Count())
	}
}
