package apiref

// source_pagination_874_test.go — #874: a RESTAction that DECLARES its own
// pagination (slice.continue / slice.cursor in its output) is never served by
// the Ship-4a page-independent full list. A page Go-sliced from the unpaginated
// full list would carry the FULL list's declaration, not this page's.
//
// The trap this pins: the first /call lands on a page whose declaration happens
// to equal the full list's (continue=true on page 1 and on the unpaginated
// resolve), so the byte-verify alone would record the RA sliceable — and the
// LAST page would then be served from the full list with continue=true, an
// endless "more" the source never said.

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jqutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

// declaringJQ windows like raSliceJQ and declares continue: true for the
// unpaginated full list (no .slice — "there is more than one page") and from
// the window otherwise. Page 1 and the full list therefore agree, so the
// byte-verify alone would record the RA sliceable.
const declaringJQ = `
(.compositionspanels // []) as $items
| ($items | sort_by(.metadata.creationTimestamp // "") | reverse) as $sorted
| ($sorted | length) as $len
| (.slice.offset  // 0)    as $offset
| (.slice.perPage // $len) as $perPage
| {
    compositionspanels: [ range($offset; $offset + $perPage) | select(. < $len) | $sorted[.] ],
    slice: { continue: ((.slice == null) or (($offset + $perPage) < $len)) }
  }
`

func TestS874_DeclaredPaginationIsNeverSliceable(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	newF6Watcher(t, f6BuildFixture()...)

	panels := panelDict(40)
	var calls atomic.Int64
	resolve := func(_ context.Context, perPage, page int) (map[string]any, error) {
		calls.Add(1)
		dict := map[string]any{"compositionspanels": panels["compositionspanels"]}
		if perPage > 0 && page > 0 {
			dict["slice"] = map[string]any{"perPage": float64(perPage), "page": float64(page),
				"offset": float64((page - 1) * perPage)}
		}
		s, err := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: declaringJQ, Data: dict})
		if err != nil {
			return nil, err
		}
		var out map[string]any
		return out, json.Unmarshal([]byte(s), &out)
	}
	ctx := ctxWithUser(t)

	// First /call on page 1: page 1 and the full list both declare continue=true.
	if _, ok, err := raFullListServe(ctx, gvr(), "krateo-system", "declaring-ra",
		ra(declaringJQ), 10, 1, nil, resolve); err != nil || !ok {
		t.Fatalf("first serve: ok=%v err=%v", ok, err)
	}
	// The last page (4 of 40/10): the source says there is no more.
	got, ok, err := raFullListServe(ctx, gvr(), "krateo-system", "declaring-ra",
		ra(declaringJQ), 10, 4, nil, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		more, _, _ := cache.SourcePagination(got)
		t.Fatalf("#874 RED: a source that declares its own pagination was served page 4 from the 4a full list "+
			"(continue=%v, want the page-keyed resolve and its continue=false)", more)
	}
}

// TestS874_CursorReachesTheRESTActionThroughApirefResolve — end to end through
// the REAL apiref.Resolve → restactions.Resolve → api dict: the RESTAction CR
// (served from the informer over a fake client) echoes the .slice.cursor it was
// handed. A cursor page bypasses the 4a full list and the seed memo (both keyed
// by page alone), so it is resolved page-keyed with the cursor.
func TestS874_CursorReachesTheRESTActionThroughApirefResolve(t *testing.T) {
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	const raName, ns = "s874-cursor-ra", edge3BackingNS
	echo := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": raName, "namespace": ns},
		"spec": map[string]any{
			// One stage (the dict, and with it .slice, is built only for an RA
			// with stages): a GET of this very RESTAction, informer-served.
			"api": []any{map[string]any{"name": "self", "verb": "GET",
				"path": "/apis/templates.krateo.io/v1/namespaces/" + ns + "/restactions/" + raName}},
			"filter": `{ refs: [], slice: { cursor: (.slice.cursor // "none"), page: .slice.page } }`,
		},
	}}
	edge3NewWatcher(t, append(f6BuildFixture(), echo)...)
	added, syncCh := cache.Global().EnsureResourceType(gvr())
	if added {
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatal("restactions informer did not sync")
		}
	}
	if !edge3WaitServable(t, gvr(), 3*time.Second) {
		t.Fatal("restactions GVR never became servable")
	}
	// A REAL seed resolve memo on ctx: its key has no cursor dimension, so a
	// cursor page must bypass it (else page 2 at cursor c10 is served page 2 at c5).
	ctx := cache.WithSeedResolveMemo(cache.WithL1KeyContext(f6CtxWithUser(t, "admin", []string{"system:masters"}), "L1_s874_cursor"),
		cache.NewSeedResolveMemo())
	ref := templatesv1.ObjectReference{
		Reference: templatesv1.Reference{Name: raName, Namespace: ns}, Resource: "restactions", APIVersion: "templates.krateo.io/v1",
	}
	for _, c := range []struct{ cursor, want string }{{"c5", "c5"}, {"c10", "c10"}, {"", "none"}} {
		out, err := Resolve(ctx, ResolveOptions{ApiRef: ref, PerPage: 5, Page: 2, Cursor: c.cursor,
			RC: &rest.Config{Host: "https://test.invalid"}})
		if err != nil {
			t.Fatalf("apiref.Resolve(cursor=%q): %v", c.cursor, err)
		}
		_, got, _ := cache.SourcePagination(out)
		if got != c.want {
			t.Fatalf("#874 RED: the RESTAction saw .slice.cursor=%q, want %q (out=%v)", got, c.want, out)
		}
	}
}
