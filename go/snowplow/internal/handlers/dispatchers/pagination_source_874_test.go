package dispatchers

// pagination_source_874_test.go — #874: the prewarm apiRef pagination drain
// pages exactly as far as the DATA SOURCE says, through the REAL widget
// resolver (widgets.Resolve, the production paginationResolvePageFn). Only the
// apiRef data source and the CRD-status validation are stood in
// (widgets.SetResolveApiRefForTest / SetValidateObjectStatusForTest), so the
// emitted status.resourcesRefs.slice — and therefore the drain's continue and
// its cursor threading — is the production computation.
//
//   - a source that ignores the slice (the 057 sidebar-nav shape): 1 page;
//   - a source declaring a bool continue from offset: pages until it says false;
//   - a source paging by cursor: every item exactly once, the cursor handed
//     from page to page, ending at the empty cursor.

import (
	"context"
	"fmt"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

var s874GVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "menus"}

// s874Widget: an apiRef + resourcesRefsTemplate widget (drain-eligible) whose
// refs fan out over the source's `.refs`. The refs are non-GET so the drain
// does not descend into them (the test is about the page loop).
func s874Widget() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Menu",
		"metadata":   map[string]any{"name": "s874-menu", "namespace": "krateo-system"},
		"spec": map[string]any{
			"apiRef":     map[string]any{"name": "s874-source", "namespace": "krateo-system"},
			"widgetData": map[string]any{"items": []any{}},
			"resourcesRefsTemplate": []any{map[string]any{
				"iterator": `${ .refs }`,
				"template": map[string]any{"id": `${ .id }`, "name": `${ .id }`,
					"apiVersion": "v1", "resource": "namespaces", "verb": "DELETE"},
			}},
		},
	}}
}

type s874Call struct {
	page   int
	cursor string
}

// s874Drain runs page 1 through the real resolver, then the real drain, with
// source as the apiRef data source. It returns every source call and every
// ref id the pages produced.
func s874Drain(t *testing.T, maxPages int,
	source func(opts widgets.ResolveOptions) map[string]any) (calls []s874Call, ids []string) {
	t.Helper()
	paginationTestMu.Lock()
	t.Cleanup(paginationTestMu.Unlock)
	drainAllCustomerInFlight()
	prevTM := xenv.TestMode()
	xenv.SetTestMode(true)
	t.Cleanup(func() { xenv.SetTestMode(prevTM) })
	old := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = maxPages
	t.Cleanup(func() { phase1MaxApiRefPagesForTest = old })

	var mu sync.Mutex
	t.Cleanup(widgets.SetResolveApiRefForTest(func(_ context.Context, opts widgets.ResolveOptions) (map[string]any, error) {
		mu.Lock()
		calls = append(calls, s874Call{page: opts.Page, cursor: opts.Cursor})
		mu.Unlock()
		return source(opts), nil
	}))
	t.Cleanup(widgets.SetValidateObjectStatusForTest(func(context.Context, *rest.Config, map[string]any) error { return nil }))
	prevFetch, prevResolve := paginationFetchPageFn, paginationResolvePageFn
	paginationFetchPageFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
		return objects.Result{Unstructured: s874Widget()}
	}
	// The PRODUCTION resolve, wrapped only to collect the page's ref ids.
	paginationResolvePageFn = func(ctx context.Context, opts widgets.ResolveOptions) (*unstructured.Unstructured, error) {
		res, err := widgets.Resolve(ctx, opts)
		collectIDs(res, &ids)
		return res, err
	}
	t.Cleanup(func() { paginationFetchPageFn, paginationResolvePageFn = prevFetch, prevResolve })

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: "https://test.invalid"}))
	ctx = cache.WithInternalRESTConfig(ctx, &rest.Config{Host: "https://test.invalid"})

	in := s874Widget()
	page1, err := widgets.Resolve(ctx, widgets.ResolveOptions{In: s874Widget(), PerPage: 5, Page: 1})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	collectIDs(page1, &ids)
	iterateApiRefPages(ctx, newPhase1Walker(nil, "ns"), in, s874GVR, page1, 1, 5, 5, 1, "ns")
	return calls, ids
}

func collectIDs(res *unstructured.Unstructured, ids *[]string) {
	if res == nil {
		return
	}
	items, _, _ := unstructured.NestedSlice(res.Object, "status", "resourcesRefs", "items")
	for _, it := range items {
		if m, ok := it.(map[string]any); ok {
			if id, _ := m["id"].(string); id != "" {
				*ids = append(*ids, id)
			}
		}
	}
}

func s874Items(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = map[string]any{"id": fmt.Sprintf("item-%02d", i)}
	}
	return out
}

func TestS874_Drain_SliceIgnoringSourceIsOnePage(t *testing.T) {
	// The 057 sidebar-nav shape: the same 7 (≥ perPage) refs on every page, no
	// declared pagination. Under the count rule this ran to the backstop.
	calls, _ := s874Drain(t, 50, func(widgets.ResolveOptions) map[string]any {
		return map[string]any{"refs": s874Items(7)}
	})
	if len(calls) != 1 {
		t.Fatalf("#874 RED: a source that declares no pagination was resolved %d times; want exactly 1 page", len(calls))
	}
}

func TestS874_Drain_BoolContinueSourcePagesUntilFalse(t *testing.T) {
	const n, per = 12, 5
	all := s874Items(n)
	calls, ids := s874Drain(t, 50, func(o widgets.ResolveOptions) map[string]any {
		off := (o.Page - 1) * o.PerPage
		end := min(off+o.PerPage, n)
		return map[string]any{"refs": all[off:end], "slice": map[string]any{"continue": end < n}}
	})
	if len(calls) != 3 {
		t.Fatalf("#874 RED: a bool-continue source over %d items at perPage %d was resolved %d times; want 3", n, per, len(calls))
	}
	if len(ids) != n {
		t.Fatalf("the pages produced %d ids, want %d", len(ids), n)
	}
}

func TestS874_Drain_CursorSourceIsThreadedToTheEnd(t *testing.T) {
	const n = 12
	all := s874Items(n)
	calls, ids := s874Drain(t, 50, func(o widgets.ResolveOptions) map[string]any {
		// A cursor source: it ignores page/offset and pages ONLY by its cursor.
		start := 0
		if o.Cursor != "" {
			fmt.Sscanf(o.Cursor, "c%d", &start)
		}
		end := min(start+o.PerPage, n)
		next := ""
		if end < n {
			next = fmt.Sprintf("c%d", end)
		}
		return map[string]any{"refs": all[start:end], "slice": map[string]any{"cursor": next, "perPage": o.PerPage}}
	})
	wantCursors := []string{"", "c5", "c10"}
	if len(calls) != len(wantCursors) {
		t.Fatalf("#874 RED: a cursor source over %d items was resolved %d times (cursors %v); want %d pages ending at the empty cursor",
			n, len(calls), calls, len(wantCursors))
	}
	for i, c := range calls {
		if c.cursor != wantCursors[i] {
			t.Fatalf("#874 RED: page %d was asked with cursor %q, want %q (calls %v)", i+1, c.cursor, wantCursors[i], calls)
		}
	}
	seen := map[string]int{}
	for _, id := range ids {
		seen[id]++
	}
	if len(seen) != n {
		t.Fatalf("the cursor walk reached %d distinct items, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("item %s produced %d times; a cursor walk visits each once", id, c)
		}
	}
}
