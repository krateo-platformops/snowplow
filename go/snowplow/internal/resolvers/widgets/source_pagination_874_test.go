package widgets

// source_pagination_874_test.go — #874: status.resourcesRefs.slice.continue is
// the DATA SOURCE's declaration, never an inference from the item count.
//
// The real Resolve runs; only the apiRef data source (resolveApiRefFn) and the
// CRD-status validation are stubbed (stubApiRef / stubValidate).

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// s874Menu is a widget whose items fan out over the apiRef RA's `.refs`
// (the sidebar-nav shape).
func s874Menu() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Menu",
		"metadata":   map[string]any{"name": "nav", "namespace": "demo-system"},
		"spec": map[string]any{
			"widgetData": map[string]any{"items": []any{}},
			"resourcesRefsTemplate": []any{
				map[string]any{
					"iterator": `${ .refs }`,
					"template": map[string]any{
						// a built-in kind, so the hermetic ref resolution needs no discovery
						"id": `${ .id }`, "name": `${ .id }`,
						"apiVersion": "v1", "resource": "namespaces", "verb": "GET",
					},
				},
			},
		},
	}}
}

func s874Refs(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = map[string]any{"id": "p" + string(rune('a'+i))}
	}
	return out
}

func s874Slice(t *testing.T, w *unstructured.Unstructured) map[string]any {
	t.Helper()
	items, ok, err := unstructured.NestedSlice(w.Object, "status", "resourcesRefs", "items")
	require.NoError(t, err)
	require.True(t, ok, "NON-VACUITY: the fan-out produced resourcesRefs items")
	require.NotEmpty(t, items)
	sl, ok, err := unstructured.NestedMap(w.Object, "status", "resourcesRefs", "slice")
	require.NoError(t, err)
	require.True(t, ok, "a paginated resolve emits status.resourcesRefs.slice")
	return sl
}

func TestS874_SliceIgnoringSourceIsOnePage(t *testing.T) {
	// 7 refs ≥ perPage 5, no slice in the source output: the count rule said
	// continue=true on every page (the sidebar-nav loop).
	stubApiRef(t, map[string]any{"refs": s874Refs(7)}, nil)
	stubValidate(t, nil)
	w := s874Menu()
	_, err := Resolve(hermeticCtx(t), ResolveOptions{In: w, PerPage: 5, Page: 1})
	require.NoError(t, err)
	sl := s874Slice(t, w)
	assert.Equal(t, false, sl["continue"], "#874 RED: a source that declares no pagination must yield exactly one page")
	assert.EqualValues(t, 1, sl["page"])
	_, hasCursor := sl["cursor"]
	assert.False(t, hasCursor)
}

func TestS874_BoolContinueIsTakenAsIs(t *testing.T) {
	for _, c := range []struct {
		declared bool
		wantPage int
	}{{true, 3}, {false, 2}} {
		stubApiRef(t, map[string]any{"refs": s874Refs(2), "slice": map[string]any{"continue": c.declared}}, nil)
		stubValidate(t, nil)
		w := s874Menu()
		_, err := Resolve(hermeticCtx(t), ResolveOptions{In: w, PerPage: 5, Page: 2})
		require.NoError(t, err)
		sl := s874Slice(t, w)
		// 2 refs < perPage: the count rule would have said false; the source says otherwise.
		assert.Equal(t, c.declared, sl["continue"], "#874 RED: slice.continue must be the source's bool")
		assert.EqualValues(t, c.wantPage, sl["page"])
	}
}

func TestS874_CursorSourceContinuesWhileCursorNonEmpty(t *testing.T) {
	var seenCursor string
	orig := resolveApiRefFn
	t.Cleanup(func() { resolveApiRefFn = orig })
	for _, c := range []struct {
		cursor   string
		wantMore bool
	}{{"tok-2", true}, {"", false}} {
		cur := c.cursor
		resolveApiRefFn = func(_ context.Context, opts ResolveOptions) (map[string]any, error) {
			seenCursor = opts.Cursor
			return map[string]any{"refs": s874Refs(5), "slice": map[string]any{"cursor": cur, "perPage": 5}}, nil
		}
		stubValidate(t, nil)
		w := s874Menu()
		_, err := Resolve(hermeticCtx(t), ResolveOptions{In: w, PerPage: 5, Page: 1, Cursor: "tok-1"})
		require.NoError(t, err)
		assert.Equal(t, "tok-1", seenCursor, "the request cursor must reach the apiRef data source")
		sl := s874Slice(t, w)
		assert.Equal(t, c.wantMore, sl["continue"], "#874 RED: continue = (source cursor != \"\")")
		if c.wantMore {
			assert.Equal(t, c.cursor, sl["cursor"], "the next page's cursor is echoed")
		} else {
			_, hasCursor := sl["cursor"]
			assert.False(t, hasCursor, "no cursor is echoed on the last page")
		}
	}
}

func TestS874_UnpaginatedRequestEmitsNoSlice(t *testing.T) {
	// The SPA's Shell fetches the Menu without page/perPage: no slice at all,
	// exactly as before #874 (the guard on opts.PerPage/Page is unchanged).
	stubApiRef(t, map[string]any{"refs": s874Refs(7), "slice": map[string]any{"continue": true}}, nil)
	stubValidate(t, nil)
	w := s874Menu()
	_, err := Resolve(hermeticCtx(t), ResolveOptions{In: w})
	require.NoError(t, err)
	_, ok, _ := unstructured.NestedMap(w.Object, "status", "resourcesRefs", "slice")
	assert.False(t, ok, "an unpaginated resolve must not emit status.resourcesRefs.slice")
}

func TestS874_ResolveApiRefHandsTheCursorToApiref(t *testing.T) {
	var got apiref.ResolveOptions
	orig := apirefResolveFn
	apirefResolveFn = func(_ context.Context, o apiref.ResolveOptions) (map[string]any, error) {
		got = o
		return map[string]any{}, nil
	}
	t.Cleanup(func() { apirefResolveFn = orig })
	w := s874Menu()
	w.Object["spec"].(map[string]any)["apiRef"] = map[string]any{"name": "src", "namespace": "demo-system"}
	if _, err := resolveApiRef(hermeticCtx(t), ResolveOptions{In: w, PerPage: 5, Page: 3, Cursor: "c10"}); err != nil {
		t.Fatal(err)
	}
	if got.Cursor != "c10" || got.Page != 3 || got.PerPage != 5 {
		t.Fatalf("#874 RED: resolveApiRef handed apiref cursor=%q page=%d perPage=%d, want c10/3/5", got.Cursor, got.Page, got.PerPage)
	}
}
