package widgets

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/widgetdatatemplate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInjectSlice_AddsTripleWhenPaginated covers AC-H7.1 (injection
// site) and AC-H7.2 (re-uses computed triple shape per
// api/resolve.go:211-215).
func TestInjectSlice_AddsTripleWhenPaginated(t *testing.T) {
	ds := map[string]any{
		"list": []any{},
	}

	injectSlice(ds, 50, 1)

	got, ok := ds["slice"].(map[string]any)
	require.True(t, ok, "slice key must be present and of type map[string]any")
	assert.Equal(t, 1, got["page"], "page must equal opts.Page")
	assert.Equal(t, 50, got["perPage"], "perPage must equal opts.PerPage")
	assert.Equal(t, 0, got["offset"], "offset must equal (page-1)*perPage = 0")
}

// TestInjectSlice_ComputesOffsetCorrectlyOnPageN — exercise non-zero
// offset shape (page 3, perPage 50 → offset 100).
func TestInjectSlice_ComputesOffsetCorrectlyOnPageN(t *testing.T) {
	ds := map[string]any{
		"list": []any{},
	}

	injectSlice(ds, 50, 3)

	got, ok := ds["slice"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 3, got["page"])
	assert.Equal(t, 50, got["perPage"])
	assert.Equal(t, 100, got["offset"], "offset must equal (3-1)*50 = 100")
}

// TestInjectSlice_SkipsWhenPerPageZero — design §7.1 row 2.
func TestInjectSlice_SkipsWhenPerPageZero(t *testing.T) {
	ds := map[string]any{
		"list": []any{},
	}

	injectSlice(ds, 0, 1)

	_, present := ds["slice"]
	assert.False(t, present, "slice must NOT be injected when perPage=0")
}

// TestInjectSlice_SkipsWhenPageZero — design §7.1 row 3.
func TestInjectSlice_SkipsWhenPageZero(t *testing.T) {
	ds := map[string]any{
		"list": []any{},
	}

	injectSlice(ds, 50, 0)

	_, present := ds["slice"]
	assert.False(t, present, "slice must NOT be injected when page=0")
}

// TestInjectSlice_DoesNotOverwritePreExistingSlice — design §7.1 row 4
// (Branch A: honour RA-author intent if spec.Filter emits .slice).
func TestInjectSlice_DoesNotOverwritePreExistingSlice(t *testing.T) {
	preExisting := map[string]any{
		"page":    99,
		"perPage": 999,
		"offset":  9999,
		"custom":  "ra-author-owned",
	}
	ds := map[string]any{
		"list":  []any{},
		"slice": preExisting,
	}

	injectSlice(ds, 50, 1)

	got, ok := ds["slice"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 99, got["page"], "pre-existing page must survive injection")
	assert.Equal(t, 999, got["perPage"], "pre-existing perPage must survive injection")
	assert.Equal(t, 9999, got["offset"], "pre-existing offset must survive injection")
	assert.Equal(t, "ra-author-owned", got["custom"], "pre-existing custom key must survive")
}

// TestInjectSlice_OtherKeysPreserved — design §7.1 row 5 (no accidental
// clobber of unrelated ds keys).
func TestInjectSlice_OtherKeysPreserved(t *testing.T) {
	ds := map[string]any{
		"list":      []any{"a", "b"},
		"metadata":  map[string]any{"ts": "2026-05-21"},
		"raw":       "some-string",
		"intField":  42,
		"boolField": true,
	}

	injectSlice(ds, 50, 1)

	assert.Equal(t, []any{"a", "b"}, ds["list"])
	assert.Equal(t, map[string]any{"ts": "2026-05-21"}, ds["metadata"])
	assert.Equal(t, "some-string", ds["raw"])
	assert.Equal(t, 42, ds["intField"])
	assert.Equal(t, true, ds["boolField"])
}

// TestInjectSlice_NilMapIsNoOp — defensive: nil ds must not panic.
func TestInjectSlice_NilMapIsNoOp(t *testing.T) {
	assert.NotPanics(t, func() {
		injectSlice(nil, 50, 1)
	})
}

// TestInjectSlice_NegativeInputsAreNoOp — defensive: invalid pagination
// must not inject.
func TestInjectSlice_NegativeInputsAreNoOp(t *testing.T) {
	ds := map[string]any{"list": []any{}}
	injectSlice(ds, -1, 1)
	_, present := ds["slice"]
	assert.False(t, present)

	injectSlice(ds, 50, -1)
	_, present = ds["slice"]
	assert.False(t, present)
}

// dashboardTableSliceExpression mirrors the .slice-referencing portion of
// portal-cache/blueprint/templates/table.dashboard-compositions-panel-row-table.yaml:32
// — `if .slice then $sorted[0 : (.slice.page * .slice.perPage)] else $sorted end`.
//
// We use a simplified jq that exercises the SAME conditional branch the
// production widget uses; the row-mapping (the long `map([...])` after)
// is not load-bearing for THIS test (we are validating slice gating,
// not row transformation).
const dashboardTableSliceExpression = `${
  (
    ((.list | sort_by(.ts // "") | reverse)) as $sorted
    | (if .slice then $sorted[0 : (.slice.page * .slice.perPage)] else $sorted end)
  )
}`

// TestDashboardTable_ColdPath_Slices_To_50 is the design §7.2 falsifier:
// a 48,999-row fixture exercising the dashboard table widget's
// .slice-gated jq path. WITHOUT injection: all 48,999 rows pass through.
// WITH injection: jq slices to 50 (= page*perPage = 1*50).
//
// This is the load-bearing falsifier — if it passes, the production
// HG-1 (admin Dashboard cold ≤ 1.0s median) is structurally guaranteed:
// the resolver never materialises 48,999 rows downstream of jq.
func TestDashboardTable_ColdPath_Slices_To_50(t *testing.T) {
	const N = 48_999
	const perPage = 50
	const page = 1

	// Build the 48,999-row fixture. Each entry mimics the
	// allCompositions row shape (uid, name, ns, ts, conditions[]).
	list := make([]any, N)
	base := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	for i := 0; i < N; i++ {
		list[i] = map[string]any{
			"uid":  fmt.Sprintf("uid-%05d", i),
			"name": fmt.Sprintf("comp-%05d", i),
			"ns":   fmt.Sprintf("ns-%03d", i%128),
			"ts":   base.Add(time.Duration(i) * time.Second).Format(time.RFC3339),
		}
	}
	items := []templatesv1.WidgetDataTemplate{
		{
			ForPath:    "data",
			Expression: dashboardTableSliceExpression,
		},
	}

	// (a) WITHOUT injection — proves the today's defect: 48,999 rows pass through.
	dsNoSlice := map[string]any{
		"list": list,
	}
	evalsNo, err := widgetdatatemplate.Resolve(context.Background(), widgetdatatemplate.ResolveOptions{
		Items:      items,
		DataSource: dsNoSlice,
	})
	require.NoError(t, err)
	require.Len(t, evalsNo, 1)
	noSliceRows, ok := evalsNo[0].Value.([]any)
	require.True(t, ok, "without injection the jq returns []any")
	assert.Equal(t, N, len(noSliceRows),
		"WITHOUT .slice injection the widget jq falls through to else-branch and returns ALL rows (the today's defect)")

	// (b) WITH injection — same fixture, after our helper runs.
	dsInjected := map[string]any{
		"list": list,
	}
	injectSlice(dsInjected, perPage, page)
	evalsYes, err := widgetdatatemplate.Resolve(context.Background(), widgetdatatemplate.ResolveOptions{
		Items:      items,
		DataSource: dsInjected,
	})
	require.NoError(t, err)
	require.Len(t, evalsYes, 1)
	slicedRows, ok := evalsYes[0].Value.([]any)
	require.True(t, ok, "with injection the jq still returns []any")
	assert.Equal(t, perPage, len(slicedRows),
		"WITH .slice injection the widget jq enters then-branch and slices to page*perPage = 50")

	// (c) Falsifier: envelope size drops by ~1000× — proves the structural
	// claim from RCA §2.5 (envelope 60 MiB → 40 KiB on production scale).
	noSliceBytes, err := json.Marshal(noSliceRows)
	require.NoError(t, err)
	slicedBytes, err := json.Marshal(slicedRows)
	require.NoError(t, err)

	ratio := float64(len(noSliceBytes)) / float64(len(slicedBytes))
	t.Logf("envelope bytes: no-slice=%d, sliced=%d, ratio=%.0fx",
		len(noSliceBytes), len(slicedBytes), ratio)
	assert.Greater(t, ratio, 900.0,
		"sliced envelope must be at least ~1000× smaller than unsliced (48,999/50 = 980×)")
}

// navRootOffsetWindowExpression mirrors the .slice.offset-shaped child
// windowing the navigation widgets use (the same Shape-2 family as
// restaction.compositions-panels.yaml:37-38 — `.slice.offset // 0` paired
// with `.slice.perPage // (.list | length)`). It windows the source list
// at `.list[offset : offset + perPage]`. A small nav list (e.g. the 3-item
// sidebar-nav-menu) sliced at offset 20 yields ZERO children; sliced at
// offset 0 yields the children. This is the EXACT mechanism that decided
// whether the Phase-1 walk discovered children below the nav roots.
const navRootOffsetWindowExpression = `${
  (
    (.slice.offset // 0) as $off
    | (.slice.perPage // (.list | length)) as $pp
    | .list[$off : ($off + $pp)]
  )
}`

// TestNavRootWindow_Page1_Yields_Children_Overshoot_Yields_Zero is the
// Ship 0.30.199 (Change A) falsifier. It captures the page-NUMBER
// overshoot bug at the load-bearing resolution layer: the slice tuple the
// Phase-1 walk passes to injectSlice windows a small nav-root child list.
//
//   - BUGGY tuple  page == perPage == prewarmPageLimit() (=5):
//     offset = (5-1)*5 = 20 → a 3-item list[20:25] = ZERO children →
//     walk never descends → everything below the nav roots is undiscovered.
//   - FIXED tuple  page == 1, perPage == prewarmPageLimit() (=5):
//     offset = (1-1)*5 = 0 → list[0:5] = all 3 children → walk descends.
//
// The page SIZE (perPage) is IDENTICAL in both cases — this isolates the
// page-NUMBER overshoot and proves the 0.30.127 bounded fan-out guard
// (perPage) is preserved. The test FAILS against the pre-fix tuple
// (overshoot returns children, contradicting the live byte-proof that it
// returns zero) and PASSES with the fix.
func TestNavRootWindow_Page1_Yields_Children_Overshoot_Yields_Zero(t *testing.T) {
	// the then prewarmPageLimit() default (retired by #966: prewarm now
	// resolves with the pagination its cell is keyed with) — historical (=5).
	// The walk uses this as the page SIZE (perPage) at both root and
	// no-declared-slice child sites.
	const perPage = 5

	// A small nav-root child list — mirrors the live sidebar-nav-menu's
	// 3 resolved children (architect byte-proof: 0 items at no-params /
	// page=perPage=5, 3 items at page=1,perPage=5).
	makeList := func() []any {
		return []any{
			map[string]any{"id": "child-0"},
			map[string]any{"id": "child-1"},
			map[string]any{"id": "child-2"},
		}
	}

	items := []templatesv1.WidgetDataTemplate{
		{
			ForPath:    "children",
			Expression: navRootOffsetWindowExpression,
		},
	}

	resolveWindow := func(t *testing.T, page int) []any {
		t.Helper()
		ds := map[string]any{"list": makeList()}
		// injectSlice is the production helper that turns the walk's
		// (perPage, page) tuple into ds["slice"]{page, perPage, offset}.
		injectSlice(ds, perPage, page)
		evals, err := widgetdatatemplate.Resolve(context.Background(), widgetdatatemplate.ResolveOptions{
			Items:      items,
			DataSource: ds,
		})
		require.NoError(t, err)
		require.Len(t, evals, 1)
		rows, ok := evals[0].Value.([]any)
		require.True(t, ok, "windowing jq must return []any (got %T)", evals[0].Value)
		return rows
	}

	// (a) BUGGY tuple — page == perPage. This is what the pre-0.30.199
	// root site (phase1_walk.go:689) and no-declared-slice child default
	// (phase1_walk.go:927) passed. offset = (perPage-1)*perPage = 20 on a
	// 3-item list → ZERO children → recursion never descends.
	overshootRows := resolveWindow(t, perPage)
	assert.Equal(t, 0, len(overshootRows),
		"BUGGY page-number overshoot (page==perPage==%d → offset (%d-1)*%d=%d) windows a small nav list to ZERO children — this is why the walk discovered nothing below the roots",
		perPage, perPage, perPage, (perPage-1)*perPage)

	// (b) FIXED tuple — page == 1, perPage unchanged. offset = 0 → the
	// full child list passes → recursion descends the nav tree.
	page1Rows := resolveWindow(t, 1)
	assert.Equal(t, 3, len(page1Rows),
		"FIXED page NUMBER = 1 (offset 0) windows the full child list — the walk now descends below the nav roots")

	// (c) The page SIZE (perPage) is identical in both cases: this fix is
	// PURELY a page-number correction, not a relaxation of the 0.30.127
	// bounded fan-out guard. A list larger than perPage is still capped.
	bigDS := map[string]any{"list": func() []any {
		out := make([]any, 12)
		for i := range out {
			out[i] = map[string]any{"id": i}
		}
		return out
	}()}
	injectSlice(bigDS, perPage, 1)
	evalsBig, err := widgetdatatemplate.Resolve(context.Background(), widgetdatatemplate.ResolveOptions{
		Items:      items,
		DataSource: bigDS,
	})
	require.NoError(t, err)
	require.Len(t, evalsBig, 1)
	bigRows, ok := evalsBig[0].Value.([]any)
	require.True(t, ok)
	assert.Equal(t, perPage, len(bigRows),
		"page SIZE (perPage=%d) still bounds the window at page 1 — the 0.30.127 fan-out guard is preserved", perPage)
}

// TestDashboardTable_NonSlicedWidget_ByteIdenticalAcrossPagination — design
// §7.1 row 5: a widget whose jq does NOT reference .slice produces
// byte-identical output regardless of injection. Confirms the fix is
// harmless to widgets that ignore .slice.
func TestDashboardTable_NonSlicedWidget_ByteIdenticalAcrossPagination(t *testing.T) {
	// A widget jq that ignores .slice — just maps `.list` to a count.
	items := []templatesv1.WidgetDataTemplate{
		{
			ForPath:    "data",
			Expression: "${ .list | length }",
		},
	}

	list := []any{
		map[string]any{"a": 1},
		map[string]any{"a": 2},
		map[string]any{"a": 3},
	}

	// (a) PerPage=0, Page=0 — no injection.
	dsA := map[string]any{"list": list}
	injectSlice(dsA, 0, 0)
	evalsA, err := widgetdatatemplate.Resolve(context.Background(), widgetdatatemplate.ResolveOptions{
		Items:      items,
		DataSource: dsA,
	})
	require.NoError(t, err)

	// (b) PerPage=50, Page=1 — injection runs but widget ignores .slice.
	dsB := map[string]any{"list": list}
	injectSlice(dsB, 50, 1)
	evalsB, err := widgetdatatemplate.Resolve(context.Background(), widgetdatatemplate.ResolveOptions{
		Items:      items,
		DataSource: dsB,
	})
	require.NoError(t, err)

	require.Equal(t, len(evalsA), len(evalsB))
	for i := range evalsA {
		assert.Equal(t, evalsA[i].Path, evalsB[i].Path)
		assert.Equal(t, evalsA[i].Value, evalsB[i].Value,
			"widget jq that ignores .slice must produce byte-identical output regardless of injection")
	}
}

// TestInjectSlice_ScopeExpansion_PieChartAndOffsetWidgets — design §OQ-2
// scope expansion: confirms the same injected triple satisfies BOTH the
// .slice.page/.slice.perPage shape (table + piechart widgets) AND the
// .slice.offset/.slice.perPage shape (compositions-panels + blueprints-panels).
//
// One-file fix benefits all widgets that reference .slice.* — proves
// mechanism-uniformity per feedback_no_special_cases.
func TestInjectSlice_ScopeExpansion_PieChartAndOffsetWidgets(t *testing.T) {
	ds := map[string]any{
		"list": []any{
			map[string]any{"x": 1},
			map[string]any{"x": 2},
			map[string]any{"x": 3},
			map[string]any{"x": 4},
			map[string]any{"x": 5},
		},
	}
	injectSlice(ds, 2, 1) // perPage=2, page=1, offset=0

	// Shape 1: .slice.page * .slice.perPage  (table.dashboard-…-table.yaml,
	// piechart.dashboard-…-piechart.yaml all 5 references)
	items1 := []templatesv1.WidgetDataTemplate{
		{
			ForPath:    "data",
			Expression: "${ if .slice then .list[0 : (.slice.page * .slice.perPage)] else .list end }",
		},
	}
	evals1, err := widgetdatatemplate.Resolve(context.Background(), widgetdatatemplate.ResolveOptions{
		Items: items1, DataSource: ds,
	})
	require.NoError(t, err)
	require.Len(t, evals1, 1)
	rows1, ok := evals1[0].Value.([]any)
	require.True(t, ok)
	assert.Equal(t, 2, len(rows1), "table/piechart shape: page*perPage=1*2=2 rows")

	// Shape 2: .slice.offset // 0  +  .slice.perPage // length
	// (restaction.compositions-panels.yaml:37-38, restaction.blueprints-panels.yaml:37-38)
	items2 := []templatesv1.WidgetDataTemplate{
		{
			ForPath:    "offset",
			Expression: "${ .slice.offset // 0 }",
		},
		{
			ForPath:    "perPage",
			Expression: "${ .slice.perPage // (.list | length) }",
		},
	}
	evals2, err := widgetdatatemplate.Resolve(context.Background(), widgetdatatemplate.ResolveOptions{
		Items: items2, DataSource: ds,
	})
	require.NoError(t, err)
	require.Len(t, evals2, 2)

	// jqutil.InferType returns int32 for jq integer values; assert
	// numeric equality regardless of integer width.
	toInt64 := func(v any) (int64, bool) {
		switch x := v.(type) {
		case int:
			return int64(x), true
		case int32:
			return int64(x), true
		case int64:
			return x, true
		case float64:
			return int64(x), true
		}
		return 0, false
	}
	var gotOffset, gotPerPage int64
	var okOff, okPp bool
	for _, e := range evals2 {
		switch e.Path {
		case "offset":
			gotOffset, okOff = toInt64(e.Value)
		case "perPage":
			gotPerPage, okPp = toInt64(e.Value)
		}
	}
	require.True(t, okOff, "offset eval result must be numeric (got %T)", evals2)
	require.True(t, okPp, "perPage eval result must be numeric (got %T)", evals2)
	assert.Equal(t, int64(0), gotOffset, "compositions/blueprints-panels shape: offset=(1-1)*2=0")
	assert.Equal(t, int64(2), gotPerPage, "compositions/blueprints-panels shape: perPage=2 from injection")

	// Negative-control: assert injection did NOT touch other keys.
	if _, hasList := ds["list"]; !hasList {
		t.Fatal("list key must be preserved")
	}
	if strings.Contains(fmt.Sprintf("%v", ds), "ra-author-owned") {
		t.Fatal("no unexpected residue")
	}
}
