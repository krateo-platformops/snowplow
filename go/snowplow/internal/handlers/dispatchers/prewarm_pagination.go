package dispatchers

import (
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
)

// prewarmPagination is THE pagination a prewarm writer uses for one widget
// cell, and the only one (#966): the writer resolves the widget with it AND
// keys the cell with it. Every prewarm writer (the root walk, the child walk,
// the walk's content populate, the per-cohort widget seed and the deep-page
// drain) carries one value of this type from the place the widget is reached
// to both the resolve and the Put, so the two cannot disagree.
//
// Why one value and not two. page/perPage change the resolved body whenever
// both are above 0: the RESTAction dict gets `.slice`, the widget jq gets
// `.slice` (injectSlice) and the wrapper gains status.resourcesRefs.slice. A
// live request resolves with exactly the pagination in its URL, and its cell
// key folds exactly that pagination (paginationInfo). A prewarm cell that is
// keyed for one pagination and resolved with another therefore holds a body
// no request with that key would compute. That is what happened before #966:
// the walk and the seed resolved a no-slice widget at page=1, perPage=5 and
// stored it under the unpaginated (-1,-1) key, so any widget whose jq reads
// `.slice` was served five rows to an unpaginated request.
//
// The fields are always NORMALIZED with normalizePagination, the same core
// paginationInfo applies to a request's URL, so a prewarm cell and a request
// cell for the same pagination fold byte-identical values into the key.
type prewarmPagination struct {
	PerPage int
	Page    int
}

// unpaginatedPrewarm is the pagination of a request whose URL carries no
// page/perPage: paginationInfo's (-1,-1). A cell keyed with it is resolved
// unpaginated, exactly as that request resolves it.
var unpaginatedPrewarm = prewarmPagination{PerPage: -1, Page: -1}

// newPrewarmPagination normalizes (perPage, page) the way paginationInfo
// normalizes a request's URL values.
func newPrewarmPagination(perPage, page int) prewarmPagination {
	pp, p := normalizePagination(perPage, page)
	return prewarmPagination{PerPage: pp, Page: p}
}

// prewarmPaginationFor is the ONE derivation of a prewarm writer's pagination
// for a widget reached through callPath — the `/call` path its parent's
// resourcesRefs emitted, which is exactly the URL the browser requests for that
// widget, or "" for a navigation root (the frontend requests a root with no
// page/perPage).
//
//   - The path declares page and perPage → that pagination.
//   - Otherwise → unpaginatedPrewarm, what paginationInfo returns for a URL
//     with no pagination.
//
// No widget kind and no widget content is consulted, so there is nothing to
// special-case. A widget-declared page size (portal#235) plugs in HERE, and
// only here: whatever pagination the browser will send for the widget is what
// this returns, and every writer follows it for both its resolve and its key.
func prewarmPaginationFor(callPath string) prewarmPagination {
	if callPath == "" {
		return unpaginatedPrewarm
	}
	page, perPage, ok := util.ParseCallPathPagination(callPath)
	if !ok {
		return unpaginatedPrewarm
	}
	return newPrewarmPagination(perPage, page)
}

// paginated reports whether the pagination windows the resolve — the same
// predicate the serve-time slice gate uses (apiref.IsPaginatedResolve), so a
// prewarm writer and a request agree on whether a resolve is paginated.
func (pg prewarmPagination) paginated() bool {
	return apiref.IsPaginatedResolve(pg.PerPage, pg.Page)
}

// pageAfter returns the pagination of the k-th page after pg: same perPage,
// page advanced by k. It is the deep-page drain's next cell, resolved and
// keyed with the result. Only meaningful for a paginated pg; for an
// unpaginated one it returns pg unchanged, because there is no next page of an
// unwindowed resolve.
func (pg prewarmPagination) pageAfter(k int) prewarmPagination {
	if !pg.paginated() {
		return pg
	}
	return prewarmPagination{PerPage: pg.PerPage, Page: pg.Page + k}
}
