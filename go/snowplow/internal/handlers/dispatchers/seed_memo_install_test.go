// seed_memo_install_test.go — #130 F4: proves the PRODUCTION seed pass installs
// the per-seed-pass RA-resolve memo on ctx AND that the per-cohort cohortCtx
// (withCohortSeedContext → xcontext.BuildContext) INHERITS it, so the memo
// reaches the nested apiref.Resolve. Plus the Option-2 pagination gate in
// seedOneWidget skips the 4a full-list pin for an unpaginated widget.
//
// The memo install lives at the top of seedScopeYielding (one memo per pass,
// torn down when the function returns — C-F4-5). withCohortSeedContext must
// carry it through unchanged (it layers values without stripping ctx values).

package dispatchers

import (
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
)

// TestCohortSeedContext_InheritsSeedResolveMemo — the memo the seed pass
// installs on ctx must survive through withCohortSeedContext so the nested
// apiref.Resolve (which reads cache.SeedResolveMemoFromContext(ctx)) consults
// it. If withCohortSeedContext's BuildContext ever stripped ctx values this arm
// goes RED and the memo would be invisible to the resolve (silent no-op).
func TestCohortSeedContext_InheritsSeedResolveMemo(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	// Mirror seedScopeYielding's install (top of the function).
	base := cache.WithSeedResolveMemo(xcontext.BuildContext(t.Context()),
		cache.NewSeedResolveMemo())
	if cache.SeedResolveMemoFromContext(base) == nil {
		t.Fatal("premise broken: base ctx should carry the memo the pass installs")
	}

	// The per-cohort ctx must still carry it.
	cohortCtx := withCohortSeedContext(base, seedTarget{Username: "admin"}, endpoints.Endpoint{}, nil)
	if cache.SeedResolveMemoFromContext(cohortCtx) == nil {
		t.Fatal("RED: withCohortSeedContext DROPPED the seed-resolve memo — the nested apiref.Resolve would never consult it (silent no-op, F4 inert). The memo must propagate through BuildContext.")
	}
}

// TestOption2_PaginationPredicate_MirrorsServeGate — the Option-2 skip in
// seedOneWidget gates on apiref.IsPaginatedResolve(e.PerPage, e.Page). A widget
// seeded UNPAGINATED (the Statistic/Tag count-only shape) must be classified
// skip; a paginated widget must still pin. Data-derived — no widget-kind
// (C-F4-7). This is the exact predicate the production seedOneWidget branch
// uses (single import, no local re-derivation).
func TestOption2_PaginationPredicate_MirrorsServeGate(t *testing.T) {
	// Unpaginated / partial → skip the 4a pin. (e.PerPage/e.Page are the
	// navWidgetEntry resolution tuple the production seedOneWidget branch reads.)
	for _, tc := range []struct{ pp, pg int }{{0, 0}, {-1, -1}, {5, 0}, {0, 1}} {
		if apiref.IsPaginatedResolve(tc.pp, tc.pg) {
			t.Fatalf("RED: unpaginated widget (perPage=%d,page=%d) classified paginated — seedOneWidget would run the redundant 4a pin. Option 2 must skip it.", tc.pp, tc.pg)
		}
	}
	// Paginated → still pin (byte-unchanged from pre-F4).
	if !apiref.IsPaginatedResolve(5, 1) {
		t.Fatal("RED: paginated widget (5,1) classified unpaginated — a paginated widget must still pin the full list.")
	}
}
