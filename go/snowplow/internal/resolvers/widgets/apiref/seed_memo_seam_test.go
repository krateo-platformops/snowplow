// seed_memo_seam_test.go — #130 F4 seam-level falsifiers at the apiref.Resolve
// chokepoint: the memo is consulted ONLY when a SeedResolveMemo is installed on
// ctx (the seed path), and NEVER on the /call / discovery-walk / refresher path
// (C-F4-8); the Option-2 pagination predicate (IsPaginatedResolve) is the exact
// serve-gate shape (C-F4-7 data-derived, no widget-kind); and identityForMemo
// reads the RBAC-determining UserInfo off ctx (C-F4-4 key-input provenance).
//
// The full apiref.Resolve integration (objects.Get against a live apiserver) is
// covered by the on-cluster acceptance run; these unit arms falsify the memo
// WIRING deterministically without an apiserver.

package apiref

import (
	"context"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	pmaps "github.com/krateo-platformops/plumbing/maps"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// TestSeedMemo_NotConsultedOffSeedPath — C-F4-8. The memo lookup in
// apiref.Resolve is `memo := cache.SeedResolveMemoFromContext(ctx)`; it must be
// nil on the REQUEST path so a /call is provably untouched.
//
// #598 NARROWED WHAT THIS PROVES, deliberately. The refresher now installs a
// memo — but it does so inside resolveAndPopulateL1, NOT in
// WithBackgroundResolve. So the refresher arm below still holds and is still
// worth keeping: being a background ctx does not by itself grant a memo, and a
// future caller that stamps WithBackgroundResolve without going through
// resolveAndPopulateL1 must not silently acquire one.
//
// What this file can no longer claim is that the refresher never has a memo.
// That property moved to dispatchers/issue598_refresh_memo_test.go ARM1, which
// drives resolveAndPopulateL1 itself — because a test that builds its own ctx
// cannot see a change to the real builder.
func TestSeedMemo_NotConsultedOffSeedPath(t *testing.T) {
	cacheOn(t)

	// Foreground /call ctx (ScopeCallWidgets) — the customer path.
	if m := cache.SeedResolveMemoFromContext(foregroundCallCtx(t)); m != nil {
		t.Fatal("RED: a foreground /call ctx carried a seed-resolve memo — the memo must be consulted ONLY under the seed context. C-F4-8.")
	}
	// Discovery-walk ctx (ScopeBootPrewarmWalk) — no memo.
	if m := cache.SeedResolveMemoFromContext(bootWalkCtx(t)); m != nil {
		t.Fatal("RED: the discovery-walk ctx carried a seed-resolve memo. C-F4-8.")
	}
	// A BARE background ctx — no memo. #598's memo is installed by
	// resolveAndPopulateL1, not by WithBackgroundResolve, so this must stay nil:
	// the stamp alone must never confer a memo.
	refCtx := cache.WithBackgroundResolve(xcontext.BuildContext(t.Context(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "admin"})))
	if m := cache.SeedResolveMemoFromContext(refCtx); m != nil {
		t.Fatal("RED: a bare WithBackgroundResolve ctx carried a resolve memo — the background " +
			"stamp must not confer one; only resolveAndPopulateL1 installs it (#598). C-F4-8.")
	}
	// Bare context — no memo.
	if m := cache.SeedResolveMemoFromContext(context.Background()); m != nil {
		t.Fatal("RED: a bare context carried a seed-resolve memo. C-F4-8.")
	}
}

// TestSeedMemo_ConsultedUnderSeedCtx — the positive: a ctx with a memo installed
// (as withCohortSeedContext does in production) IS consulted, and the identity
// the memo keys on is the one on ctx.
func TestSeedMemo_ConsultedUnderSeedCtx(t *testing.T) {
	cacheOn(t)
	memo := cache.NewSeedResolveMemo(pmaps.DeepCopyJSON)
	seedCtx := cache.WithSeedResolveMemo(
		xcontext.BuildContext(t.Context(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: "cyberjoker", Groups: []string{"dev-team"}})),
		memo)

	if cache.SeedResolveMemoFromContext(seedCtx) == nil {
		t.Fatal("RED: seed ctx did not carry the memo — the seed path must consult it.")
	}
	// identityForMemo must read the ctx UserInfo (the RBAC-determining tuple the
	// memo key folds — same UserInfo the RA resolver filters on).
	u, g := identityForMemo(seedCtx)
	if u != "cyberjoker" || len(g) != 1 || g[0] != "dev-team" {
		t.Fatalf("RED: identityForMemo did not read the ctx UserInfo; got (%q, %v). The memo key must fold the RBAC identity. C-F4-4.", u, g)
	}
	// No UserInfo on ctx ⇒ ("", nil) — a distinct, non-colliding identity.
	if u2, g2 := identityForMemo(context.Background()); u2 != "" || g2 != nil {
		t.Fatalf("RED: identityForMemo on an identity-less ctx returned (%q, %v), want (\"\", nil).", u2, g2)
	}
}

// TestIsPaginatedResolve_Option2Predicate — C-F4-7. Option 2 skips the 4a pin
// for an UNPAGINATED widget via IsPaginatedResolve — a DATA-DERIVED predicate
// (reads the pagination tuple only, no widget-kind). It must be true iff both
// perPage>0 AND page>0, matching shouldServeRAFullList's serve gate exactly (one
// derivation site, no drift).
func TestIsPaginatedResolve_Option2Predicate(t *testing.T) {
	cacheOn(t)
	// Paginated ⇒ 4a pin RUNS (byte-unchanged from pre-F4).
	if !IsPaginatedResolve(5, 1) {
		t.Fatal("RED: (perPage=5,page=1) not paginated — a paginated widget must still pin the full list.")
	}
	// Unpaginated-consuming shapes ⇒ 4a pin SKIPPED (the F4 Option-2 cut).
	for _, tc := range []struct{ pp, pg int }{{0, 0}, {-1, -1}, {0, 1}, {5, 0}, {-1, 1}, {5, -1}} {
		if IsPaginatedResolve(tc.pp, tc.pg) {
			t.Fatalf("RED: (perPage=%d,page=%d) reported paginated — an unpaginated/partial tuple must skip the 4a pin (Option 2). C-F4-7.", tc.pp, tc.pg)
		}
	}
	// The predicate must agree with the serve gate for the same tuple + a
	// non-walk cache-on ctx (single derivation site — shouldServeRAFullList
	// wraps IsPaginatedResolve).
	fg := foregroundCallCtx(t)
	for _, tc := range []struct{ pp, pg int }{{5, 1}, {0, 0}, {5, 0}, {0, 1}} {
		serve := shouldServeRAFullList(fg, tc.pp, tc.pg)
		if serve != IsPaginatedResolve(tc.pp, tc.pg) {
			t.Fatalf("RED: IsPaginatedResolve(%d,%d)=%v disagrees with the serve gate=%v — the Option-2 skip predicate drifted from the serve gate.",
				tc.pp, tc.pg, IsPaginatedResolve(tc.pp, tc.pg), serve)
		}
	}
}
