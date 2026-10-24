// issue598_refresh_memo_test.go — #598: the refresher installs the RA-resolve
// memo, and the /call path still does not.
//
// WHY ARM1 DRIVES THE REAL FUNCTION. The pre-existing C-F4-8 seam test asserts
// "the refresher ctx carried no memo" against a ctx it builds BY HAND with
// cache.WithBackgroundResolve. That assertion is still true after this change
// and still worth keeping — being a background ctx does not by itself grant a
// memo — but it is blind to the install, because production installs it inside
// resolveAndPopulateL1 rather than in WithBackgroundResolve. A test that builds
// its own state cannot see a change to the real builder
// (feedback_falsifier_must_drive_real_boundary_not_install_crossed_state), so
// ARM1 drives resolveAndPopulateL1 and inspects the ctx it actually hands the
// resolver, through the production resolveOnceFn seam.
//
// ARM2 is the half of C-F4-8 that must NOT move: a /call context still carries
// no memo. That is the leak-facing invariant — this change relaxes the "only the
// seed" contract, and ARM2 pins what relaxing it must not cost.

package dispatchers

import (
	"context"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// ARM1 — the real boundary: resolveAndPopulateL1 hands the resolver a ctx
// carrying a memo. RED before this change.
func TestI598_Arm1_RefresherCtxCarriesTheMemo(t *testing.T) {
	cacheOnForMemo(t)

	var sawMemo *cache.SeedResolveMemo
	var called bool
	prev := resolveOnceFn
	resolveOnceFn = func(ctx context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		called = true
		sawMemo = cache.SeedResolveMemoFromContext(ctx)
		return []byte(`{}`), nil
	}
	t.Cleanup(func() { resolveOnceFn = prev })

	_ = resolveAndPopulateL1(
		cache.WithBackgroundResolve(xcontext.BuildContext(t.Context(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: "admin"}))),
		cache.ResolvedKeyInputs{}, nil, nil)

	if !called {
		t.Fatal("#598 ARM1 setup: resolveOnceFn was never reached, so the ctx under test was never produced")
	}
	if sawMemo == nil {
		t.Fatal("#598 ARM1 VIOLATED: the refresher handed the resolver a ctx with NO resolve memo. " +
			"Without it apiref.Resolve re-resolves every widget on a dirtied coordinate independently — " +
			"44 resolves of one RESTAction per identity on 057's hottest coordinate, each paying its own " +
			"parseListEnvelope.")
	}
}

// ARM2 — the invariant this change must NOT cost: a /call ctx still has no memo.
func TestI598_Arm2_CallPathStillHasNoMemo(t *testing.T) {
	cacheOnForMemo(t)

	callCtx := xcontext.BuildContext(t.Context(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}}))
	if m := cache.SeedResolveMemoFromContext(callCtx); m != nil {
		t.Fatal("#598 ARM2 VIOLATED: a foreground /call ctx carried a resolve memo. #598 relaxed " +
			"'only the seed installs one' to admit the refresher; it must NOT admit the request path. " +
			"A memo reachable from /call could serve one caller a body resolved for another (C-F4-8).")
	}
	if m := cache.SeedResolveMemoFromContext(context.Background()); m != nil {
		t.Fatal("#598 ARM2 VIOLATED: a bare context carried a resolve memo")
	}
}

// ARM3 — the memo is SHARED across resolves. A per-resolve memo would be
// correct and useless: nothing to hit.
func TestI598_Arm3_MemoIsSharedAcrossResolves(t *testing.T) {
	cacheOnForMemo(t)
	resetRefreshMemoForTest()

	a := refreshResolveMemo()
	b := refreshResolveMemo()
	if a == nil || b == nil {
		t.Fatal("#598 ARM3 setup: refreshResolveMemo returned nil")
	}
	if a != b {
		t.Fatal("#598 ARM3 VIOLATED: two refresher resolves got DIFFERENT memos. The whole point is " +
			"that the second widget on a dirtied coordinate hits the body the first one stored; a " +
			"per-resolve memo can never hit.")
	}
}

// ARM4 — the bound is DERIVED from the live cell count, and absent when there
// are no cells. A hardcoded cap would pass ARM3 and fail this.
func TestI598_Arm4_BoundIsDerivedFromLiveEntryCount(t *testing.T) {
	cacheOnForMemo(t)
	resetRefreshMemoForTest()

	// With no published cache there are no entries, so no bound may be applied —
	// otherwise a cold process would swap its memo on every resolve.
	if cap := refreshMemoCap(); cap != 0 {
		t.Fatalf("#598 ARM4 VIOLATED: refreshMemoCap() = %d with no live cache; want 0 "+
			"(no bound). A non-zero cap here means the bound is an invented constant rather "+
			"than the live entry count.", cap)
	}
	before := RefreshMemoSwaps()
	for i := 0; i < 50; i++ {
		refreshResolveMemo()
	}
	if got := RefreshMemoSwaps() - before; got != 0 {
		t.Fatalf("#598 ARM4 VIOLATED: %d memo swaps with no bound in force; want 0", got)
	}
}

// ARM5 — identity is folded into the key, so two identities can never share a
// body. This is the leak F4's own divergent-output arm guards; repeated here
// because #598 widens WHO holds a memo, and a reader of this file should see the
// property restated at the new call site rather than have to find it in F4.
func TestI598_Arm5_IdentityNeverCollapses(t *testing.T) {
	memo := cache.NewSeedResolveMemo()
	kAlice := memo.Key("krateo-system", "incident-detail", "alice", []string{"devs"}, "cls", "x", -1, -1)
	kBob := memo.Key("krateo-system", "incident-detail", "bob", []string{"devs"}, "cls", "x", -1, -1)
	kAliceOps := memo.Key("krateo-system", "incident-detail", "alice", []string{"ops"}, "cls", "x", -1, -1)
	if kAlice == kBob {
		t.Fatal("#598 ARM5 VIOLATED: two usernames derived the SAME memo key — one identity's " +
			"RBAC-filtered body would be served to the other")
	}
	if kAlice == kAliceOps {
		t.Fatal("#598 ARM5 VIOLATED: two group sets derived the SAME memo key")
	}
	// ...and the collapse #598 exists for DOES happen: same identity, same RA,
	// same extras, different widgets => one key.
	kSame := memo.Key("krateo-system", "incident-detail", "alice", []string{"devs"}, "cls", "x", -1, -1)
	if kAlice != kSame {
		t.Fatal("#598 ARM5 VIOLATED: identical (RA, identity, class, extras, page) derived different " +
			"keys — the widget dimension would not collapse and the change buys nothing")
	}
}

// resetRefreshMemoForTest drops the process memo so an arm starts clean.
func resetRefreshMemoForTest() { cache.UntrackDepMoves(refreshMemo.Swap(nil)) }

func cacheOnForMemo(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
}
