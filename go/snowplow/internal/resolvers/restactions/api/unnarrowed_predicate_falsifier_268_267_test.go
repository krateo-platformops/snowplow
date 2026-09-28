//go:build falsifier_268_267

// unnarrowed_predicate_falsifier_268_267_test.go — Arm E (predicate level) for the
// UserInfo-err residual (design §6 / question 4). internalDispatchServesUnnarrowed
// clause (c) serves branch C UN-narrowed when the ctx has NO UserInfo. The design
// verdict is that clause (c) is UNREACHABLE for a real authenticated end-user
// (the auth middleware sets WithUserInfo on every validated request), so this is
// NOT a live leak — but the predicate itself must keep narrowing a real end-user.
//
// This arm GUARDS THE PREDICATE (PM condition 3b: the predicate assertion, not the
// middleware invariant — the middleware arm lives in internal/handlers/middleware):
//
//   - a REAL end-user identity (non-SA Username on ctx)  → serveUnnarrowed==false
//     (NARROWED). RED against a mutant that re-adds a Username=="" exemption or
//     drops the real-user narrowing.
//   - a genuinely identity-free ctx (no UserInfo)         → serveUnnarrowed==true
//     (clause c) — the legitimate identity-free populate.
//   - a canonical ServiceAccount username                 → serveUnnarrowed==true
//     (clause d).
//   - a GROUP-ONLY end-user (empty Username, groups set)  → serveUnnarrowed==false
//     (RC2 — a group-only user is a REAL end-user, NOT exempt).
//
// These assert the CURRENT correct behaviour (a GUARD/baseline arm, GREEN now and
// MUST STAY GREEN post-fix — the design leaves the predicate unchanged).
package api

import (
	"context"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

func TestUnnarrowedPredicate_ArmE_UserInfoResidual(t *testing.T) {
	// (1) real end-user → NARROWED (serveUnnarrowed==false). The residual clause
	// (c) does NOT fire for a real user, so a per-user SA-credentialed serve is
	// re-gated, not served un-narrowed.
	realUser := ctxWithUser("alice", "tenant-a")
	if got := internalDispatchServesUnnarrowed(realUser); got {
		t.Fatalf("Arm E — RED: a REAL end-user (alice) resolved serveUnnarrowed=true — clause (c)/(d) wrongly fired for a real subject; a per-user SA-credentialed read would be served UN-narrowed (leak).")
	}
	t.Logf("Arm E: real end-user alice → serveUnnarrowed=false (narrowed). GREEN.")

	// (2) identity-free ctx (no UserInfo) → clause (c) → un-narrowed. This is the
	// legitimate identity-free populate (e.g. cluster_list async), gated on read.
	idFree := xcontext.BuildContext(context.Background())
	if got := internalDispatchServesUnnarrowed(idFree); !got {
		t.Fatalf("Arm E: an identity-free ctx (no UserInfo) must serveUnnarrowed=true (clause c); got false.")
	}
	t.Logf("Arm E: identity-free ctx → serveUnnarrowed=true (clause c). GREEN.")

	// (3) canonical ServiceAccount identity → clause (d) → un-narrowed.
	saCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "system:serviceaccount:krateo-system:snowplow"}))
	if got := internalDispatchServesUnnarrowed(saCtx); !got {
		t.Fatalf("Arm E: a canonical SA username must serveUnnarrowed=true (clause d); got false.")
	}
	t.Logf("Arm E: canonical SA identity → serveUnnarrowed=true (clause d). GREEN.")

	// (4) group-only end-user (empty Username, non-empty Groups) → NARROWED (RC2).
	// Exempting on Username=="" would re-open the leak for group-only identities.
	groupOnly := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "", Groups: []string{"devs"}}))
	if got := internalDispatchServesUnnarrowed(groupOnly); got {
		t.Fatalf("Arm E — RED (RC2): a group-only end-user (empty Username, groups=[devs]) resolved serveUnnarrowed=true — an empty Username must NOT be exempt; a per-user SA-credentialed read would be served UN-narrowed.")
	}
	t.Logf("Arm E: group-only end-user → serveUnnarrowed=false (RC2 narrowed). GREEN.")
}
