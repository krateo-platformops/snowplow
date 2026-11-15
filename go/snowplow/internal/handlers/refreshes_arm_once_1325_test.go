// refreshes_arm_once_1325_test.go — #1325 review minor findings, wire level:
// the REAL chain (RefreshAuth → Refreshes → validateSubscription → BehindKeys →
// RefreshStream.CatchUp → the bytes on the wire).
//
//	A1  m1: a key the #710 owed-push emit sinks at subscribe AND that the tab is
//	    behind on (its `held` digest is the stale body's) gets ONE frame on the
//	    arm, not two. That is the correlated case the review found: a D2 stale
//	    serve whose replacement was published while the SPA re-opened its
//	    stream leaves an owed mark, and the tab holds the stale body's digest.
//	    RED on main ad657d95: 2 frames.
//	A2  m2: one arm, two coordinates on one key (two widgets of one tab), the
//	    first holding the CURRENT digest and the second an OLD one: the key is
//	    behind (the second widget shows old bytes) → one frame. Kills the
//	    review's Mb1 (checking only the first held digest).
//	A3  m3: every BehindKeys call in this package reads on the #101 arm ctx.

package handlers

import (
	"encoding/base64"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestRefreshes1325_A1_AnOwedPushAndACatchUpOnOneKeyAreOneFrame(t *testing.T) {
	const name = "ao-a1"
	setup819(t, name)
	cache.ResetStaleOwedForTest()
	t.Cleanup(cache.ResetStaleOwedForTest)
	k := widgetsKey819(t, name)
	stale := `{"v":"stale-derived"}`
	put1324(t, k, name, `{"v":"fresh"}`) // the replacement is stored
	// The stale-derived response the tab holds, whose replacement's frame was
	// published while no stream was armed: an owed mark, and a stale `held`.
	cache.MarkStaleOwed(k, []byte(stale))
	held := cache.ContentDigest([]byte(stale))
	before := cache.RefreshBroadcasterStatsSnapshot()

	base := refreshServer(t)
	frames, cancel := openArmedStreamSub(t, base, sub1324([]string{name}, map[string]string{name: held}), []string{k})
	defer cancel()
	if got := drain1324(frames, 500*time.Millisecond); len(got) != 1 || got[0] != k {
		t.Fatalf("A1 RED: frames %v on arm, want exactly one on %q — the owed-push emit and the catch-up both announced "+
			"the same change; one arm must write at most one frame per key (#1325 review m1)", got, k)
	}
	after := cache.RefreshBroadcasterStatsSnapshot()
	if d := after.ArmDuplicateAbsorbed - before.ArmDuplicateAbsorbed; d != 1 {
		t.Fatalf("A1: arm_duplicate_absorbed Δ=%d, want 1 (the catch-up offer absorbed)", d)
	}
	if d := after.ArmCatchUp - before.ArmCatchUp; d != 0 {
		t.Fatalf("A1: arm_catch_up Δ=%d, want 0 — the frame on the wire is the owed emit's", d)
	}
	// Scope: the arm is closed; a later change on the key is published as usual.
	cache.PublishRefresh(k)
	if f, ok := awaitFrame(t, frames, 3*time.Second); !ok || f.key != k {
		t.Fatalf("A1 RED: a publish after the arm was absorbed (got %q ok=%v) — only arm-time offers may be", f.key, ok)
	}
}

func TestRefreshes1325_A2_TwoWidgetsOnOneKeyCatchUpWhenAnyHoldsOldBytes(t *testing.T) {
	const name = "ao-a2"
	setup819(t, name)
	k := widgetsKey819(t, name)
	put1324(t, k, name, `{"v":1}`)
	old := cache.ContentDigest([]byte(`{"v":1}`)) // the second widget's /call answer
	put1324(t, k, name, `{"v":2}`)
	cur := cache.ContentDigest([]byte(`{"v":2}`)) // the first widget's (it refetched)

	coord := func(held string) map[string]any {
		return map[string]any{
			"class": "widgets", "group": panelGVR.Group, "version": panelGVR.Version,
			"resource": panelGVR.Resource, "namespace": evictTestNS, "name": name, "perPage": 5, "page": 1,
			"held": held,
		}
	}
	raw, _ := json.Marshal([]map[string]any{coord(cur), coord(old)}) // the current one FIRST
	sub := base64.StdEncoding.EncodeToString(raw)

	base := refreshServer(t)
	frames, cancel := openArmedStreamSub(t, base, sub, []string{k})
	defer cancel()
	if got := drain1324(frames, 500*time.Millisecond); len(got) != 1 || got[0] != k {
		t.Fatalf("A2 RED: frames %v, want exactly one on %q — one of the two widgets armed on the key holds old bytes, "+
			"so the key is behind whatever the first held digest says (#1325 review m2)", got, k)
	}
}

// A3 — the arm catch-up check reads on the #101 arm ctx. Since #1430 the check
// is the rotation tracker's (Behind), which reads on the ctx it was built
// with: so every tracker in this package is built on refreshArmCtx(req)
// (directly, or through a variable assigned from it), the check is called on
// a tracker, and no call of the ctx-taking BehindKeys bypasses it.
func TestRefreshes1325_A3_BehindKeysReadsOnTheArmCtx(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	trackers, checks := 0, 0
	for _, p := range files {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		// Variables assigned exactly refreshArmCtx(...).
		isArmCtxCall := func(e ast.Expr) bool {
			c, ok := e.(*ast.CallExpr)
			if !ok {
				return false
			}
			fn, _ := c.Fun.(*ast.Ident)
			return fn != nil && fn.Name == "refreshArmCtx"
		}
		armVars, otherAssigned := map[string]bool{}, map[string]bool{}
		ast.Inspect(f, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok || len(as.Lhs) != len(as.Rhs) {
				return true
			}
			for i, l := range as.Lhs {
				if id, ok := l.(*ast.Ident); ok {
					if isArmCtxCall(as.Rhs[i]) {
						armVars[id.Name] = true
					} else {
						otherAssigned[id.Name] = true
					}
				}
			}
			return true
		})
		onArmCtx := func(e ast.Expr) bool {
			if isArmCtxCall(e) {
				return true
			}
			id, ok := e.(*ast.Ident)
			return ok && armVars[id.Name] && !otherAssigned[id.Name]
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "NewSubscriptionRotationTracker", "BehindKeys":
				if sel.Sel.Name == "NewSubscriptionRotationTracker" {
					trackers++
				} else {
					checks++
				}
				if len(call.Args) == 0 || !onArmCtx(call.Args[0]) {
					t.Fatalf("A3 RED: %s at %s does not read on refreshArmCtx(req) — #101: every /refreshes arm-time "+
						"read is informer-only (#1325 review m3)", sel.Sel.Name, fset.Position(call.Pos()))
				}
			case "Behind":
				checks++
			}
			return true
		})
	}
	if trackers == 0 || checks == 0 {
		t.Fatalf("A3: %d tracker constructions and %d catch-up checks found — the scan is not looking at the handler", trackers, checks)
	}
}
