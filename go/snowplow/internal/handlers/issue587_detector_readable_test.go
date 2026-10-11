// issue587_detector_readable_test.go — the arming quartet must distinguish a
// REFUSAL from a HEALTHY IDLE STREAM.
//
// THE DEFECT THESE ARMS CLOSE, measured on 057 before the fix: six subscribes,
// every one answered 400 by snowplow, left the quartet reading
// `arrived 6, rejected 0, streams_open 0`. Reading that honestly, nothing was
// refused. Everything was.
//
// There were two holes, and the second is worse than the first:
//
//  1. Only ONE of the two 400 paths bumped SubscribeRejected. The
//     validateSubscription error counted; the blessed C64-1 honest-400
//     (`len(armed) == 0` — warm, but nothing the requester asked for is cached
//     or permitted, which is the EXPECTED outcome on a userAccessFilter-backed
//     page, #561) did not.
//
//  2. serveIdleSSE does not bump RefreshStreamsOpen — the only bump site is
//     after the armed check. So the warmup divert and the cache-off divert,
//     both of which are HEALTHY (the browser is connected and will receive
//     publishes), produced byte-identical counters to the refusal in (1):
//     arrived +1, rejected +0, streams_open +0.
//
// Three opposite outcomes, one reading. Telling them apart needed a pod-log
// read, which is the exact cost this detector exists to avoid
// (feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector).
//
// WHY ONE ARM IS STRUCTURAL. The cache-off divert is reachable in a unit test
// because it is the handler's earliest return and needs no identity and no warm
// pod. The warmup divert and the honest-400 sit behind authentication and
// warmup state. Rather than stand up that machinery — and rather than quietly
// leave them untested, which is how (1) survived in the first place — the
// second arm reads the handler's own source and asserts that EVERY non-stream
// exit after the arrival bump is accounted for by exactly one counter. That is
// the same shape as the Put-site enumeration guards: it fails when someone adds
// a fourth way out and forgets to say so.
package handlers

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// runHandlerToCompletion drives the streaming handler with an already-cancelled
// context so it terminates, behind a deadline so a future hang fails THIS test
// rather than parking the package until the 600s global timeout (which is what
// the first version of the #560 test did).
func runHandlerToCompletion(t *testing.T, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		Refreshes()(rec, req)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Refreshes() did not return on a cancelled context within 5s — a streaming " +
			"handler under test must terminate, or it parks the package")
	}
	return rec
}

// TestIssue587_CacheOffIdleStreamIsNotMistakenForARefusal pins the distinction
// end to end on the one divert a unit test can reach.
//
// RED before the fix: IdleServedCacheOff does not exist, and the cache-off
// divert is indistinguishable from the honest-400 — both leave arrived=1,
// rejected=0, streams_open=0.
func TestIssue587_CacheOffIdleStreamIsNotMistakenForARefusal(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	if cache.RefreshSSEEnabled() {
		t.Fatal("precondition: RefreshSSEEnabled must be false with CACHE_ENABLED=false; " +
			"this test is not exercising the cache-off divert")
	}
	cache.ResetLiveRefreshArmingStatsForTest()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/refreshes?sub=", nil).WithContext(ctx)
	runHandlerToCompletion(t, req)

	got := cache.LiveRefreshArmingStatsSnapshot()

	// It arrived.
	if got.SubscribeArrived != 1 {
		t.Fatalf("SubscribeArrived=%d want 1 — the arrival counter has moved below an early return",
			got.SubscribeArrived)
	}
	// It is NOT a refusal. This is the assertion that makes the quartet
	// readable: without the idle counter below, asserting only this would be
	// satisfied by a detector that says nothing at all.
	if got.SubscribeRejected != 0 {
		t.Fatalf("cache-off is a healthy fallback, not a refusal: SubscribeRejected=%d want 0",
			got.SubscribeRejected)
	}
	// It is POSITIVELY identified as an idle stream — the #587 fix.
	if got.IdleServedCacheOff != 1 {
		t.Fatalf("IdleServedCacheOff=%d want 1.\n"+
			"Without this the cache-off divert reads IDENTICALLY to the len(armed)==0 "+
			"refusal — same arrived, same rejected, same streams_open — and a healthy "+
			"pod cannot be told from one refusing every subscriber.", got.IdleServedCacheOff)
	}
	// And it must not be attributed to the other cause.
	if got.IdleServedWarmup != 0 {
		t.Fatalf("IdleServedWarmup=%d want 0 — the cache-off divert is being counted as a warmup divert",
			got.IdleServedWarmup)
	}
	// An idle stream is not an armed stream.
	if got.StreamsOpen != 0 {
		t.Fatalf("StreamsOpen=%d want 0 — an idle keepalive stream must not move the armed gauge",
			got.StreamsOpen)
	}
}

// TestIssue587_EveryNonStreamExitIsAccountedFor is the enumeration guard.
//
// It parses refreshes.go and walks the Refreshes() handler body, collecting
// every `return` that leaves without opening an armed stream. Each must be
// preceded, within its own block, by exactly one accounting call. A new exit
// added without one fails this test by name.
//
// This is what stops hole (1) recurring: the honest-400 was added as a
// deliberate, well-documented branch, and simply nobody noticed it needed to
// say so in the counters.
func TestIssue587_EveryNonStreamExitIsAccountedFor(t *testing.T) {
	const src = "refreshes.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, src, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing %s: %v", src, err)
	}

	var body *ast.BlockStmt
	ast.Inspect(f, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "Refreshes" || fn.Body == nil {
			return true
		}
		// The handler itself is the closure returned by Refreshes().
		ast.Inspect(fn.Body, func(m ast.Node) bool {
			lit, ok := m.(*ast.FuncLit)
			if !ok {
				return true
			}
			body = lit.Body
			return false
		})
		return false
	})
	if body == nil {
		t.Fatal("could not locate the handler closure inside Refreshes() — the guard is " +
			"reading nothing and would pass vacuously")
	}

	// The calls that ACCOUNT for a non-stream exit.
	accounting := map[string]bool{
		"BumpRefreshSubscribeRejected":  true,
		"BumpRefreshIdleServedWarmup":   true,
		"BumpRefreshIdleServedCacheOff": true,
		"BumpRefreshConnectWriteFailed": true,
		"Unauthorized":                  true, // response.Unauthorized — the 401 exit
		"RefreshStreamOpened":           true, // the armed-stream path
	}

	// Walk every block; for each `return` inside it, look back for an
	// accounting call earlier in the SAME block.
	// `inherited` carries the enclosing block's accounting down into nested
	// blocks. A return inside `if !writeFrame(...) { return }` AFTER
	// RefreshStreamOpened() is already accounted for by the enclosing scope —
	// resetting on recursion flagged it as a hole, which was this guard's own
	// first bug.
	var unaccounted []string
	var walk func(stmts []ast.Stmt, inherited string)
	walk = func(stmts []ast.Stmt, inherited string) {
		seen := inherited
		for _, st := range stmts {
			// Only a call at THIS statement level counts. Descending into
			// nested blocks here is wrong and was this guard's second bug: the
			// warmup divert's bump, nested inside an inner `if`, satisfied the
			// OUTER block's return, so deleting the honest-400's own bump — the
			// precise defect #587 is about — went undetected.
			if es, ok := st.(*ast.ExprStmt); ok {
				if call, ok := es.X.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && accounting[sel.Sel.Name] {
						seen = sel.Sel.Name
					}
				}
			}
			if _, isRet := st.(*ast.ReturnStmt); isRet && seen == "" {
				unaccounted = append(unaccounted,
					fset.Position(st.Pos()).String())
			}
			// Recurse into nested blocks with their own accounting scope.
			switch s := st.(type) {
			case *ast.IfStmt:
				walk(s.Body.List, seen)
				if eb, ok := s.Else.(*ast.BlockStmt); ok {
					walk(eb.List, seen)
				}
			case *ast.BlockStmt:
				walk(s.List, seen)
			}
		}
	}
	walk(body.List, "")

	if len(unaccounted) > 0 {
		t.Errorf("%d exit(s) from the /refreshes handler leave without being counted:\n  %s\n\n"+
			"Every way out after the arrival bump must say which it was, or the quartet "+
			"cannot tell a refusal from a healthy idle stream. Add the matching\n"+
			"cache.BumpRefresh* call, or add it to the accounting set here if it is a new KIND of exit.",
			len(unaccounted), strings.Join(unaccounted, "\n  "))
	}
}
