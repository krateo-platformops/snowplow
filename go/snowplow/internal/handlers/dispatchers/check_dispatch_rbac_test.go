// check_dispatch_rbac_test.go — M5 (coverage-audit) SECURITY: the cache=on
// dispatch RBAC gate (checkDispatchRBAC, helpers.go) is FAIL-CLOSED on every
// failure mode and carries the correct EvaluateRBAC contract.
//
// checkDispatchRBAC is the ONLY thing standing between a cache=on /call and the
// dispatched CR (in cache=on mode fetchObject does NOT enforce RBAC for that
// GET — the gate must). So its failure modes are security-critical:
//
//	no UserInfo on ctx      → false (deny)
//	EvaluateRBAC returns err → false (deny)
//	allowed=false            → false (deny)
//	allowed=true             → true  (permit)
//
// AND it MUST call EvaluateRBAC with Verb=="get" + SkipBindingUID==true (the
// per-item posture — it discards matchedBindingUID, so skipping the stable-sort
// is both correct and the 50K-scale CPU win, Ship L1), about the dispatched
// object ITSELF: its namespace and its NAME (#1046 — without the name a
// resourceNames-scoped grant can never match).
//
// RED arm (fail-OPEN): if checkDispatchRBAC returned true on a missing UserInfo
// or an evaluator error, an unauthenticated / evaluator-degraded request would
// be dispatched. The no-UserInfo and eval-error rows below are GREEN only
// because the gate fails CLOSED; a fail-open impl reds them. Proven by the
// shadow-wrong-impl in TestM5_FailClosed_RED_ShadowFailOpen.

package dispatchers

import (
	"context"
	"fmt"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// setEvaluateRBACForTest swaps the checkDispatchRBAC evaluation seam and returns
// a restore. Production never reassigns evaluateRBACFn.
func setEvaluateRBACForTest(fn func(ctx context.Context, opts rbac.EvaluateOptions) (bool, string, error)) func() {
	prev := evaluateRBACFn
	evaluateRBACFn = fn
	return func() { evaluateRBACFn = prev }
}

var m5GVR = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}

// m5Obj is the dispatched CR the gate is asked about.
var m5Obj = &metav1.ObjectMeta{Namespace: "krateo-system", Name: "m5-ra"}

func m5CtxWithUser() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}}))
}

// TestM5_CheckDispatchRBAC_Table drives every verdict/error branch through the
// REAL checkDispatchRBAC with a stubbed evaluator, and asserts the fail-closed
// contract + the EvaluateOptions the gate must send.
func TestM5_CheckDispatchRBAC_Table(t *testing.T) {
	cases := []struct {
		name       string
		withUser   bool
		evalRet    bool
		evalErr    error
		wantResult bool
		evalCalled bool // whether the evaluator should even be reached
	}{
		{name: "no UserInfo → deny (evaluator never reached)", withUser: false, wantResult: false, evalCalled: false},
		{name: "evaluator error → deny", withUser: true, evalErr: fmt.Errorf("snapshot not yet built"), wantResult: false, evalCalled: true},
		{name: "allowed=false → deny", withUser: true, evalRet: false, wantResult: false, evalCalled: true},
		{name: "allowed=true → permit", withUser: true, evalRet: true, wantResult: true, evalCalled: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				called  bool
				gotOpts rbac.EvaluateOptions
			)
			restore := setEvaluateRBACForTest(func(ctx context.Context, opts rbac.EvaluateOptions) (bool, string, error) {
				called = true
				gotOpts = opts
				return tc.evalRet, "", tc.evalErr
			})
			defer restore()

			ctx := context.Background()
			if tc.withUser {
				ctx = m5CtxWithUser()
			}

			got := checkDispatchRBAC(ctx, m5GVR, m5Obj)
			if got != tc.wantResult {
				t.Fatalf("checkDispatchRBAC = %v, want %v", got, tc.wantResult)
			}
			if called != tc.evalCalled {
				t.Fatalf("evaluator called = %v, want %v (a missing UserInfo must SHORT-CIRCUIT before evaluating)", called, tc.evalCalled)
			}
			if tc.evalCalled {
				// The load-bearing contract: this is a per-item GET check that
				// discards the BindingUID, so it MUST skip the stable-sort.
				if gotOpts.Verb != "get" {
					t.Fatalf("EvaluateOptions.Verb = %q, want \"get\" (the dispatch gate is a GET-permit check)", gotOpts.Verb)
				}
				if !gotOpts.SkipBindingUID {
					t.Fatalf("EvaluateOptions.SkipBindingUID = false, want true (per-item caller discards matchedBindingUID — Ship L1 CPU posture)")
				}
				if gotOpts.Group != m5GVR.Group || gotOpts.Resource != m5GVR.Resource {
					t.Fatalf("EvaluateOptions group/resource = %q/%q, want %q/%q", gotOpts.Group, gotOpts.Resource, m5GVR.Group, m5GVR.Resource)
				}
				if gotOpts.Namespace != "krateo-system" {
					t.Fatalf("EvaluateOptions.Namespace = %q, want krateo-system", gotOpts.Namespace)
				}
				// #1046 — the gate is about the dispatched object: its name
				// rides along, or a resourceNames-scoped grant never matches.
				if gotOpts.Name != m5Obj.Name {
					t.Fatalf("EvaluateOptions.Name = %q, want %q (the dispatched CR's own name, #1046)", gotOpts.Name, m5Obj.Name)
				}
				if gotOpts.Username != "alice" {
					t.Fatalf("EvaluateOptions.Username = %q, want alice (the ctx identity)", gotOpts.Username)
				}
			}
		})
	}
}

// TestM5_FailClosed_RED_ShadowFailOpen proves the fail-closed arms are
// DISCRIMINATING: a shadow gate that fails OPEN (returns true when UserInfo is
// missing or the evaluator errored — the classic RBAC regression) gives the
// WRONG (permit) answer on exactly the inputs the real gate denies. The real
// checkDispatchRBAC returns the SAFE (deny) answer for the same inputs.
func TestM5_FailClosed_RED_ShadowFailOpen(t *testing.T) {
	// A wrong impl that fails OPEN. This is what the RED mutation of the prod
	// gate would look like.
	shadowFailOpen := func(ctx context.Context, gvr schema.GroupVersionResource, namespace string) bool {
		ui, err := xcontext.UserInfo(ctx)
		if err != nil {
			return true // RED: fail OPEN on missing identity
		}
		allowed, _, evalErr := evaluateRBACFn(ctx, rbac.EvaluateOptions{
			Username: ui.Username, Groups: ui.Groups, Verb: "get",
			Group: gvr.Group, Resource: gvr.Resource, Namespace: namespace, SkipBindingUID: true,
		})
		if evalErr != nil {
			return true // RED: fail OPEN on evaluator error
		}
		return allowed
	}

	// Arm 1: missing UserInfo.
	restore := setEvaluateRBACForTest(func(ctx context.Context, opts rbac.EvaluateOptions) (bool, string, error) {
		return false, "", nil
	})
	defer restore()

	noUserCtx := context.Background()
	if real := checkDispatchRBAC(noUserCtx, m5GVR, m5Obj); real != false {
		t.Fatalf("real gate must DENY a no-UserInfo request; got %v", real)
	}
	if shadow := shadowFailOpen(noUserCtx, m5GVR, "krateo-system"); shadow != true {
		t.Fatalf("shadow fail-open must (wrongly) PERMIT the no-UserInfo request — else the RED arm is not discriminating; got %v", shadow)
	}

	// Arm 2: evaluator error.
	restore2 := setEvaluateRBACForTest(func(ctx context.Context, opts rbac.EvaluateOptions) (bool, string, error) {
		return false, "", fmt.Errorf("degraded")
	})
	defer restore2()

	userCtx := m5CtxWithUser()
	if real := checkDispatchRBAC(userCtx, m5GVR, m5Obj); real != false {
		t.Fatalf("real gate must DENY on an evaluator error; got %v", real)
	}
	if shadow := shadowFailOpen(userCtx, m5GVR, "krateo-system"); shadow != true {
		t.Fatalf("shadow fail-open must (wrongly) PERMIT on an evaluator error — the discriminant that proves fail-closed matters; got %v", shadow)
	}
}
