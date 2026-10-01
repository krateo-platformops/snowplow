// issue385_foreign_snapshot_test.go — #385 deterministic RED-first arm.
//
// The cross-test flake (TestC4_RealCycleStop_NoEdgelessResidentCell, and the
// api RBAC-narrowing GET siblings) is: in the shared package test binary the
// process-global RBAC snapshot (cache.rbacSnap) can still hold a NEIGHBOUR
// test's snapshot — one lacking this test's seeded bindings — when the resolve
// runs, because the harness waited only for informer HasSynced, not for this
// watcher's own async initial RBAC-snapshot publish. A spurious RBAC deny then
// makes objects.Get fall through to the per-user apiserver path, which has no
// per-user *Endpoint on an internal-driver resolve → "user *Endpoint not found
// in context".
//
// This arm reproduces that DETERMINISTICALLY (no reliance on async timing) by
// publishing a FOREIGN snapshot (granting nobody) right before the resolve, and
// proves the fix's mechanism — a synchronous own-publish of THIS watcher's
// snapshot (cache.RebuildRBACSnapshotForTest, which the harness now calls) —
// clears it. The harness post-condition (newNestedCallWatcherWithInner) already
// asserts the grant is live after setup; here we additionally drive the full
// resolve symptom and its heal.
package dispatchers

import (
	"context"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/client-go/rest"
)

func issue385C4Granted(ns string) bool {
	allowed, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "c4-user", Verb: "get",
		Group:    nestedCallInnerGVR.Group,
		Resource: nestedCallInnerGVR.Resource,
		Namespace: ns,
	})
	return err == nil && allowed
}

func issue385C4Ctx() context.Context {
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "c4-user"}),
	)
	ctx = cache.WithL1KeyContext(ctx, "issue385-driving-l1")
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
	ctx = cache.WithInternalRESTConfig(ctx, &rest.Config{})
	return ctx
}

// TestIssue385_ForeignRBACSnapshot_DeniesThenOwnPublishHeals is the #385
// deterministic arm.
func TestIssue385_ForeignRBACSnapshot_DeniesThenOwnPublishHeals(t *testing.T) {
	const ns, name = "demo-system", "fsa-c4-self-composition-resources"
	rw := newNestedCallWatcherWithInner(t, ns, name,
		nestedSelfReferentialRA(ns, name),
		nestedCallRoleBinding(ns, "c4-user"))
	restactionsapi.RegisterNestedCallResolver(ResolveNestedCall)

	// Fix postcondition: after the harness (own-publish) the grant is live.
	if !issue385C4Granted(ns) {
		t.Fatalf("#385: after harness setup the global snapshot must grant c4-user — the own-publish did not land")
	}

	ref := templates.ObjectReference{
		Reference:  templates.Reference{Name: name, Namespace: ns},
		Resource:   nestedCallInnerGVR.Resource,
		APIVersion: nestedCallInnerGVR.Group + "/" + nestedCallInnerGVR.Version,
	}

	// --- RED arm: a foreign snapshot (grants nobody) clobbers the global. -----
	foreign := &cache.RBACSnapshot{PublishSeq: 1 << 40}
	cache.RebuildSubjectIndexesForTest(foreign) // empty indexes → grants nobody
	cache.PublishRBACSnapshotForTest(foreign)
	if issue385C4Granted(ns) {
		t.Fatalf("#385 arm precondition: the foreign snapshot must DENY c4-user (grants nobody)")
	}
	_, err := ResolveNestedCall(issue385C4Ctx(), ref, 0, 0, nil)
	if err == nil || !strings.Contains(err.Error(), "Endpoint not found in context") {
		t.Fatalf("#385 arm: with a foreign RBAC snapshot the nested resolve must surface "+
			"the per-user fall-through endpoint error; got err=%v", err)
	}

	// --- HEAL via the fix's mechanism: synchronously republish THIS watcher. ---
	cache.RebuildRBACSnapshotForTest(rw)
	if !issue385C4Granted(ns) {
		t.Fatalf("#385: republishing THIS watcher's own snapshot must restore the c4-user grant")
	}
	raw, err := ResolveNestedCall(issue385C4Ctx(), ref, 0, 0, nil)
	if err != nil {
		t.Fatalf("#385: after the own-publish heal the nested resolve must complete cleanly; got %v", err)
	}
	if !strings.Contains(string(raw), "c4resolved") {
		t.Fatalf("#385: healed resolve must yield the RA marker; got %s", raw)
	}
}
