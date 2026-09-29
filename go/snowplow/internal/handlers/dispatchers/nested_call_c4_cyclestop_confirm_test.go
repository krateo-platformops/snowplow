// nested_call_c4_cyclestop_confirm_test.go — #280 C4 confirm-arm.
//
// WHAT C4 IS. The nested-resolve CYCLE-STOP (nested_call.go, the
// NestedResolveAncestorPresent branch): when a resolve target's node is
// already on the current root→here descent path, ResolveNestedCall returns the
// RAW CR bytes INLINE — no store.Put, no Deps().Record — instead of recursing.
// The edge-3 serve-seam staleness class (#277/#281) asked: can this path leave
// an EDGE-LESS resident L1 cell (a cached cell with no dep edge → never
// invalidated → stale)? The in-code answer was ruled benign by CODE INSPECTION
// ONLY (the nested_call.go:169-173 comment marks it INFERRED, not verified):
// the cycle-stop returns bytes inline and Puts nothing, so structurally it
// cannot create such a cell.
//
// WHY THIS TEST. INFERRED is not TRACED. This arm CONFIRMS the assumption by
// driving the REAL cycle boundary — a genuine RESTAction→RA→same-RA recursion
// that NATURALLY re-enters the cycle-stop — NOT the installed-ancestor shortcut
// cache.WithNestedResolveAncestor(base, node) (that "installed crossed state" is
// exactly why TestRC2_SelfReferenceReturnsRawCR does not count as a C4
// confirmation: it hand-installs the ancestor and hits the stop on the FIRST
// entry, never exercising the real descent-then-reentry). Here the seed RA
// references its OWN direct apiserver path with resolve:true; ResolveNestedCall
// descends into it (installing its node as an ancestor at nested_call.go:195),
// and the RA's self-stage re-enters ResolveNestedCall and trips the stop at
// depth 1 — verified by the "cycle-stop ... depth=1" log the real recursion
// emits (NOT a hand-installed ancestor).
//
// EXPECTED GREEN = INFERRED→TRACED. Three real failure modes make this arm
// able-to-fail (it is not vacuous coverage):
//   (1) the in-process cycle-stop never fired (its Debug line is absent) — the
//       reachability claim would be false;
//   (2) the depth-8 backstop fired instead of the ancestor stop (a "depth limit
//       exceeded" appears / the RA truncates) — the cycle-stop is broken;
//   (3) the self-referenced node is left WITHOUT a dep edge (CollectMatchesForTest
//       empty) — the exact edge-3 staleness defect on the self-reference path.
// If arm (3) ever goes RED the cycle-stop DID leave an orphaned/edge-less cell:
// STOP and escalate — C4 then needs a production fix, do not patch the test.
//
// Reachability note (assertion strength): there is no exported enumerator over
// resident L1 cells, so this arm cannot sweep "every resident cell has an edge"
// directly. It pins the reachable behaviour: the real cycle-stop fires,
// terminates cleanly (not depth-8), and leaves the self-node dep-edge-covered
// (invalidatable) under the driving L1 key — which, together with the structural
// code fact that the cycle-stop Puts nothing, confirms no edge-less resident
// cell arises here.

package dispatchers

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

// nestedSelfReferentialRA builds a RESTAction whose SINGLE api stage points at
// its OWN direct apiserver path with resolve:true — a genuine self-reference.
// Resolving it drives ResolveNestedCall to descend into itself (installing its
// node as an ancestor), so the self-stage's own resolve:true re-enters
// ResolveNestedCall and trips the ancestor cycle-stop at the REAL boundary.
//
// The top-level spec.filter projects a CONSTANT marker, so the RA resolves to
// deterministic non-empty content REGARDLESS of the self-stage output — a clean
// completion is observable, and a depth-8 break (broken cycle-stop) would
// truncate the stage instead of yielding the marker.
func nestedSelfReferentialRA(ns, name string) *unstructured.Unstructured {
	self := map[string]any{
		"name":    "self",
		"path":    directInnerRAPath(ns, name),
		"verb":    "GET",
		"resolve": true,
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata": map[string]any{
			"namespace": ns,
			"name":      name,
		},
		"spec": map[string]any{
			"api":    []any{self},
			"filter": `{"c4resolved":true,"name":"` + name + `"}`,
		},
	}}
}

// TestC4_RealCycleStop_NoEdgelessResidentCell drives a REAL self-referential RA
// recursion through the actual resolve:true pipeline (no installed ancestor) and
// confirms the cycle-stop return leaves NO edge-less resident L1 cell for the
// self-referenced node. This closes #280 (INFERRED→TRACED).
func TestC4_RealCycleStop_NoEdgelessResidentCell(t *testing.T) {
	const ns, name = "demo-system", "fsa-c4-self-composition-resources"

	// Seed the self-referential RA + an RBAC grant for the resolving identity.
	// newNestedCallWatcherWithInner turns CACHE_ENABLED + RESOLVED_CACHE_ENABLED
	// on and resets the resolved cache + dep tracker for this test.
	newNestedCallWatcherWithInner(t, ns, name,
		nestedSelfReferentialRA(ns, name),
		nestedCallRoleBinding(ns, "c4-user"))

	// Wire the REAL seam (production wiring; re-register so the test is hermetic
	// even if a sibling left it swapped).
	restactionsapi.RegisterNestedCallResolver(ResolveNestedCall)

	// Capture the cycle-stop's Debug line by forcing a Debug buffer logger onto
	// the ctx: ResolveNestedCall logs via xcontext.Logger(ctx), and BuildContext
	// + WithLogger installs it for every nested hop (context.WithValue preserves
	// it through WithNestedCallDepth / WithNestedResolveAncestor). This is the
	// "prove the arm ran" signal — the in-process cycle-stop has no counter.
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	const drivingL1 = "c4-driving-l1-cell"
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "c4-user"}),
		xcontext.WithLogger(logger),
	)
	// Thread the driving L1 key + the internal endpoint + internal REST config so
	// the direct apiserver paths take the informer pivot (an internal driver
	// resolve, as a prewarm/refresher would): the self-stage then re-dispatches
	// in-process and re-enters ResolveNestedCall, and its dep edge records against
	// this L1 key. Without the internal REST config rcFromCtx is nil and the inner
	// resolve falls to the per-user path — the self-stage never re-dispatches and
	// the real cycle boundary is never reached (that is precisely the wiring the
	// production internal-driver resolve carries).
	ctx = cache.WithL1KeyContext(ctx, drivingL1)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
	ctx = cache.WithInternalRESTConfig(ctx, &rest.Config{})

	// Drive the seam directly at the OUTER boundary — depth 0, NO ancestor set.
	// The descent installs the ancestor for real; the RA's self-stage re-enters
	// at depth 1 and trips the cycle-stop.
	ref := templates.ObjectReference{
		Reference:  templates.Reference{Name: name, Namespace: ns},
		Resource:   nestedCallInnerGVR.Resource,
		APIVersion: nestedCallInnerGVR.Group + "/" + nestedCallInnerGVR.Version,
	}
	raw, err := ResolveNestedCall(ctx, ref, 0, 0, nil)

	logText := logBuf.String()

	// (pre) CLEAN COMPLETION at the seam boundary. A broken cycle-stop would
	// recurse to the depth backstop and surface a bounded error; the real
	// ancestor stop returns cleanly.
	if err != nil {
		t.Fatalf("C4: the self-referential resolve must complete cleanly via the cycle-stop, got error: %v\nlogs:\n%s", err, logText)
	}
	if len(raw) == 0 {
		t.Fatalf("C4: the self-referential resolve produced EMPTY content — the recursion did not complete.\nlogs:\n%s", logText)
	}

	// (1) THE REAL CYCLE-STOP FIRED (reachability / "prove the arm ran"). The
	// in-process cycle-stop emits this Debug line naming the node; its presence
	// proves the genuine descent-then-reentry tripped the ancestor stop — the
	// REAL boundary, driven without an installed ancestor.
	selfNode := nestedResolveNodeKey(nestedCallInnerGVR.Resource, ns, name)
	if !strings.Contains(logText, "nested resolve cycle-stop") {
		t.Fatalf("C4: the in-process cycle-stop did NOT fire — its Debug line is absent. The real "+
			"self-referential recursion never re-entered the ancestor stop, so this arm did not "+
			"exercise the C4 boundary.\nlogs:\n%s", logText)
	}
	if !strings.Contains(logText, selfNode) {
		t.Fatalf("C4: the cycle-stop fired but not for the self node %q.\nlogs:\n%s", selfNode, logText)
	}

	// (2) NOT the depth-8 backstop. A neutered cycle-stop would recurse to the
	// NestedCallMaxDepth backstop and surface a bounded "depth limit exceeded"
	// error; its absence (with the cycle-stop's own depth=1 line above) proves
	// the ancestor stop caught the cycle at the FIRST reentry, not after 8 hops.
	if strings.Contains(logText, "depth limit exceeded") {
		t.Fatalf("C4: a 'depth limit exceeded' surfaced — the ancestor cycle-stop did NOT catch the "+
			"cycle at reentry; it recursed to the depth-8 backstop (broken cycle-stop).\nlogs:\n%s", logText)
	}

	// (3) CLEAN RESOLVE MARKER. The self-referential RA resolved to its constant
	// marker — the self-stage returned the cycle-stopped raw CR and the RA's
	// top-level filter still yielded (a depth-8 break would have truncated it).
	if !strings.Contains(string(raw), "c4resolved") {
		t.Fatalf("C4: the resolved RA marker is missing — the recursion did not resolve the RA "+
			"cleanly; got %s", raw)
	}

	// (4) THE C4 CORE — NO EDGE-LESS SELF-NODE. The cycle-stop returns its raw CR
	// INLINE (no store.Put, no Deps().Record), so it must not leave the
	// self-referenced node as an orphaned resident cell with no dep edge. The
	// self-reference IS recorded (by the resolver's direct-apiserver-path dep
	// site, under the driving L1 key) — so the node is invalidatable. An EMPTY
	// match set here means the self-reference path left the node edge-less: the
	// exact edge-3 staleness defect. If this goes RED, STOP and escalate — C4
	// needs a production fix, not a test patch.
	matches := cache.Deps().CollectMatchesForTest(nestedCallInnerGVR, ns, name)
	if len(matches) == 0 {
		t.Fatalf("C4 DEFECT: the self-referenced node (gvr=%s ns=%s name=%s) has NO dep edge after the "+
			"real cycle-stop recursion — it is an edge-less/orphaned cell that a CR edit would NOT "+
			"invalidate (the edge-3 staleness class). Editing the RA would serve stale forever. "+
			"matches=%#v", nestedCallInnerGVR, ns, name, matches)
	}
	// The self-edge must be attached to the driving L1 key (the "if it does, the
	// cell carries its self-edge" clause): the node is covered by the descent's
	// L1 key, not some unrelated key.
	if _, ok := matches[drivingL1]; !ok {
		t.Fatalf("C4: the self-node dep edge exists but not under the driving L1 key %q — the "+
			"self-reference was recorded against an unexpected key. matches=%#v", drivingL1, matches)
	}
}
