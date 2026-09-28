// refilter_concurrency_test.go — L13 hermetic falsifiers for the
// restactions/api UAF refilter dispatch path.
//
//	(a) -race over concurrent iterator workers running the REAL per-worker
//	    refilter (applyUserAccessFilterOnPig / refilterSlice). Each iterator
//	    stage worker (resolve.go g.Go) shares ONE read-only *UserAccessFilterSpec
//	    (mapping) + the process-global RBAC snapshot; the hoisted nsCode/nameCode
//	    are per-CALL compiled *gojq.Code (immutable, safe to run concurrently).
//	    The K>1 × M>1 harness runs the real path under -race → GREEN (no race).
//	    RED: a SHARED-MUTABLE nsCode (a package-var mutated per item across
//	    workers) races — demonstrated by racingRefilterSlice, which the SAME
//	    harness flags under -race (proving the harness discriminates a racing
//	    shared-nsCode impl from the real per-call-compile one). See
//	    feedback_falsifier_shape_must_discriminate (K>1×M>1 + a RED arm).
//
//	(b) SA-endpoint ACQUIRE FAILURE → empty items, fail-closed-but-respond:
//	    resolveStageEndpoint's UAF branch, when dynamic.ServiceAccountEndpoint
//	    (via the serviceAccountEndpointFn seam) errors, MUST write
//	    dict[id]={items:[]} + return stageContinue (the C-2 posture) and NEVER
//	    fall through to the per-user (user-bearer) dispatch. RED: neuter the
//	    saErr branch so it returns stageProceed with a zero endpoint → the stage
//	    proceeds with no SA endpoint (the user-bearer leak class).
//
//	(c) evalJQString / NamespaceFrom MULTI-YIELD → fails closed: a NamespaceFrom
//	    that yields >1 value surfaces ErrMultiYield → evalSingle denies the item
//	    (never a silent concatenated-garbage namespace flowing into EvaluateRBAC).
//	    RED: map ErrMultiYield to a permit (or to ns="") → the item is kept.

package api

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/itchyny/gojq"
	"github.com/krateo-platformops/plumbing/endpoints"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ─────────────────────────────────────────────────────────────────────────
// L13(a) — -race over concurrent iterator workers running the refilter stage.
// ─────────────────────────────────────────────────────────────────────────

// seedCompReaderRBAC grants the "devs" group `list compositions` in each of
// the given namespaces (a Role+RoleBinding per ns), the realistic narrow-RBAC
// shape a per-namespace iterator fan-out refilters against.
func seedCompReaderRBAC(namespaces ...string) []runtime.Object {
	var out []runtime.Object
	for _, ns := range namespaces {
		out = append(out,
			&rbacv1.Role{
				TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
				ObjectMeta: metav1.ObjectMeta{Name: "comp-reader", Namespace: ns},
				Rules: []rbacv1.PolicyRule{
					{Verbs: []string{"list"}, APIGroups: []string{"composition.krateo.io"}, Resources: []string{"compositions"}},
				},
			},
			&rbacv1.RoleBinding{
				TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
				ObjectMeta: metav1.ObjectMeta{Name: "comp-reader-binding", Namespace: ns},
				Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "devs"}},
				RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "comp-reader"},
			},
		)
	}
	return out
}

// nsItems builds M composition objects, half in a GRANTED namespace and half
// in a DENIED one, so a worker's expected (kept, dropped) split is non-trivial
// (proves the concurrent workers each compute the RIGHT answer, not just "no
// crash"). granted are the namespaces the user can list; denied are refused.
func nsItems(m int, granted, denied string) []any {
	items := make([]any, 0, m)
	for i := 0; i < m; i++ {
		ns := granted
		if i%2 == 1 {
			ns = denied
		}
		items = append(items, map[string]any{
			"metadata": map[string]any{"name": padName3(i), "namespace": ns},
		})
	}
	return items
}
func padName3(i int) string {
	return "c-" + string(rune('0'+(i/100)%10)) + string(rune('0'+(i/10)%10)) + string(rune('0'+i%10))
}

// TestRefilter_ConcurrentIteratorWorkers_Race is L13(a) GREEN. K>1 concurrent
// goroutines model the iterator errgroup workers; each runs the REAL
// applyUserAccessFilterOnPig over its OWN pig (a distinct per-worker map, index
// -aligned like resolve.go's dispatchOneCall bundle) with M>1 items, all sharing
// ONE read-only *UserAccessFilterSpec + the process-global RBAC snapshot. Under
// -race this must be clean; each worker must independently narrow to the granted
// half. (Run with -race to arm the concurrency tooth; the correctness assertions
// run in both modes.)
func TestRefilter_ConcurrentIteratorWorkers_Race(t *testing.T) {
	const (
		K       = 8  // workers > 1 (feedback_falsifier_shape_must_discriminate)
		M       = 24 // items per worker > 1
		granted = "bench-ns-01"
		denied  = "bench-ns-02"
	)
	newRefilterTestWatcher(t, seedCompReaderRBAC(granted)...)

	// ONE shared, read-only mapping (mirrors the single *templates.API the stage
	// loop hands every iterator worker).
	sharedUAF := &templates.UserAccessFilterSpec{
		Verb:          "list",
		Group:         "composition.krateo.io",
		Resource:      "compositions",
		NamespaceFrom: ".metadata.namespace",
	}
	ctx := ctxWithUser("cyberjoker", "devs")

	var wg sync.WaitGroup
	errs := make([]error, K)
	kept := make([]int, K) // disjoint slot per worker (no lock needed)
	for k := 0; k < K; k++ {
		k := k
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each worker owns its pig — the shared state is sharedUAF (read-only)
			// + the RBAC global. This is exactly the resolve.go per-worker shape.
			pig := map[string]any{
				"compositions": map[string]any{"items": nsItems(M, granted, denied)},
			}
			res := applyUserAccessFilterOnPig(ctx, pig, nil, "compositions", sharedUAF)
			kept[k] = res.Kept
			if res.Kept != M/2 || res.Dropped != M/2 {
				errs[k] = &countMismatch{worker: k, kept: res.Kept, dropped: res.Dropped, want: M / 2}
			}
		}()
	}
	wg.Wait()

	for k := 0; k < K; k++ {
		if errs[k] != nil {
			t.Fatalf("worker %d: %v", k, errs[k])
		}
		if kept[k] != M/2 {
			t.Fatalf("worker %d: kept=%d want %d (granted half only)", k, kept[k], M/2)
		}
	}
}

type countMismatch struct {
	worker, kept, dropped, want int
}

func (e *countMismatch) Error() string {
	return "refilter miscount: kept=" + itoaSmall(e.kept) + " dropped=" + itoaSmall(e.dropped) + " want kept/dropped=" + itoaSmall(e.want)
}
func itoaSmall(i int) string {
	if i < 10 {
		return string(rune('0' + i))
	}
	return string(rune('0'+i/10)) + string(rune('0'+i%10))
}

// racingSharedNsCode is the DELIBERATELY-BROKEN shared-mutable *gojq.Code slot
// the RED arm uses. The real refilterSlice hoists a per-CALL nsCode local; a
// (hypothetical) "optimisation" that cached one *gojq.Code in a package var and
// REASSIGNED it per item across workers would create a data race on THIS write.
var racingSharedNsCode *gojq.Code

// racingRefilterSlice mimics refilterSlice but WRITES the shared package var
// racingSharedNsCode on every item — the exact wrong-shape L13(a) guards
// against. It exists ONLY so TestRefilter_SharedNsCode_RED_Races proves the
// K×M harness (and -race) actually catch a shared-mutable-nsCode impl; the REAL
// path (TestRefilter_ConcurrentIteratorWorkers_Race) never touches it.
func racingRefilterSlice(ctx context.Context, uaf *templates.UserAccessFilterSpec, items []any) int {
	kept := 0
	for range items {
		// The racing write: a shared *gojq.Code reassigned per item, concurrently
		// across workers → the -race arm flags it. (Value intentionally recomputed
		// so the compiler cannot elide the store.)
		code, _ := gojq.Parse(uaf.NamespaceFrom)
		racingSharedNsCode, _ = gojq.Compile(code)
		_ = ctx
		kept++
	}
	return kept
}

// TestRefilter_SharedNsCode_RED_Races is the L13(a) RED-arm PROOF: under -race
// the SHARED-MUTABLE nsCode impl (racingRefilterSlice) races, so the same K×M
// harness that passes clean for the real path FAILS the race detector here.
// This is a MANUAL RED (skipped in normal runs) — running it under -race shows
// the harness + -race discriminate the wrong shared-nsCode shape. Un-skip +
// `go test -race -run TestRefilter_SharedNsCode_RED_Races` to observe the RED.
func TestRefilter_SharedNsCode_RED_Races(t *testing.T) {
	t.Skip("RED-arm proof (manual): un-skip and run under -race to observe the shared-mutable nsCode data race the L13(a) harness+detector catch")

	const (
		K = 8
		M = 24
	)
	uaf := &templates.UserAccessFilterSpec{NamespaceFrom: ".metadata.namespace"}
	items := nsItems(M, "a", "b")
	var wg sync.WaitGroup
	for k := 0; k < K; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			racingRefilterSlice(context.Background(), uaf, items)
		}()
	}
	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────
// L13(b) — SA-endpoint acquire failure → empty items, fail-closed-but-respond.
// ─────────────────────────────────────────────────────────────────────────

// TestResolveStageEndpoint_UAF_SAAcquireFailure_FailClosedButResponds is L13(b).
// When the SA-endpoint acquire (serviceAccountEndpointFn) errors, the UAF branch
// of resolveStageEndpoint MUST: (1) write dict[id]={items:[]} (respond, empty),
// (2) return stageContinue (the loop continues — downstream stages still run),
// (3) return a ZERO endpoint (never a user-bearer endpoint). The fail-closed-
// but-respond posture (Revision 5): there is NO fallback to the per-user path,
// because that would leak the user's bearer token to a SA-marked stage.
//
// RED: neuter the saErr branch to return (zeroEP, stageProceed) instead of
// writing the empty result + stageContinue → the stage proceeds WITHOUT a SA
// endpoint (the user-bearer / no-endpoint leak class this branch prevents).
func TestResolveStageEndpoint_UAF_SAAcquireFailure_FailClosedButResponds(t *testing.T) {
	// Seam-inject a FAILING SA-endpoint acquire (hermetic — does not depend on
	// the ambient /var/run/secrets files). Restore on cleanup.
	orig := serviceAccountEndpointFn
	t.Cleanup(func() { serviceAccountEndpointFn = orig })
	var acquireCalls atomic.Int64
	serviceAccountEndpointFn = func() (*endpoints.Endpoint, error) {
		acquireCalls.Add(1)
		return nil, context.DeadlineExceeded // any non-nil error
	}

	r := &resolveRun{
		log:  scalarFalsifierLogger(),
		dict: map[string]any{},
	}
	apiCall := &templates.API{
		Name: "compositions",
		UserAccessFilter: &templates.UserAccessFilterSpec{
			Verb: "list", Group: "composition.krateo.io", Resource: "compositions",
		},
	}

	ep, _, action := r.resolveStageEndpoint("compositions", apiCall, true /*uafActive*/)

	if acquireCalls.Load() != 1 {
		t.Fatalf("SA-endpoint acquire must be attempted exactly once via the seam, got %d", acquireCalls.Load())
	}
	// (2) stageContinue — the stage produces its empty result and the loop
	// continues; it must NOT be stageProceed (proceed would dispatch with a
	// zero/absent endpoint) nor stageReturn (that would truncate the resolve).
	if action != stageContinue {
		t.Fatalf("L13(b) RED: SA-acquire failure must return stageContinue (fail-closed-but-respond); got %v", action)
	}
	// (3) ZERO endpoint — NEVER a user-bearer endpoint. No token, no server URL.
	if ep != (endpoints.Endpoint{}) {
		t.Fatalf("L13(b) RED: SA-acquire failure must return a ZERO endpoint (never a user-bearer fallback); got %+v", ep)
	}
	// (1) dict[id] is the canonical empty result — respond, but empty (no leak).
	raw, ok := r.dict["compositions"]
	if !ok {
		t.Fatalf("L13(b) RED: SA-acquire failure must WRITE an empty result at dict[id] (fail-closed-but-RESPOND); dict[id] absent")
	}
	m, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("L13(b): dict[id] must be a map with an empty items slice; got %T", raw)
	}
	itemsRaw, hasItems := m["items"]
	if !hasItems {
		t.Fatalf("L13(b): dict[id] must carry an empty items slice; got %v", m)
	}
	items, ok := itemsRaw.([]any)
	if !ok || len(items) != 0 {
		t.Fatalf("L13(b) RED: dict[id].items must be EMPTY on SA-acquire failure (no SA-dispatched or user-dispatched rows leak); got %v", itemsRaw)
	}
}

// TestResolveStageEndpoint_UAF_SAAcquireSuccess_Proceeds is the discriminating
// control: with a SUCCESSFUL SA-endpoint acquire the same branch returns the SA
// endpoint + stageProceed (proving the fail-closed path keys on the ACQUIRE
// ERROR, not on uafActive per se).
func TestResolveStageEndpoint_UAF_SAAcquireSuccess_Proceeds(t *testing.T) {
	orig := serviceAccountEndpointFn
	t.Cleanup(func() { serviceAccountEndpointFn = orig })
	saEP := &endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: "sa-token"}
	serviceAccountEndpointFn = func() (*endpoints.Endpoint, error) { return saEP, nil }

	r := &resolveRun{log: scalarFalsifierLogger(), dict: map[string]any{}}
	apiCall := &templates.API{
		Name:             "compositions",
		UserAccessFilter: &templates.UserAccessFilterSpec{Verb: "list", Group: "composition.krateo.io", Resource: "compositions"},
	}

	ep, isSA, action := r.resolveStageEndpoint("compositions", apiCall, true)
	if action != stageProceed {
		t.Fatalf("control: a successful SA acquire must return stageProceed; got %v", action)
	}
	if ep.ServerURL != saEP.ServerURL || ep.Token != saEP.Token {
		t.Fatalf("control: a successful SA acquire must return the SA endpoint; got %+v", ep)
	}
	// #267 PROVENANCE (load-bearing): the UAF stage dials the SA endpoint, so it
	// MUST report isSAEndpoint=true — this is what makes runStage stamp
	// WithServiceAccountDial and the dial thread the self-reloading SA token FILE.
	// A regression that dropped the stamp (returned false) would silently disable
	// the #267 fix; assert the TRUE side, not only the false side.
	if !isSA {
		t.Fatalf("#267: a UAF stage (SA-endpoint dispatch) must report isSAEndpoint=true; got false — the dial-site SA token-file wiring would be silently disabled")
	}
	if _, wrote := r.dict["compositions"]; wrote {
		t.Fatalf("control: the SUCCESS path must NOT write the empty-result placeholder")
	}
}

// ─────────────────────────────────────────────────────────────────────────
// L13(c) — evalJQString / NamespaceFrom multi-yield → fails closed.
// ─────────────────────────────────────────────────────────────────────────

// TestEvalJQString_MultiYield_FailsClosed proves the evalJQString path itself
// surfaces ErrMultiYield (the DELIBERATE §3.4.5 change) — a NamespaceFrom that
// yields >1 value returns ("", ErrMultiYield), never a silent concatenated
// invalid-JSON namespace string. This is the low-level tooth L13(c) rests on.
func TestEvalJQString_MultiYield_FailsClosed(t *testing.T) {
	// `.[]` over a 2-element array yields TWO values → multi-yield.
	s, err := evalJQString(context.Background(), ".[]", []any{"ns-a", "ns-b"})
	if err == nil {
		t.Fatalf("L13(c) RED: a multi-yield NamespaceFrom must return an error (ErrMultiYield), got string=%q err=nil (silent garbage would flow into EvaluateRBAC)", s)
	}
	if s != "" {
		t.Fatalf("L13(c): a multi-yield must return the empty string alongside the error; got %q", s)
	}
	// Discriminating control: a SINGLE-yield expression returns the value + nil err.
	one, err := evalJQString(context.Background(), ".metadata.namespace",
		map[string]any{"metadata": map[string]any{"namespace": "ns-a"}})
	if err != nil || one != "ns-a" {
		t.Fatalf("control: a single-yield NamespaceFrom must succeed; got %q err=%v", one, err)
	}
}

// TestEvalSingle_MultiYieldNamespaceFrom_DeniesItem is L13(c): a NamespaceFrom
// that MULTI-YIELDS on the item makes evalSingle fail closed (deny), so a
// permitted-looking object is DROPPED rather than admitted under a garbage
// namespace. Driven through the REAL compileNamespaceFrom + evalSingle so the
// ErrMultiYield → deny contract is exercised end-to-end.
//
// RED: change jqValueToString to map ErrMultiYield to ("", nil) (or evalSingle
// to permit on a NamespaceFrom error) → the item is KEPT under an empty/garbage
// namespace — the leak this test forbids.
func TestEvalSingle_MultiYieldNamespaceFrom_DeniesItem(t *testing.T) {
	// admin has cluster-admin: EvaluateRBAC would PERMIT for any resolvable
	// namespace, so a KEPT result can ONLY come from the multi-yield NOT failing
	// closed. This isolates the fail-closed tooth from the RBAC decision.
	newRefilterTestWatcher(t, adminClusterAdmin()...)

	// A NamespaceFrom that yields TWO values for the item → ErrMultiYield.
	multiYieldUAF := &templates.UserAccessFilterSpec{
		Verb:          "get",
		Group:         "composition.krateo.io",
		Resource:      "compositions",
		NamespaceFrom: ".metadata | .name, .namespace", // 2 yields → multi-yield
	}
	item := map[string]any{"metadata": map[string]any{"name": "c1", "namespace": "ns-a"}}

	nsExpr, nsCode := compileNamespaceFrom(multiYieldUAF)
	nameExpr, nameCode := compileNameFrom(multiYieldUAF)
	permitted := evalSingle(ctxWithUser("admin"), scalarFalsifierLogger(),
		"admin", nil, multiYieldUAF, []string{"compositions"},
		item, nsExpr, nsCode, nameExpr, nameCode)
	if permitted {
		t.Fatalf("L13(c) RED: a multi-yield NamespaceFrom must DENY the item (fail-closed); evalSingle PERMITTED it — a garbage/empty namespace reached EvaluateRBAC and matched admin's cluster-wide grant (leak)")
	}

	// Discriminating control: the SAME admin + item with a SINGLE-yield
	// NamespaceFrom IS permitted (proves the deny came from the multi-yield, not
	// from a missing grant).
	okUAF := &templates.UserAccessFilterSpec{
		Verb: "get", Group: "composition.krateo.io", Resource: "compositions",
		NamespaceFrom: ".metadata.namespace",
	}
	okNsExpr, okNsCode := compileNamespaceFrom(okUAF)
	okNameExpr, okNameCode := compileNameFrom(okUAF)
	if !evalSingle(ctxWithUser("admin"), scalarFalsifierLogger(), "admin", nil, okUAF,
		[]string{"compositions"}, item, okNsExpr, okNsCode, okNameExpr, okNameCode) {
		t.Fatalf("control: a single-yield NamespaceFrom on the same admin/item must be PERMITTED (proves the multi-yield deny is the discriminator)")
	}
}

// adminClusterAdmin builds a cluster-admin ClusterRole + a CRB binding it to
// user "admin" — so EvaluateRBAC permits admin on any (verb,group,resource,ns).
func adminClusterAdmin() []runtime.Object {
	return []runtime.Object{
		&rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: "cluster-admin"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"*"}, APIGroups: []string{"*"}, Resources: []string{"*"}}},
		},
		&rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: "admin-binding"},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "admin"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "cluster-admin"},
		},
	}
}
