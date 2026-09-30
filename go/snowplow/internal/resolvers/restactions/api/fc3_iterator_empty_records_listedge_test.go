// fc3_iterator_empty_records_listedge_test.go — #279 C3 ROOT-FIX falsifiers.
//
// DEFECT (TRACED, resolve.go collapseOrFanoutPlan): an iterator stage that fans
// over ZERO items returns before the per-call edge-3 loop (`for i := range tmp`),
// so the fanned GVR's (gvr, ns, "*") LIST dep edge is never recorded under the
// widget L1 key. The warm cell is then invalidated by nothing on a later CR ADD
// of that GVR — a stale-negative (#279; #285's F-C3 only fences the seed
// re-walk). FIX: at the short-circuit, parse the STATIC path skeleton
// (cache.ParseAPIServerListDepSkeleton) and RecordList the fanned GVR's LIST edge
// (+ EnsureResourceType), so the empty-fan cell is invalidatable.
//
// pm-freshness #279 conditions covered:
//   (1) version+resource must be STATIC → templated ⇒ ok=false ⇒ #285 fence.
//   (2) GVR-parity golden: skeleton GVR == ParseAPIServerPathToDep(concrete) GVR.
//   (3) own-GVR list-then-get-each scope (documented in path_parse.go).
//   (4) content-level falsifier: EdgesUnder has the LIST edge AND the re-resolved
//       body reflects the item; + discriminating control (a DIFFERENT gvr/ns ADD
//       must NOT dirty the cell).
//
// HARNESS: real full api.Resolve (newF1Watcher), nil-EndpointRef stages resolve
// via cache.WithInternalEndpoint (the boot-seed internal/SA transport;
// unreachable URL — informer serves the GET, and the empty-fan edge records from
// the path skeleton with no dispatch). The fanned GVR is the newF1Watcher-
// registered widgets GVR (an UNREGISTERED GVR panics the dynamic-fake reflector
// in lazyRegisterInnerCallPaths); the C3 short-circuit + fix are GVR-agnostic and
// the widget KEYS are unique, so no cross-test edge bleeds into the assertions.

package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

const fc3NS = "team-a"              // an f1AllNamespaces member (broad-user authorized; pre-seeds widget-team-a)
const fc3Widget = "widget-" + fc3NS // the pre-seeded widget the empty fan misses

// fc3GetEachPath — static-ns list-then-get-each: GVR + ns STATIC, only the name
// is item-templated. The skeleton drops the trailing ${.name} → a LIST edge
// (widgets, team-a, "*"). Static ns lets the discriminating control assert a
// DIFFERENT-ns ADD does not dirty the cell.
const fc3GetEachPath = `${ "/apis/widgets.krateo.io/v1/namespaces/team-a/widgets/" + .name }`

// fc3TemplatedVersionPath — condition 1: a TEMPLATED version segment ⇒
// ParseAPIServerListDepSkeleton ok=false ⇒ no edge ⇒ #285 fence.
const fc3TemplatedVersionPath = `${ "/apis/widgets.krateo.io/" + .v + "/namespaces/team-a/widgets/" + .name }`

func fc3FannedGVR() schema.GroupVersionResource { return f1WidgetsGVR }

func fc3IterStage(path string) *templates.API {
	return &templates.API{
		Name:            "fc3-iter-stage",
		Path:            path,
		Verb:            ptr.To("GET"),
		ContinueOnError: ptr.To(true),
		ErrorKey:        ptr.To("error"),
		DependsOn:       &templates.Dependency{Iterator: ptr.To(".vals")},
	}
}

// fc3Resolve drives a single iterator stage under widget L1 key l1Key, fanning
// over `vals` (empty ⇒ the C3 short-circuit). Returns the resolved dict.
func fc3Resolve(t *testing.T, rw *cache.ResourceWatcher, l1Key, path string, vals []any) map[string]any {
	t.Helper()
	iterFailFastRetries(t)
	rc := &rest.Config{Host: "http://127.0.0.1:1"}
	ctx := cache.WithInternalEndpoint(
		cache.WithL1KeyContext(
			xcontext.BuildContext(context.Background(),
				xcontext.WithUserInfo(jwtutil.UserInfo{Username: f1BroadUser})),
			l1Key),
		&endpoints.Endpoint{ServerURL: "http://test.invalid"})
	return Resolve(ctx, ResolveOptions{
		RC:      rc,
		Watcher: rw,
		Items:   []*templates.API{fc3IterStage(path)},
		Extras:  map[string]any{"vals": vals},
	})
}

func fc3EdgesHaveFannedGVR(l1Key string) bool {
	for _, e := range cache.Deps().EdgesUnder(l1Key) {
		if e.GVR == fc3FannedGVR() {
			return true
		}
	}
	return false
}

// TestFC3_IteratorEmpty_RecordsFannedListEdge — the main RED→GREEN arm + the
// content and discriminating conditions. RED on the UNFIXED tree: the empty-fan
// resolve records NO fanned-GVR edge (EdgesUnder=[]). GREEN after the fix.
func TestFC3_IteratorEmpty_RecordsFannedListEdge(t *testing.T) {
	rw := newF1Watcher(t)
	fanned := fc3FannedGVR()

	const emptyKey = "L1_fc3_empty"
	const controlKey = "L1_fc3_control"

	// CONTROL (right-reason guard + content): a NON-empty fan over the SAME GVR
	// GETs the pre-seeded widget → records the edge (edge-3 loop reached) AND the
	// resolved body reflects the item (content, not a counter).
	ctrlBody := fc3Resolve(t, rw, controlKey, fc3GetEachPath, []any{map[string]any{"name": fc3Widget}})
	if !fc3EdgesHaveFannedGVR(controlKey) {
		t.Fatalf("#279 CONTROL FAILED (harness inert, not a real RED): a NON-empty fan over %s recorded "+
			"NO edge under %q — the resolver's edge-3 loop was never reached. EdgesUnder=%v",
			fanned.String(), controlKey, cache.Deps().EdgesUnder(controlKey))
	}
	if !fc3BodyReflects(t, ctrlBody, fc3Widget) {
		t.Fatalf("#279 CONTENT: the non-empty fan's resolved body did not reflect the GET'd item %q — "+
			"body=%s", fc3Widget, fc3Marshal(t, ctrlBody))
	}

	// RED TARGET: the EMPTY fan over the SAME GVR records the LIST edge from the
	// static path skeleton (post-fix). RED on the unfixed tree = EdgesUnder=[].
	emptyBody := fc3Resolve(t, rw, emptyKey, fc3GetEachPath, []any{})
	if fc3BodyReflects(t, emptyBody, fc3Widget) {
		t.Fatalf("#279 setup: the EMPTY-fan body must NOT already reflect %q (the cold empty fan makes no "+
			"call) — the stale-negative premise is broken", fc3Widget)
	}
	if !fc3EdgesHaveFannedGVR(emptyKey) {
		t.Fatalf("#279 RED (expected on the UNFIXED tree): an iterator stage fanning over an EMPTY prior "+
			"list short-circuited in collapseOrFanoutPlan BEFORE the edge-3 loop, so the fanned GVR %s "+
			"recorded NO (gvr,ns,\"*\") LIST edge under %q → a later CR ADD dirty-marks nothing "+
			"(stale-negative, #279). The fix records the static-skeleton LIST edge. EdgesUnder=%v",
			fanned.String(), emptyKey, cache.Deps().EdgesUnder(emptyKey))
	}

	// The recorded edge must be the ns-scoped LIST edge (widgets, team-a, "*").
	if !fc3HasListEdge(emptyKey, fanned, fc3NS) {
		t.Fatalf("#279: expected an ns-scoped LIST edge (%s, %s, \"*\") under %q; got %v",
			fanned.String(), fc3NS, emptyKey, cache.Deps().EdgesUnder(emptyKey))
	}

	// DISCRIMINATING CONTROL (condition 4 — proves keying): a CR ADD of the SAME
	// gvr+ns dirties the cell; a DIFFERENT ns, or a DIFFERENT gvr, must NOT.
	hook, snap := depRecordCapturedHook()
	cache.Deps().SetRefreshHook(hook)

	before := fc3CountKey(snap(), emptyKey)
	cache.Deps().OnAdd(fanned, fc3NS, "brand-new-widget")
	if fc3CountKey(snap(), emptyKey) <= before {
		t.Fatalf("#279 MECHANISM: a CR ADD of %s in ns %q did NOT dirty-mark the cell %q — the recorded "+
			"LIST edge does not invalidate on a same-gvr+ns ADD", fanned.String(), fc3NS, emptyKey)
	}
	before = fc3CountKey(snap(), emptyKey)
	cache.Deps().OnAdd(fanned, "team-b", "widget-elsewhere")
	if fc3CountKey(snap(), emptyKey) > before {
		t.Fatalf("#279 DISCRIMINATING: a CR ADD of %s in a DIFFERENT ns (team-b) dirtied %q — the edge is "+
			"over-broad (should be ns-scoped to %q)", fanned.String(), emptyKey, fc3NS)
	}
	otherGVR := schema.GroupVersionResource{Group: "unrelated.krateo.io", Version: "v1", Resource: "frobs"}
	before = fc3CountKey(snap(), emptyKey)
	cache.Deps().OnAdd(otherGVR, fc3NS, "a-frob")
	if fc3CountKey(snap(), emptyKey) > before {
		t.Fatalf("#279 DISCRIMINATING: a CR ADD of an UNRELATED gvr %s dirtied %q — the edge is mis-keyed",
			otherGVR.String(), emptyKey)
	}

	// CONTENT (end-to-end): the edge would trigger the refresher to re-resolve;
	// simulate that re-resolve with the item now present in the fan → the body
	// reflects it (proving the invalidatable cell converges to fresh content).
	reBody := fc3Resolve(t, rw, emptyKey, fc3GetEachPath, []any{map[string]any{"name": fc3Widget}})
	if !fc3BodyReflects(t, reBody, fc3Widget) {
		t.Fatalf("#279 CONTENT: the re-resolved body did not reflect the item %q the empty fan missed — "+
			"body=%s", fc3Widget, fc3Marshal(t, reBody))
	}
}

// TestFC3_TemplatedVersionResource_Declines — condition 1: a templated version
// (or resource) segment ⇒ ParseAPIServerListDepSkeleton ok=false ⇒ NO edge
// recorded (no wrong/underdetermined coordinate) ⇒ #285 fence handoff, no crash.
func TestFC3_TemplatedVersionResource_Declines(t *testing.T) {
	if _, _, ok := cache.ParseAPIServerListDepSkeleton(fc3TemplatedVersionPath); ok {
		t.Fatalf("#279 cond1: templated VERSION must decline (ok=false); it did not")
	}
	templRes := `${ "/apis/widgets.krateo.io/v1/namespaces/team-a/" + .resource }`
	if _, _, ok := cache.ParseAPIServerListDepSkeleton(templRes); ok {
		t.Fatalf("#279 cond1: templated RESOURCE must decline (ok=false); it did not")
	}

	rw := newF1Watcher(t)
	const templKey = "L1_fc3_templated"
	_ = fc3Resolve(t, rw, templKey, fc3TemplatedVersionPath, []any{})
	if got := cache.Deps().EdgesUnder(templKey); len(got) != 0 {
		t.Fatalf("#279 cond1: a templated-version empty fan recorded a (wrong) edge under %q: %v — it must "+
			"decline and leave the residual to the #285 fence", templKey, got)
	}
}

// TestFC3_GVRParity_SkeletonMatchesConcrete — condition 2: the skeleton parser's
// GVR MUST equal ParseAPIServerPathToDep's GVR on a concrete instance of the same
// path (drift here means the fix records the wrong coordinate / is inert).
func TestFC3_GVRParity_SkeletonMatchesConcrete(t *testing.T) {
	cases := []struct {
		name     string
		template string
		concrete string
	}{
		// ns-scope hardening: templated-ns namespaced paths now DECLINE (they are
		// asserted in TestFC3_TemplatedNamespace_NamespacedResource_Declines), so
		// the ok-parity set is STATIC-ns + genuinely cluster-scoped only.
		{"grouped-get-each-static-ns",
			fc3GetEachPath,
			"/apis/widgets.krateo.io/v1/namespaces/team-a/widgets/widget-team-a"},
		{"grouped-cluster-scope",
			`${ "/apis/apps.krateo.io/v1/compositions/" + .name }`,
			"/apis/apps.krateo.io/v1/compositions/comp-1"},
		{"core-namespaced-static-ns",
			`${ "/api/v1/namespaces/team-a/configmaps/" + .name }`,
			"/api/v1/namespaces/team-a/configmaps/cm-1"},
		{"core-cluster-scope",
			`${ "/api/v1/nodes/" + .name }`,
			"/api/v1/nodes/node-1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			skelGVR, _, ok := cache.ParseAPIServerListDepSkeleton(c.template)
			if !ok {
				t.Fatalf("skeleton parse declined a static-GVR template %q", c.template)
			}
			concGVR, _, _, cok := cache.ParseAPIServerPathToDep(c.concrete)
			if !cok {
				t.Fatalf("concrete parse failed for %q", c.concrete)
			}
			if skelGVR != concGVR {
				t.Fatalf("#279 GVR-PARITY DRIFT: skeleton %v != concrete %v (template %q / concrete %q)",
					skelGVR, concGVR, c.template, c.concrete)
			}
		})
	}
}

// TestFC3_TemplatedNamespace_NamespacedResource_Declines — #279 ns-scope
// hardening: a TEMPLATED namespace on a NAMESPACED resource must DECLINE
// (ok=false → #285 fence), never a guessed cluster-wide (gvr,"","*") edge that
// would feed the #239 dirty-mark fan-out at 50K×1000. A genuinely CLUSTER-scoped
// path (no /namespaces/) is unaffected — its ns="" edge is its TRUE scope.
//
// RED on the pre-hardening tree: the templated-ns empty fan records (gvr,"","*")
// (ns templated → ""), so the "no cluster-wide edge" assertion REDs. GREEN after
// the namespaced branches decline on a templated ns.
func TestFC3_TemplatedNamespace_NamespacedResource_Declines(t *testing.T) {
	const groupedTemplNs = `${ "/apis/widgets.krateo.io/v1/namespaces/" + .ns + "/widgets/" + .name }`
	const coreTemplNs = `${ "/api/v1/namespaces/" + .ns + "/configmaps/" + .name }`

	// (a) UNIT: templated-ns namespaced paths (grouped AND core) decline.
	if _, _, ok := cache.ParseAPIServerListDepSkeleton(groupedTemplNs); ok {
		t.Fatalf("#279 ns-hardening: a templated-ns GROUPED namespaced path must decline (ok=false); it did not")
	}
	if _, _, ok := cache.ParseAPIServerListDepSkeleton(coreTemplNs); ok {
		t.Fatalf("#279 ns-hardening: a templated-ns CORE namespaced path must decline (ok=false); it did not")
	}

	// (b) END-TO-END: a real empty fan over a templated-ns namespaced widgets path
	// records NO edge — specifically NO cluster-wide (widgets,"","*").
	rw := newF1Watcher(t)
	fanned := fc3FannedGVR()
	const templNsL1 = "L1_fc3_templ_ns"
	_ = fc3Resolve(t, rw, templNsL1, groupedTemplNs, []any{})
	edges := cache.Deps().EdgesUnder(templNsL1)
	for _, e := range edges {
		if e.GVR == fanned && e.Namespace == "" {
			t.Fatalf("#279 ns-hardening RED (pre-fix): a templated-ns empty fan recorded the cluster-wide "+
				"edge (%s,\"\",\"*\") under %q — the #239 super-scope this hardening removes. It must DECLINE.",
				fanned.String(), templNsL1)
		}
	}
	if len(edges) != 0 {
		t.Fatalf("#279 ns-hardening: a templated-ns namespaced empty fan must record NO edge (decline → "+
			"#285 fence); got %v under %q", edges, templNsL1)
	}

	// (c) POSITIVE counterpart: a genuinely CLUSTER-scoped empty fan (no
	// /namespaces/) over the registered widgets GVR STILL records (widgets,"","*")
	// — its true scope, unchanged by the hardening (cluster branch untouched).
	const clusterPath = `${ "/apis/widgets.krateo.io/v1/widgets/" + .name }`
	const clusterL1 = "L1_fc3_cluster"
	_ = fc3Resolve(t, rw, clusterL1, clusterPath, []any{})
	if !fc3HasListEdge(clusterL1, fanned, "") {
		t.Fatalf("#279 ns-hardening: a genuinely cluster-scoped empty fan must STILL record (%s,\"\",\"*\") "+
			"(its true scope, cluster branch unchanged); EdgesUnder=%v", fanned.String(), cache.Deps().EdgesUnder(clusterL1))
	}
}

// --- helpers ---

func fc3HasListEdge(l1Key string, gvr schema.GroupVersionResource, ns string) bool {
	for _, e := range cache.Deps().EdgesUnder(l1Key) {
		if e.GVR == gvr && e.Namespace == ns && e.Name == "*" {
			return true
		}
	}
	return false
}

func fc3Marshal(t *testing.T, m map[string]any) string {
	t.Helper()
	b, _ := json.Marshal(m)
	return string(b)
}

// fc3BodyReflects reports whether the resolved dict's marshaled form contains the
// item marker (content-level check — the GET'd object's name appears in the body).
func fc3BodyReflects(t *testing.T, body map[string]any, marker string) bool {
	t.Helper()
	return strings.Contains(fc3Marshal(t, body), marker)
}

// fc3CountKey counts occurrences of key in the captured-enqueue snapshot.
func fc3CountKey(snap []string, key string) int {
	n := 0
	for _, k := range snap {
		if k == key {
			n++
		}
	}
	return n
}
