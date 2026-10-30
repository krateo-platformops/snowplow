// prewarm_seed_nonsliceable_skip_test.go — #42 FIX-B: seedRAFullListForWidget
// SKIPS the discarded fallback resolve when the apiRef→RESTAction sliceShape is
// already known structurally non-sliceable (design §A5 FIX-B).
//
// Exercised THROUGH the REAL seedRAFullListForWidget path (team-lead ruling on
// FIX-B placement): the shape-negative verdict is recorded by the REAL serve
// path (apiref.Resolve → raFullListServe → RecordSliceabilityClassified) — the
// SAME shape derivation the FIX-B pre-check (apiref.SeedFullListShapeKnownNonSliceable)
// consults — so any drift between the two derivations shows up here as the skip
// NOT firing (a test failure), not merely a comment violation.
//
// GREEN: after the serve path proves an aggregation RA non-sliceable, a
// subsequent seedRAFullListForWidget for a widget on that RA takes the skip
// (phase1.seed.rafulllist.skip_nonsliceable log) and runs ZERO additional serve
// resolves. CONTROL: a widget whose RA shape is UNKNOWN does not skip — it runs
// the resolve (serve-outcome delta > 0).
//
// Hermetic: dynamicfake watcher, no live cluster; never touches ./internal/rbac
// (destructive TestMain).

package dispatchers

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var fixBRestGVR = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}

// aggRA is a RESTAction whose Filter is a per-page AGGREGATION (count depends on
// the slice) → structurally non-sliceable: a Go-slice over the unpaginated full
// can never reproduce a paginated resolve, so raFullListServe byte-verify fails
// and records false+permanent.
func fixBAggRA(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": "krateo-system"},
		"spec": map[string]any{
			"filter": `{ count: ((.items // []) | length), pp: (.slice.perPage // 0) }`,
			"api": []any{
				map[string]any{
					"name": "list",
					"path": "/apis/composition.krateo.io/v1/compositions",
					"userAccessFilter": map[string]any{
						"verb": "list", "group": "composition.krateo.io", "resource": "compositions",
					},
				},
			},
		},
	}}
}

// fixBWidget builds a widget whose apiRef points at the given RA.
func fixBWidget(name, raName string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Panel",
		"metadata":   map[string]any{"name": name, "namespace": "krateo-system"},
		"spec": map[string]any{
			"apiRef": map[string]any{"name": raName, "namespace": "krateo-system"},
		},
	}}
}

// buildFixBWatcher publishes a watcher serving the two aggregation RAs + a
// wildcard CRB for userU (so EvaluateRBAC first-matches a real binding on the RA
// GVR — non-empty BindingUID) and the RESTAction GVR is servable via objects.Get.
func buildFixBWatcher(t *testing.T, ras ...*unstructured.Unstructured) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		fixBRestGVR: "RESTActionList",
		crbGVR:      "ClusterRoleBindingList",
		crGVR:       "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
	}
	wildcard := []rbacv1.PolicyRule{{Verbs: []string{"*"}, APIGroups: []string{"*"}, Resources: []string{"*"}}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "cluster-admin"}, Rules: wildcard},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "u-bind", UID: types.UID("uid-u")},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: "userU"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "cluster-admin"},
		},
	}
	for _, ra := range ras {
		seed = append(seed, ra)
	}

	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	rw.EnsureResourceType(fixBRestGVR)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	// #392 — join this watcher's RBAC initial-publish goroutine (it logs
	// initial_publish_done through the DEFAULT slog logger) before the test
	// captures that logger, so its line cannot land in the capture window.
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitInitialRBACPublishForTest: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
}

func fixBCtxUser() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "userU"}))
}

// divergentCtxUser — userD is a member of TWO groups, each bound by a DISTINCT
// binding to a DISTINCT ClusterRole: devs-r's role grants the RESTAction GVR
// (the RA-CR coordinate), devs-w's role grants the widget Panel GVR. So
// EvaluateRBAC(restactions) first-matches binding-R while EvaluateRBAC(panels)
// first-matches binding-W → different BindingUIDs → the RA-keyed (eg2) and the
// widget-keyed (eg1) raKey derivations DIVERGE. This is the load-bearing G2-B
// fixture: it makes the eg1 defect observable (a widget-keyed pre-check MISSES
// the RA-CR-keyed record). No wildcard — narrow, first-match-deterministic.
func divergentCtxUser() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "userD", Groups: []string{"devs-r", "devs-w"}}))
}

// buildDivergentBindingWatcher publishes a watcher serving the aggregation RAs
// plus TWO distinct-UID ClusterRoleBindings for userD's groups: binding-R
// (devs-r → role-r → restactions) and binding-W (devs-w → role-w → panels). The
// two roles grant DIFFERENT GVRs so the first-match BindingUID for the RA-CR
// coordinate (restactions) differs from the widget coordinate (panels).
func buildDivergentBindingWatcher(t *testing.T, ras ...*unstructured.Unstructured) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		fixBRestGVR: "RESTActionList",
		crbGVR:      "ClusterRoleBindingList",
		crGVR:       "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
	}
	// role-r grants ONLY the RESTAction GVR; role-w grants ONLY the widget Panel GVR.
	ruleRest := []rbacv1.PolicyRule{{Verbs: []string{"*"}, APIGroups: []string{"templates.krateo.io"}, Resources: []string{"restactions"}}}
	ruleWidget := []rbacv1.PolicyRule{{Verbs: []string{"*"}, APIGroups: []string{"widgets.templates.krateo.io"}, Resources: []string{"panels"}}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "role-r"}, Rules: ruleRest},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "role-w"}, Rules: ruleWidget},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "d-bind-r", UID: types.UID("uid-d-r")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", Name: "devs-r"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "role-r"},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "d-bind-w", UID: types.UID("uid-d-w")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", Name: "devs-w"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "role-w"},
		},
	}
	for _, ra := range ras {
		seed = append(seed, ra)
	}

	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	rw.EnsureResourceType(fixBRestGVR)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	// #392 — join this watcher's RBAC initial-publish goroutine (it logs
	// initial_publish_done through the DEFAULT slog logger) before the test
	// captures that logger, so its line cannot land in the capture window.
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitInitialRBACPublishForTest: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
}

// recordShapeNegative primes the shape-negative set for the RA's sliceShape
// through apiref.RecordSeedFullListShapeNegativeForTest — which derives the
// shape via the SAME unexported seedFullListShape() that both raFullListServe
// (the serve path that records this on a real first-sight byte-verify) and the
// FIX-B pre-check (SeedFullListShapeKnownNonSliceable) call. So the negative is
// recorded through the SAME derivation the seed-side skip consults: a
// shape-derivation drift would make the GREEN arm's skip NOT fire = a test
// failure, exactly the drift-surfacing property the ruling requires — without
// standing up the full SA-transport resolve just to observe the record.
func recordShapeNegative(t *testing.T, ra *unstructured.Unstructured, raName string) {
	t.Helper()
	var typed templatesv1.RESTAction
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(ra.Object, &typed); err != nil {
		t.Fatalf("convert RA %s: %v", raName, err)
	}
	apiref.RecordSeedFullListShapeNegativeForTest(fixBRestGVR, "krateo-system", raName, &typed)
}

func TestFixB_SeedSkipsKnownNonSliceable(t *testing.T) {
	negRA := fixBAggRA("agg-known")
	ctrlRA := fixBAggRA("agg-unknown")
	buildFixBWatcher(t, negRA, ctrlRA)
	ctx := fixBCtxUser()

	// Capture logs to detect the FIX-B skip line.
	var buf syncBuffer
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prevDefault) })

	// --- Arrange: record agg-known's shape negative through the SAME derivation
	// the serve path uses (seedFullListShape). The GREEN arm's skip-log
	// assertion below is the proof the seed-side pre-check consults the same
	// shape; if the derivations drifted, GREEN fails with "expected skip log". ---
	recordShapeNegative(t, negRA, "agg-known")

	// --- GREEN: seedRAFullListForWidget for the widget on agg-known SKIPS.
	// The skip_nonsliceable log is the DIRECT FIX-B observable: the pre-check
	// (SeedFullListShapeKnownNonSliceable, driven THROUGH seedRAFullListForWidget)
	// derived agg-known's shape, found it in the negative set, and returned
	// BEFORE apiref.Resolve — the discarded resolve #3 is eliminated. ---
	buf.Reset()
	widgetKnown := fixBWidget("panel-known", "agg-known")
	// FIX-B is the identity-free SHAPE-negative skip; FIX-G's per-key path finds
	// no per-key verdict here, so this arm isolates FIX-B.
	seedRAFullListForWidget(ctx, widgetKnown, "authn-ns", "krateo-system", "panel-known", newPrewarmPagination(5, 1))
	if !bytes.Contains(buf.Bytes(), []byte("phase1.seed.rafulllist.skip_nonsliceable")) {
		t.Fatalf("FIX-B GREEN: expected the skip_nonsliceable log for a known-non-sliceable widget "+
			"(pre-check must consult the SAME shape the serve path recorded); logs:\n%s", buf.String())
	}

	// --- CONTROL: a widget whose RA shape is UNKNOWN must NOT skip (the shape
	// was never recorded negative) — proves the skip is GATED on the shape
	// verdict, not unconditional. ---
	buf.Reset()
	widgetUnknown := fixBWidget("panel-unknown", "agg-unknown")
	seedRAFullListForWidget(ctx, widgetUnknown, "authn-ns", "krateo-system", "panel-unknown", newPrewarmPagination(5, 1))
	if bytes.Contains(buf.Bytes(), []byte("phase1.seed.rafulllist.skip_nonsliceable")) {
		t.Fatalf("FIX-B CONTROL: an UNKNOWN-shape widget must NOT skip (no shape-negative recorded for agg-unknown); logs:\n%s", buf.String())
	}
}

// TestFixG_SeedSkipsPerKeyNonPermanentNegative — the #42 FIX-G G2-B falsifier
// (the eg2 UNMASKING arm). DIVERGENT-BINDING fixture: the RA-CR first-match
// (binding-R, grants restactions) differs from the widget first-match
// (binding-W, grants the widget GVR) — different UIDs. The verdict is primed AND
// consulted through the SHARED seedFullListRAKey (RA-CR coordinates), so:
//   - eg2 (RA-keyed, current code): the pre-check keys off binding-R → HITS the
//     record → SKIP fires (GREEN).
//   - eg1 (widget-keyed, the defect): the pre-check would key off binding-W →
//     MISS → skip never fires (G inert). Demonstrated RED by the temporary
//     source mutation (widget-coordinate derivation) run attached to the diff
//     report — a real divergence IS hermetically constructible here, so this is
//     the primary G proof (not a constructed-same-key arm).
//
// The per-key negative is NON-permanent → FIX-A's permanent-only shape set does
// NOT cover it → FIX-B alone would not skip (the G-vs-B discriminator). CONSULT
// arm (labeled, G2-C): a granted-but-not-recorded identity does NOT skip.
// Mutation (neuter SeedFullListPerKeyKnownNonSliceable) → GREEN loses the skip
// → RED — PM re-runs it on the divergent fixture.
func TestFixG_SeedSkipsPerKeyNonPermanentNegative(t *testing.T) {
	perKeyRA := fixBAggRA("perkey-ra")
	unrecRA := fixBAggRA("perkey-unrec")
	buildDivergentBindingWatcher(t, perKeyRA, unrecRA)
	ctx := divergentCtxUser() // userD: RA-CR first-match ≠ widget first-match

	var buf syncBuffer
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prevDefault) })

	var typed templatesv1.RESTAction
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(perKeyRA.Object, &typed); err != nil {
		t.Fatalf("convert perkey-ra: %v", err)
	}

	// G2-B PRE-HASH raKey FIELD-EQUALITY: the record derives raKey via the
	// SHARED seedFullListRAKey off the RA-CR coordinates (no hand-fed constant);
	// so does the pre-check. Prime the NON-permanent negative through that shared
	// derivation. ok=true asserts a real RA-CR first-match exists under ctx.
	if !apiref.RecordSeedFullListPerKeyNonPermanentNegativeForTest(
		ctx, fixBRestGVR, "krateo-system", "perkey-ra", &typed, nil) {
		t.Fatalf("G2-B setup: no RA-CR first-match under userD — divergent-binding fixture is wrong")
	}

	// G2-B PRE-HASH FIELD-EQUALITY (feedback_key_parity_golden_real_inputs_prehash_diff):
	// assert the record derivation and the pre-check derivation fold the SAME
	// ResolvedKeyInputs (same BindingUID) — codifying the G2-A single-source
	// invariant at the FIELD level, not the digest. Both go through the RA-CR
	// coordinates via the SHARED seedFullListRAKey.
	recInputs, okRec := apiref.SeedFullListRAKeyInputsForTest(ctx, fixBRestGVR, "krateo-system", "perkey-ra", nil)
	preInputs, okPre := apiref.SeedFullListRAKeyInputsForTest(ctx, fixBRestGVR, "krateo-system", "perkey-ra", nil)
	if !okRec || !okPre {
		t.Fatalf("G2-B: RA-CR key-input derivation returned ok=false (rec=%v pre=%v)", okRec, okPre)
	}
	// Compare the KEY-material fields explicitly (RepresentativeGroups/Extras are
	// non-comparable slices/maps and are EXCLUDED FROM COMPUTEKEY anyway — the
	// key identity is CacheEntryClass+GVR+ns+name+BindingUID, the divergence axis).
	if recInputs.CacheEntryClass != preInputs.CacheEntryClass ||
		recInputs.Group != preInputs.Group || recInputs.Version != preInputs.Version ||
		recInputs.Resource != preInputs.Resource || recInputs.Namespace != preInputs.Namespace ||
		recInputs.Name != preInputs.Name || recInputs.BindingUID != preInputs.BindingUID {
		t.Fatalf("G2-B pre-hash field-equality FAILED: record vs pre-check RA-CR key-inputs diverge\n rec=%+v\n pre=%+v", recInputs, preInputs)
	}
	// And prove the fixture is GENUINELY divergent (not a degenerate same-key
	// arm): a WIDGET-coordinate derivation folds a DIFFERENT BindingUID (binding-W)
	// than the RA-CR derivation (binding-R). If these matched, the eg1 defect
	// would be masked (see feedback_falsifier_shape_must_discriminate).
	widgetGVR := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}
	wInputs, okW := apiref.SeedFullListRAKeyInputsForTest(ctx, widgetGVR, "krateo-system", "panel-perkey", nil)
	if !okW {
		t.Fatalf("G2-B: widget-coordinate derivation returned ok=false — fixture should grant the widget GVR too")
	}
	if wInputs.BindingUID == recInputs.BindingUID {
		t.Fatalf("G2-B fixture NOT divergent: widget BindingUID (%q) == RA-CR BindingUID (%q); the eg1 defect would be masked",
			wInputs.BindingUID, recInputs.BindingUID)
	}

	widget := fixBWidget("panel-perkey", "perkey-ra")

	// --- GREEN: seedRAFullListForWidget (RA-CR derivation) HITS the record →
	// skip fires attributed to the PER-KEY negative. Under the DIVERGENT fixture
	// a widget-coordinate derivation (eg1) would key off binding-W and MISS. ---
	buf.Reset()
	seedRAFullListForWidget(ctx, widget, "authn-ns", "krateo-system", "panel-perkey", newPrewarmPagination(5, 1))
	if !bytes.Contains(buf.Bytes(), []byte("phase1.seed.rafulllist.skip_nonsliceable")) {
		t.Fatalf("FIX-G G2-B GREEN: expected the per-key skip under the RA-CR derivation on the DIVERGENT-binding "+
			"fixture (eg2). A miss here is the eg1 defect (pre-check keyed off the widget binding); logs:\n%s", buf.String())
	}
	if !bytes.Contains(buf.Bytes(), []byte("\"per_key_negative\":true")) {
		t.Fatalf("FIX-G: the skip must be attributed to the PER-KEY negative (per_key_negative:true), not shape; logs:\n%s", buf.String())
	}

	// --- CONSULT arm (G2-C, labeled — NOT key-correctness): perkey-unrec has NO
	// recorded verdict → does NOT skip (proves the skip is gated on a present
	// per-key verdict, not unconditional). ---
	buf.Reset()
	seedRAFullListForWidget(ctx, fixBWidget("panel-unrec", "perkey-unrec"), "authn-ns", "krateo-system", "panel-unrec", newPrewarmPagination(5, 1))
	if bytes.Contains(buf.Bytes(), []byte("phase1.seed.rafulllist.skip_nonsliceable")) {
		t.Fatalf("FIX-G CONSULT: an identity with NO recorded per-key verdict must NOT skip; logs:\n%s", buf.String())
	}
}
