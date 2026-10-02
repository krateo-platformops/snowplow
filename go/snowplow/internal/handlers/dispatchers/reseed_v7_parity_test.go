package dispatchers

// reseed_v7_parity_test.go — #258 on the v7 key (#423/#424).
//
//   - TestS258_V7_ReseededKeyEqualsRotatedCustomerKey: after a REAL rotation (a
//     binding ADD for carol through the informer → flush path), the reseed mints
//     carol's cell through seedTerminalPut, which now carries #424's
//     identity-class-drift guard. The guard must NOT decline it (the new key is
//     minted after the flush, so the class matches), and the minted key must be
//     byte-identical to the key carol's own /call derives (same
//     SubjectBindingSet digest, same post-rotation RBACSubGen) — and that key
//     must HIT. RED if the reseed minted under any other identity dimension
//     (key mismatch / miss) or if the guard declined it (drift counter moved).
//   - TestS258_V7_ReMintIsUnderTheClassDriftGuard: ReplaceIfGenReMint's only
//     caller is seedTerminalPut, and the #424 guard runs before every write
//     there, so a re-mint whose identity class drifted is declined with no
//     write. RED if the guard is moved below the re-mint branch.

import (
	"context"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func TestS258_V7_ReseededKeyEqualsRotatedCustomerKey(t *testing.T) {
	registerS258WideningHook(t)
	dyn, rw := s258BuildWatcher(t)
	stubWidgetResolve(t)
	s258ResetSink()

	navHarv := newNavWidgetHarvester()
	e := reseedWidgetEntry()
	navHarv.harvestNavWidget(e.W, e.GVR, e.PerPage, e.Page, e.KeyPerPage, e.KeyPage)
	deps := rePrewarmDeps{harvester: newContentPrewarmHarvester(), navHarv: navHarv, authnNS: h1NS}

	// The rotation: a real binding ADD for carol.
	crb := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "s258-v7-carol", UID: types.UID("uid-s258-v7-carol")},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: s258WideningUser}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "s258-reader"},
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(crb)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(s258CRBGVR).Create(context.Background(), &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var rotated cache.RotatedSubjectSet
	deadline := time.Now().Add(10 * time.Second)
	for {
		if rs, ok := s258RotatedFor(s258WideningUser); ok {
			rotated = rs
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the binding ADD never produced a flush naming carol")
		}
		cache.RebuildRBACSnapshotForTest(rw)
		time.Sleep(20 * time.Millisecond)
	}

	reqs := enumerateRotatedResidentTargets(context.Background(), deps, rotated)
	if s258TargetsFor(reqs, s258WideningUser) != 1 {
		t.Fatalf("precondition: one reseed target for carol, got reqs=%d", len(reqs))
	}

	driftBefore := identityClassDriftDeclinedForTest("seed", "binding_set") +
		identityClassDriftDeclinedForTest("seed", "rbac_subgen")
	if re := reseedTargets(context.Background(), deps, reqs); len(re) != 0 {
		t.Fatalf("reseed left %d to re-enqueue", len(re))
	}
	if d := identityClassDriftDeclinedForTest("seed", "binding_set") +
		identityClassDriftDeclinedForTest("seed", "rbac_subgen") - driftBefore; d != 0 {
		t.Fatalf("#424 guard DECLINED %d reseed write(s) after the rotation — the reseed must mint the "+
			"post-flush class, which the guard re-derives identically", d)
	}

	// carol's OWN /call key (customer identity on ctx, exactly as the dispatcher
	// handler sees it) vs the key the seed minted under the cohort identity.
	custCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: s258WideningUser}))
	custKey, handle, custIn := reseedWidgetKey(t, custCtx, e)
	seedKey, _, seedIn := reseedWidgetKey(t, s258IdentityCtx(s258WideningUser), e)
	if custIn.SubjectBindingSet == "" {
		t.Fatal("precondition: v7 key inputs carry a non-empty SubjectBindingSet")
	}
	if custIn.RBACSubGen == 0 {
		t.Fatal("precondition: carol's sub-gen must have moved (the rotation), got 0")
	}
	if seedIn.SubjectBindingSet != custIn.SubjectBindingSet || seedIn.RBACSubGen != custIn.RBACSubGen ||
		seedIn.BindingUID != custIn.BindingUID {
		t.Fatalf("key dimensions diverge: seed{bs=%s sg=%d uid=%s} customer{bs=%s sg=%d uid=%s}",
			seedIn.SubjectBindingSet, seedIn.RBACSubGen, seedIn.BindingUID,
			custIn.SubjectBindingSet, custIn.RBACSubGen, custIn.BindingUID)
	}
	if seedKey != custKey {
		t.Fatalf("KEY PARITY RED: reseed minted %q but carol's /call derives %q", seedKey, custKey)
	}
	if _, ok := handle.Get(custKey); !ok {
		t.Fatalf("carol's first navigation after the rotation must HIT the reseeded cell %q", custKey)
	}
}

func TestS258_V7_ReMintIsUnderTheClassDriftGuard(t *testing.T) {
	s258BuildWatcher(t)
	h := &s394RecordingHandle{gen: 7}
	g := seedTerminalGuardFor(seedModeReMint, h, "k")
	if !g.reMint {
		t.Fatal("precondition: remint guard")
	}
	ctx := s258IdentityCtx(a1Alice)
	// A key minted for a DIFFERENT class than alice's current one.
	drifted := &cache.ResolvedEntry{Inputs: &cache.ResolvedKeyInputs{
		CacheEntryClass:   "widgets",
		SubjectBindingSet: "not-alices-binding-set",
	}}
	before := identityClassDriftDeclinedForTest("seed", "binding_set")
	if seedTerminalPut(ctx, h, "k", drifted, g) {
		t.Fatal("a re-mint whose identity class drifted must be DECLINED")
	}
	if h.reMints != 0 || h.putIfGens != 0 || h.puts != 0 {
		t.Fatalf("a declined re-mint must write nothing; reMints=%d putIfGens=%d puts=%d", h.reMints, h.putIfGens, h.puts)
	}
	if got := identityClassDriftDeclinedForTest("seed", "binding_set") - before; got != 1 {
		t.Fatalf("drift counter moved by %d, want 1", got)
	}
}
