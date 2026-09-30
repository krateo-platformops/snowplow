// issue263_forced_verify_test.go — #263: reflector_path_by_gvr mislabels the
// store-verification forced LIST (store_verify_deadline.go forcedVerify) as a
// reflector establishment, flipping the RBAC GVRs to 'list' every hour and
// WARNing falsely. The forced LIST carries resourceVersion=lastSyncRV with
// ResourceVersionMatch=NotOlderThan, so on the wire it is byte-identical to a
// classic reflector re-list — yet it is NOT an establishment. The fix tags the
// verifier's own request context (positive self-identification) and excludes
// tagged requests from per-GVR path attribution.
//
// FALSIFIER SHAPE — a SPLIT the freshness-audit ruled faithful (not a
// seam-floor dodge), because the two arms meet at ONE production-identical
// boundary: the call rw.metaClient.Resource(gvr).Namespace(All).List(taggedCtx,
// opts) that forcedVerify makes (store_verify_deadline.go:270).
//
//   T2  (TestIssue263_ForcedVerifyTagsItsListCtx) drives the REAL forcedVerify
//       and proves it puts the tag ON that boundary ctx — asserted through the
//       EXACT predicate the classifier reads, isForcedVerifyRequest.
//
//   T1  (TestIssue263_TaggedForcedListIsExcludedButUntaggedIsNot) drives a REAL
//       metadata client built with the production ReflectorPathWrapper over an
//       httptest stub — the SAME wrapper stack as production rw.metaClient — and
//       proves that a ctx tagged at that boundary PROPAGATES through client-go
//       to req.Context() at reflector_path.go's RoundTrip and is skipped, while
//       an UNTAGGED request of the byte-identical wire shape is still attributed.
//
// Composition: forcedVerify tags the boundary ctx (T2) + a boundary ctx that is
// tagged propagates and is excluded, while untagged-same-shape is recorded (T1)
// ⟹ forcedVerify's LIST is excluded in production AND a genuine (untagged)
// reflector re-list on the same GVR is still detected. The propagation of the
// ctx VALUE to req.Context() — the deep frame — is driven for real in T1 (no
// seam below metaClient.List), so it is falsified, not assumed.

package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/metadata"
	metadatafake "k8s.io/client-go/metadata/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
)

// forcedVerifyListShape is the exact wire shape store_verify_deadline.go's
// forcedVerify sends: a watch-cache-served LIST at our own sync position. Both
// T1 arms issue THIS, so the only difference between them is the ctx tag.
func forcedVerifyListShape(rv string) metav1.ListOptions {
	return metav1.ListOptions{
		ResourceVersion:      rv,
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
		Limit:                0,
	}
}

// TestIssue263_TaggedForcedListIsExcludedButUntaggedIsNot — T1.
//
// The roundtripper + propagation arm. It does NOT call forcedVerify (its
// servable/lastSyncRV preconditions belong to T2); it issues the LIST directly
// so the ONLY variable under test is the production tag on the request ctx.
func TestIssue263_TaggedForcedListIsExcludedButUntaggedIsNot(t *testing.T) {
	// Production runs the RBAC GVRs on the watch-list regime; the package
	// TestMain forces WatchListClient=false, so opt in. SetFeatureDuringTest
	// refuses a concurrent override → no t.Parallel.
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)

	t.Setenv("CACHE_ENABLED", "true")
	ResetReflectorPathStatsForTest()
	t.Cleanup(ResetReflectorPathStatsForTest)

	// --- the harness: the REAL reflectorPathRoundTripper (rc.WrapTransport,
	// exactly main.go's wiring) over an httptest stub, and a REAL metadata client
	// built from the SAME rc — condition #1: its ctx→req propagation IS the one
	// production's rw.metaClient uses, not a bespoke client. ---
	stub := &apiserverStub{failedWatch: map[string]bool{}, watchList: true}
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)

	rc := &rest.Config{Host: srv.URL}
	rc.WrapTransport = transport.Wrappers(rc.WrapTransport, ReflectorPathWrapper())
	dyn, err := dynamic.NewForConfig(rc)
	if err != nil {
		t.Fatalf("dynamic.NewForConfig: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rw, err := NewResourceWatcher(ctx, dyn)
	if err != nil {
		cancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop(); cancel() })
	metaCli, err := metadata.NewForConfig(rc)
	if err != nil {
		t.Fatalf("metadata.NewForConfig: %v", err)
	}

	gvr := reflectorPathGVR

	// Baseline: the informer establishes this GVR via WATCH-LIST — the
	// production path for the RBAC GVRs, and the path #263's false flip moves
	// OFF of.
	_, syncCh := rw.EnsureResourceType(gvr)
	select {
	case <-syncCh:
	case <-time.After(20 * time.Second):
		t.Fatalf("informer never synced\n%s", recordedDump(stub))
	}
	reflectorPathWaitFor(t, "watch-list establishment", 10*time.Second, func() bool {
		return ReflectorPathsSnapshot()[gvr.String()] == reflectorPathWatchList
	})

	// --- ARM A (condition #2): a TAGGED request of the forced-verify shape must
	// record NO transition. The tag is set via the PRODUCTION withForcedVerifyTag,
	// and the skip asserted is the real one — the path staying watch-list means
	// recordReflectorPath was genuinely not invoked, no WARN emitted. ---
	const rv = "4242"
	_, _ = metaCli.Resource(gvr).List(withForcedVerifyTag(ctx), forcedVerifyListShape(rv))

	// Vacuity: the tagged LIST DID reach the wire through the roundtripper (its
	// resourceVersionMatch=NotOlderThan shape) — so a non-flip is the TAG being
	// read at RoundTrip, not the request never happening.
	reflectorPathWaitFor(t, "the tagged forced-shape LIST reaches the wire", 10*time.Second, func() bool {
		for _, line := range stub.recorded() {
			if strings.Contains(line, "resourceVersionMatch=NotOlderThan") {
				return true
			}
		}
		return false
	})
	// No settle: classifyReflectorRequest runs synchronously inside RoundTrip and
	// the tagged LIST has already returned (its wire arrival is asserted above), so
	// a would-be flip is already recorded — its absence here is the skip, not a race.
	if p := ReflectorPathsSnapshot()[gvr.String()]; p != reflectorPathWatchList {
		t.Fatalf("RED (#263): after a TAGGED forced-verification LIST the reflector path flipped "+
			"%q→%q — the verifier's ctx tag did not propagate to req.Context() at RoundTrip, or the "+
			"classifier did not skip it, so the RV=lastSyncRV LIST was counted as a reflector "+
			"establishment. That is the false hourly 'list' WARN on the RBAC GVRs.",
			reflectorPathWatchList, p)
	}

	// --- ARM B (conditions #2, #3): an UNTAGGED request of the BYTE-IDENTICAL
	// wire shape MUST still be attributed, flipping the path to 'list'. Same
	// opts, same GVR — the TAG is the sole difference — so a flip here proves the
	// exclusion is scoped to the verifier's tag, not to the GVR or the shape: a
	// genuine (untagged) reflector re-list stays visible and the detector is not
	// blinded. ---
	_, _ = metaCli.Resource(gvr).List(ctx, forcedVerifyListShape(rv))
	reflectorPathWaitFor(t, "an UNTAGGED same-shape LIST is still attributed as 'list'", 10*time.Second,
		func() bool { return ReflectorPathsSnapshot()[gvr.String()] == reflectorPathList })
}

// --- T2: forcedVerify tags its own List ctx --------------------------------

// ctxRecordingMetadataClient captures the CONTEXT of every metadata LIST, so an
// arm can assert forcedVerify tagged the ctx it hands to metaClient.List. It is
// distinct from store_verify_repair_falsifier_test.go's recordingMetadataClient
// (which captures ListOptions) — #263 is about the ctx, not the wire options.
type ctxRecordingMetadataClient struct {
	inner metadata.Interface
	mu    sync.Mutex
	ctxs  []context.Context
}

func (c *ctxRecordingMetadataClient) Resource(gvr schema.GroupVersionResource) metadata.Getter {
	return &ctxRecordingGetter{Getter: c.inner.Resource(gvr), c: c}
}

func (c *ctxRecordingMetadataClient) record(ctx context.Context) {
	c.mu.Lock()
	c.ctxs = append(c.ctxs, ctx)
	c.mu.Unlock()
}

func (c *ctxRecordingMetadataClient) recordedCtxs() []context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]context.Context(nil), c.ctxs...)
}

type ctxRecordingGetter struct {
	metadata.Getter
	c *ctxRecordingMetadataClient
}

func (g *ctxRecordingGetter) Namespace(ns string) metadata.ResourceInterface {
	return &ctxRecordingResource{ResourceInterface: g.Getter.Namespace(ns), c: g.c}
}

func (g *ctxRecordingGetter) List(ctx context.Context, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	g.c.record(ctx)
	return g.Getter.List(ctx, opts)
}

type ctxRecordingResource struct {
	metadata.ResourceInterface
	c *ctxRecordingMetadataClient
}

func (r *ctxRecordingResource) List(ctx context.Context, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	r.c.record(ctx)
	return r.ResourceInterface.List(ctx, opts)
}

// TestIssue263_ForcedVerifyTagsItsListCtx — T2.
//
// Drives the REAL forcedVerify over the proven silent-watch fixture (its
// servable + lastSyncRV preconditions hold there) and proves it puts the tag on
// the ctx it hands to metaClient.List — the boundary T1's tagged arm exercises.
// The assertion is made through isForcedVerifyRequest, the EXACT predicate the
// reflector-path classifier reads (condition #4), not "some ctx value is set".
func TestIssue263_ForcedVerifyTagsItsListCtx(t *testing.T) {
	c3Setup(t)
	resetVerificationForArm(t)

	rw, _ := silentWatchCluster(t, panelObj("demo", "panel-a", "1", "uid-a"))
	rec := &ctxRecordingMetadataClient{inner: newForcedVerifyMetaInner(t)}
	rw.SetMetadataClient(rec)

	before := storeVerifyForcedListsTotal.Load()
	forcedVerify(context.Background(), repairGVR)

	// VACUITY GUARD: a forced LIST actually issued, or the ctx assertion is
	// vacuous (a precondition skip records nothing).
	if storeVerifyForcedListsTotal.Load() == before {
		t.Fatalf("VACUITY: forcedVerify issued no forced LIST (a precondition skip) — nothing about "+
			"the List ctx was exercised")
	}
	ctxs := rec.recordedCtxs()
	if len(ctxs) != 1 {
		t.Fatalf("recorded %d forced-LIST contexts, want exactly 1", len(ctxs))
	}

	// Applied through the production predicate the classifier reads: build the
	// request client-go would, carrying forcedVerify's own List ctx, and ask the
	// exact question reflector_path.go:333 asks.
	req := (&http.Request{}).WithContext(ctxs[0])
	if !isForcedVerifyRequest(req) {
		t.Fatalf("RED (#263): forcedVerify's List ctx does NOT carry the forced-verify tag that "+
			"reflector_path's classifier reads (isForcedVerifyRequest). The roundtripper therefore "+
			"cannot distinguish the verifier's RV=lastSyncRV LIST from a genuine reflector re-list, "+
			"and mis-counts it as an establishment — the false hourly 'list' flip.")
	}
}

// newForcedVerifyMetaInner builds the fake metadata client attachMetaClient
// would, holding the apiserver's view of panel-a — enough for forcedVerify's
// LIST to complete so the ctx is recorded.
func newForcedVerifyMetaInner(t *testing.T) metadata.Interface {
	t.Helper()
	sch := metadatafake.NewTestScheme()
	if err := metav1.AddMetaToScheme(sch); err != nil {
		t.Fatalf("AddMetaToScheme: %v", err)
	}
	objs := []k8sruntime.Object{partialMeta("demo", "panel-a", "1", "uid-a")}
	return metadatafake.NewSimpleMetadataClient(sch, objs...)
}
