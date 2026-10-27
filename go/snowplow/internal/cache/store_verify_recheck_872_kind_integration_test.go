//go:build integration
// +build integration

// store_verify_recheck_872_kind_integration_test.go — #872 against a REAL
// apiserver (kind): high update churn must not make the forced pass publish a
// divergence or queue a repair.
//
// WHY KIND. The artifact is a race between a LIST in flight and a store that
// keeps applying watch events, so it needs a real watch and a real apiserver
// serving LIST from its watch cache while writes land. Before #872 this arm
// measured (3 runs, 3,000 objects, ~420 patches/s): 63-126 published lost
// updates per 5 passes, 100% of them with the store AHEAD of the LIST and 0
// really lost once churn stopped.
//
// CLUSTER. Its own kind cluster, krateo-sptest-<pid> (#522, #742), created and
// destroyed inside the test, as issue624_rbac_kind_integration_test.go does.
package cache

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/testsupport/kindguard"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/e2e-framework/support/kind"
)

const (
	k872Group   = "composition.krateo.io"
	k872Version = "v1-2-2"
	k872Plural  = "churn872s"
	k872Objects = 3000
	k872NS      = 10
	k872Rate    = 500 // patches per second asked of the churner
	k872Passes  = 5
)

func TestStoreVerification872_Kind_HighChurnPublishesNoDivergence(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	ctx := context.Background()
	clusterName := kindguard.ClusterName() // krateo-sptest-<pid>: per-process (#522), marked as ours (#742)
	kindguard.Prepare(clusterName)
	provider := kind.NewProvider().WithName(clusterName)
	cctx, ccancel := context.WithTimeout(ctx, 6*time.Minute)
	defer ccancel()
	kubeconfig, err := provider.Create(cctx)
	if err != nil {
		t.Fatalf("create kind cluster %s: %v", clusterName, err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer dcancel()
		if err := provider.Destroy(dctx); err != nil {
			t.Logf("destroy kind cluster %s: %v", clusterName, err)
		}
		kindguard.Release()
	})
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg.QPS, cfg.Burst = 1000, 2000
	dyn := dynamic.NewForConfigOrDie(cfg)
	gvr := schema.GroupVersionResource{Group: k872Group, Version: k872Version, Resource: k872Plural}

	// One CRD, 3,000 objects over 10 namespaces.
	ax := apiextclient.NewForConfigOrDie(cfg)
	if _, err := ax.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, &apiextv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: k872Plural + "." + k872Group},
		Spec: apiextv1.CustomResourceDefinitionSpec{
			Group: k872Group, Scope: apiextv1.NamespaceScoped,
			Names: apiextv1.CustomResourceDefinitionNames{Kind: "Churn872", ListKind: "Churn872List", Plural: k872Plural, Singular: "churn872"},
			Versions: []apiextv1.CustomResourceDefinitionVersion{{Name: k872Version, Served: true, Storage: true,
				Schema: &apiextv1.CustomResourceValidation{OpenAPIV3Schema: &apiextv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptr.To(true)}}}},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create CRD: %v", err)
	}
	for end := time.Now().Add(60 * time.Second); ; time.Sleep(500 * time.Millisecond) {
		if _, err := dyn.Resource(gvr).List(ctx, metav1.ListOptions{Limit: 1}); err == nil {
			break
		} else if time.Now().After(end) {
			t.Fatalf("CRD never served: %v", err)
		}
	}
	nsName := func(i int) string { return fmt.Sprintf("k872-%02d", i%k872NS) }
	nsGVR := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	for i := 0; i < k872NS; i++ {
		if _, err := dyn.Resource(nsGVR).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": nsName(i)}}}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create namespace: %v", err)
		}
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for i := 0; i < k872Objects; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := dyn.Resource(gvr).Namespace(nsName(i)).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": k872Group + "/" + k872Version, "kind": "Churn872",
				"metadata": map[string]any{"name": fmt.Sprintf("c-%05d", i)},
			}}, metav1.CreateOptions{}); err != nil {
				t.Errorf("create object %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	// The watcher, wired like main.go (metadata client set), synced and confirmed.
	ResetStoreVerificationStatsForTest()
	t.Cleanup(ResetStoreVerificationStatsForTest)
	rw, err := NewResourceWatcher(ctx, dyn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rw.Stop)
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })
	rw.SetMetadataClient(metadata.NewForConfigOrDie(cfg))
	_, ch := rw.EnsureResourceType(gvr)
	select {
	case <-ch:
	case <-time.After(90 * time.Second):
		t.Fatal("informer never synced")
	}
	rw.ConfirmResourceType(ctx, gvr)

	// Churn: merge-patch random objects at k872Rate per second.
	var patched atomic.Int64
	stop := make(chan struct{})
	churnDone := make(chan struct{})
	go func() {
		defer close(churnDone)
		rnd := rand.New(rand.NewSource(872))
		tick := time.NewTicker(time.Second / k872Rate)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			i := rnd.Intn(k872Objects)
			go func(i int) {
				p := fmt.Sprintf(`{"metadata":{"annotations":{"churn":"%d"}}}`, time.Now().UnixNano())
				if _, err := dyn.Resource(gvr).Namespace(nsName(i)).Patch(ctx, fmt.Sprintf("c-%05d", i),
					types.MergePatchType, []byte(p), metav1.PatchOptions{}); err == nil {
					patched.Add(1)
				}
			}(i)
		}
	}()
	time.Sleep(5 * time.Second) // steady churn before the first pass

	t0, p0 := time.Now(), patched.Load()
	for p := 0; p < k872Passes; p++ {
		rw.ConfirmResourceType(ctx, gvr) // the discovery-refresh pass that advances lastSyncRV
		forcedVerify(ctx, gvr)
	}
	rate := float64(patched.Load()-p0) / time.Since(t0).Seconds()
	close(stop)
	<-churnDone

	candidates := storeDivergenceCandidates.Load()
	gets := storeRecheckGETsTotal.Load()
	cleared := storeRecheckClearedTotal.Load()
	undelivered := storeRecheckUndeliveredTotal.Load()
	skipped := VerifySkippedByReasonSnapshot()
	lu, ld, la, um := divergenceCounts(verifySiteForced)
	t.Logf("#872 kind: %d forced passes at %.0f patches/s over %d objects: candidates=%d, re-check GETs=%d, cleared=%d, counted without a re-check=%d, published update/delete/add/uid=%d/%d/%d/%d, repairs pending=%d, refused-unsupported=%d, skipped{recheck_error}=%d skipped{no_sync_position}=%d",
		k872Passes, rate, k872Objects, candidates, gets, cleared, undelivered, lu, ld, la, um, storeRepairsRequestedForTest(), storeRepairUnsupportedTotal.Load(),
		skipped[verifySkipRecheckError], skipped[verifySkipNoSyncPosition])

	// Non-vacuity: the churn really produced LIST-vs-store disagreement, so a
	// zero below is the re-check working, not a quiet cluster.
	if candidates == 0 {
		t.Fatalf("precondition: %d passes at %.0f patches/s produced no candidate — the arm tests nothing", k872Passes, rate)
	}
	if lu+ld+la+um != 0 {
		t.Errorf("#872 RED: high update churn published %d forced divergences on a store that lost nothing", lu+ld+la+um)
	}
	// A repair ATTEMPT, whether queued or refused as unsupported: this GVR's
	// informer may not be rebuildable here, and counting only the queue would
	// make the assertion vacuous on exactly that path.
	if d, u := storeRepairsRequestedForTest(), storeRepairUnsupportedTotal.Load(); d+int64(u) != 0 {
		t.Errorf("#872 RED: %d repair attempt(s) under churn alone (queued %d, refused-unsupported %d)", d+int64(u), d, u)
	}
	if n := skipped[verifySkipRecheckError] + skipped[verifySkipNoSyncPosition]; n != 0 {
		t.Errorf("%d pass(es) did not complete against a healthy apiserver (recheck_error %d, no_sync_position %d)",
			n, skipped[verifySkipRecheckError], skipped[verifySkipNoSyncPosition])
	}
	// The passes really ran: a hang or a skip would leave this short.
	if got := storeVerifyForcedListsTotal.Load(); got != k872Passes {
		t.Errorf("%d forced LISTs for %d passes", got, k872Passes)
	}
	// The re-check really ran and cleared what churn produced: a zero above
	// must be the re-check working, not candidates silently dropped.
	if cleared == 0 {
		t.Errorf("the re-check cleared nothing although churn produced %d candidates — it did not run", candidates)
	}
	// #875 — with nothing lost, every candidate is one the store moved, so
	// every one was delivered during its pass; one counted without a re-check
	// would be a delivery the record missed.
	if undelivered != 0 {
		t.Errorf("#875 RED: %d candidate(s) counted without a re-check under churn alone — a delivery the record missed", undelivered)
	}
	// Bound: at most two quorum GETs per candidate, never the object count.
	if gets > 2*candidates {
		t.Errorf("re-check issued %d GETs for %d candidates, above the 2-per-candidate bound", gets, candidates)
	}
}
