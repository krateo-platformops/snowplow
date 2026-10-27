//go:build integration
// +build integration

// store_verify_recheck_872_kind_integration_test.go — #872 and #918 against a
// REAL apiserver (kind): update churn must not make the forced pass publish a
// divergence or queue a repair.
//
// WHY KIND. The artifacts are races between reads in flight and a store that
// keeps applying watch events, so they need a real watch and a real apiserver
// serving LIST from its watch cache while writes land.
//
//   - #872: high churn (3,000 objects, ~500 patches/s), fast reads. Before
//     #872 this arm measured 63-126 published lost updates per 5 passes, 100%
//     of them with the store AHEAD of the LIST and 0 really lost.
//   - #918: the bench run-16 shape. About 1,000 objects of a large GVR change
//     once every 20 s (50 patches/s), and the re-check reads at the metadata
//     client's default 5 reads/s, as the pod's in-cluster client does (#920).
//     The LIST is large enough that the churn moves well over 100 keys past
//     it, so the re-check sweep takes longer than an object's update interval
//     and most objects change after their read. Before #918 that published
//     lost updates and queued a full repair (run 16: 264 of 379, one repair).
//
// CLUSTER. One kind cluster for both arms, krateo-sptest-<pid> (#522, #742),
// created and destroyed inside the test, as issue624_rbac_kind_integration_test.go
// does. Each arm has its own CRD and its own watcher.
package cache

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
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
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/e2e-framework/support/kind"
)

const (
	kChurnGroup   = "composition.krateo.io"
	kChurnVersion = "v1-2-2"
	kChurnNS      = 10
)

func TestStoreVerification_Kind_ChurnPublishesNoDivergence(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cfg := kChurnCluster(t)
	t.Run("872_high_churn_fast_reads", func(t *testing.T) { k872Arm(t, cfg) })
	t.Run("918_run16_shape_slow_reads", func(t *testing.T) { k918Arm(t, cfg) })
}

// kChurnCluster creates this process's kind cluster and the namespaces both
// arms use, and returns a config whose clients are fast enough to seed and
// churn (the clients under test get their own rate).
func kChurnCluster(t *testing.T) *rest.Config {
	t.Helper()
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
	nsGVR := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	for i := 0; i < kChurnNS; i++ {
		if _, err := dyn.Resource(nsGVR).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": kChurnNSName(i)}}}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create namespace: %v", err)
		}
	}
	return cfg
}

func kChurnNSName(i int) string { return fmt.Sprintf("kchurn-%02d", i%kChurnNS) }

func kChurnObjName(i int) string { return fmt.Sprintf("c-%05d", i) }

// kChurnSeed creates a CRD named plural/kind and n objects of it; pad, when
// non-zero, is the size of one annotation every object carries, which is what
// makes the metadata LIST large.
func kChurnSeed(t *testing.T, cfg *rest.Config, plural, kindName string, n, pad int) schema.GroupVersionResource {
	t.Helper()
	ctx := context.Background()
	dyn := dynamic.NewForConfigOrDie(cfg)
	gvr := schema.GroupVersionResource{Group: kChurnGroup, Version: kChurnVersion, Resource: plural}
	ax := apiextclient.NewForConfigOrDie(cfg)
	if _, err := ax.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, &apiextv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: plural + "." + kChurnGroup},
		Spec: apiextv1.CustomResourceDefinitionSpec{
			Group: kChurnGroup, Scope: apiextv1.NamespaceScoped,
			Names: apiextv1.CustomResourceDefinitionNames{Kind: kindName, ListKind: kindName + "List", Plural: plural, Singular: strings.ToLower(kindName)},
			Versions: []apiextv1.CustomResourceDefinitionVersion{{Name: kChurnVersion, Served: true, Storage: true,
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
	meta := map[string]any{}
	if pad > 0 {
		meta["annotations"] = map[string]any{"pad": strings.Repeat("x", pad)}
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			m := map[string]any{"name": kChurnObjName(i)}
			for k, v := range meta {
				m[k] = v
			}
			if _, err := dyn.Resource(gvr).Namespace(kChurnNSName(i)).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": kChurnGroup + "/" + kChurnVersion, "kind": kindName, "metadata": m,
			}}, metav1.CreateOptions{}); err != nil {
				t.Errorf("create object %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	return gvr
}

// kChurnWatcher builds a watcher wired like main.go, with metaCfg as the
// metadata client's config, and waits for gvr to sync and be confirmed.
func kChurnWatcher(t *testing.T, cfg, metaCfg *rest.Config, gvr schema.GroupVersionResource) *ResourceWatcher {
	t.Helper()
	ctx := context.Background()
	ResetStoreVerificationStatsForTest()
	t.Cleanup(ResetStoreVerificationStatsForTest)
	rw, err := NewResourceWatcher(ctx, dynamic.NewForConfigOrDie(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rw.Stop)
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })
	rw.SetMetadataClient(metadata.NewForConfigOrDie(metaCfg))
	_, ch := rw.EnsureResourceType(gvr)
	select {
	case <-ch:
	case <-time.After(3 * time.Minute):
		t.Fatal("informer never synced")
	}
	rw.ConfirmResourceType(ctx, gvr)
	return rw
}

// kChurn merge-patches objects of gvr at rate per second, picking each with
// pick(tick), until the returned stop is called. It returns the patch count.
func kChurn(t *testing.T, cfg *rest.Config, gvr schema.GroupVersionResource, rate int, pick func(tick int) int) (patched *atomic.Int64, stop func()) {
	t.Helper()
	ctx := context.Background()
	dyn := dynamic.NewForConfigOrDie(cfg)
	patched = &atomic.Int64{}
	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(time.Second / time.Duration(rate))
		defer tick.Stop()
		for n := 0; ; n++ {
			select {
			case <-stopCh:
				return
			case <-tick.C:
			}
			i := pick(n)
			go func(i int) {
				p := fmt.Sprintf(`{"metadata":{"annotations":{"churn":"%d"}}}`, time.Now().UnixNano())
				if _, err := dyn.Resource(gvr).Namespace(kChurnNSName(i)).Patch(ctx, kChurnObjName(i),
					types.MergePatchType, []byte(p), metav1.PatchOptions{}); err == nil {
					patched.Add(1)
				}
			}(i)
		}
	}()
	var once sync.Once
	stop = func() { once.Do(func() { close(stopCh); <-done }) }
	t.Cleanup(stop)
	return patched, stop
}

// kChurnAssertNothingPublished is the verdict both arms share: churn alone
// published nothing, attempted no repair, and every pass completed.
func kChurnAssertNothingPublished(t *testing.T, issue string, passes int) {
	t.Helper()
	skipped := VerifySkippedByReasonSnapshot()
	lu, ld, la, um := divergenceCounts(verifySiteForced)
	if lu+ld+la+um != 0 {
		t.Errorf("%s RED: churn alone published %d forced divergences (update/delete/add/uid = %d/%d/%d/%d) on a store that lost nothing",
			issue, lu+ld+la+um, lu, ld, la, um)
	}
	// A repair ATTEMPT, whether queued or refused as unsupported: this GVR's
	// informer may not be rebuildable here, and counting only the queue would
	// make the assertion vacuous on exactly that path.
	if d, u := storeRepairQueueDepth(), storeRepairUnsupportedTotal.Load(); d+int64(u) != 0 {
		t.Errorf("%s RED: %d repair attempt(s) under churn alone (queued %d, refused-unsupported %d)", issue, d+int64(u), d, u)
	}
	if n := skipped[verifySkipRecheckError] + skipped[verifySkipNoSyncPosition]; n != 0 {
		t.Errorf("%d pass(es) did not complete against a healthy apiserver (recheck_error %d, no_sync_position %d)",
			n, skipped[verifySkipRecheckError], skipped[verifySkipNoSyncPosition])
	}
	// The passes really ran: a hang or a skip would leave this short.
	if got := storeVerifyForcedListsTotal.Load(); got != uint64(passes) {
		t.Errorf("%d forced LISTs for %d passes", got, passes)
	}
}

const (
	k872Objects = 3000
	k872Rate    = 500 // patches per second asked of the churner
	k872Passes  = 5
)

// k872Arm — high churn, fast reads (the client at 1000 QPS).
func k872Arm(t *testing.T, cfg *rest.Config) {
	ctx := context.Background()
	gvr := kChurnSeed(t, cfg, "churn872s", "Churn872", k872Objects, 0)
	rw := kChurnWatcher(t, cfg, cfg, gvr)
	rnd := rand.New(rand.NewSource(872))
	patched, stop := kChurn(t, cfg, gvr, k872Rate, func(int) int { return rnd.Intn(k872Objects) })
	time.Sleep(5 * time.Second) // steady churn before the first pass

	t0, p0 := time.Now(), patched.Load()
	for p := 0; p < k872Passes; p++ {
		rw.ConfirmResourceType(ctx, gvr) // the discovery-refresh pass that advances lastSyncRV
		forcedVerify(ctx, gvr)
	}
	rate := float64(patched.Load()-p0) / time.Since(t0).Seconds()
	stop()

	candidates := storeDivergenceCandidates.Load()
	gets := storeRecheckGETsTotal.Load()
	cleared := storeRecheckClearedTotal.Load()
	undelivered := storeRecheckUndeliveredTotal.Load()
	t.Logf("#872 kind: %d forced passes at %.0f patches/s over %d objects: candidates=%d, re-check GETs=%d, cleared=%d (delivered after the read %d, read state received %d), counted without a re-check=%d",
		k872Passes, rate, k872Objects, candidates, gets, cleared, storeRecheckDeliveredAfterTotal.Load(), storeRecheckReceivedReadStateTotal.Load(), undelivered)

	// Non-vacuity: the churn really produced LIST-vs-store disagreement, so a
	// zero below is the rule working, not a quiet cluster.
	if candidates == 0 {
		t.Fatalf("precondition: %d passes at %.0f patches/s produced no candidate — the arm tests nothing", k872Passes, rate)
	}
	kChurnAssertNothingPublished(t, "#872", k872Passes)
	// The re-check really ran and cleared what churn produced: a zero above
	// must be the rule working, not candidates silently dropped.
	if cleared == 0 {
		t.Errorf("the re-check cleared nothing although churn produced %d candidates — it did not run", candidates)
	}
	// #875 — with nothing lost, every candidate is one the store moved, so
	// every one was delivered during its pass; one counted without a re-check
	// would be a delivery the record missed.
	if undelivered != 0 {
		t.Errorf("#875 RED: %d candidate(s) counted without a re-check under churn alone — a delivery the record missed", undelivered)
	}
	// Bound (#918): one quorum GET per candidate at most, never the object count.
	if gets > candidates {
		t.Errorf("re-check issued %d GETs for %d candidates, above one per candidate", gets, candidates)
	}
}

const (
	// k918Objects × k918Pad is the metadata LIST's size (~120 MB): large
	// enough that the LIST is in flight for seconds, so 50 patches/s move
	// over 100 keys past it — the run-16 shape (379 at 50K), at a size a CI
	// kind cluster seeds in under a minute.
	k918Objects = 5000
	k918Pad     = 24 << 10
	// k918Churned objects change round-robin at k918Rate per second: each one
	// every k918Churned/k918Rate = 20 s, run 16's interval.
	k918Churned = 1000
	k918Rate    = 50
	// k918MaxPasses bounds the arm's runtime. It stops early once a pass's
	// re-check sweep has outlasted the update interval AND a delivery after a
	// read has been seen, i.e. once the run-16 mechanism has happened.
	k918MaxPasses = 3
)

// k918Arm — the run-16 shape: slow reads (the metadata client at client-go's
// default rate, 5/s), a sweep longer than the objects' update interval.
func k918Arm(t *testing.T, cfg *rest.Config) {
	ctx := context.Background()
	gvr := kChurnSeed(t, cfg, "churn918s", "Churn918", k918Objects, k918Pad)
	// The client under test reads at client-go's default rate (QPS 0 => 5/s,
	// burst 10), as the pod's in-cluster metadata client does (#920). That
	// rate is the condition, not the fix: the rule must hold at any rate.
	metaCfg := rest.CopyConfig(cfg)
	metaCfg.QPS, metaCfg.Burst = 0, 0
	rw := kChurnWatcher(t, cfg, metaCfg, gvr)
	patched, stop := kChurn(t, cfg, gvr, k918Rate, func(n int) int { return n % k918Churned })
	interval := time.Duration(k918Churned/k918Rate) * time.Second
	time.Sleep(interval) // every churned object has changed once before the first pass

	t0, p0 := time.Now(), patched.Load()
	passes, longest := 0, time.Duration(0)
	for passes < k918MaxPasses {
		rw.ConfirmResourceType(ctx, gvr)
		gets0 := storeRecheckGETsTotal.Load()
		start := time.Now()
		forcedVerify(ctx, gvr)
		passes++
		took := time.Since(start)
		t.Logf("#918 kind pass %d: %s, %d re-check GETs", passes, took.Round(time.Millisecond), storeRecheckGETsTotal.Load()-gets0)
		if took > longest {
			longest = took
		}
		if longest > interval && storeRecheckDeliveredAfterTotal.Load() > 0 {
			break
		}
	}
	rate := float64(patched.Load()-p0) / time.Since(t0).Seconds()
	stop()

	candidates := storeDivergenceCandidates.Load()
	gets := storeRecheckGETsTotal.Load()
	after := storeRecheckDeliveredAfterTotal.Load()
	t.Logf("#918 kind: %d forced passes at %.0f patches/s over %d of %d objects (each every %s); longest pass %s: candidates=%d, re-check GETs=%d, cleared=%d (delivered after the read %d, read state received %d), counted without a re-check=%d",
		passes, rate, k918Churned, k918Objects, interval, longest.Round(time.Millisecond), candidates, gets,
		storeRecheckClearedTotal.Load(), after, storeRecheckReceivedReadStateTotal.Load(), storeRecheckUndeliveredTotal.Load())

	// Non-vacuity: the run-16 mechanism happened. The sweep outlasted the
	// update interval, and objects changed — and were delivered — after their
	// read. Before #918 each such key was published as a lost update unless
	// it changed after its second read too, which a sweep this long makes
	// likely: this arm published and queued a repair.
	if longest <= interval {
		t.Fatalf("precondition: the longest pass took %s, not longer than the %s update interval — the run-16 shape was not reached (candidates=%d, GETs=%d)",
			longest.Round(time.Millisecond), interval, candidates, gets)
	}
	if after == 0 {
		t.Fatalf("precondition: no object changed after its read in %d passes — the arm tests nothing", passes)
	}
	kChurnAssertNothingPublished(t, "#918", passes)
	if gets > candidates {
		t.Errorf("re-check issued %d GETs for %d candidates, above one per candidate", gets, candidates)
	}
}
