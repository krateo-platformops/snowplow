//go:build integration
// +build integration

// store_verify_judge_kind_integration_test.go — #872 and #918 against a
// REAL apiserver (kind): update churn must not make the forced pass publish a
// divergence or queue a repair.
//
// WHY KIND. The artifacts are races between reads in flight and a store that
// keeps applying watch events, so they need a real watch and a real apiserver
// serving LIST from its watch cache while writes land.
//
//   - #872: high churn (3,000 objects, ~500 patches/s). Before #872 this arm
//     measured 63-126 published lost updates per 5 passes, 100% of them with
//     the store AHEAD of the LIST and 0 really lost.
//   - #918: bench run 16's churn rate. About 1,000 objects of a larger GVR
//     change once every 20 s (50 patches/s). Before #918 the forced pass
//     re-read its candidates at 5 reads/s and compared after the whole sweep,
//     which outlasted the objects' update interval, and published lost
//     updates (run 16: 264 of 379, one repair). Now every candidate is ordered
//     against the LIST's own revisions, with no re-read, so the client's rate
//     no longer matters and the arm uses the same client as the other.
//
// Both arms report their numbers to the CI job's step summary (kChurnReport),
// so they are visible without -v.
//
// CLUSTER. One kind cluster for both arms, krateo-sptest-<pid> (#522, #742),
// created and destroyed inside the test, as issue624_rbac_kind_integration_test.go
// does. Each arm has its own CRD and its own watcher.
package cache

import (
	"context"
	"fmt"
	"math/rand"
	"os"
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
	t.Run("918_churn_50_per_second", func(t *testing.T) { k918Arm(t, cfg) })
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

// kChurnReport logs one arm's numbers where CI shows them without -v: the
// job's step summary (GITHUB_STEP_SUMMARY), as well as the test log. A passing
// test's t.Logf is invisible in `go test ./...` output.
func kChurnReport(t *testing.T, line string) {
	t.Helper()
	t.Log(line)
	if path := os.Getenv("GITHUB_STEP_SUMMARY"); path != "" {
		if f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			_, _ = fmt.Fprintf(f, "- `%s`: %s\n", t.Name(), line)
			_ = f.Close()
		}
	}
}

// kChurnSettle waits, after the churn has stopped, until no key is pending:
// every pending key is re-judged by the GVR's next applied event, and the
// churn's own last events are still being applied.
func kChurnSettle(t *testing.T) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Minute); storePendingKeys.Load() != 0; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d key(s) still pending two minutes after the churn stopped: the store never caught up with their reads", storePendingKeys.Load())
		}
	}
}

// kChurnVerdict is the verdict both arms share: churn alone published nothing,
// attempted no repair, resolved no pending key as lost, completed every pass,
// and — the non-vacuity the #923 review asked for — cleared at least one key
// whose store was AHEAD of the read, the mechanism that published false losses
// before #872 and #918 (mutation M1 of the PR turns exactly these RED).
func kChurnVerdict(t *testing.T, issue string, passes int, rate float64) {
	t.Helper()
	lu, ld, la, um := divergenceCounts(verifySiteForced)
	candidates, ahead := storeDivergenceCandidates.Load(), storeAheadTotal.Load()
	kChurnReport(t, fmt.Sprintf("%s: %d passes at %.0f patches/s: candidates=%d cleared=%d (store ahead %d, received %d) undecided=%d pending resolved lost/received/superseded=%d/%d/%d published update/delete/add/uid=%d/%d/%d/%d repairs queued=%d refused=%d",
		issue, passes, rate, candidates, storeClearedTotal.Load(), ahead, storeReceivedTotal.Load(), storeUndecidedTotal.Load(),
		storePendingResolvedLostTotal.Load(), storePendingResolvedReceivedTotal.Load(), storePendingSupersededTotal.Load(),
		lu, ld, la, um, storeRepairQueueDepth(), storeRepairUnsupportedTotal.Load()))
	// The verdict first, so a mutant that publishes is reported as what it
	// is; the non-vacuity preconditions follow.
	if lu+ld+la+um != 0 {
		t.Errorf("%s RED: churn alone published %d forced divergences (update/delete/add/uid = %d/%d/%d/%d) on a store that lost nothing",
			issue, lu+ld+la+um, lu, ld, la, um)
	}
	if n := storePendingResolvedLostTotal.Load(); n != 0 {
		t.Errorf("%s RED: %d pending key(s) resolved as lost under churn alone", issue, n)
	}
	// A repair ATTEMPT, whether queued or refused as unsupported: this GVR's
	// informer may not be rebuildable here, and counting only the queue would
	// make the assertion vacuous on exactly that path.
	if d, u := storeRepairQueueDepth(), storeRepairUnsupportedTotal.Load(); d+int64(u) != 0 {
		t.Errorf("%s RED: %d repair attempt(s) under churn alone (queued %d, refused-unsupported %d)", issue, d+int64(u), d, u)
	}
	if n := VerifySkippedByReasonSnapshot()[verifySkipNoSyncPosition]; n != 0 {
		t.Errorf("%d pass(es) did not complete against a healthy apiserver (no_sync_position)", n)
	}
	// The passes really ran: a hang or a skip would leave this short.
	if got := storeVerifyForcedListsTotal.Load(); got != uint64(passes) {
		t.Errorf("%d forced LISTs for %d passes", got, passes)
	}
	if candidates == 0 {
		t.Fatalf("precondition: %d passes at %.0f patches/s produced no candidate — the arm tests nothing", passes, rate)
	}
	if ahead == 0 {
		t.Fatalf("precondition: no candidate had the store ahead of its read — the arm does not exercise the false-loss mechanism")
	}
}

const (
	k872Objects = 3000
	k872Rate    = 500 // patches per second asked of the churner
	k872Passes  = 5
)

// k872Arm — high churn: 3,000 objects at ~500 patches/s.
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
	kChurnSettle(t)
	kChurnVerdict(t, "#872", k872Passes, rate)
}

const (
	// Bench run 16's churn (#918): 1,000 objects of a larger GVR change
	// round-robin at 50 patches/s, each one every 20 s. The forced pass no
	// longer re-reads anything, so neither the LIST's size nor the client's
	// rate decides a verdict; the GVR is sized to what the churn needs and no
	// more (F5 of the #923 review).
	k918Objects = 5000
	k918Churned = 1000
	k918Rate    = 50
	k918Passes  = 3
)

// k918Arm — run 16's churn rate.
func k918Arm(t *testing.T, cfg *rest.Config) {
	ctx := context.Background()
	gvr := kChurnSeed(t, cfg, "churn918s", "Churn918", k918Objects, 0)
	rw := kChurnWatcher(t, cfg, cfg, gvr)
	patched, stop := kChurn(t, cfg, gvr, k918Rate, func(n int) int { return n % k918Churned })
	time.Sleep(5 * time.Second) // steady churn before the first pass

	t0, p0 := time.Now(), patched.Load()
	for p := 0; p < k918Passes; p++ {
		rw.ConfirmResourceType(ctx, gvr)
		forcedVerify(ctx, gvr)
	}
	rate := float64(patched.Load()-p0) / time.Since(t0).Seconds()
	stop()
	kChurnSettle(t)
	kChurnVerdict(t, "#918", k918Passes, rate)
}
