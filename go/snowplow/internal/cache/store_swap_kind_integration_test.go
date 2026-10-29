//go:build integration
// +build integration

// store_swap_kind_integration_test.go — #870 against a REAL apiserver (kind):
// the production construction path (the H5 streaming informer with the
// verifying ListerWatcher), a real forced pass that detects a REAL lost update
// (a watch event swallowed on its way to the live copy), and a repair under
// churn.
//
// WATCH-LIST. The package's TestMain turns client-go's WatchListClient gate
// OFF for the whole test binary (featuregate_watchlist_test.go: the fake
// ListerWatchers never send the initial-events bookmark). Production runs it
// ON: it is the v0.36.5 default and nothing in snowplow opts out
// (store_verify.go). This arm re-enables it for its own duration with
// client-go's scoped test override (features/testing.SetFeatureDuringTest),
// before any informer is built (the reflector reads the gate at
// construction), so the transform running twice through the reflector's
// temporary store — and the reuse it does there — is exercised against a real
// apiserver (#928 F4).
//
// What it asserts (design v5 §8 K1, K3 and the acceptance shape, scaled to
// kind — the 50K run is the bench's):
//   - K1: during the build the GVR stays servable on every read, and the
//     watcher's own REST config issues exactly ONE initial listing of the
//     GVR's data for the repair: the new copy's (k870ListCounter, at that
//     config's RoundTripper). That is REFLECTOR traffic only: a resolve's
//     LISTs travel on per-user configs (credhttp.NewClientConfig) this counter
//     never sees, and this arm drives no resolves, so it claims nothing about
//     resolve-originated LISTs. That assertion is #988's (the production
//     wire counter, and real resolves during the build);
//   - K3: every divergent key equals a quorum GET after the swap;
//   - acceptance shape: 0 type_event unanchors, 0 type-path dirty-marks,
//     keys_total{divergence,swap} = the injected losses, exactly; and, from
//     the arm's OWN record of the keys it churned while the build ran (C, no
//     slack constant): losses ≤ swap deliveries ≤ |M| ≤ losses + |C| (C1), and
//     reused ≥ objects − losses − |C|.
//
// CLUSTER. Its own kind cluster, krateo-sptest-<pid> (#522, #742), created and
// destroyed inside the test. CI runs it under the integration tag.
package cache

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
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
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/e2e-framework/support/kind"
)

const (
	k870Group   = "composition.krateo.io"
	k870Version = "v1-2-2"
	k870Plural  = "swap870s"
	k870Objects = 5000
	k870NS      = 10
	k870Rate    = 50 // patches per second (design v5 §8)
	k870Lost    = 3  // real lost updates injected
)

// k870ListCounter counts the INITIAL LISTINGS of the GVR's DATA a copy makes,
// on either path: a watch-list start (a WATCH with sendInitialEvents=true, the
// production path) or a classic LIST's first page (the fallback). Not the
// forced pass's metadata LIST, not a continue page, not a plain WATCH.
type k870ListCounter struct {
	inner http.RoundTripper
	path  string
	n     *atomic.Int64 // shared by every client the config builds
}

func (c *k870ListCounter) RoundTrip(r *http.Request) (*http.Response, error) {
	q := r.URL.Query()
	if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, c.path) &&
		!strings.Contains(r.Header.Get("Accept"), "PartialObjectMetadata") {
		watchList := q.Get("watch") == "true" && q.Get("sendInitialEvents") == "true"
		firstPage := q.Get("watch") != "true" && q.Get("continue") == ""
		if watchList || firstPage {
			c.n.Add(1)
		}
	}
	return c.inner.RoundTrip(r)
}

// k870SwallowDyn swallows the WATCH events of the keys in drop on their way to
// an informer — a real lost update, as #237 measured one.
type k870SwallowDyn struct {
	dynamic.Interface
	gvr  schema.GroupVersionResource
	mu   *sync.Mutex
	drop map[string]bool
}

func (d k870SwallowDyn) Resource(g schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	r := d.Interface.Resource(g)
	if g != d.gvr {
		return r
	}
	return k870SwallowRes{NamespaceableResourceInterface: r, d: d}
}

type k870SwallowRes struct {
	dynamic.NamespaceableResourceInterface
	d k870SwallowDyn
}

func (r k870SwallowRes) Namespace(ns string) dynamic.ResourceInterface {
	return k870SwallowNS{ResourceInterface: r.NamespaceableResourceInterface.Namespace(ns), d: r.d}
}

type k870SwallowNS struct {
	dynamic.ResourceInterface
	d k870SwallowDyn
}

func (r k870SwallowNS) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	w, err := r.ResourceInterface.Watch(ctx, opts)
	if err != nil {
		return w, err
	}
	out := make(chan watch.Event, 1024)
	go func() {
		defer close(out)
		for ev := range w.ResultChan() {
			if u, ok := ev.Object.(*unstructured.Unstructured); ok && ev.Type == watch.Modified {
				r.d.mu.Lock()
				drop := r.d.drop[u.GetNamespace()+"/"+u.GetName()]
				r.d.mu.Unlock()
				if drop {
					continue
				}
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				w.Stop()
				return
			}
		}
	}()
	return watch.NewProxyWatcher(out), nil
}

func TestStoreSwap870_Kind_RepairUnderChurn(t *testing.T) {
	// Production's watch-list setting (the v0.36.5 default), for this arm only.
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	ctx := context.Background()
	clusterName := kindguard.ClusterName()
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
	gvr := schema.GroupVersionResource{Group: k870Group, Version: k870Version, Resource: k870Plural}
	admin := dynamic.NewForConfigOrDie(cfg)

	ax := apiextclient.NewForConfigOrDie(cfg)
	if _, err := ax.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, &apiextv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: k870Plural + "." + k870Group},
		Spec: apiextv1.CustomResourceDefinitionSpec{
			Group: k870Group, Scope: apiextv1.NamespaceScoped,
			Names: apiextv1.CustomResourceDefinitionNames{Kind: "Swap870", ListKind: "Swap870List", Plural: k870Plural, Singular: "swap870"},
			Versions: []apiextv1.CustomResourceDefinitionVersion{{Name: k870Version, Served: true, Storage: true,
				Schema: &apiextv1.CustomResourceValidation{OpenAPIV3Schema: &apiextv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptr.To(true)}}}},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create CRD: %v", err)
	}
	for end := time.Now().Add(60 * time.Second); ; time.Sleep(500 * time.Millisecond) {
		if _, err := admin.Resource(gvr).List(ctx, metav1.ListOptions{Limit: 1}); err == nil {
			break
		} else if time.Now().After(end) {
			t.Fatalf("CRD never served: %v", err)
		}
	}
	nsName := func(i int) string { return fmt.Sprintf("k870-%02d", i%k870NS) }
	objName := func(i int) string { return fmt.Sprintf("s-%05d", i) }
	nsGVR := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	for i := 0; i < k870NS; i++ {
		if _, err := admin.Resource(nsGVR).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": nsName(i)}}}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create namespace: %v", err)
		}
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for i := 0; i < k870Objects; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := admin.Resource(gvr).Namespace(nsName(i)).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": k870Group + "/" + k870Version, "kind": "Swap870",
				"metadata": map[string]any{"name": objName(i)}, "spec": map[string]any{"v": "0"},
			}}, metav1.CreateOptions{}); err != nil {
				t.Errorf("create object %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	// The watcher, wired like main.go: the REST config (the streaming
	// informer), the metadata client (the forced pass) and a counted transport.
	counter := &k870ListCounter{path: "/" + k870Plural, n: &atomic.Int64{}}
	wcfg := rest.CopyConfig(cfg)
	wcfg.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		return &k870ListCounter{inner: rt, path: counter.path, n: counter.n}
	}
	var dropMu sync.Mutex
	drop := map[string]bool{}
	wdyn := k870SwallowDyn{Interface: dynamic.NewForConfigOrDie(wcfg), gvr: gvr, mu: &dropMu, drop: drop}
	ResetStoreVerificationStatsForTest()
	t.Cleanup(ResetStoreVerificationStatsForTest)
	rw, err := NewResourceWatcher(ctx, wdyn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rw.Stop)
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })
	rw.SetRESTConfig(wcfg)
	rw.SetMetadataClient(metadata.NewForConfigOrDie(cfg))
	_, ch := rw.EnsureResourceType(gvr)
	select {
	case <-ch:
	case <-time.After(120 * time.Second):
		t.Fatal("informer never synced")
	}
	rw.ConfirmResourceType(ctx, gvr)
	if live := rw.liveInstance(gvr); live == nil || live.shape != shapeStreaming {
		t.Fatalf("precondition: the production streaming informer, got %+v", live)
	}

	// Churn at k870Rate on objects 100.., while objects 0..k870Lost-1 lose an
	// update for real: the watch event is swallowed on its way to the store.
	stop := make(chan struct{})
	churnDone := make(chan struct{})
	// churned: every key the churn patches while recording is on (from the
	// forced pass to the swap's completion) — the arm's own count C.
	var churnMu sync.Mutex
	recording := false
	churned := map[string]bool{}
	go func() {
		defer close(churnDone)
		rnd := rand.New(rand.NewSource(870))
		tick := time.NewTicker(time.Second / k870Rate)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			i := 100 + rnd.Intn(k870Objects-100)
			p := fmt.Sprintf(`{"spec":{"v":"%d"}}`, time.Now().UnixNano())
			churnMu.Lock()
			if recording {
				churned[nsName(i)+"/"+objName(i)] = true
			}
			churnMu.Unlock()
			_, _ = admin.Resource(gvr).Namespace(nsName(i)).Patch(ctx, objName(i), types.MergePatchType, []byte(p), metav1.PatchOptions{})
		}
	}()
	t.Cleanup(func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		<-churnDone
	})
	dropMu.Lock()
	for i := 0; i < k870Lost; i++ {
		drop[nsName(i)+"/"+objName(i)] = true
	}
	dropMu.Unlock()
	for i := 0; i < k870Lost; i++ {
		if _, err := admin.Resource(gvr).Namespace(nsName(i)).Patch(ctx, objName(i), types.MergePatchType,
			[]byte(`{"spec":{"v":"lost"}}`), metav1.PatchOptions{}); err != nil {
			t.Fatalf("patch: %v", err)
		}
	}
	time.Sleep(2 * time.Second) // the swallowed events have passed the live watch
	dropMu.Lock()
	for k := range drop {
		delete(drop, k)
	}
	dropMu.Unlock()

	d := Deps()
	type0 := typeMarksAnyCause(d)
	ls0 := ListCellStatsSnapshot()
	lists0 := counter.n.Load()

	// The real forced pass detects the losses and requests the build.
	churnMu.Lock()
	recording = true
	churnMu.Unlock()
	rw.ConfirmResourceType(ctx, gvr)
	forcedVerify(ctx, gvr)
	if lu, _, _, _ := divergenceCounts(verifySiteForced); lu != k870Lost {
		t.Fatalf("precondition: the forced pass confirmed %d lost updates, want %d", lu, k870Lost)
	}
	// K1: servable on every read while the build runs.
	for {
		if !rw.IsServable(gvr) {
			t.Fatal("K1 RED: the GVR left servable during the build")
		}
		if StoreSwapStatsSnapshot().Swaps == 1 {
			break
		}
		if _, ok := rw.ListObjectsServable(gvr, nsName(1)); !ok {
			t.Fatal("K1 RED: a read during the build fell through")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The swap's own completion event (step 6 done), not the install count.
	select {
	case <-rw.liveInstance(gvr).delivered:
	case <-time.After(60 * time.Second):
		t.Fatal("the swap was installed but never completed its delivery")
	}
	churnMu.Lock()
	recording = false
	nChurned := len(churned)
	churnMu.Unlock()
	if got := counter.n.Load() - lists0; got != 1 {
		t.Fatalf("K1 RED: the watcher's config issued %d initial listing(s) of the GVR's data for one repair, want 1 (the new copy's own; reflector traffic only)", got)
	}
	// K3: every divergent key equals a quorum GET.
	for end := time.Now().Add(60 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		ok := true
		for i := 0; i < k870Lost; i++ {
			q, err := admin.Resource(gvr).Namespace(nsName(i)).Get(ctx, objName(i), metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			st := rw.StoreObjectState(gvr, nsName(i), objName(i))
			if st.ResourceVersion != q.GetResourceVersion() {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(end) {
			t.Fatal("K3 RED: a divergent key never equalled the quorum read")
		}
	}
	close(stop)
	<-churnDone

	s := StoreSwapStatsSnapshot()
	ls := ListCellStatsSnapshot()
	t.Logf("#870 kind (watch-list on): |M|=%d deliveries=%d reused=%d regressed=%d keys_total{divergence,swap}=%d data LISTs=%d churned during the build=%d",
		s.MKeys, s.Deliveries, s.Reused, s.RegressedKeys, storeSwapStats.keys.get(reasonDivergence, outcomeSwap), counter.n.Load()-lists0, nChurned)
	if n := typeMarksAnyCause(d) - type0; n != 0 {
		t.Errorf("acceptance RED: %d type-path dirty-mark(s) during the repair", n)
	}
	if n := ls.Unanchors["type_event"] - ls0.Unanchors["type_event"]; n != 0 {
		t.Errorf("acceptance RED: %d type_event unanchor(s)", n)
	}
	// M holds the losses and at most the keys the arm churned during the build
	// (a k-fold M — the whole store — fails here); every loss is delivered
	// (its consumer never acted on S), and no more than M (C1).
	if s.MKeys < k870Lost || s.MKeys > uint64(k870Lost+nChurned) {
		t.Errorf("acceptance RED: |M|=%d, want %d ≤ |M| ≤ %d (the losses + the %d keys churned during the build)", s.MKeys, k870Lost, k870Lost+nChurned, nChurned)
	}
	if s.Deliveries < k870Lost || s.Deliveries > s.MKeys {
		t.Errorf("acceptance RED: deliveries=%d, want %d ≤ deliveries ≤ |M|=%d", s.Deliveries, k870Lost, s.MKeys)
	}
	if got := storeSwapStats.keys.get(reasonDivergence, outcomeSwap); got != k870Lost {
		t.Errorf("acceptance RED: keys_total{divergence,swap} = %d, want the %d injected", got, k870Lost)
	}
	// Every object neither lost nor churned during the build is byte-equal in
	// both copies when the new copy lists it: shared.
	if floor := uint64(k870Objects - k870Lost - nChurned); s.Reused < floor {
		t.Errorf("reuse RED: %d objects shared with the live copy, want ≥ %d (objects − losses − churned)", s.Reused, floor)
	}
}
