//go:build js && wasm

package dispatchers

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/authn"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/sandbox/fakeapiserver"
	k8sdynamic "k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// Phase 1's own reader of the frontend ConfigMap (readFrontendConfig, at frontendConfigConfigMapName
// in the authn namespace, as the ServiceAccount) against the Agentiko sandbox's fake apiserver reads
// INIT from the ConfigMap the fake synthesises from the config the SPA gets (fakeapiserver §8): the
// capture's Phase 1 prewarm has its navigation root, and its read is no 404 (mutations: the fake not
// synthesising it, or under another name, turn this RED).
func TestPhase1ReadsINITFromTheFakeEnvsFrontendConfigMap(t *testing.T) {
	dir := filepath.Join("..", "..", "sandbox", "fakeapiserver", "testdata")
	snap, err := os.ReadFile(filepath.Join(dir, "snapshot.json"))
	if err != nil {
		t.Fatal(err)
	}
	page, err := os.ReadFile(filepath.Join(dir, "objects", "000.json"))
	if err != nil {
		t.Fatal(err)
	}
	const init = "/call?resource=navmenus&apiVersion=widgets.templates.krateo.io/v1beta1&name=sidebar-nav-menu&namespace=krateo-system"
	s, err := fakeapiserver.New(fakeapiserver.Options{
		Snapshot: snap, Pages: [][]byte{page}, SAToken: "sa-token", VisitorToken: "visitor-token",
		FrontendConfig: []byte(`{"api":{"INIT":"` + init + `"},"params":{"FRONTEND_NAMESPACE":"krateo-system"}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(s)
	defer srv.Close()
	dyn, err := k8sdynamic.NewForConfig(&rest.Config{Host: srv.URL, BearerToken: "sa-token"})
	if err != nil {
		t.Fatal(err)
	}
	// the fixture's identity: authnNamespace krateo-system, as Phase 1's AUTHN_NAMESPACE
	cfg, err := readFrontendConfig(context.Background(), dyn, "krateo-system", frontendConfigConfigMapName())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.API.Init != init {
		t.Fatalf("INIT: want %q, got %q", init, cfg.API.Init)
	}
}

// #1530: Phase 1 against the sandbox's fake apiserver, with authn as the sandbox has it: no authn-audience token
// file, so every token exchange fails at once. Phase 1's own lister (phase1RootsLister, Phase1Warmup's), through the
// production client builder (credhttp.Dynamic), reads the frontend ConfigMap the fake synthesises and the root its
// INIT names (a navigation root is whatever CR INIT names; the fixture's Incident here), and Phase 1 completes with
// that root inside the sandbox's own budget (PHASE1_TIMEOUT_SECONDS 25), reporting its elapsed time. Before #1530 the
// Phase 1 SA context spent the whole budget retrying the exchange and the read ran on an expired context: no root
// (mutation: fetching the token while the context is built turns this RED).
func TestPhase1CompletesWithTheRootFromINITAgainstTheFakeWithAuthnUnavailable(t *testing.T) {
	dir := filepath.Join("..", "..", "sandbox", "fakeapiserver", "testdata")
	snap, err := os.ReadFile(filepath.Join(dir, "snapshot.json"))
	if err != nil {
		t.Fatal(err)
	}
	page, err := os.ReadFile(filepath.Join(dir, "objects", "000.json"))
	if err != nil {
		t.Fatal(err)
	}
	const root = "sre-volume-attach-failure-20261012-140005"
	const init = "/call?resource=incidents&apiVersion=observability.krateo.io/v1alpha1&name=" + root + "&namespace=krateo-system"
	s, err := fakeapiserver.New(fakeapiserver.Options{
		Snapshot: snap, Pages: [][]byte{page}, SAToken: "sa-token", VisitorToken: "visitor-token",
		FrontendConfig: []byte(`{"api":{"INIT":"` + init + `"}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(s)
	defer srv.Close()

	// authn as in the sandbox: the projected authn-audience token file does not exist
	t.Cleanup(func() { SetSeedLoopbackTokenProvider(nil) })
	SetSeedLoopbackTokenProvider(authn.New("http://authn.sandbox.invalid:8082", "").Token)
	rc := &rest.Config{Host: srv.URL, BearerToken: "sa-token"}
	dyn, err := credhttp.Dynamic(rc)
	if err != nil {
		t.Fatal(err)
	}
	lister := phase1RootsLister(endpoints.Endpoint{ServerURL: srv.URL, Token: "sa-token"}, rc, dyn, "krateo-system")
	var roots []navigationRoot
	resolve := func(_ context.Context, r navigationRoot) error {
		roots = append(roots, r)
		return nil
	}
	const budget = 25 * time.Second // the sandbox's PHASE1_TIMEOUT_SECONDS
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	start := time.Now()
	werr := phase1WarmupWith(ctx, nil, lister, resolve, nil, nil, nil, nil)
	elapsed := time.Since(start)
	t.Logf("boot: Phase 1 completed in %v of its %v budget with %d root(s), err=%v", elapsed, budget, len(roots), werr)
	if len(roots) != 1 || roots[0].Root == nil || roots[0].Root.GetName() != root {
		t.Fatalf("Phase 1's roots: want the one INIT names (%s), got %d", root, len(roots))
	}
	if ctx.Err() != nil {
		t.Fatalf("Phase 1 ran to its budget's end (%v): it waited on something", elapsed)
	}
}
