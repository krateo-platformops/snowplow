//go:build js && wasm

package dispatchers

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

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
