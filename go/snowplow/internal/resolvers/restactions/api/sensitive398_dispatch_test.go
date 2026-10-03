// sensitive398_dispatch_test.go — #398 Gate 5a / 5b reason arms. Each gate must
// fall through with ITS OWN fall-through reason, so a refactor that silently
// drops a gate (and lets the call fall to Gate 6's not-synced arm, or be served
// by the informer) fails here.
package api

import (
	"net/http"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestS398_Gate5b_SensitiveFallthroughReason(t *testing.T) {
	rw := newDispatchWatcher(t)
	cache.ResetFallthroughCountersForTest()
	t.Cleanup(cache.ResetFallthroughCountersForTest)
	ctx := cache.WithFallthroughScope(dispatchCtx(), cache.ScopeCallWidgets)
	secrets := schema.GroupVersionResource{Version: "v1", Resource: "secrets"}

	for _, p := range []string{"/api/v1/namespaces/x/secrets/y", "/api/v1/namespaces/x/secrets"} {
		if raw, served := dispatchViaInformer(ctx, buildCall(http.MethodGet, p)); served {
			t.Fatalf("Gate 5b: a sensitive read must never be informer-served (%s); got %d bytes", p, len(raw))
		}
	}
	if got := cache.FallthroughCount(cache.ScopeCallWidgets, secrets.String(), cache.ReasonInformerSensitive); got != 2 {
		t.Fatalf("Gate 5b: ReasonInformerSensitive count = %d, want 2 (one per sensitive call)", got)
	}
	if got := cache.FallthroughCount(cache.ScopeCallWidgets, secrets.String(), cache.ReasonInformerNotSynced); got != 0 {
		t.Fatalf("Gate 5b: a sensitive call must not reach Gate 6 (not-synced count = %d)", got)
	}
	if rw.IsRegistered(secrets) {
		t.Fatalf("Gate 5b: the dispatch must not register a v1/secrets informer")
	}
}

func TestS398_Gate5a_RepresentationFallthroughReason(t *testing.T) {
	newDispatchWatcher(t, newTestRestActionRuntimeObject("default", "a", "alpha"))
	cache.ResetFallthroughCountersForTest()
	t.Cleanup(cache.ResetFallthroughCountersForTest)
	ctx := cache.WithFallthroughScope(dispatchCtx(), cache.ScopeCallWidgets)
	path := "/apis/templates.krateo.io/v1/namespaces/default/restactions"

	// CONTROL: the default representation is served from the synced informer.
	if _, served := dispatchViaInformer(ctx, buildCall(http.MethodGet, path)); !served {
		t.Fatalf("CONTROL: a default-representation LIST on a synced informer must be served")
	}
	for _, accept := range []string{
		"Accept: application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1",
		"accept: application/json; as=Table; g=meta.k8s.io; v=v1, application/json",
	} {
		call := buildCall(http.MethodGet, path)
		call.Headers = []string{accept}
		if raw, served := dispatchViaInformer(ctx, call); served {
			t.Fatalf("Gate 5a: %q must fall through to the apiserver; the informer served %d bytes", accept, len(raw))
		}
	}
	if got := cache.FallthroughCount(cache.ScopeCallWidgets, dispatchTestGVR.String(), cache.ReasonInformerRepresentation); got != 2 {
		t.Fatalf("Gate 5a: ReasonInformerRepresentation count = %d, want 2", got)
	}
	// The resolver's default Accept carries no `as=` and must stay informer-servable.
	call := buildCall(http.MethodGet, path)
	call.Headers = []string{"Accept: application/json, application/x-yaml, text/yaml"}
	if _, served := dispatchViaInformer(ctx, call); !served {
		t.Fatalf("Gate 5a must not catch the resolver's default Accept header")
	}
}
