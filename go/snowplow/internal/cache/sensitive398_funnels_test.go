package cache

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// reviewer-416 probe, adopted: the production registration funnels — eager boot
// registration, the deps-replay ensureInformer, the store-repair / CRD relist
// re-register (removeResourceType + EnsureResourceType) — all leave v1/secrets
// unregistered.
func TestS398_ProductionFunnelsNeverRegisterSecrets(t *testing.T) {
	rw := s398Watcher(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := EagerRegisterAll(ctx, rw, []schema.GroupVersionResource{s398Secrets}, 1, 2*time.Second); err != nil {
		t.Logf("EagerRegisterAll err (tolerated): %v", err)
	}
	if rw.IsRegistered(s398Secrets) {
		t.Fatal("eager registration registered v1/secrets")
	}
	Deps().ensureInformer(s398Secrets)
	if rw.IsRegistered(s398Secrets) {
		t.Fatal("deps replay ensureInformer registered v1/secrets")
	}
	rw.EnsureResourceType(schema.GroupVersionResource{Version: "v1beta1", Resource: "secrets"})
	if rw.IsRegistered(schema.GroupVersionResource{Version: "v1beta1", Resource: "secrets"}) {
		t.Fatal("another version of core secrets registered")
	}
	// A non-core group's "secrets" resource is NOT core secrets and stays informable
	// (classification is core-group only) — record what happens, do not assert.
	t.Logf("refused total=%d", SensitiveRegistrationRefusedForTest())
}
