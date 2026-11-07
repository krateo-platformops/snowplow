package cache

// rbac_readiness_1231_test.go — #1231: an RBAC informer the watcher does not
// have is named as unsynced, not skipped. AssertRBACSnapshotWired makes the case
// unreachable for a watcher that runs the RBAC informers; a passthrough watcher
// runs none, and RBACReadiness does not ask it.

import (
	"context"
	"testing"

	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestIssue1231_AMissingRBACInformerIsNamed(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(rbacScheme(t), rbacListKinds())
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	if !rw.runsRBACInformers() {
		t.Fatal("PRE: a cache-on watcher runs the RBAC informers")
	}

	rw.mu.Lock()
	gi := rw.informers[rolesTypedGVR]
	delete(rw.informers, rolesTypedGVR)
	rw.mu.Unlock()
	t.Cleanup(func() {
		rw.mu.Lock()
		rw.informers[rolesTypedGVR] = gi
		rw.mu.Unlock()
	})

	found := false
	for _, r := range unsyncedRBACInformers(rw) {
		if r == "roles" {
			found = true
		}
	}
	if !found {
		t.Fatalf("RED: the missing roles informer is not named: %v", unsyncedRBACInformers(rw))
	}
}

// A passthrough watcher (CACHE_ENABLED=false) runs no RBAC informers.
func TestIssue1231_APassthroughWatcherRunsNoRBACInformers(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(rbacScheme(t), rbacListKinds())
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher with the cache off: %v, %v", rw, err)
	}
	t.Cleanup(rw.Stop)
	if rw.runsRBACInformers() {
		t.Fatal("RED: a passthrough watcher claims RBAC informers")
	}
	var none *ResourceWatcher
	if none.runsRBACInformers() {
		t.Fatal("RED: no watcher claims RBAC informers")
	}
}
