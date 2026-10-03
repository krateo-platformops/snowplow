// sensitive398_test.go — #398 cache-package arms.
//
//	TestS398_EveryRegistrationPathRefusesSecrets — EnsureResourceType,
//	    AddResourceType and EnsureResourceTypeMetadataOnly all leave v1/secrets
//	    unregistered (every registration path funnels through the locked
//	    helpers), while a non-sensitive core resource still registers.
//	TestS398_AuthnClientconfigInformerUnaffected — with the generic watcher
//	    refusing v1/secrets, the AUTHN_NAMESPACE clientconfig informer
//	    (secrets_informer.go, a separate namespace-scoped typed factory) still
//	    starts, serves and publishes the per-user clientconfig Secrets.
package cache

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

var s398Secrets = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}

func s398Watcher(t *testing.T) *ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	listKinds := map[schema.GroupVersionResource]string{
		s398Secrets:                             "SecretList",
		{Version: "v1", Resource: "configmaps"}: "ConfigMapList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds)
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop(); time.Sleep(20 * time.Millisecond) })
	prev := Global()
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(prev) })
	return rw
}

func TestS398_EveryRegistrationPathRefusesSecrets(t *testing.T) {
	rw := s398Watcher(t)

	if added, ch := rw.EnsureResourceType(s398Secrets); added || ch == nil {
		t.Fatalf("EnsureResourceType(v1/secrets): want a closed skip (added=false, non-nil closed ch); added=%v ch=%v", added, ch != nil)
	} else {
		select {
		case <-ch:
		default:
			t.Fatalf("EnsureResourceType(v1/secrets): the skip channel must be CLOSED (a waiter must not block)")
		}
	}
	rw.AddResourceType(s398Secrets)
	rw.EnsureResourceTypeMetadataOnly(s398Secrets)
	// Every served version of the resource is sensitive.
	rw.AddResourceType(schema.GroupVersionResource{Version: "v2", Resource: "secrets"})
	if rw.IsRegistered(s398Secrets) || rw.IsRegistered(schema.GroupVersionResource{Version: "v2", Resource: "secrets"}) {
		t.Fatalf("#398: a v1/secrets informer was registered — a cluster-wide store of every Secret's data")
	}

	// CONTROL: the classification is not a blanket core-group block.
	cm := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	if _, ch := rw.EnsureResourceType(cm); ch != nil {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
		}
	}
	if !rw.IsRegistered(cm) {
		t.Fatalf("CONTROL: v1/configmaps must still register (only the sensitive set is refused)")
	}
	if !IsSensitiveResource(s398Secrets) || IsSensitiveResource(cm) ||
		IsSensitiveResource(schema.GroupVersionResource{Group: "external-secrets.io", Version: "v1", Resource: "secrets"}) {
		t.Fatalf("classification: exactly core-group secrets (any version)")
	}
}

func TestS398_AuthnClientconfigInformerUnaffected(t *testing.T) {
	rw := s398Watcher(t)
	resetSecretsForTest(t)

	const ns = "krateo-system"
	cli := fake.NewSimpleClientset(mkSecret(ns, "alice-clientconfig", map[string]string{
		"server-url": "https://alice.example/k8s",
		"token":      "alice-token",
	}))
	restore := SetSecretsClientForTest(cli)
	t.Cleanup(restore)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := StartSecretsInformer(ctx, nil, ns); err != nil {
		t.Fatalf("StartSecretsInformer: %v", err)
	}
	if !SecretsCacheServable() {
		t.Fatalf("#398 must not affect the AUTHN clientconfig informer: SecretsCacheServable=false")
	}
	snap := SecretsSnapshotLoad()
	if snap == nil || snap.ByName["alice-clientconfig"] == nil ||
		string(snap.ByName["alice-clientconfig"].Data["server-url"]) != "https://alice.example/k8s" {
		t.Fatalf("#398 must not affect the AUTHN clientconfig snapshot; got %+v", snap)
	}
	if rw.IsRegistered(s398Secrets) {
		t.Fatalf("the AUTHN informer must stay a SEPARATE typed factory — it must not register v1/secrets on the generic watcher")
	}
}
