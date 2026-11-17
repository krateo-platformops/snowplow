//go:build js && wasm

package fakeapiserver

import (
	"strings"
	"syscall/js"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/frontendconfig"
)

const frontendCM = "/api/v1/namespaces/krateo-system/configmaps/" + frontendconfig.Default

// The synthesised frontend ConfigMap (§8) is served with exactly the config.json text it was given,
// at the name Phase 1 reads, to a get, a list pinned by metadata.name and a watch from it, as Phase
// 1's Get and its config-vars informer read it.
func TestFrontendConfigMapServesExactlyTheGivenConfig(t *testing.T) {
	h := newHarness(t, opts{})
	g := h.do("GET", frontendCM, req{as: "sa"})
	eq(t, g.code, 200, "get")
	eq(t, get(g.body, "data", frontendconfig.DataKey), frontendConfigText, "config.json: the exact bytes")
	eq(t, get(g.body, "metadata", "namespace"), "krateo-system", "the authn namespace")
	if get(g.body, "metadata", "uid") == nil || get(g.body, "metadata", "resourceVersion") == nil {
		t.Fatal("no uid or resourceVersion:", g.body["metadata"])
	}
	l := h.do("GET", "/api/v1/namespaces/krateo-system/configmaps?fieldSelector=metadata.name%3D"+frontendconfig.Default, req{as: "sa"})
	eq(t, len(items(l)), 1, "the informer's pinned list")
	w := h.watch("/api/v1/namespaces/krateo-system/configmaps?watch=1&fieldSelector=metadata.name%3D"+frontendconfig.Default+
		"&resourceVersion="+get(l.body, "metadata", "resourceVersion").(string), req{as: "sa"})
	defer w.cancel()
	eq(t, <-w.code, 200, "the informer's watch")
	w.quiet()
}

// The name is Phase 1's own (frontendconfig.ConfigMapName): an operator's FRONTEND_CONFIG_CONFIGMAP
// moves it for both (mutation: the fake naming its own default turns this RED).
func TestFrontendConfigMapFollowsPhase1sName(t *testing.T) {
	t.Setenv(frontendconfig.Env, "tenant-frontend-cfg")
	h := newHarness(t, opts{})
	eq(t, h.do("GET", "/api/v1/namespaces/krateo-system/configmaps/tenant-frontend-cfg", req{as: "sa"}).code, 200, "renamed")
	eq(t, h.do("GET", frontendCM, req{as: "sa"}).code, 404, "the default name holds nothing")
}

// The namespace is the snapshot identity's AuthnNamespace (Phase 1's AUTHN_NAMESPACE, the clientconfig
// Secret's namespace), never a literal (mutation: a fixed namespace turns this RED).
func TestFrontendConfigMapIsInTheIdentitysAuthnNamespace(t *testing.T) {
	h := newHarness(t, opts{edit: func(snap map[string]any) {
		snap["identity"].(map[string]any)["authnNamespace"] = "tenant-authn"
	}})
	g := h.do("GET", "/api/v1/namespaces/tenant-authn/configmaps/"+frontendconfig.Default, req{as: "sa"})
	eq(t, g.code, 200, "in the identity's authn namespace")
	eq(t, get(g.body, "data", frontendconfig.DataKey), frontendConfigText, "config.json")
	eq(t, h.do("GET", frontendCM, req{as: "sa"}).code, 404, "nothing in krateo-system")
}

// Its readers' access is the snapshot's RBAC over configmaps, as for any other ConfigMap: with
// configmaps out of the role that grants them, neither the ServiceAccount nor the visitor reads it
// (mutation: a rule of its own for the synthesised object turns this RED).
func TestFrontendConfigMapAccessIsTheSnapshotsRBAC(t *testing.T) {
	h := newHarness(t, opts{})
	eq(t, h.do("GET", frontendCM, req{as: "sa"}).code, 200, "sa, granted by the snapshot")
	eq(t, h.do("GET", frontendCM, req{as: "visitor"}).code, 200, "visitor, granted by the snapshot")
	h = newHarness(t, opts{pageEdit: func(objs []map[string]any) {
		for _, o := range objs {
			if o["kind"] != "ClusterRole" || get(o, "metadata", "name") != "sandbox-test-reader" {
				continue
			}
			for _, r := range o["rules"].([]any) {
				rm := r.(map[string]any)
				var kept []any
				for _, res := range rm["resources"].([]any) {
					if res != "configmaps" {
						kept = append(kept, res)
					}
				}
				rm["resources"] = kept
			}
		}
	}})
	eq(t, h.do("GET", frontendCM, req{as: "sa"}).code, 403, "sa, no grant")
	eq(t, h.do("GET", frontendCM, req{as: "visitor"}).code, 403, "visitor, no grant")
}

// A missing or invalid config is refused, naming it, never defaulted (mutations: either check
// dropped turns its case RED).
func TestNewRefusesAMissingOrInvalidFrontendConfig(t *testing.T) {
	for _, c := range []struct{ cfg, why string }{
		{"", "Options.FrontendConfig is empty"},
		{"{not json", "Options.FrontendConfig is not a JSON object"},
		// JSON, and not an object: config.json is the object Phase 1 unmarshals (json.Valid takes these)
		{"null", "Options.FrontendConfig is not a JSON object"},
		{`[{"api":{}}]`, "Options.FrontendConfig is not a JSON object"},
		{`"x"`, "Options.FrontendConfig is not a JSON object"},
	} {
		cfg := c.cfg
		_, err := loadHarness(t, opts{frontend: &cfg})
		if err == nil || !strings.Contains(err.Error(), c.why) {
			t.Fatalf("frontendConfig %q: want %q, got %v", c.cfg, c.why, err)
		}
	}
}

// A snapshot that also holds the ConfigMap is refused: a synthesised object is never one a cluster's
// snapshot supplied (mutation: the snapshot's copy kept, or overwritten silently, turns this RED).
func TestNewRefusesASnapshotHoldingTheFrontendConfigMap(t *testing.T) {
	_, err := loadHarness(t, opts{extra: []map[string]any{{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": frontendconfig.Default, "namespace": "krateo-system", "uid": "u-1", "resourceVersion": "1"},
		"data":     map[string]any{frontendconfig.DataKey: `{"api":{}}`},
	}}})
	if err == nil || !strings.Contains(err.Error(), "ConfigMap krateo-system/"+frontendconfig.Default+" is in the snapshot, and the fake env synthesises it") {
		t.Fatal(err)
	}
}

// The same rule for the other synthesised object: a snapshot that holds the visitor's clientconfig
// Secret is refused, naming its key, never overwritten (mutation: the overwrite restored turns this
// RED).
func TestNewRefusesASnapshotHoldingTheClientconfigSecret(t *testing.T) {
	_, err := loadHarness(t, opts{extra: []map[string]any{{
		"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
		"metadata": map[string]any{"name": "sandbox-visitor-clientconfig", "namespace": "krateo-system", "uid": "u-2", "resourceVersion": "1"},
		"data":     map[string]any{"token": "Y2x1c3Rlci10b2tlbg=="},
	}}})
	if err == nil || !strings.Contains(err.Error(), "Secret krateo-system/sandbox-visitor-clientconfig is in the snapshot, and the fake env synthesises it") {
		t.Fatal(err)
	}
}

// Listen reads frontendConfig from the host config and refuses it missing or not JSON, naming the
// field, before it listens (mutations: either check dropped turns its case RED).
func TestListenRefusesAMissingOrInvalidFrontendConfig(t *testing.T) {
	prev := js.Global().Get(HostConfig)
	defer js.Global().Set(HostConfig, prev)
	snap := string(readFixture(t, "snapshot.json"))
	page := string(readFixture(t, "objects/000.json"))
	for _, c := range []struct {
		set  func(o js.Value)
		want string
	}{
		{func(js.Value) {}, HostConfig + ".frontendConfig must be a non-empty string"},
		{func(o js.Value) { o.Set("frontendConfig", "{not json") }, HostConfig + ".frontendConfig is not a JSON object"},
		{func(o js.Value) { o.Set("frontendConfig", "null") }, HostConfig + ".frontendConfig is not a JSON object"},
		{func(o js.Value) { o.Set("frontendConfig", `[{"api":{}}]`) }, HostConfig + ".frontendConfig is not a JSON object"},
		{func(o js.Value) { o.Set("frontendConfig", `"x"`) }, HostConfig + ".frontendConfig is not a JSON object"},
	} {
		o := js.Global().Get("Object").New()
		o.Set("snapshot", snap)
		o.Set("pages", js.ValueOf([]any{page}))
		o.Set("saToken", "sa-token")
		o.Set("visitorToken", "visitor-token")
		o.Set("port", 1)
		c.set(o)
		js.Global().Set(HostConfig, o)
		if _, err := Listen(); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("want %q, got %v", c.want, err)
		}
	}
}
