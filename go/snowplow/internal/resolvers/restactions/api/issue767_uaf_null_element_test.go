//go:build unit || integration

package api

// issue767_uaf_null_element_test.go — #767: a null resourcesFrom element is
// skipped like "" by the refilter, and stays fail-closed.
//
// THE DEFECT (main 7372771f): blueprint-formdef's namespaces stage filters by
// `resourcesFrom: [.compdef.status.resource]`. Whenever compdef has no resource
// — not dispatched (no blueprint named: the keep-warm resolve, 19 per 45-min
// burst on 057) or not reconciled yet — that is `[null]`, a non-string element,
// so the resource set was "unresolved": a WARN plus an ERROR, and since #761 a
// stage error, so the answer is never cached. `[.x // ""]` meant the same thing
// and was silent.
//
// ARMS
//   N1  the refilter's set: null elements are skipped (counted), "" and
//       duplicates as before; a null never masks a non-string element
//       (number, bool, object) — still unresolved.
//   N1d the skip logs ONE DEBUG line per stage with api, expr and the count;
//       no line when nothing was skipped.
//   N2  the STRICT form is unchanged: resolveUAFResources fails closed on
//       null, and InspectReadSet (GET /rbac, an extras-only dict) still
//       reports the stage UNRESOLVABLE rather than an empty read-set.
//   N3  blueprint-formdef's exact api through the real Resolve: with no
//       extras, only `name`, or a compdef not reconciled, namespaces is []
//       with no WARN/ERROR and no stage error; the requester HOLDS create on
//       fireworksapps in ns-a and is still denied (fail-closed). Reconciled:
//       ["ns-a"], unchanged.
//   N4  a null next to a non-string element through Resolve is still a stage
//       error (#651), so it is never cached.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

type n767Buf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *n767Buf) Write(p []byte) (int, error) { w.mu.Lock(); defer w.mu.Unlock(); return w.b.Write(p) }
func (w *n767Buf) String() string              { w.mu.Lock(); defer w.mu.Unlock(); return w.b.String() }

func n767Logger(buf *n767Buf) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestIssue767_N1_RefilterSkipsNullElements(t *testing.T) {
	cases := []struct {
		name string
		expr string
		dict map[string]any
		want []string
		ok   bool
	}{
		{"absent input", "[.compdef.status.resource]", map[string]any{}, []string{}, true},
		{"input without the field", "[.compdef.status.resource]", map[string]any{"compdef": map[string]any{"status": map[string]any{}}}, []string{}, true},
		{"all null", "[null, null]", map[string]any{}, []string{}, true},
		{"null among strings", `["a", null, "a", "", "b"]`, map[string]any{}, []string{"a", "b"}, true},
		{"resolved", "[.compdef.status.resource]", map[string]any{"compdef": map[string]any{"status": map[string]any{"resource": "fireworksapps"}}}, []string{"fireworksapps"}, true},
		{"null then number", "[null, 1]", map[string]any{}, nil, false},
		{"null then bool", "[null, true]", map[string]any{}, nil, false},
		{"null then object", "[null, {}]", map[string]any{}, nil, false},
		{"number alone", "[1]", map[string]any{}, nil, false},
		{"bare null", "null", map[string]any{}, nil, false},
		{"jq error", "[ (.x", map[string]any{}, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uaf := &templates.UserAccessFilterSpec{ResourcesFrom: tc.expr}
			got, ok := resolveRefilterUAFResources(context.Background(), discardLogger(), "stage", uaf, tc.dict)
			if ok != tc.ok {
				t.Fatalf("N1: %s: ok=%v, want %v (got %v)", tc.expr, ok, tc.ok, got)
			}
			if tc.ok && !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("N1: %s: resources=%#v, want %#v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestIssue767_N1d_SkipLogsOneDebugLinePerStage(t *testing.T) {
	var buf n767Buf
	uaf := &templates.UserAccessFilterSpec{ResourcesFrom: "[.a, .b, \"x\"]"}
	if _, ok := resolveRefilterUAFResources(context.Background(), n767Logger(&buf), "namespaces", uaf, map[string]any{}); !ok {
		t.Fatal("N1d PRE: [null, null, \"x\"] must resolve")
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("N1d: want exactly one log line, got %d: %s", len(lines), buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("N1d: %v", err)
	}
	want := map[string]any{
		"level": "DEBUG", "api": "namespaces", "expr": uaf.ResourcesFrom, "skipped_null": float64(2),
		"msg": "userAccessFilter: ResourcesFrom yielded null element(s); skipped (no resource to grant against)",
	}
	for k, v := range want {
		if rec[k] != v {
			t.Fatalf("N1d: %s=%v, want %v (line %s)", k, rec[k], v, lines[0])
		}
	}
	for k := range rec {
		if _, ok := want[k]; !ok && k != "time" {
			t.Fatalf("N1d: the skip line carries an unexpected field %q: %s", k, lines[0])
		}
	}

	var quiet n767Buf
	if _, ok := resolveRefilterUAFResources(context.Background(), n767Logger(&quiet), "namespaces",
		&templates.UserAccessFilterSpec{ResourcesFrom: `["x"]`}, map[string]any{}); !ok || quiet.String() != "" {
		t.Fatalf("N1d: no null skipped must log nothing; ok=%v log=%q", ok, quiet.String())
	}
}

func TestIssue767_N2_StrictFormAndInspectUnchanged(t *testing.T) {
	uaf := &templates.UserAccessFilterSpec{ResourcesFrom: "[.compdef.status.resource]"}
	if got, ok := resolveUAFResources(context.Background(), discardLogger(), uaf, map[string]any{}); ok {
		t.Fatalf("N2: the strict form resolved [null] to %v — inspect would report an empty read-set", got)
	}

	withInspectSARESTConfig(t, fakeDiscoveryServer(t))
	ra := &templates.RESTAction{Spec: templates.RESTActionSpec{API: []*templates.API{{
		Name: "namespaces", Path: "/api/v1/namespaces",
		UserAccessFilter: &templates.UserAccessFilterSpec{Verb: "create", Group: "composition.krateo.io",
			ResourcesFrom: "[.compdef.status.resource]", NamespaceFrom: ".metadata.name"},
	}}}}
	rows, unresolved, err := InspectReadSet(context.Background(), ra, map[string]any{"namespace": "krateo-system", "name": "bp"})
	if err != nil {
		t.Fatalf("InspectReadSet: %v", err)
	}
	if len(unresolved) != 1 || unresolved[0].Stage != "namespaces" {
		t.Fatalf("N2: a resourcesFrom over upstream data must be UNRESOLVABLE at inspect; unresolved=%+v rows=%+v", unresolved, rows)
	}
}

// --- N3/N4: blueprint-formdef's api through the real Resolve -----------------

const (
	n767CompdefPath = "/apis/core.krateo.io/v1alpha1/namespaces/krateo-system/compositiondefinitions/bp-767"
	n767Ready       = `{"apiVersion":"core.krateo.io/v1alpha1","kind":"CompositionDefinition","metadata":{"name":"bp-767","namespace":"krateo-system"},"status":{"resource":"fireworksapps","apiVersion":"composition.krateo.io/v1-2-0","kind":"FireworksApp"}}`
	n767NotReady    = `{"apiVersion":"core.krateo.io/v1alpha1","kind":"CompositionDefinition","metadata":{"name":"bp-767","namespace":"krateo-system"},"status":{"conditions":[{"type":"Ready","status":"False"}]}}`
)

// n767API is blueprint-formdef's spec.api as it is on 057 (2026-10-19).
func n767API(resourcesFrom string) []*templates.API {
	s := func(v string) *string { return &v }
	return []*templates.API{
		{
			Name: "compdef", Verb: ptr.To(http.MethodGet), Headers: []string{"Accept: application/json"},
			DependsOn: &templates.Dependency{Name: "", Iterator: s(`[ { namespace: (.namespace // ""), name: (.name // "") } | select(.namespace != "" and .name != "") ]`)},
			Path:      `${ "/apis/core.krateo.io/v1alpha1/namespaces/" + .namespace + "/compositiondefinitions/" + .name }`,
		},
		{
			Name: "jsonschema", Verb: ptr.To(http.MethodGet), Headers: []string{"Accept: application/json"},
			ContinueOnError: ptr.To(true), ErrorKey: s("jsonschemaError"),
			DependsOn: &templates.Dependency{Name: "compdef", Iterator: s(`[ .compdef | objects | { namespace: (.metadata.namespace // "krateo-system"), resource: (.status.resource // ""), version: (((.status.apiVersion // "") | split("/"))[1] // "") } | select(.resource != "" and .version != "") ]`)},
			Path:      `${ "/api/v1/namespaces/" + .namespace + "/configmaps/" + .resource + "-" + .version + "-jsonschema-configmap" }`,
		},
		{
			Name: "crd", Verb: ptr.To(http.MethodGet), Headers: []string{"Accept: application/json"},
			ContinueOnError: ptr.To(true), ErrorKey: s("crdError"),
			DependsOn: &templates.Dependency{Name: "compdef", Iterator: s(`[ .compdef | objects | { resource: (.status.resource // ""), group: (((.status.apiVersion // "") | split("/"))[0] // "") } | select(.resource != "" and .group != "") ]`)},
			Path:      `${ "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/" + .resource + "." + .group }`,
		},
		{
			Name: "namespaces", Verb: ptr.To(http.MethodGet), Headers: []string{"Accept: application/json"},
			DependsOn: &templates.Dependency{Name: "crd"},
			Filter:    s(`[.namespaces.items[] | .metadata.name]`),
			Path:      "/api/v1/namespaces",
			UserAccessFilter: &templates.UserAccessFilterSpec{Verb: "create", Group: "composition.krateo.io",
				ResourcesFrom: resourcesFrom, NamespaceFrom: ".metadata.name", NameFrom: ".metadata.name"},
		},
	}
}

func n767RBAC() []runtime.Object {
	subj := []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "u-767"}}
	return []runtime.Object{
		// u-767 may create fireworksapps in ns-a only.
		&rbacv1.Role{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
			ObjectMeta: metav1.ObjectMeta{Name: "creator", Namespace: "ns-a"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"create"}, APIGroups: []string{"composition.krateo.io"}, Resources: []string{"fireworksapps"}}},
		},
		&rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: "creator", Namespace: "ns-a"},
			Subjects:   subj,
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "creator"},
		},
		&rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: "reader"},
			Rules: []rbacv1.PolicyRule{{Verbs: []string{"get", "list"},
				APIGroups: []string{"", "core.krateo.io", "apiextensions.k8s.io"},
				Resources: []string{"namespaces", "configmaps", "compositiondefinitions", "customresourcedefinitions"}}},
		},
		&rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: "reader"},
			Subjects:   subj,
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "reader"},
		},
	}
}

type n767Result struct {
	namespaces any
	logs       string
	stageErrs  int64
	stageErr   string
}

// n767Resolve runs blueprint-formdef's api once through Resolve. compdef ""
// means the CompositionDefinition does not exist. Each call builds its own
// watcher and apiserver, and resets the process-global L1 on both sides, so
// one arm's compdef is never served to another.
func n767Resolve(t *testing.T, resourcesFrom string, extras map[string]any, compdef string) n767Result {
	t.Helper()
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	sch := runtime.NewScheme()
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1.AddToScheme: %v", err)
	}
	kinds := refilterRBACListKinds()
	kinds[schema.GroupVersionResource{Group: "core.krateo.io", Version: "v1alpha1", Resource: "compositiondefinitions"}] = "CompositionDefinitionList"
	kinds[schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}] = "CustomResourceDefinitionList"
	kinds[schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}] = "NamespaceList"
	kinds[schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}] = "ConfigMapList"
	obj := func(js string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{}
		if err := json.Unmarshal([]byte(js), &u.Object); err != nil {
			t.Fatalf("obj: %v", err)
		}
		return u
	}
	seed := append(n767RBAC(),
		obj(`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ns-a"}}`),
		obj(`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"ns-b"}}`),
		obj(`{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition","metadata":{"name":"fireworksapps.composition.krateo.io"}}`),
	)
	if compdef != "" {
		seed = append(seed, obj(compdef))
	}
	rw, err := cache.NewResourceWatcher(context.Background(), dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, kinds, seed...))
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	wctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(wctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == n767CompdefPath && compdef != "":
			fmt.Fprint(w, compdef)
		case r.URL.Path == "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/fireworksapps.composition.krateo.io":
			fmt.Fprint(w, `{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition","metadata":{"name":"fireworksapps.composition.krateo.io"}}`)
		case r.URL.Path == "/api/v1/namespaces/krateo-system/configmaps/fireworksapps-v1-2-0-jsonschema-configmap":
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"fireworksapps-v1-2-0-jsonschema-configmap","namespace":"krateo-system"},"data":{"values.schema.json":"{}"}}`)
		case r.URL.Path == "/api/v1/namespaces":
			fmt.Fprint(w, `{"kind":"NamespaceList","apiVersion":"v1","items":[{"metadata":{"name":"ns-a"}},{"metadata":{"name":"ns-b"}}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"kind":"Status","status":"Failure","code":404}`)
		}
	}))
	t.Cleanup(srv.Close)
	orig := serviceAccountEndpointFn
	serviceAccountEndpointFn = func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: srv.URL, Token: "sa-767"}, nil
	}
	t.Cleanup(func() { serviceAccountEndpointFn = orig })

	var logs n767Buf
	ctx := xcontext.BuildContext(ctxWithUser("u-767"), xcontext.WithLogger(n767Logger(&logs)))
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srv.URL})
	ctx, sink := cache.WithStageErrorSink(ctx)
	dict := Resolve(ctx, ResolveOptions{
		RC: &rest.Config{}, Items: n767API(resourcesFrom), Extras: extras,
		RESTActionNamespace: "krateo-system", RESTActionName: "blueprint-formdef", Provenance: ProvenanceStored,
	})
	stage, _ := sink.Sample()
	return n767Result{namespaces: dict["namespaces"], logs: logs.String(), stageErrs: sink.Count(), stageErr: stage}
}

var n767Named = map[string]any{"namespace": "krateo-system", "name": "bp-767"}

func n767AssertQuietDenyAll(t *testing.T, arm string, r n767Result) {
	t.Helper()
	ns, _ := json.Marshal(r.namespaces)
	if string(ns) != "[]" {
		t.Fatalf("N3 %s: namespaces=%s, want [] — the requester holds create in ns-a only for a resource that does not exist yet, so the empty set must deny it", arm, ns)
	}
	if r.stageErrs != 0 {
		t.Fatalf("N3 %s RED: stage-error sink=%d (stage %q) — a null element is still an unresolved resource set, so the answer is never cached", arm, r.stageErrs, r.stageErr)
	}
	for _, m := range []string{"ResourcesFrom yielded a non-string element", "resource set unresolved"} {
		if strings.Contains(r.logs, m) {
			t.Fatalf("N3 %s RED: %q still logged", arm, m)
		}
	}
	if !strings.Contains(r.logs, `"skipped_null":1`) {
		t.Fatalf("N3 %s: the skip's DEBUG line is missing", arm)
	}
}

func TestIssue767_N3a_NoExtras_KeepWarm(t *testing.T) {
	n767AssertQuietDenyAll(t, "no extras", n767Resolve(t, "[.compdef.status.resource]", nil, n767Ready))
}

func TestIssue767_N3b_OnlyName(t *testing.T) {
	n767AssertQuietDenyAll(t, "only name", n767Resolve(t, "[.compdef.status.resource]", map[string]any{"name": "bp-767"}, n767Ready))
}

func TestIssue767_N3c_NamedNotReconciled(t *testing.T) {
	n767AssertQuietDenyAll(t, "named, not reconciled", n767Resolve(t, "[.compdef.status.resource]", n767Named, n767NotReady))
}

func TestIssue767_N3d_NamedReconciled_Unchanged(t *testing.T) {
	r := n767Resolve(t, "[.compdef.status.resource]", n767Named, n767Ready)
	ns, _ := json.Marshal(r.namespaces)
	if string(ns) != `["ns-a"]` || r.stageErrs != 0 {
		t.Fatalf("N3 reconciled: namespaces=%s stage-errs=%d (%q), want [\"ns-a\"] and 0", ns, r.stageErrs, r.stageErr)
	}
	if strings.Contains(r.logs, "skipped_null") {
		t.Fatal("N3 reconciled: nothing was null, yet the skip logged")
	}
}

func TestIssue767_N4_NullBesideANonStringIsStillAStageError(t *testing.T) {
	r := n767Resolve(t, "[.compdef.status.resource, 1]", nil, n767Ready)
	ns, _ := json.Marshal(r.namespaces)
	if string(ns) != "[]" {
		t.Fatalf("N4: namespaces=%s, want [] (fail-closed)", ns)
	}
	if r.stageErrs == 0 || r.stageErr != "namespaces" {
		t.Fatalf("N4 RED: [null, 1] did not record the namespaces stage error (sink=%d stage=%q) — a skipped null masked a non-string element", r.stageErrs, r.stageErr)
	}
	if !strings.Contains(r.logs, "resource set unresolved") {
		t.Fatal("N4: the unresolved ERROR is missing")
	}
}
