//go:build unit || integration

package api

// issue759_uaf_no_filter_reread_test.go — #759 (SECURITY) RED-first arms.
//
// THE DEFECT (main c5af86bf, handler.go jsonHandlerCore): the stage's raw
// value is put at pig[key], the userAccessFilter refilter narrows pig, and the
// merge then evaluates the stage filter on pig — but with NO stage filter it
// merged tmp, the value from BEFORE the refilter. Whenever the refilter
// REPLACED pig[key] instead of narrowing it in place, tmp was still the raw
// service-account response, and the requester received objects their RBAC
// denies. The replace branches: UserInfo missing and resourcesFrom unresolved
// (the canonical empty list), a denied single object ({}), and a bare-array
// response (the narrowed slice — on the SUCCESS path too). Only the {items}
// envelope narrowed in place was safe.
//
// THE ARMS, each a stage with a userAccessFilter and NO filter:
//   1. UserInfo missing                    → {"items":[]}
//   2. resourcesFrom unresolved (jq error) → {"items":[]}
//   3. a denied single object, in the shape of the one reachable live case: a
//      GET-by-name of a namespaced custom resource (an agent) the requester
//      has no grant on → {} — at the handler and through Resolve;
//   4. a bare-array response, one item granted and one not → the granted one.
// CONTROL: the same four with a stage filter (jq on pig) — unchanged by the
// fix, and the proof the arms discriminate the no-filter merge only.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

const a759Sentinel = "RAW-SA-SENTINEL-759"

const a759AgentPath = "/apis/kagent.dev/v1alpha2/namespaces/team-a/agents/agent-x"

// a759Agent is a by-name custom-resource object the requester is not granted.
var a759Agent = fmt.Sprintf(`{"apiVersion":"kagent.dev/v1alpha2","kind":"Agent",`+
	`"metadata":{"name":"agent-x","namespace":"team-a"},"spec":{"systemMessage":%q}}`, a759Sentinel)

var a759List = fmt.Sprintf(`{"kind":"AnyPluralList","apiVersion":"example.test/v1","items":[`+
	`{"metadata":{"name":"item-a","namespace":"ns-a"}},`+
	`{"metadata":{"name":"item-b","namespace":"ns-b"},"secret":%q}]}`, a759Sentinel)

var a759BareArray = fmt.Sprintf(`[`+
	`{"metadata":{"name":"item-a","namespace":"ns-a"}},`+
	`{"metadata":{"name":"item-b","namespace":"ns-b"},"secret":%q}]`, a759Sentinel)

// a759RBAC grants user "u759" list/get on example.test/anyplural in ns-a only.
// Nothing grants kagent.dev agents to anyone.
func a759RBAC(t *testing.T) {
	t.Helper()
	newRefilterTestWatcher(t, a759RBACObjects()...)
}

func a759RBACObjects() []runtime.Object {
	return []runtime.Object{
		&rbacv1.Role{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
			ObjectMeta: metav1.ObjectMeta{Name: "anyplural-reader", Namespace: "ns-a"},
			Rules: []rbacv1.PolicyRule{{
				Verbs: []string{"list", "get"}, APIGroups: []string{"example.test"}, Resources: []string{"anyplural"},
			}},
		},
		&rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: "anyplural-reader", Namespace: "ns-a"},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "u759"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "anyplural-reader"},
		},
	}
}

// a759Watcher is newRefilterTestWatcher whose fake apiserver also serves the
// agents resource, seeded with a759RBACObjects plus objs.
func a759Watcher(t *testing.T, objs ...runtime.Object) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	sch := runtime.NewScheme()
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1.AddToScheme: %v", err)
	}
	kinds := refilterRBACListKinds()
	kinds[schema.GroupVersionResource{Group: "kagent.dev", Version: "v1alpha2", Resource: "agents"}] = "AgentList"
	seed := append(a759RBACObjects(), objs...)
	rw, err := cache.NewResourceWatcher(context.Background(), dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, kinds, seed...))
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
}

type a759Case struct {
	name string
	ctx  func() context.Context
	uaf  *templates.UserAccessFilterSpec
	raw  string
	// want is the stage value the requester may see; filter, when set, is the
	// with-filter control's projection and wantFiltered its result.
	want         string
	filter       string
	wantFiltered string
}

func a759Cases() []a759Case {
	listUAF := &templates.UserAccessFilterSpec{Verb: "list", Group: "example.test", Resource: "anyplural", NamespaceFrom: ".metadata.namespace"}
	return []a759Case{
		{
			name: "1_userinfo_missing", ctx: context.Background, uaf: listUAF, raw: a759List,
			want: `{"items":[]}`, filter: `.stage.items`, wantFiltered: `[]`,
		},
		{
			name: "2_resources_from_unresolved", ctx: func() context.Context { return ctxWithUser("u759") },
			uaf: &templates.UserAccessFilterSpec{Verb: "list", Group: "example.test",
				ResourcesFrom: "[ .crds[] | .plural + 1 ]", NamespaceFrom: ".metadata.namespace"},
			raw: a759List, want: `{"items":[]}`, filter: `.stage.items`, wantFiltered: `[]`,
		},
		{
			name: "3_denied_single_object_by_name", ctx: func() context.Context { return ctxWithUser("u759") },
			uaf: &templates.UserAccessFilterSpec{Verb: "get", Group: "kagent.dev", Resource: "agents",
				NamespaceFrom: ".metadata.namespace", NameFrom: ".metadata.name"},
			raw: a759Agent, want: `{}`, filter: `.stage`, wantFiltered: `{}`,
		},
		{
			name: "4_bare_array_mixed", ctx: func() context.Context { return ctxWithUser("u759") }, uaf: listUAF,
			raw:          a759BareArray,
			want:         `[{"metadata":{"name":"item-a","namespace":"ns-a"}}]`,
			filter:       `[.stage[] | .metadata.name]`,
			wantFiltered: `["item-a"]`,
		},
	}
}

func a759Handle(t *testing.T, c a759Case, filter *string) string {
	t.Helper()
	dict := map[string]any{"crds": []any{map[string]any{"plural": "anyplural"}}}
	h := jsonHandlerBytes(c.ctx(), jsonHandlerOptions{
		key: "stage", out: dict, dict: dict, filter: filter, uaf: c.uaf, apiCallName: "stage",
	})
	if err := h([]byte(c.raw)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	b, err := json.Marshal(dict["stage"])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

// TestIssue759_UAFStageWithNoFilterNeverServesTheRawResponse — RED on c5af86bf.
func TestIssue759_UAFStageWithNoFilterNeverServesTheRawResponse(t *testing.T) {
	a759RBAC(t)
	for _, c := range a759Cases() {
		t.Run(c.name, func(t *testing.T) {
			got := a759Handle(t, c, nil)
			if strings.Contains(got, a759Sentinel) {
				t.Fatalf("#759 RED: the raw service-account response was served on a UAF stage with no filter: %s", got)
			}
			if got != c.want {
				t.Fatalf("stage value = %s, want %s", got, c.want)
			}
		})
	}
}

// TestIssue759_UAFStageWithAFilter_Control — the jq-on-pig path, correct on
// main and after: the arms above discriminate the no-filter merge only.
func TestIssue759_UAFStageWithAFilter_Control(t *testing.T) {
	a759RBAC(t)
	for _, c := range a759Cases() {
		t.Run(c.name, func(t *testing.T) {
			got := a759Handle(t, c, ptr.To(c.filter))
			if strings.Contains(got, a759Sentinel) || got != c.wantFiltered {
				t.Fatalf("control: stage value = %s, want %s", got, c.wantFiltered)
			}
		})
	}
}

// TestIssue759_DeniedAgentByName_ThroughResolve drives arm 3's live shape end
// to end: a RESTAction step GETting one agent by name on the apiserver path,
// userAccessFilter, no filter, the requester not granted. The agent is in the
// watcher's store, so the GET is served the way production serves it (the
// informer); the service-account endpoint seam is only the fall-through.
func TestIssue759_DeniedAgentByName_ThroughResolve(t *testing.T) {
	var agent unstructured.Unstructured
	if err := json.Unmarshal([]byte(a759Agent), &agent.Object); err != nil {
		t.Fatalf("agent: %v", err)
	}
	a759Watcher(t, &agent)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == a759AgentPath {
			fmt.Fprint(w, a759Agent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"kind":"Status","status":"Failure","code":404}`)
	}))
	t.Cleanup(srv.Close)
	orig := serviceAccountEndpointFn
	serviceAccountEndpointFn = func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: srv.URL, Token: "sa-759"}, nil
	}
	t.Cleanup(func() { serviceAccountEndpointFn = orig })
	t.Setenv("CLIENT_MAX_RETRIES", "0")

	ctx := cache.WithInternalEndpoint(ctxWithUser("u759"), &endpoints.Endpoint{ServerURL: srv.URL})
	dict := Resolve(ctx, ResolveOptions{
		RC: &rest.Config{},
		Items: []*templates.API{{
			Name: "agent", Path: a759AgentPath, Verb: ptr.To(http.MethodGet),
			UserAccessFilter: &templates.UserAccessFilterSpec{Verb: "get", Group: "kagent.dev", Resource: "agents",
				NamespaceFrom: ".metadata.namespace", NameFrom: ".metadata.name"},
		}},
		RESTActionNamespace: "krateo-system",
		RESTActionName:      "ra-759",
		Provenance:          ProvenanceStored,
	})
	b, _ := json.Marshal(dict)
	if strings.Contains(string(b), a759Sentinel) {
		t.Fatalf("#759 RED: a requester with no grant on the agent was served it: %s", b)
	}
	got, _ := json.Marshal(dict["agent"])
	if string(got) != `{}` {
		t.Fatalf("dict[agent] = %s, want {} (dict keys %v)", got, keysOf(dict))
	}
}
