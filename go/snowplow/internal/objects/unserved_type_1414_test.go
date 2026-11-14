package objects

// unserved_type_1414_test.go (#1414): what objects.Get answers when the read's type, or the
// object, is not there, and what it answers when the apiserver cannot say.
//
// The route is the real one, end to end: Get (cache off) → getFromAPIServer → the requester's
// client config → dynamic.NewClient, whose RESTMapper discovers the API surface before the object
// GET. The apiserver is an httptest server that answers as kube-apiserver does on the wire:
//
//   - discovery is the aggregated document (apidiscovery.k8s.io/v2 APIGroupDiscoveryList) when the
//     request's Accept asks for it, as every apiserver since 1.27 answers; with legacyDiscovery it
//     is the unaggregated documents (APIVersions, APIGroupList, APIResourceList), as an apiserver
//     with the aggregated endpoint off answers;
//   - a path no handler serves (an unserved group, version or resource) is the apiserver's
//     `404 page not found` in text/plain, not a Status;
//   - a served resource's missing object is the apiserver's NotFound Status in JSON;
//   - a group whose aggregated backend is down answers 503 `service unavailable` in text/plain on
//     its discovery document and on its resources alike (the aggregator's proxy error), and is
//     marked Stale in the aggregated document.

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

const (
	widgetsGroup   = "widgets.templates.krateo.io"
	widgetsGV      = widgetsGroup + "/v1beta1"
	aggregatedType = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList"
)

// apiserver1414 is the test apiserver. It serves core/v1 configmaps and widgets…/v1beta1 buttons,
// one object of each (ns/x); nothing else. flexes has no CRD.
type apiserver1414 struct {
	legacyDiscovery   bool // answer the unaggregated discovery documents only
	discoveryDown     bool // /api and /apis answer 503
	widgetsBackendOff bool // the widgets group's backend is down: 503 on its discovery and resources

	mu   sync.Mutex
	seen []string // the request paths, in order
}

func (a *apiserver1414) paths() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.seen...)
}

func (a *apiserver1414) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.seen = append(a.seen, r.URL.Path)
	a.mu.Unlock()

	plain := func(code int, body string) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}
	json := func(code int, contentType, body string) {
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}
	aggregated := !a.legacyDiscovery && strings.Contains(r.Header.Get("Accept"), "g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList")
	verbs := `["create","delete","deletecollection","get","list","patch","update","watch"]`
	underWidgets := strings.HasPrefix(r.URL.Path, "/apis/"+widgetsGV)

	switch {
	case a.discoveryDown && (r.URL.Path == "/api" || r.URL.Path == "/apis"):
		plain(http.StatusServiceUnavailable, "service unavailable\n")

	case r.URL.Path == "/api" && aggregated:
		json(http.StatusOK, aggregatedType, `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[`+
			`{"metadata":{"creationTimestamp":null},"versions":[{"version":"v1","resources":[`+
			`{"resource":"configmaps","responseKind":{"group":"","version":"","kind":"ConfigMap"},"scope":"Namespaced","singularResource":"configmap","verbs":`+verbs+`,"shortNames":["cm"]}`+
			`],"freshness":"Current"}]}]}`)
	case r.URL.Path == "/apis" && aggregated:
		freshness := "Current"
		if a.widgetsBackendOff {
			// The aggregator keeps the group in the document with what it last knew, marked Stale.
			freshness = "Stale"
		}
		json(http.StatusOK, aggregatedType, `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[`+
			`{"metadata":{"name":"`+widgetsGroup+`","creationTimestamp":null},"versions":[{"version":"v1beta1","resources":[`+
			`{"resource":"buttons","responseKind":{"group":"`+widgetsGroup+`","version":"v1beta1","kind":"Button"},"scope":"Namespaced","singularResource":"button","verbs":`+verbs+`,"categories":["widgets","krateo"]}`+
			`],"freshness":"`+freshness+`"}]}]}`)

	case r.URL.Path == "/api":
		json(http.StatusOK, "application/json", `{"kind":"APIVersions","versions":["v1"],"serverAddressByClientCIDRs":[{"clientCIDR":"0.0.0.0/0","serverAddress":"10.0.0.1:443"}]}`)
	case r.URL.Path == "/apis":
		json(http.StatusOK, "application/json", `{"kind":"APIGroupList","apiVersion":"v1","groups":[{"name":"`+widgetsGroup+`",`+
			`"versions":[{"groupVersion":"`+widgetsGV+`","version":"v1beta1"}],"preferredVersion":{"groupVersion":"`+widgetsGV+`","version":"v1beta1"}}]}`)
	case r.URL.Path == "/api/v1":
		json(http.StatusOK, "application/json", `{"kind":"APIResourceList","groupVersion":"v1","resources":[`+
			`{"name":"configmaps","singularName":"configmap","namespaced":true,"kind":"ConfigMap","verbs":`+verbs+`,"shortNames":["cm"]}]}`)

	case underWidgets && a.widgetsBackendOff:
		plain(http.StatusServiceUnavailable, "service unavailable\n")
	case r.URL.Path == "/apis/"+widgetsGV:
		json(http.StatusOK, "application/json", `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"`+widgetsGV+`","resources":[`+
			`{"name":"buttons","singularName":"button","namespaced":true,"kind":"Button","verbs":`+verbs+`,"categories":["widgets","krateo"]}]}`)

	case r.URL.Path == "/api/v1/namespaces/ns/configmaps/x":
		json(http.StatusOK, "application/json", `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x","namespace":"ns"}}`)
	case r.URL.Path == "/apis/"+widgetsGV+"/namespaces/ns/buttons/x":
		json(http.StatusOK, "application/json", `{"apiVersion":"`+widgetsGV+`","kind":"Button","metadata":{"name":"x","namespace":"ns"}}`)
	case strings.HasPrefix(r.URL.Path, "/apis/"+widgetsGV+"/namespaces/ns/buttons/"):
		name := strings.TrimPrefix(r.URL.Path, "/apis/"+widgetsGV+"/namespaces/ns/buttons/")
		json(http.StatusNotFound, "application/json", `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",`+
			`"message":"buttons.`+widgetsGroup+` \"`+name+`\" not found","reason":"NotFound",`+
			`"details":{"name":"`+name+`","group":"`+widgetsGroup+`","kind":"buttons"},"code":404}`)
	case strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/ns/configmaps/"):
		name := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/ns/configmaps/")
		json(http.StatusNotFound, "application/json", `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",`+
			`"message":"configmaps \"`+name+`\" not found","reason":"NotFound","details":{"name":"`+name+`","kind":"configmaps"},"code":404}`)

	default:
		plain(http.StatusNotFound, "404 page not found\n")
	}
}

// get1414 reads ref through objects.Get under a requester endpoint pointing at the test apiserver.
func get1414(t *testing.T, a *apiserver1414, closeFirst bool, ref templatesv1.ObjectReference) Result {
	t.Helper()
	prev := env.TestMode()
	env.SetTestMode(true) // keep the endpoint's server URL as given
	t.Cleanup(func() { env.SetTestMode(prev) })
	t.Setenv("CACHE_ENABLED", "") // Get takes the apiserver path
	t.Setenv("DEBUG", "false")

	srv := httptest.NewTLSServer(a)
	t.Cleanup(srv.Close)
	ca := base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	ep := endpoints.Endpoint{ServerURL: srv.URL, CertificateAuthorityData: ca, Username: "requester", Token: randomToken(t)}
	if closeFirst {
		srv.Close() // the address now refuses connections
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(logger), xcontext.WithUserConfig(ep))
	return Get(ctx, ref)
}

func ref1414(apiVersion, resource, name string) templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference: templatesv1.Reference{Name: name, Namespace: "ns"}, APIVersion: apiVersion, Resource: resource,
	}
}

// (a) A read of a type the apiserver does not serve is a 404, with the apiserver's own meaning,
// never a 500. Under both discovery forms, and whether the group is served without the resource
// (a widgets group missing one CRD, the #1414 case) or not served at all.
func TestUnservedType_Is404(t *testing.T) {
	for _, c := range []struct {
		name   string
		legacy bool
		ref    templatesv1.ObjectReference
	}{
		{"group_served_resource_not", false, ref1414(widgetsGV, "flexes", "header-context")},
		{"group_served_resource_not_legacy_discovery", true, ref1414(widgetsGV, "flexes", "header-context")},
		{"version_not_served", false, ref1414(widgetsGroup+"/v1", "buttons", "x")},
		{"group_not_served", false, ref1414("absent.example.org/v1", "things", "x")},
		{"group_not_served_legacy_discovery", true, ref1414("absent.example.org/v1", "things", "x")},
		{"core_resource_not_served", false, ref1414("v1", "things", "x")},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := &apiserver1414{legacyDiscovery: c.legacy}
			res := get1414(t, a, false, c.ref)
			if res.Err == nil {
				t.Fatalf("the read of an unserved type succeeded")
			}
			t.Logf("answer: code=%d reason=%q message=%q; requests: %v", res.Err.Code, res.Err.Reason, res.Err.Message, a.paths())
			if res.Err.Code != http.StatusNotFound {
				t.Fatalf("an unserved type must answer 404, got %d: %s", res.Err.Code, res.Err.Message)
			}
			if !strings.Contains(res.Err.Message, "could not find the requested resource") {
				t.Errorf("the 404 must carry the apiserver's meaning (the resource is not served), got %q", res.Err.Message)
			}
			if res.Unstructured != nil {
				t.Errorf("a 404 carries no object")
			}
		})
	}
}

// (b) When the apiserver cannot say whether the type is served, the answer is a 5xx, never a 404:
// an outage must not read as "not there".
func TestUnservedType_DiscoveryOrBackendFailureIsNever404(t *testing.T) {
	flexes := ref1414(widgetsGV, "flexes", "header-context")
	button := ref1414(widgetsGV, "buttons", "x")
	for _, c := range []struct {
		name       string
		a          *apiserver1414
		closeFirst bool
		ref        templatesv1.ObjectReference
	}{
		{"apiserver_unreachable", &apiserver1414{}, true, flexes},
		{"discovery_answers_503", &apiserver1414{discoveryDown: true}, false, flexes},
		{"discovery_answers_503_legacy", &apiserver1414{discoveryDown: true, legacyDiscovery: true}, false, flexes},
		// The group's backend is down. Legacy discovery drops the group (its document is a 503),
		// so the mapper has no mapping for a resource that IS served when the backend is up.
		{"group_backend_down_legacy_served_resource", &apiserver1414{widgetsBackendOff: true, legacyDiscovery: true}, false, button},
		{"group_backend_down_legacy_unserved_resource", &apiserver1414{widgetsBackendOff: true, legacyDiscovery: true}, false, flexes},
		{"group_backend_down_aggregated_served_resource", &apiserver1414{widgetsBackendOff: true}, false, button},
		{"group_backend_down_aggregated_unserved_resource", &apiserver1414{widgetsBackendOff: true}, false, flexes},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := get1414(t, c.a, c.closeFirst, c.ref)
			if res.Err == nil {
				t.Fatalf("the read succeeded against a failing apiserver")
			}
			t.Logf("answer: code=%d reason=%q message=%q; requests: %v", res.Err.Code, res.Err.Reason, res.Err.Message, c.a.paths())
			if res.Err.Code < 500 || res.Err.Code > 599 {
				t.Fatalf("a failing apiserver must answer 5xx, got %d: %s", res.Err.Code, res.Err.Message)
			}
		})
	}
}

// (c) A served type's missing object is still the apiserver's 404; and the instrument can say
// yes: the served objects are read.
func TestUnservedType_ServedTypeKeepsItsAnswers(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "aggregated_discovery"
		if legacy {
			name = "legacy_discovery"
		}
		t.Run(name, func(t *testing.T) {
			for _, ref := range []templatesv1.ObjectReference{ref1414(widgetsGV, "buttons", "x"), ref1414("v1", "configmaps", "x")} {
				res := get1414(t, &apiserver1414{legacyDiscovery: legacy}, false, ref)
				if res.Err != nil || res.Unstructured == nil || res.Unstructured.GetName() != "x" {
					t.Fatalf("%s/%s: a served object must be read, got err=%v", ref.APIVersion, ref.Resource, res.Err)
				}
			}
			for ref, want := range map[templatesv1.ObjectReference]string{
				ref1414(widgetsGV, "buttons", "gone"): `buttons.` + widgetsGroup + ` "gone" not found`,
				ref1414("v1", "configmaps", "gone"):   `configmaps "gone" not found`,
			} {
				res := get1414(t, &apiserver1414{legacyDiscovery: legacy}, false, ref)
				if res.Err == nil || res.Err.Code != http.StatusNotFound {
					t.Fatalf("%s/%s: a missing object must answer 404, got %+v", ref.APIVersion, ref.Resource, res.Err)
				}
				if res.Err.Message != want {
					t.Errorf("%s/%s: 404 message = %q, want the apiserver's %q", ref.APIVersion, ref.Resource, res.Err.Message, want)
				}
			}
		})
	}
}
