package objects

// unserved_type_1414_test.go (#1414): what objects.Get answers when the read's type, or the
// object, is not there, and what it answers when the apiserver cannot say.
//
//   - the type is not served: 424 Failed Dependency, reason FailedDependency, and a message built
//     from the type's coordinates alone;
//   - the type is served and the object is missing: the apiserver's 404;
//   - the apiserver cannot say (unreachable, discovery failing, a group's backend down): 5xx;
//   - the reference is not an apiserver path (a group, version or resource that is not one path
//     segment): 400, and nothing is sent.
//
// The route is the real one, end to end: Get (cache off) → getFromAPIServer → the requester's
// client config → dynamic.NewClient, whose RESTMapper discovers the API surface before the object
// GET. The apiserver is internal/testsupport/unservedapi, which answers as kube-apiserver does on
// the wire and records every request with how it authenticated.

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/testsupport/unservedapi"
)

const (
	widgetsGroup = unservedapi.Group
	widgetsGV    = unservedapi.GV
)

// get1414 reads ref through objects.Get under a requester endpoint (a bearer token, the only
// credential the apiserver accepts) pointing at the test apiserver.
func get1414(t *testing.T, a *unservedapi.Server, closeFirst bool, ref templatesv1.ObjectReference) Result {
	t.Helper()
	prev := env.TestMode()
	env.SetTestMode(true) // keep the endpoint's server URL as given
	t.Cleanup(func() { env.SetTestMode(prev) })
	t.Setenv("CACHE_ENABLED", "") // Get takes the apiserver path
	t.Setenv("DEBUG", "false")

	a.Token = randomToken(t)
	srv := httptest.NewTLSServer(a)
	t.Cleanup(srv.Close)
	ca := base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	ep := endpoints.Endpoint{ServerURL: srv.URL, CertificateAuthorityData: ca, Username: "requester", Token: a.Token}
	if closeFirst {
		srv.Close() // the address now refuses connections
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(logger), xcontext.WithUserConfig(ep))
	res := Get(ctx, ref)

	// Whatever the read answered, on every arm: each request carried the requester's credential
	// and asked for no impersonation, and nothing but discovery documents and the path of one
	// namespaced object was ever requested.
	for _, r := range a.Requests() {
		if r.Auth != "bearer-match" || r.Impersonates {
			t.Errorf("%s %s: Authorization=%s impersonates=%v, want the requester's bearer token and no impersonation",
				r.Method, r.Path, r.Auth, r.Impersonates)
		}
	}
	if _, malformed := a.ObjectPaths(); len(malformed) > 0 {
		t.Errorf("the apiserver received paths that are not one object's: %v", malformed)
	}
	return res
}

func ref1414(apiVersion, resource, name string) templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference: templatesv1.Reference{Name: name, Namespace: "ns"}, APIVersion: apiVersion, Resource: resource,
	}
}

// (a) A read of a type the apiserver does not serve is a 424 Failed Dependency: never a 500, and
// never the 404 of a missing object. Under both discovery forms, and whether the group is served
// without the resource (a widgets group missing one CRD, the #1414 case) or not served at all.
// The message names the type and nothing else.
func TestUnservedType_Is424(t *testing.T) {
	for _, c := range []struct {
		name   string
		legacy bool
		ref    templatesv1.ObjectReference
		want   string
	}{
		{"group_served_resource_not", false, ref1414(widgetsGV, "flexes", "header-context"),
			"The type flexes (widgets.templates.krateo.io/v1beta1) is not installed on this cluster."},
		{"group_served_resource_not_legacy_discovery", true, ref1414(widgetsGV, "flexes", "header-context"),
			"The type flexes (widgets.templates.krateo.io/v1beta1) is not installed on this cluster."},
		{"version_not_served", false, ref1414(widgetsGroup+"/v1", "buttons", "header-context"),
			"The type buttons (widgets.templates.krateo.io/v1) is not installed on this cluster."},
		{"group_not_served", false, ref1414("absent.example.org/v1", "things", "header-context"),
			"The type things (absent.example.org/v1) is not installed on this cluster."},
		{"group_not_served_legacy_discovery", true, ref1414("absent.example.org/v1", "things", "header-context"),
			"The type things (absent.example.org/v1) is not installed on this cluster."},
		// The core group has no name: the version stands alone.
		{"core_resource_not_served", false, ref1414("v1", "things", "header-context"),
			"The type things (v1) is not installed on this cluster."},
		{"core_resource_not_served_legacy_discovery", true, ref1414("v1", "things", "header-context"),
			"The type things (v1) is not installed on this cluster."},
		// A subresource is not a type: pods/log is in the core document, "log" is not served.
		{"a_subresource_name_is_not_a_type", true, ref1414("v1", "log", "header-context"),
			"The type log (v1) is not installed on this cluster."},
		// The name as the caller gave it, when no type answers to it (see the resource-name test
		// for a name that does).
		{"singular_upper_case_of_an_unserved_type", false, ref1414(widgetsGV, "Flex", "header-context"),
			"The type Flex (widgets.templates.krateo.io/v1beta1) is not installed on this cluster."},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := &unservedapi.Server{LegacyDiscovery: c.legacy}
			res := get1414(t, a, false, c.ref)
			if res.Err == nil {
				t.Fatalf("the read of an unserved type succeeded")
			}
			t.Logf("answer: code=%d reason=%q message=%q; requests: %v", res.Err.Code, res.Err.Reason, res.Err.Message, a.Paths())
			if res.Err.Code != http.StatusFailedDependency {
				t.Fatalf("an unserved type must answer 424, got %d: %s", res.Err.Code, res.Err.Message)
			}
			if string(res.Err.Reason) != "FailedDependency" {
				t.Errorf("reason = %q, want FailedDependency", res.Err.Reason)
			}
			if res.Err.Status != "Failure" || res.Err.Kind != "Status" || res.Err.APIVersion != "v1" {
				t.Errorf("the answer must be a Failure Status, got kind=%q apiVersion=%q status=%q", res.Err.Kind, res.Err.APIVersion, res.Err.Status)
			}
			if res.Err.Message != c.want {
				t.Errorf("message = %q, want exactly %q", res.Err.Message, c.want)
			}
			// The message is shown to users: it never names the object, its namespace or the
			// requester, and carries none of the apiserver's text.
			for _, leak := range []string{c.ref.Name, " ns", "ns/", "requester", "not found", "could not find"} {
				if strings.Contains(res.Err.Message, leak) {
					t.Errorf("the message carries %q: %q", leak, res.Err.Message)
				}
			}
			if res.Unstructured != nil {
				t.Errorf("a 424 carries no object")
			}
			// The evidence is the group/version's own discovery document: no object is requested.
			if all, _ := a.ObjectPaths(); len(all) != 0 {
				t.Errorf("an unserved type must not be requested from the apiserver, saw %v", all)
			}
		})
	}
}

// (b) When the apiserver cannot say whether the type is served, the answer is a 5xx, never a 424
// and never a 404: an outage must not read as "not installed" or "not there".
func TestUnservedType_DiscoveryOrBackendFailureIsNever424Or404(t *testing.T) {
	flexes := ref1414(widgetsGV, "flexes", "header-context")
	button := ref1414(widgetsGV, "buttons", "x")
	for _, c := range []struct {
		name       string
		a          *unservedapi.Server
		closeFirst bool
		ref        templatesv1.ObjectReference
	}{
		{"apiserver_unreachable", &unservedapi.Server{}, true, flexes},
		{"discovery_answers_503", &unservedapi.Server{DiscoveryDown: true}, false, flexes},
		{"discovery_answers_503_legacy", &unservedapi.Server{DiscoveryDown: true, LegacyDiscovery: true}, false, flexes},
		// The group's backend is down. Discovery drops the group (legacy: its document is a 503;
		// aggregated: it is Stale), so the mapper has no mapping for a resource that IS served
		// when the backend is up.
		{"group_backend_down_legacy_served_resource", &unservedapi.Server{WidgetsBackendOff: true, LegacyDiscovery: true}, false, button},
		{"group_backend_down_legacy_unserved_resource", &unservedapi.Server{WidgetsBackendOff: true, LegacyDiscovery: true}, false, flexes},
		{"group_backend_down_aggregated_served_resource", &unservedapi.Server{WidgetsBackendOff: true}, false, button},
		{"group_backend_down_aggregated_unserved_resource", &unservedapi.Server{WidgetsBackendOff: true}, false, flexes},
		// The group's own document cannot be read while its resources are served: nothing says
		// whether the type is served.
		{"group_document_down_legacy_unserved_resource", &unservedapi.Server{LegacyDiscovery: true, WidgetsDiscoveryLags: true, WidgetsDocFailures: -1}, false, flexes},
		{"group_document_down_aggregated_unserved_resource", &unservedapi.Server{WidgetsDiscoveryLags: true, WidgetsDocFailures: -1}, false, flexes},
		{"group_document_down_aggregated_missing_object", &unservedapi.Server{WidgetsDiscoveryLags: true, WidgetsDocFailures: -1}, false, ref1414(widgetsGV, "buttons", "gone")},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := get1414(t, c.a, c.closeFirst, c.ref)
			if res.Err == nil {
				t.Fatalf("the read succeeded against a failing apiserver")
			}
			t.Logf("answer: code=%d reason=%q message=%q; requests: %v", res.Err.Code, res.Err.Reason, res.Err.Message, c.a.Paths())
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
				res := get1414(t, &unservedapi.Server{LegacyDiscovery: legacy}, false, ref)
				if res.Err != nil || res.Unstructured == nil || res.Unstructured.GetName() != "x" {
					t.Fatalf("%s/%s: a served object must be read, got err=%v", ref.APIVersion, ref.Resource, res.Err)
				}
			}
			for ref, want := range map[templatesv1.ObjectReference]string{
				ref1414(widgetsGV, "buttons", "gone"): `buttons.` + widgetsGroup + ` "gone" not found`,
				ref1414("v1", "configmaps", "gone"):   `configmaps "gone" not found`,
			} {
				res := get1414(t, &unservedapi.Server{LegacyDiscovery: legacy}, false, ref)
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

// (c2) The mapper has no mapping for a SERVED type, because the group's discovery is behind its
// serving. A miss is then not evidence of anything: the group's own document says the type is
// served, so the object is read and a missing object is the apiserver's 404. Never a 424.
func TestUnservedType_AMapperMissOnAServedTypeIsNever424(t *testing.T) {
	for _, c := range []struct {
		name string
		a    func() *unservedapi.Server
	}{
		// The mapper's read of the group's document fails; the next one answers.
		{"legacy_document_recovers", func() *unservedapi.Server {
			return &unservedapi.Server{LegacyDiscovery: true, WidgetsDiscoveryLags: true, WidgetsDocFailures: 1}
		}},
		{"aggregated_stale", func() *unservedapi.Server {
			return &unservedapi.Server{WidgetsDiscoveryLags: true}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			// PRECONDITION: the mapper really misses here. The unserved resource of the same
			// group would otherwise be answered from the mapper; on the miss route the group's
			// own document is read once more than the mapper read it.
			a := c.a()
			res := get1414(t, a, false, ref1414(widgetsGV, "buttons", "x"))
			if res.Err != nil || res.Unstructured == nil || res.Unstructured.GetName() != "x" {
				t.Fatalf("a served object must be read through a mapper miss, got err=%+v; requests: %v", res.Err, a.Paths())
			}
			docReads := 0
			for _, p := range a.Paths() {
				if p == "/apis/"+widgetsGV {
					docReads++
				}
			}
			if wantReads := map[bool]int{true: 2, false: 1}[a.LegacyDiscovery]; docReads != wantReads {
				t.Fatalf("PRECONDITION: the group's own document was read %d time(s), want %d: this arm must run on the mapper-miss route; requests: %v",
					docReads, wantReads, a.Paths())
			}

			a = c.a()
			res = get1414(t, a, false, ref1414(widgetsGV, "buttons", "gone"))
			if res.Err == nil {
				t.Fatalf("the read of a missing object succeeded")
			}
			t.Logf("answer: code=%d reason=%q message=%q; requests: %v", res.Err.Code, res.Err.Reason, res.Err.Message, a.Paths())
			if res.Err.Code == http.StatusFailedDependency {
				t.Fatalf("a served type's missing object was answered 424 %q", res.Err.Message)
			}
			if res.Err.Code != http.StatusNotFound {
				t.Fatalf("a served type's missing object must answer 404, got %d: %s", res.Err.Code, res.Err.Message)
			}
			if want := `buttons.` + widgetsGroup + ` "gone" not found`; res.Err.Message != want {
				t.Errorf("404 message = %q, want the apiserver's %q", res.Err.Message, want)
			}
		})
	}
}

// (d) A resource named in the singular or with upper case. The mapper resolves such a name to
// the served resource; the miss route resolves it the same way from the group's own document, so
// the read reaches the same object on either route, at the resource's own path.
func TestUnservedType_SingularAndUpperCaseResourceNames(t *testing.T) {
	const want = "/apis/" + widgetsGV + "/namespaces/ns/buttons/x"
	for _, resource := range []string{"button", "Button", "BUTTONS"} {
		for _, c := range []struct {
			name string
			a    *unservedapi.Server
		}{
			{"mapped", &unservedapi.Server{}},
			{"mapper_miss", &unservedapi.Server{WidgetsDiscoveryLags: true}},
		} {
			t.Run(resource+"_"+c.name, func(t *testing.T) {
				res := get1414(t, c.a, false, ref1414(widgetsGV, resource, "x"))
				if res.Err != nil || res.Unstructured == nil || res.Unstructured.GetName() != "x" {
					t.Fatalf("resource=%s must read the served object, got err=%+v; requests: %v", resource, res.Err, c.a.Paths())
				}
				if all, _ := c.a.ObjectPaths(); len(all) != 1 || all[0] != want {
					t.Fatalf("resource=%s dialled %v, want exactly [%s]", resource, all, want)
				}
			})
		}
	}
}

// (e) A reference whose group, version or resource is not one apiserver path segment is refused
// with a 400 before anything is sent: no discovery, no dial. Each of these would otherwise leave
// the resource it names; the test apiserver serves every target, so an escape would succeed.
func TestUnservedType_AReferenceThatIsNotOnePathIsRefusedBeforeAnyRequest(t *testing.T) {
	for _, c := range []struct {
		name string
		ref  templatesv1.ObjectReference
	}{
		{"resource_traverses_to_another_namespace", ref1414(widgetsGV, "../../../../../api/v1/namespaces/other/configmaps", "x")},
		{"resource_names_a_subresource", ref1414("v1", "pods/p", "log")},
		{"resource_names_a_proxy", ref1414("v1", "services/svc:80/proxy", "metrics")},
		{"resource_traverses_to_a_non_resource_path", ref1414("v1", "../../../../metrics", "x")},
		{"resource_is_dot_dot", ref1414(widgetsGV, "..", "x")},
		{"resource_is_percent_encoded", ref1414(widgetsGV, "buttons%2Fx", "x")},
		{"version_traverses", ref1414(widgetsGroup+"/..", "buttons", "x")},
		{"group_is_dot_dot", ref1414("../v1", "configmaps", "x")},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := &unservedapi.Server{}
			res := get1414(t, a, false, c.ref)
			if res.Err == nil {
				t.Fatalf("the read succeeded: %v; requests: %v", res.Unstructured.Object, a.Paths())
			}
			if res.Err.Code != http.StatusBadRequest {
				t.Errorf("code = %d (%s), want 400", res.Err.Code, res.Err.Message)
			}
			if got := a.Paths(); len(got) != 0 {
				t.Fatalf("nothing may be sent for such a reference, the apiserver saw %v", got)
			}
		})
	}
}
