package dynamic

// mapping_miss_1414_test.go (#1414): what the client does, verb by verb, when its mapper has no
// mapping for a resource.
//
//   - a read (Get, List) asks the apiserver: the group/version's own discovery document, read
//     now, says whether the resource is served. Not served is a *NotServedError and nothing else
//     is dialled; served is read at the resource's own path;
//   - a write (Create, Delete) keeps the mapper's error and sends nothing: a write never acts on
//     a resource the client's copy of discovery does not know;
//   - a group, version or resource that is not one path segment is refused before any discovery
//     or dial, on every verb.
//
// The long-lived client arm is the SA discovery singleton's shape: one client used before and
// after a CRD is installed. client-go's deferred mapper re-reads discovery on a miss only while
// its cache is not populated (DeferredDiscoveryRESTMapper.KindFor: `!d.cl.Fresh()`), so a
// populated copy that predates the CRD keeps missing until something resets it. A read must not
// depend on that reset.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/apipath"
	"github.com/krateo-platformops/snowplow/internal/testsupport/unservedapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// crdInstallApiserver serves widgets…/v1beta1 buttons, and flexes once install() is called.
// Aggregated discovery documents, the group/version's own document, `404 page not found` in
// text/plain for an unserved path, a NotFound Status for a missing object.
type crdInstallApiserver struct {
	mu        sync.Mutex
	installed bool // the flexes CRD is installed
	root      int  // /api and /apis requests answered (the mapper's discovery)
	seen      []string
}

func (a *crdInstallApiserver) install() {
	a.mu.Lock()
	a.installed = true
	a.mu.Unlock()
}

func (a *crdInstallApiserver) rootDiscoveryRequests() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.root
}

func (a *crdInstallApiserver) paths() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.seen...)
}

func (a *crdInstallApiserver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	installed := a.installed
	a.seen = append(a.seen, r.Method+" "+r.URL.Path)
	if r.URL.Path == "/api" || r.URL.Path == "/apis" {
		a.root++
	}
	a.mu.Unlock()

	const aggregated = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList"
	const gv = "widgets.templates.krateo.io/v1beta1"
	const flexes = "/apis/" + gv + "/namespaces/ns/flexes"
	verbs := `["create","delete","deletecollection","get","list","patch","update","watch"]`
	resource := func(plural, singular, kind string) string {
		return `{"resource":"` + plural + `","responseKind":{"group":"widgets.templates.krateo.io","version":"v1beta1","kind":"` + kind + `"},` +
			`"scope":"Namespaced","singularResource":"` + singular + `","verbs":` + verbs + `}`
	}
	json := func(code int, contentType, body string) {
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}
	switch {
	case r.URL.Path == "/api":
		json(http.StatusOK, aggregated, `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[`+
			`{"metadata":{"creationTimestamp":null},"versions":[{"version":"v1","resources":[`+
			`{"resource":"configmaps","responseKind":{"group":"","version":"","kind":"ConfigMap"},"scope":"Namespaced","singularResource":"configmap","verbs":`+verbs+`}`+
			`],"freshness":"Current"}]}]}`)
	case r.URL.Path == "/apis":
		resources := resource("buttons", "button", "Button")
		if installed {
			resources += "," + resource("flexes", "flex", "Flex")
		}
		json(http.StatusOK, aggregated, `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[`+
			`{"metadata":{"name":"widgets.templates.krateo.io","creationTimestamp":null},"versions":[{"version":"v1beta1","resources":[`+resources+`],"freshness":"Current"}]}]}`)
	case r.URL.Path == "/apis/"+gv:
		resources := `{"name":"buttons","singularName":"button","namespaced":true,"kind":"Button","verbs":` + verbs + `}`
		if installed {
			resources += `,{"name":"flexes","singularName":"flex","namespaced":true,"kind":"Flex","verbs":` + verbs + `}`
		}
		json(http.StatusOK, "application/json", `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"`+gv+`","resources":[`+resources+`]}`)
	case installed && r.Method == http.MethodGet && r.URL.Path == flexes+"/x":
		json(http.StatusOK, "application/json", `{"apiVersion":"`+gv+`","kind":"Flex","metadata":{"name":"x","namespace":"ns"}}`)
	case installed && r.Method == http.MethodGet && r.URL.Path == flexes:
		json(http.StatusOK, "application/json", `{"apiVersion":"`+gv+`","kind":"FlexList","metadata":{"resourceVersion":"1"},"items":[`+
			`{"apiVersion":"`+gv+`","kind":"Flex","metadata":{"name":"x","namespace":"ns"}}]}`)
	case installed && r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, flexes+"/"):
		name := strings.TrimPrefix(r.URL.Path, flexes+"/")
		json(http.StatusNotFound, "application/json", `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",`+
			`"message":"flexes.widgets.templates.krateo.io \"`+name+`\" not found","reason":"NotFound",`+
			`"details":{"name":"`+name+`","group":"widgets.templates.krateo.io","kind":"flexes"},"code":404}`)
	case installed && strings.HasPrefix(r.URL.Path, flexes):
		// A write to the installed type would succeed: the write arms assert it is never sent.
		json(http.StatusOK, "application/json", `{"apiVersion":"`+gv+`","kind":"Flex","metadata":{"name":"x","namespace":"ns"}}`)
	default:
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "404 page not found\n")
	}
}

var flexes1414 = Options{
	Namespace: "ns",
	GVR:       schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"},
}

func newClient1414(t *testing.T, h http.Handler) Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	cli, err := NewClient(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return cli
}

func TestMappingMiss_ALongLivedClientReadsATypeInstalledAfterItsDiscovery(t *testing.T) {
	a := &crdInstallApiserver{}
	cli := newClient1414(t, a)
	ctx := context.Background()

	// Before the CRD: not served, on both reads, and no object is requested.
	_, err := cli.Get(ctx, "x", flexes1414)
	if !IsNotServed(err) {
		t.Fatalf("Get before the CRD is installed must be a NotServedError, got %T: %v", err, err)
	}
	if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
		t.Fatalf("not served must be neither the mapper's miss nor an object's NotFound: %v", err)
	}
	if _, err = cli.List(ctx, flexes1414); !IsNotServed(err) {
		t.Fatalf("List before the CRD is installed must be a NotServedError, got %T: %v", err, err)
	}
	for _, p := range a.paths() {
		if strings.Contains(p, "/flexes") {
			t.Fatalf("an unserved resource was requested: %s", p)
		}
	}
	before := a.rootDiscoveryRequests()
	if before == 0 {
		t.Fatalf("PRECONDITION: the mapper must have discovered the API surface")
	}

	a.install()

	// After it, on the SAME client: the object and the list. The mapper's copy still predates
	// the CRD.
	got, err := cli.Get(ctx, "x", flexes1414)
	if err != nil {
		t.Fatalf("after the CRD is installed the same client must read the object, got %v", err)
	}
	if got.GetName() != "x" || got.GetKind() != "Flex" {
		t.Fatalf("read the wrong object: %s %q", got.GetKind(), got.GetName())
	}
	list, err := cli.List(ctx, flexes1414)
	if err != nil || len(list.Items) != 1 {
		t.Fatalf("after the CRD is installed the same client must list the objects, got %v", err)
	}
	// A missing object of the now-served type, still through the stale copy: the object's 404,
	// not "not served".
	_, err = cli.Get(ctx, "gone", flexes1414)
	if !apierrors.IsNotFound(err) || IsNotServed(err) {
		t.Fatalf("a missing object of a served type must be the apiserver's NotFound and not \"not served\", got %T: %v", err, err)
	}
	// The precondition that makes this the stale-copy arm: the mapper did not re-read discovery,
	// so the answers came from asking the apiserver, not from a refresh.
	if after := a.rootDiscoveryRequests(); after != before {
		t.Fatalf("PRECONDITION: the mapper re-read discovery (%d → %d requests); this arm must run on a copy that predates the CRD", before, after)
	}
}

// A write on a resource the mapper does not know keeps the mapper's error and sends nothing,
// even when the apiserver would accept it.
func TestMappingMiss_AWriteKeepsTheMappersErrorAndSendsNothing(t *testing.T) {
	a := &crdInstallApiserver{}
	cli := newClient1414(t, a)
	ctx := context.Background()
	// Populate the mapper's copy before the CRD, then install: the apiserver now serves flexes
	// and would answer a write.
	if _, err := cli.Get(ctx, "x", flexes1414); !IsNotServed(err) {
		t.Fatalf("PRECONDITION: want NotServedError before the install, got %v", err)
	}
	a.install()
	if _, err := cli.Get(ctx, "x", flexes1414); err != nil {
		t.Fatalf("PRECONDITION: the apiserver must now serve flexes, got %v", err)
	}
	sent := len(a.paths())

	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "Flex",
		"metadata": map[string]any{"name": "new", "namespace": "ns"},
	}}
	if _, err := cli.Create(ctx, obj, flexes1414); !meta.IsNoMatchError(err) {
		t.Errorf("Create on a mapping miss must keep the mapper's error, got %T: %v", err, err)
	}
	if err := cli.Delete(ctx, "x", flexes1414); !meta.IsNoMatchError(err) {
		t.Errorf("Delete on a mapping miss must keep the mapper's error, got %T: %v", err, err)
	}
	if got := a.paths(); len(got) != sent {
		t.Fatalf("a write on a mapping miss sent requests: %v", got[sent:])
	}
}

// A reference that does not name a version cannot be asked for as named: the mapper's miss stays
// the answer.
func TestMappingMiss_APartialReferenceKeepsTheMappersError(t *testing.T) {
	cli := newClient1414(t, &crdInstallApiserver{})
	_, err := cli.Get(context.Background(), "x", Options{Namespace: "ns", GVR: schema.GroupVersionResource{Resource: "flexes"}})
	if !meta.IsNoMatchError(err) {
		t.Fatalf("a resource named without its version must keep the mapper's no-match error, got %T: %v", err, err)
	}
}

// The client is the last place before the wire: a group, version or resource that is not one
// path segment is refused on every verb, and nothing is discovered or dialled. The apiserver
// here serves every target, so an escape would succeed.
func TestMappingMiss_ANonSegmentValueIsRefusedOnEveryVerbBeforeAnyRequest(t *testing.T) {
	for _, gvr := range []schema.GroupVersionResource{
		{Group: unservedapi.Group, Version: "v1beta1", Resource: "../../../../../api/v1/namespaces/other/configmaps"},
		{Version: "v1", Resource: "pods/p"},
		{Version: "v1", Resource: "services/svc:80/proxy"},
		{Version: "v1", Resource: "../../../../metrics"},
		{Group: unservedapi.Group, Version: "..", Resource: "buttons"},
		{Group: "..", Version: "v1", Resource: "configmaps"},
		{Group: unservedapi.Group, Version: "v1beta1", Resource: "buttons%2Fx"},
	} {
		t.Run(gvr.String(), func(t *testing.T) {
			a := &unservedapi.Server{}
			cli := newClient1414(t, a)
			ctx := context.Background()
			opts := Options{Namespace: "ns", GVR: gvr}
			var bad *apipath.BadSegmentError

			_, err := cli.Get(ctx, "x", opts)
			if !errors.As(err, &bad) {
				t.Errorf("Get: want a BadSegmentError, got %T: %v", err, err)
			}
			if _, err = cli.List(ctx, opts); !errors.As(err, &bad) {
				t.Errorf("List: want a BadSegmentError, got %T: %v", err, err)
			}
			if _, err = cli.Create(ctx, &unstructured.Unstructured{Object: map[string]any{}}, opts); !errors.As(err, &bad) {
				t.Errorf("Create: want a BadSegmentError, got %T: %v", err, err)
			}
			if err = cli.Delete(ctx, "x", opts); !errors.As(err, &bad) {
				t.Errorf("Delete: want a BadSegmentError, got %T: %v", err, err)
			}
			if got := a.Paths(); len(got) != 0 {
				t.Fatalf("nothing may be sent for such a value, the apiserver saw %v", got)
			}
		})
	}
}
