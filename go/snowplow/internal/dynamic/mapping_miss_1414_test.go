package dynamic

// mapping_miss_1414_test.go (#1414): a client whose mapper has no mapping for a resource asks the
// apiserver, so the answer is the apiserver's and never the mapper's copy of discovery.
//
// This arm is the long-lived client: one client (as the SA discovery singleton is) used before
// and after a CRD is installed. client-go's deferred mapper re-reads discovery on a miss only
// while its cache is not populated (DeferredDiscoveryRESTMapper.KindFor: `!d.cl.Fresh()`), so a
// populated copy that predates the CRD keeps missing until something resets it. The read must
// not depend on that reset.
//
// The apiserver is an httptest server answering as kube-apiserver does: aggregated discovery
// documents, `404 page not found` in text/plain for an unserved path.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

type crdInstallApiserver struct {
	mu        sync.Mutex
	installed bool // the flexes CRD is installed
	discovery int  // discovery requests answered
}

func (a *crdInstallApiserver) install() {
	a.mu.Lock()
	a.installed = true
	a.mu.Unlock()
}

func (a *crdInstallApiserver) discoveryRequests() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.discovery
}

func (a *crdInstallApiserver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	installed := a.installed
	if r.URL.Path == "/api" || r.URL.Path == "/apis" {
		a.discovery++
	}
	a.mu.Unlock()

	const aggregated = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList"
	const gv = "widgets.templates.krateo.io/v1beta1"
	verbs := `["create","delete","deletecollection","get","list","patch","update","watch"]`
	resource := func(plural, singular, kind string) string {
		return `{"resource":"` + plural + `","responseKind":{"group":"widgets.templates.krateo.io","version":"v1beta1","kind":"` + kind + `"},` +
			`"scope":"Namespaced","singularResource":"` + singular + `","verbs":` + verbs + `}`
	}
	switch {
	case r.URL.Path == "/api":
		w.Header().Set("Content-Type", aggregated)
		_, _ = io.WriteString(w, `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[`+
			`{"metadata":{"creationTimestamp":null},"versions":[{"version":"v1","resources":[`+
			`{"resource":"configmaps","responseKind":{"group":"","version":"","kind":"ConfigMap"},"scope":"Namespaced","singularResource":"configmap","verbs":`+verbs+`}`+
			`],"freshness":"Current"}]}]}`)
	case r.URL.Path == "/apis":
		resources := resource("buttons", "button", "Button")
		if installed {
			resources += "," + resource("flexes", "flex", "Flex")
		}
		w.Header().Set("Content-Type", aggregated)
		_, _ = io.WriteString(w, `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[`+
			`{"metadata":{"name":"widgets.templates.krateo.io","creationTimestamp":null},"versions":[{"version":"v1beta1","resources":[`+resources+`],"freshness":"Current"}]}]}`)
	case installed && r.URL.Path == "/apis/"+gv+"/namespaces/ns/flexes/x":
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"`+gv+`","kind":"Flex","metadata":{"name":"x","namespace":"ns"}}`)
	default:
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "404 page not found\n")
	}
}

func TestMappingMiss_ALongLivedClientReadsATypeInstalledAfterItsDiscovery(t *testing.T) {
	a := &crdInstallApiserver{}
	srv := httptest.NewServer(a)
	t.Cleanup(srv.Close)

	cli, err := NewClient(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	flexes := Options{
		Namespace: "ns",
		GVR:       schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"},
	}

	// Before the CRD: the apiserver's 404, as a Status.
	_, err = cli.Get(context.Background(), "x", flexes)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("before the CRD is installed the read must be the apiserver's NotFound, got %T: %v", err, err)
	}
	if meta.IsNoMatchError(err) {
		t.Fatalf("the mapper's miss leaked to the caller: %v", err)
	}
	before := a.discoveryRequests()
	if before == 0 {
		t.Fatalf("PRECONDITION: the mapper must have discovered the API surface")
	}

	a.install()

	// After it, on the SAME client: the object. The mapper's copy still predates the CRD.
	got, err := cli.Get(context.Background(), "x", flexes)
	if err != nil {
		t.Fatalf("after the CRD is installed the same client must read the object, got %v", err)
	}
	if got.GetName() != "x" || got.GetKind() != "Flex" {
		t.Fatalf("read the wrong object: %s %q", got.GetKind(), got.GetName())
	}
	// The precondition that makes this the stale-copy arm: the mapper did not re-read discovery
	// for the second read, so the object came from asking the apiserver, not from a refresh.
	if after := a.discoveryRequests(); after != before {
		t.Fatalf("PRECONDITION: the mapper re-read discovery (%d → %d requests); this arm must run on a copy that predates the CRD", before, after)
	}
}

// A reference that does not name a version or a resource cannot be requested as named: the
// mapper's miss stays the answer.
func TestMappingMiss_APartialReferenceKeepsTheMappersError(t *testing.T) {
	srv := httptest.NewServer(&crdInstallApiserver{})
	t.Cleanup(srv.Close)
	cli, err := NewClient(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = cli.Get(context.Background(), "x", Options{Namespace: "ns", GVR: schema.GroupVersionResource{Resource: "flexes"}})
	if !meta.IsNoMatchError(err) {
		t.Fatalf("a resource named without its version must keep the mapper's no-match error, got %T: %v", err, err)
	}
}
