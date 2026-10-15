package dispatchers

// issue729_refused_redirect_502_test.go — #729: a required stage whose
// credentialed dial got a redirect (refused per #723/#724, internal/credhttp)
// must reach the client as 502 Bad Gateway, never a bare 302/307 with no
// Location, and must cache nothing.
//
// Driven through the REAL chain on the #443 ps harness (cache ON, a real
// watcher, the requester's own clientconfig endpoint pointing at a fake
// apiserver): the RESTAction /call handler → restactions.Resolve →
// api.Resolve → the per-user fetch. The fake apiserver answers the stage's GET
// with a 3xx whose Location points at a second server that records any hop.
//
//	R1  /call answers 502 with no Location; the message names the RESTAction
//	    and the stage and carries no upstream text or identity.
//	R2  the second server received nothing (the credential never left).
//	R3  nothing was cached: the L1 key set (RESTAction cells and apistage
//	    content cells alike) is unchanged, and neither Put-decline counter
//	    moved: the failure returns before the Put chain, so no gate had to
//	    decline it. Control: on the same harness a RESTAction that resolves
//	    DOES add cells, so R3 can see a Put when one happens.
//	R4  the widget apiRef frame (apiref.Resolve) returns a typed 502.

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

const i729Path = "/api/v1/namespaces/" + psTargetNS + "/configmaps/redir729"

// i729Filter yields no value when the stage failed: the #709 condition under
// which a required stage's failure becomes the answer.
const i729Filter = `.crd.data.k // empty | {k: .}`

func i729Stage() map[string]any {
	return map[string]any{"name": "crd", "verb": "GET", "path": i729Path}
}

// i729Redirect makes the fake apiserver answer the stage's GET with code and a
// Location on another host; it returns that host's hop counter.
func i729Redirect(t *testing.T, f *in443API, code int) *atomic.Int64 {
	t.Helper()
	var hops atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hops.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"CustomResourceDefinition","apiVersion":"apiextensions.k8s.io/v1","metadata":{"name":"stolen-SNT729"}}`))
	}))
	t.Cleanup(other.Close)
	f.mu.Lock()
	f.extra["GET "+i729Path] = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", other.URL+"/stolen-SNT729")
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`<a href="` + other.URL + `/stolen-SNT729">Found SNT729</a>`))
	}
	f.mu.Unlock()
	return &hops
}

func TestIssue729_RequiredStageRefusedRedirect_Is502AndCachesNothing(t *testing.T) {
	for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			ra := in443RA("redirect-729", i729Filter, i729Stage())
			f := in443Setup(t, &unstructured.Unstructured{Object: ra})
			hops := i729Redirect(t, f, code)

			before := takeIn443State(t)
			rec := serveStored(t, f, in443Ctx(f, psAlice), ra)
			after := takeIn443State(t)
			body := rec.Body.String()

			// R1
			if rec.Code != http.StatusBadGateway {
				t.Fatalf("#729 RED: code=%d, want 502: %s", rec.Code, body)
			}
			if loc := rec.Header().Get("Location"); loc != "" {
				t.Fatalf("#729: the answer carries a Location %q", loc)
			}
			st := i709Status(t, rec)
			msg, _ := st["message"].(string)
			if st["kind"] != "Status" || st["code"] != float64(http.StatusBadGateway) {
				t.Fatalf("want a 502 Status, got %s", body)
			}
			for _, want := range []string{"restaction " + h1NS + "/redirect-729", `stage "crd"`, `configmaps "redir729"`,
				"a redirect snowplow does not follow"} {
				if !strings.Contains(msg, want) {
					t.Fatalf("the message must contain %q, got %q", want, msg)
				}
			}
			for _, bad := range []string{"SNT729", "stolen", "href", psAlice, "tok-", "clientconfig"} {
				if strings.Contains(body, bad) {
					t.Fatalf("upstream or identity text %q reached the answer: %s", bad, body)
				}
			}
			// R2
			if n := hops.Load(); n != 0 {
				t.Fatalf("#723 regression: the redirect was followed %d time(s)", n)
			}
			// R3
			if !reflect.DeepEqual(before.l1Keys, after.l1Keys) {
				t.Fatalf("#729: a failed resolve changed the L1 key set: %d → %d keys", len(before.l1Keys), len(after.l1Keys))
			}
			if before.extSkipped != after.extSkipped || before.uafDeclined != after.uafDeclined {
				t.Fatalf("#729: the failed resolve reached the Put chain (external-skipped %d → %d, UAF-declined %d → %d)",
					before.extSkipped, after.extSkipped, before.uafDeclined, after.uafDeclined)
			}
		})
	}
}

// R4: the widget apiRef frame returns a typed 502 for the same RESTAction.
func TestIssue729_WidgetApiRef_RefusedRedirect_Is502(t *testing.T) {
	for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			ra := in443RA("redirect-729", i729Filter, i729Stage())
			f := in443Setup(t, &unstructured.Unstructured{Object: ra})
			hops := i729Redirect(t, f, code)
			// objects.Get serves the RESTAction from its informer: sync it first.
			if rw := cache.Global(); rw != nil {
				if _, ch := rw.EnsureResourceType(h1RAGVR); ch != nil {
					select {
					case <-ch:
					case <-time.After(5 * time.Second):
						t.Fatal("restactions informer did not sync")
					}
				}
			}
			_, err := apiref.Resolve(in443Ctx(f, psAlice), apiref.ResolveOptions{
				RC: &rest.Config{Host: f.srv.URL}, ApiRef: i431ApiRef("redirect-729"), AuthnNS: psAuthnNS,
			})
			var se *apierrors.StatusError
			if !errors.As(err, &se) || se.Status().Code != http.StatusBadGateway {
				t.Fatalf("#729 RED: apiref.Resolve: want a typed 502, got %T: %v", err, err)
			}
			if msg := se.Status().Message; !strings.Contains(msg, "restaction "+h1NS+"/redirect-729") || !strings.Contains(msg, `stage "crd"`) ||
				strings.Contains(msg, "SNT729") || strings.Contains(msg, psAlice) {
				t.Fatalf("apiref.Resolve: message %q", msg)
			}
			if hops.Load() != 0 {
				t.Fatal("#723 regression: apiref.Resolve followed the redirect")
			}
		})
	}
}

// Control for R3: on the same harness a RESTAction that resolves (its stage
// informer-served, so no external-fetch decline applies) adds L1 cells.
func TestIssue729_Control_ResolvedRESTActionCaches(t *testing.T) {
	ra := in443RA("control-729", `.target`, targetStage())
	f := in443Setup(t, &unstructured.Unstructured{Object: ra})
	before := takeIn443State(t)
	rec := serveStored(t, f, in443Ctx(f, psAlice), ra)
	after := takeIn443State(t)
	if rec.Code != http.StatusOK {
		t.Fatalf("control: code=%d: %s", rec.Code, rec.Body.String())
	}
	t.Logf("control: L1 keys %d -> %d", len(before.l1Keys), len(after.l1Keys))
	if len(after.l1Keys) < len(before.l1Keys)+2 {
		t.Fatalf("control: want the RESTAction cell and the stage's content cell, L1 keys %d -> %d, so R3 cannot see a Put",
			len(before.l1Keys), len(after.l1Keys))
	}
}
