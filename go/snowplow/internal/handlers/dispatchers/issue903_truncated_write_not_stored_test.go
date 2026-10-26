package dispatchers

// issue903_truncated_write_not_stored_test.go — #903 rule 2, through the REAL
// restActionHandler.ServeHTTP and the REAL L1 (the #681 harness).
//
// The RESTAction reads a provider object and then DELETEs it, both through an
// endpointRef Secret. At the first /call the Secret does not exist: the
// resolve stops at the read (R-2 stageReturn) and the handler answers 200.
//
//   T1  that truncated result is NOT stored. Stored, it would hand the
//       refresher a cell whose re-resolve — once the Secret exists — runs the
//       whole sequence with the stored extras.
//   T2  with the Secret present, a background (refresher) re-resolve of the
//       same RESTAction sends 0 writes (rule 1), and the next customer /call
//       sends the DELETE exactly once (a real user request).
//
// Mutation (logic-only, owned): drop the firstWriteStageFrom bump in
// ResolveWithFailure → T1 RED (the truncated cell is stored).

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/plumbing/kubeutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

// t903Call serves one customer /call for id through the real handler.
func (e *c681Env) t903Call(t *testing.T, id ident681) {
	t.Helper()
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: e.ra.DeepCopy()}
	})
	defer restore()
	h := &restActionHandler{authnNS: c681AuthnNS, saRC: &rest.Config{Host: e.user.URL}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(id.ctx()))
	if rec.Code != http.StatusOK {
		t.Fatalf("/call for %s: status %d body=%s", id.user, rec.Code, rec.Body.String())
	}
}

const t903EP = "provider-ep-903"

func t903RA() *unstructured.Unstructured {
	ref := map[string]any{"name": t903EP, "namespace": c681AuthnNS}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": c681RA, "namespace": h1NS},
		"spec": map[string]any{"api": []any{
			map[string]any{"name": "server", "path": "/servers/s1", "endpointRef": ref,
				"continueOnError": true, "errorKey": "serverErr"},
			map[string]any{"name": "remove", "verb": "DELETE", "path": "/servers/s1", "endpointRef": ref,
				"dependsOn": map[string]any{"name": "server"}, "continueOnError": true, "errorKey": "removeErr"},
		}},
	}}
}

// t903PublishSecrets republishes the #681 harness's clientconfigs, plus the
// provider endpoint Secret when withEP.
func t903PublishSecrets(e *c681Env, upstreamURL string, withEP bool) {
	secrets := map[string]*corev1.Secret{}
	for _, i := range []ident681{c681A, c681B, c681C} {
		n := kubeutil.MakeDNS1123Compatible(i.user) + "-clientconfig"
		secrets[n] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: c681AuthnNS, Name: n},
			Data: map[string][]byte{"server-url": []byte(e.user.URL), "token": []byte(i.token())}}
	}
	if withEP {
		secrets[t903EP] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: c681AuthnNS, Name: t903EP},
			Data: map[string][]byte{"server-url": []byte(upstreamURL)}}
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: secrets})
}

func TestIssue903_T1_T2_TruncatedBeforeAWriteIsNeverStored(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	e := c681Setup(t)
	up := newB903Counter(t)
	e.ra = t903RA()
	t903PublishSecrets(e, up.srv.URL, false)

	e.t903Call(t, c681A)
	if n := up.count(http.MethodGet) + up.writes(); n != 0 {
		t.Fatalf("PRE: with the endpoint Secret missing nothing reaches the provider, got %d request(s)", n)
	}
	if _, stored := e.cell(c681A, t); stored {
		t.Fatal("#903 RED (T1): a resolve that stopped before its DELETE stage was stored — a later refresh would run the sequence")
	}

	// The Secret appears.
	t903PublishSecrets(e, up.srv.URL, true)

	// A background re-resolve of the same RESTAction never writes.
	var cr templatesv1.RESTAction
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(e.ra.Object, &cr); err != nil {
		t.Fatalf("PRE: %v", err)
	}
	bg := refresherResolveCtx(c681A.ctx())
	restactionsapi.Resolve(bg, restactionsapi.ResolveOptions{
		RC: &rest.Config{Host: e.user.URL}, AuthnNS: c681AuthnNS, Items: cr.Spec.API,
		RESTActionNamespace: h1NS, RESTActionName: c681RA})
	if up.count(http.MethodGet) < 1 {
		t.Fatal("PRE (non-vacuity): the background re-resolve never read the provider")
	}
	if n := up.writes(); n != 0 {
		t.Fatalf("#903 RED (T2): a background re-resolve sent %d write(s) once the Secret existed", n)
	}

	// A real user request sends it exactly once.
	e.t903Call(t, c681A)
	if n := up.count(http.MethodDelete); n != 1 {
		t.Fatalf("the customer /call must send the DELETE exactly once, got %d", n)
	}
}
