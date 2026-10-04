// inline443_uaf_test.go — #443 part 2, reviewer-424 C1.
//
//	e5 CallerSuppliedUAFOverInformerContentIsNarrowed (reviewer probe U1,
//	   adopted): a caller-supplied UAF stage over an INFORMER-SERVED path (the
//	   identity-free apistage content cell, filled through snowplow's
//	   ServiceAccount). bob cannot list configmaps in ns z, but the UAF stanza
//	   names a resource bob CAN get there (pods, via bob-pods). Stored semantics
//	   serve the content un-narrowed and let the refilter keep z's items, so bob
//	   would read a configmap his RBAC denies. Caller-supplied semantics narrow
//	   the content to bob's own RBAC first. This is the arm for the resolve.go
//	   uafContent change (arm e3 uses pods, which the informer never serves).
//	e4 NestedStoredUAFKeepsStoredSemantics: a STORED UAF RESTAction nested
//	   (in-process) inside an inline draft resolves exactly as it does nested
//	   inside the same spec resolved STORED: Provenance is per-resolve, so the
//	   nested resolve is Stored.
package dispatchers

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

func TestS443_InlineUAF(t *testing.T) {
	t.Run("e5_CallerSuppliedUAFOverInformerContentIsNarrowed", func(t *testing.T) {
		cmZ := &corev1.ConfigMap{
			TypeMeta:   psTypeMeta(in443Arm, "ConfigMap"),
			ObjectMeta: metav1.ObjectMeta{Namespace: psOtherNS, Name: "z-secretish"},
			Data:       map[string]string{"password": psSentinel + "-z"},
		}
		f := in443Setup(t, cmZ)
		obj := in443RA("draft-u1", "", map[string]any{"name": "uaf", "continueOnError": true, "errorKey": "uafErr",
			"path":             "/api/v1/namespaces/" + psOtherNS + "/configmaps",
			"userAccessFilter": map[string]any{"verb": "get", "resource": "pods", "group": ""}})
		f.reset()
		rec := serveInline(t, f, in443Ctx(f, psBob), obj)
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
		if psHasSentinel(rec.Body.Bytes()) {
			t.Fatalf("LEAK: bob's caller-supplied UAF draft read a configmap he cannot list, served from the SA-filled apistage content: %s",
				psTrunc(rec.Body.String(), 400))
		}
	})

	t.Run("e4_NestedStoredUAFKeepsStoredSemantics", func(t *testing.T) {
		const innerName = "inner-uaf-443"
		inner := map[string]any{
			"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version, "kind": "RESTAction",
			"metadata": map[string]any{"name": innerName, "namespace": h1NS},
			"spec": map[string]any{"api": []any{map[string]any{
				"name": "uaf", "continueOnError": true, "errorKey": "uafErr",
				"path":             "/api/v1/namespaces/" + psTargetNS + "/pods",
				"userAccessFilter": map[string]any{"verb": "get", "resource": "pods", "group": ""},
			}}},
		}
		innerObj := &unstructured.Unstructured{Object: inner}
		f := in443Setup(t, innerObj.DeepCopy())
		// The inner CR as the apiserver serves it (the inert outer registers no
		// informer, so its nested fetch reads the apiserver as the caller).
		innerJSON, _ := innerObj.MarshalJSON()
		f.extra["GET /apis/"+h1RAGVR.Group+"/"+h1RAGVR.Version+"/namespaces/"+h1NS+"/restactions/"+innerName] = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(innerJSON)
		}
		// A caller dial of the UAF path would be the apiserver's 403 (stored
		// semantics never dial the caller for a UAF stage).
		f.extra["GET /api/v1/namespaces/"+psTargetNS+"/pods"] = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","message":"pods is forbidden (caller dial)","reason":"Forbidden","code":403}`))
		}
		// Make the nested fetch deterministic: the RESTAction informer is
		// registered and servable, so both the stored and the inline outer read
		// the inner CR from it (an inert resolve registers nothing, but serves
		// from an informer that already exists).
		if rw := cache.Global(); rw != nil {
			if _, ch := rw.EnsureResourceType(h1RAGVR); ch != nil {
				select {
				case <-ch:
				case <-time.After(5 * time.Second):
					t.Fatal("restactions informer did not sync")
				}
			}
			if !rw.IsServable(h1RAGVR) {
				t.Fatal("PRE: restactions informer must be servable")
			}
		}
		restactionsapi.RegisterNestedCallResolver(ResolveNestedCall)
		// A nested in-process resolve on a live request has no RC and uses the
		// in-cluster config; point it at the fake so the nested stages run.
		t.Cleanup(restactionsapi.SetInClusterConfigForTest(func() (*rest.Config, error) {
			return &rest.Config{Host: f.srv.URL}, nil
		}))

		outer := in443RA(psRAName, `{nested: .nest}`, map[string]any{"name": "nest", "continueOnError": true, "resolve": true,
			"path": "/apis/" + h1RAGVR.Group + "/" + h1RAGVR.Version + "/namespaces/" + h1NS + "/restactions/" + innerName})
		bob := in443Ctx(f, psBob)
		stored := serveStored(t, f, bob, outer)
		inline := serveInline(t, f, bob, outer)
		if stored.Code != http.StatusOK || inline.Code != http.StatusOK {
			t.Fatalf("codes stored=%d inline=%d (inline=%s)", stored.Code, inline.Code, psTrunc(inline.Body.String(), 300))
		}
		ss, is := statusOf443(t, stored.Body.Bytes()), statusOf443(t, inline.Body.Bytes())
		if ss != is {
			t.Errorf("the nested STORED UAF RESTAction resolved differently inside the inline draft\n stored outer: %s\n inline outer: %s", ss, is)
		}
		if strings.Contains(is, "caller dial") {
			t.Errorf("the nested stored UAF stage dialed the CALLER (caller-supplied semantics leaked into a nested stored resolve): %s", is)
		}
		if !strings.Contains(ss, `"uaf":{`) && !strings.Contains(ss, `"uafErr"`) {
			t.Fatalf("PRE: the nested resolve did not run (no uaf stage output in %s) — the arm cannot discriminate", ss)
		}
	})
}

func statusOf443(t *testing.T, b []byte) string {
	t.Helper()
	s := string(b)
	i := strings.Index(s, `"status":`)
	if i < 0 {
		t.Fatalf("no status in %s", psTrunc(s, 300))
	}
	return s[i:]
}
