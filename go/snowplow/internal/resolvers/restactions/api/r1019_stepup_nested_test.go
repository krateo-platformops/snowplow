package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

// Reviewer arm S1 (#1019 x #1024): a krateo:loa:2 request resolves a nested
// RESTAction in process. Inside it, the nil-ref stage must dial the caller's
// STEP-UP credential and the named-ref stage its OWN endpoint credential —
// never the session credential, never the outer stage's.
//
// Adopted from reviewer-1019 (#1019 issuecomment-6541500707). RED under
// mutation SG: a resolve run inside another resolve's stage builds its
// endpoint mapper without the requester's groups ("a step-up request dialled
// the SESSION credential").
func TestR1019_S1_StepUpThroughANestedResolveDialsEachStagesOwnCredential(t *testing.T) {
	api, dials := su994Upstream(t)
	var rmu sync.Mutex
	var rdials []string
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		rmu.Lock()
		rdials = append(rdials, r.Method+" "+r.URL.Path+" "+strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		rmu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":"remote"}`)
	}))
	t.Cleanup(remote.Close)
	su994Secrets(t, api.URL, map[string]string{
		credsecret.SessionName(su994User): su994TokSess,
		credsecret.StepUpName(su994User):  su994TokUp,
	})
	// re-publish the snapshot with the named remote endpoint Secret added
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		credsecret.SessionName(su994User): mkSecret(su994NS, credsecret.SessionName(su994User), map[string]string{"server-url": api.URL, "token": su994TokSess}),
		credsecret.StepUpName(su994User):  mkSecret(su994NS, credsecret.StepUpName(su994User), map[string]string{"server-url": api.URL, "token": su994TokUp}),
		"remote-994":                      mkSecret(su994NS, "remote-994", map[string]string{"server-url": remote.URL, "token": "TOK-REMOTE-994"}),
	}})
	cache.ForceSecretsCacheReadyForTest(su994NS)

	innerRan := false
	prev := nestedCallResolver
	nestedCallResolver = func(nctx context.Context, _ templates.ObjectReference, _, _ int, _ map[string]any) ([]byte, error) {
		innerRan = true
		dict := Resolve(nctx, ResolveOptions{RC: &rest.Config{Host: api.URL}, AuthnNS: su994NS,
			RESTActionNamespace: su994NS, RESTActionName: "inner994",
			Items: []*templates.API{
				su994Stage("innerOwn", http.MethodGet, su994WritePth+"/inner-own"),
				{Name: "innerRemote", Path: su994WritePth + "/inner-remote", Verb: ptr.To(http.MethodGet),
					EndpointRef:     &templates.Reference{Name: "remote-994", Namespace: su994NS},
					ContinueOnError: ptr.To(true), ErrorKey: ptr.To("innerRemoteErr")},
			}})
		return json.Marshal(map[string]any{"kind": "RESTAction", "status": dict})
	}
	t.Cleanup(func() { nestedCallResolver = prev })

	Resolve(ctxWithUser(su994User, su994Group, credsecret.StepUpGroup), ResolveOptions{
		RC: &rest.Config{Host: api.URL}, AuthnNS: su994NS, RESTActionNamespace: su994NS, RESTActionName: "outer994",
		Items: []*templates.API{{Name: "outer", Path: "/apis/templates.krateo.io/v1/namespaces/" + su994NS + "/restactions/inner994",
			Verb: ptr.To(http.MethodGet), Resolve: ptr.To(true), ContinueOnError: ptr.To(true), ErrorKey: ptr.To("outerErr")}},
	})
	if !innerRan {
		t.Fatal("setup: the outer stage did not resolve in process")
	}
	var innerOwn []string
	for _, d := range dials() {
		if strings.Contains(d.path, "/inner-own") {
			innerOwn = append(innerOwn, d.bearer)
		}
		if strings.Contains(d.path, "/inner-remote") {
			t.Fatalf("S1 RED: the nested named-ref stage was sent to THIS cluster (bearer %q)", d.bearer)
		}
		if d.bearer == su994TokSess {
			t.Fatalf("S1 RED: a step-up request dialled the SESSION credential: %s %s", d.method, d.path)
		}
	}
	if len(innerOwn) != 1 || innerOwn[0] != su994TokUp {
		t.Fatalf("S1 RED: the nested nil-ref stage must dial the step-up credential once; got %v", innerOwn)
	}
	rmu.Lock()
	defer rmu.Unlock()
	if len(rdials) != 1 || !strings.HasSuffix(rdials[0], "/inner-remote TOK-REMOTE-994") {
		t.Fatalf("S1 RED: the nested named-ref stage must dial its own endpoint with its own credential once; remote saw %v", rdials)
	}
	t.Logf("S1: inner own-stage bearers %v; remote %v", innerOwn, rdials)
}
