//go:build js && wasm

package fakeapiserver

// §8 after #1382: the visitor's credential is a bearer-token Secret under the exact name
// (credsecret.For's first name), owned through the username annotation (plumbing v1.61.0
// endpoints.OwnsSecret's annotation arm). snowplow's own-credential reader serves it to the visitor
// and to no one else.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

const visitor = "sandbox-visitor"

// serveTransport answers a client-go request with the fake itself, as the bridge does.
type serveTransport struct{ s *Server }

func (t serveTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	t.s.ServeHTTP(rec, r)
	return rec.Result(), nil
}

// saConfig is the ServiceAccount's client config against the fake, as snowplow's in-cluster one is.
func saConfig(h *harness) *rest.Config {
	return &rest.Config{Host: "https://kubernetes.sandbox.invalid", BearerToken: "sa-token", Transport: serveTransport{h.s}}
}

func visitorSecret(t *testing.T, h *harness, name string) *corev1.Secret {
	t.Helper()
	r := h.do("GET", "/api/v1/namespaces/krateo-system/secrets/"+name, req{as: "sa"})
	eq(t, r.code, 200, "the ServiceAccount reads the visitor Secret "+name)
	b, _ := json.Marshal(r.body)
	sec := &corev1.Secret{}
	if err := json.Unmarshal(b, sec); err != nil {
		t.Fatal(err)
	}
	return sec
}

// (1) The Secret passes OwnsSecret for the visitor, through the username annotation, and fails it
// for another username, including one whose legacy name is the visitor's.
func TestVisitorSecret_OwnedByTheVisitorOnly(t *testing.T) {
	h := newHarness(t, opts{})
	names := credsecret.For(visitor, nil)
	sec := visitorSecret(t, h, names[0])
	eq(t, sec.Annotations[endpoints.UsernameAnnotation], visitor, "the username annotation")
	eq(t, string(sec.Data["token"]), "visitor-token", "the bearer token the fake authenticates")
	eq(t, string(sec.Data["server-url"]), "https://kubernetes.sandbox.invalid", "the server URL")
	for _, k := range []string{"client-certificate-data", "client-key-data", "certificate-authority-data"} {
		if _, ok := sec.Data[k]; ok {
			t.Fatalf("the Secret holds %s: fetch cannot present a client certificate, so the fake writes none", k)
		}
	}
	if !endpoints.OwnsSecret(sec, visitor) {
		t.Fatal("OwnsSecret refuses the visitor its own Secret")
	}
	for _, other := range []string{"Sandbox-Visitor", "sandbox-visitor2", "someone"} {
		if endpoints.OwnsSecret(sec, other) {
			t.Fatalf("OwnsSecret gives %q the visitor's Secret", other)
		}
	}
}

// (2) snowplow's real reader serves the visitor this Secret through the fake; a second user whose
// names reach the same Secret (Sandbox-Visitor: no exact name, and its legacy name is the
// visitor's) gets the NotFound that the UserConfig middleware answers 401.
func TestVisitorSecret_SnowplowsReaderServesTheVisitorOnly(t *testing.T) {
	h := newHarness(t, opts{})
	ep, err := api.FromAPIServerOwnSecret(context.Background(), saConfig(h), "krateo-system", visitor, credsecret.For(visitor, nil))
	if err != nil {
		t.Fatalf("the visitor's credential: %v", err)
	}
	eq(t, ep.Token, "visitor-token", "the visitor is served its token")
	eq(t, ep.ServerURL, "https://kubernetes.sandbox.invalid", "and its server URL")

	other := "Sandbox-Visitor"
	names := credsecret.For(other, nil)
	if names[len(names)-1] != credsecret.For(visitor, nil)[0] {
		t.Fatalf("the test's premise: %q's names %v reach the visitor's Secret", other, names)
	}
	_, err = api.FromAPIServerOwnSecret(context.Background(), saConfig(h), "krateo-system", other, names)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("another user is refused with the NotFound of a missing credential (401), got %v", err)
	}
}

// A step-up visitor's Secret is the step-up name (credsecret.For's first for its groups), and the
// reader serves it to the step-up identity.
func TestVisitorSecret_StepUpName(t *testing.T) {
	h := newHarness(t, opts{edit: func(snap map[string]any) {
		v := snap["identity"].(map[string]any)["visitor"].(map[string]any)
		gs, _ := v["groups"].([]any)
		v["groups"] = append(gs, credsecret.StepUpGroup)
	}})
	groups := []string{credsecret.StepUpGroup}
	name := credsecret.For(visitor, groups)[0]
	if !credsecret.IsStepUpName(name) {
		t.Fatalf("the step-up name %q", name)
	}
	visitorSecret(t, h, name)
	ep, err := api.FromAPIServerOwnSecret(context.Background(), saConfig(h), "krateo-system", visitor, credsecret.For(visitor, groups))
	if err != nil || ep.Token != "visitor-token" {
		t.Fatalf("the step-up visitor's credential: %v %q", err, ep.Token)
	}
}
