//go:build unit
// +build unit

// #282 — /call subresource routing (e.g. status). Internal unit arms for the
// unexported seam: buildURIPath (path append), validateRequest (subresource
// param + validation), and dispatcherForMethod (the load-bearing read-side
// fall-through gate). RBAC is apiserver-enforced via the user's creds (no
// snowplow SAR) — that path-correctness + apiserver-403 guarantee is covered
// by the kind-backed arm in call_test.go's #156 family (a caller without the
// grant → apiserver 403); here we prove the request carries the subresource
// segment so the apiserver checks resource=<res>,subresource=<sub> natively.

package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// Arm 1 — buildURIPath appends the subresource after the name for a by-name
// verb, in both the namespaced and cluster-scoped branches, and is byte-
// identical to the whole-object path when no subresource is addressed.
func TestSubresource282_BuildURIPath_AppendsAfterName(t *testing.T) {
	cases := []struct {
		name string
		opts callOptions
		want string
	}{
		{
			name: "namespaced PUT status",
			opts: callOptions{
				gvr:         schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"},
				nsn:         types.NamespacedName{Name: "w1", Namespace: "ns1"},
				verb:        http.MethodPut,
				namespaced:  true,
				subresource: "status",
			},
			want: "/apis/example.com/v1/namespaces/ns1/widgets/w1/status",
		},
		{
			name: "cluster GET status",
			opts: callOptions{
				gvr:         schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "clusterthings"},
				nsn:         types.NamespacedName{Name: "c1"},
				verb:        http.MethodGet,
				namespaced:  false,
				subresource: "status",
			},
			want: "/apis/example.com/v1/clusterthings/c1/status",
		},
		{
			name: "no subresource is unchanged (backward-compat)",
			opts: callOptions{
				gvr:        schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"},
				nsn:        types.NamespacedName{Name: "w1", Namespace: "ns1"},
				verb:       http.MethodPut,
				namespaced: true,
			},
			want: "/apis/example.com/v1/namespaces/ns1/widgets/w1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildURIPath(tc.opts)
			if err != nil {
				t.Fatalf("buildURIPath: %v", err)
			}
			if got != tc.want {
				t.Fatalf("path = %q, want %q", got, tc.want)
			}
		})
	}
}

// Arm 3 — validateRequest reads the subresource param, requires a by-name verb
// (rejects a subresource + POST), and leaves opts.subresource empty when none
// is addressed (backward-compat). Namespace is supplied so the scope resolver
// is never consulted (the #156 namespaced branch).
func TestSubresource282_Validate_RejectsSubresourceWithPost(t *testing.T) {
	r := &callHandler{}
	req := httptest.NewRequest(http.MethodPost,
		"/call?apiVersion=example.com/v1&resource=widgets&namespace=ns1&name=w1&subresource=status", nil)
	if _, err := r.validateRequest(req); err == nil {
		t.Fatal("expected error: a subresource cannot ride a POST (collection create has no subresource target)")
	}
}

func TestSubresource282_Validate_AcceptsSubresourceWithByNameVerb(t *testing.T) {
	for _, verb := range []string{http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(verb, func(t *testing.T) {
			r := &callHandler{}
			req := httptest.NewRequest(verb,
				"/call?apiVersion=example.com/v1&resource=widgets&namespace=ns1&name=w1&subresource=status", nil)
			opts, err := r.validateRequest(req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if opts.subresource != "status" {
				t.Fatalf("opts.subresource = %q, want status", opts.subresource)
			}
		})
	}
}

func TestSubresource282_Validate_NoSubresourceIsEmpty(t *testing.T) {
	r := &callHandler{}
	req := httptest.NewRequest(http.MethodGet,
		"/call?apiVersion=example.com/v1&resource=widgets&namespace=ns1&name=w1", nil)
	opts, err := r.validateRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.subresource != "" {
		t.Fatalf("opts.subresource = %q, want empty (backward-compat)", opts.subresource)
	}
}

// Arm 4 (load-bearing, part-2 read-side gate) — the dispatcher must NOT serve a
// subresource request from a cache/resolve handler: it keys on the whole object
// (no subresource), so a subresource GET would be mis-served the PARENT's body.
// A request carrying a subresource must fall through to `next` (handlers.Call),
// which builds the subresource path and hits the apiserver. The non-subresource
// control proves the arm is not vacuous (that GET DOES reach the mapped handler).
func TestSubresource282_Dispatcher_FallsThroughOnSubresource(t *testing.T) {
	var served string
	mapped := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served = "handler" })
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served = "next" })
	// group "example.com" → dispatcher map key "example.com".
	h := dispatcherForMethod(map[string]http.Handler{"example.com": mapped}, http.MethodGet)(next)

	// GET + subresource → must fall through to next, NOT the mapped handler.
	served = ""
	subReq := httptest.NewRequest(http.MethodGet,
		"/call?apiVersion=example.com/v1&resource=widgets&name=w1&subresource=status", nil)
	h.ServeHTTP(httptest.NewRecorder(), subReq)
	if served != "next" {
		t.Fatalf("subresource GET served by %q, want \"next\" — a resolve/cache handler must never serve a subresource (it would return the parent body)", served)
	}

	// Control (non-vacuity): same GET WITHOUT subresource routes to the mapped handler.
	served = ""
	plainReq := httptest.NewRequest(http.MethodGet,
		"/call?apiVersion=example.com/v1&resource=widgets&name=w1", nil)
	h.ServeHTTP(httptest.NewRecorder(), plainReq)
	if served != "handler" {
		t.Fatalf("non-subresource GET served by %q, want \"handler\" — the gate must not over-trigger", served)
	}
}
