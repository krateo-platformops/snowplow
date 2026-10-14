//go:build unit
// +build unit

package api

// issue709_status_shaping_test.go — #709 unit arms for the two places an
// upstream answer was lost before it reached the stage's errorKey, and for the
// StageFailure derivation.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue709_NonOKStatus_KeepsTheUpstreamCode(t *testing.T) {
	t.Run("empty-body", func(t *testing.T) {
		st, b, _, _, err := fetchAgainst(t, http.StatusNotFound, "application/json", "", noAuth)
		if err != nil || b != nil {
			t.Fatalf("err=%v bytes=%q", err, b)
		}
		if st.Status != response.StatusFailure || st.Code != 404 || st.Reason != response.StatusReasonNotFound ||
			st.Message != "upstream answered 404 Not Found with an empty body" {
			t.Fatalf("empty-body 404 lost its answer: %+v", st)
		}
	})
	t.Run("json-body-not-a-status", func(t *testing.T) {
		st, b, _, _, err := fetchAgainst(t, http.StatusConflict, "application/json", `{"error":"busy"}`, noAuth)
		if err != nil || b != nil {
			t.Fatalf("err=%v bytes=%q", err, b)
		}
		if st.Status != response.StatusFailure || st.Code != 409 || st.Message != `{"error":"busy"}` {
			t.Fatalf("a non-Status JSON 409 must stay a 409 Failure: %+v", st)
		}
	})
	t.Run("json-message-403-gets-no-forbidden-reason", func(t *testing.T) {
		// #681 F2 reads Reason==Forbidden; a rate-limit 403 must not gain it.
		st, _, _, _, _ := fetchAgainst(t, http.StatusForbidden, "application/json", `{"message":"rate limited"}`, noAuth)
		if st.Code != 403 || st.Reason != "" || st.Message != "rate limited" || st.Status != response.StatusFailure {
			t.Fatalf("rate-limit 403: %+v", st)
		}
	})
}

func TestIssue709_Empty2xxBody_IsNull(t *testing.T) {
	for _, body := range []string{"", "  \n"} {
		st, b, _, _, err := fetchAgainst(t, http.StatusOK, "application/json", body, noAuth)
		if err != nil || st.Status == response.StatusFailure || string(b) != "null" {
			t.Fatalf("empty 2xx %q: st=%+v bytes=%q err=%v, want success with null", body, st, b, err)
		}
	}
	// The in-memory feed paths say the same.
	out := map[string]any{}
	if err := jsonHandlerBytes(context.Background(), jsonHandlerOptions{key: "s", out: out, dict: out})(nil); err != nil {
		t.Fatalf("empty in-memory body: %v", err)
	}
	if v, ok := out["s"]; !ok || v != nil {
		t.Fatalf("empty in-memory body: want s=null, got %#v (present=%v)", v, ok)
	}
}

func TestIssue709_StageFailure_TakesTheRecordedStatus(t *testing.T) {
	itemErr := errors.New("api crd item 0 failed: x")

	get := func(path string) []httpcall.RequestOptions {
		return []httpcall.RequestOptions{{RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(http.MethodGet)}}}
	}
	first := func(accumVal any, err error, calls []httpcall.RequestOptions) *StageFailure {
		return firstStageFailure([]error{newStageItemFailure("crd", accumVal, err)}, calls)
	}

	// The external fetch's upstream Status, as recorded: code and reason are
	// kept, the upstream message is not.
	m, _ := response.AsMap(response.New(http.StatusNotFound, fmt.Errorf(`not found for User "alice"`)))
	f := first(m, itemErr, get("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/agents.kagent.dev"))
	if f.Code != 404 || f.Reason != "NotFound" || f.Message != `customresourcedefinitions.apiextensions.k8s.io "agents.kagent.dev" not found` {
		t.Fatalf("recorded Status: %+v", f)
	}

	// #681: a forbidden reports the canonical identity-free text.
	canon := canonicalForbiddenMessage(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, "b", "c", "get")
	raw := apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "c", errors.New(`User "alice" cannot get`))
	f = first(canonicalForbiddenBody(canon), fmt.Errorf("w: %w", raw), get("/api/v1/namespaces/b/configmaps/c"))
	if f.Code != 403 || f.Message != canon {
		t.Fatalf("forbidden must report the canonical body: %+v", f)
	}

	// A typed StatusError in the chain (internal-rest-config records a string).
	nf := apierrors.NewNotFound(schema.GroupResource{Resource: "things"}, "t")
	f = first(nf.Error(), fmt.Errorf("w: %w", nf), get("/apis/example.io/v1/namespaces/n/things/t"))
	if f.Code != 404 || f.Reason != "NotFound" || f.Message != `things.example.io "t" not found` {
		t.Fatalf("StatusError in chain: %+v", f)
	}

	// Anything else is a 500 built from the coordinates, not the recorded text.
	f = first(`boom for User "alice"`, itemErr, get("/apis/example.io/v1/namespaces/n/things/t"))
	if f.Code != 500 || f.Reason != "InternalError" ||
		f.Message != `things.example.io "t" in namespace "n": upstream answered 500 Internal Server Error` {
		t.Fatalf("untyped: %+v", f)
	}

	// A non-apiserver path: the code only, never the URL.
	f = first(m, itemErr, get("/repos/x?token=s3cr3t"))
	if f.Message != "upstream answered 404 Not Found" {
		t.Fatalf("external path: %+v", f)
	}
	se := f.StatusError("ns", "ra")
	if !apierrors.IsNotFound(se) || se.Status().Message != `restaction ns/ra stage "crd" failed: upstream answered 404 Not Found` {
		t.Fatalf("StatusError: %+v", se.Status())
	}

	// The lowest-index failing item wins, whatever finished first.
	errs := []error{nil, newStageItemFailure("s", m, itemErr), newStageItemFailure("s", "x", itemErr)}
	calls := append(get("/a"), get("/apis/example.io/v1/namespaces/n/things/second")...)
	calls = append(calls, get("/apis/example.io/v1/namespaces/n/things/third")...)
	if got := firstStageFailure(errs, calls); got == nil || got.Code != 404 || got.Message != `things.example.io "second" not found` {
		t.Fatalf("firstStageFailure: %+v", got)
	}
	if firstStageFailure([]error{nil, nil}, nil) != nil {
		t.Fatal("no failure must be nil")
	}
}

// The #618 notServing waiver for canonicalStageMessage: it only formats error
// text, so its output must never carry the request's query string — no
// selector, limit or continue token can shape it. Every status class and path
// shape (named GET, LIST, cluster-scoped, non-apiserver) with a query attached.
func TestIssue709_CanonicalStageMessage_CarriesNoQuery(t *testing.T) {
	const q = "?labelSelector=team%3DSNTQ&fieldSelector=metadata.name%3DSNTQ&limit=7&continue=SNTQ"
	paths := []string{
		"/api/v1/namespaces/n/configmaps/c",
		"/api/v1/namespaces/n/configmaps",
		"/apis/apiextensions.k8s.io/v1/customresourcedefinitions/agents.kagent.dev",
		"/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
		"/repos/x/issues",
	}
	codes := []int{400, 401, 403, 404, 409, 422, 500, 503}
	for _, p := range paths {
		for _, code := range codes {
			for _, verb := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
				call := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: p + q, Verb: ptr.To(verb)}}
				msg := canonicalStageMessage(code, call)
				for _, bad := range []string{"?", "SNTQ", "labelSelector", "fieldSelector", "limit=", "continue="} {
					if strings.Contains(msg, bad) {
						t.Fatalf("canonicalStageMessage(%d, %s %s) carries query text %q: %q", code, verb, p+q, bad, msg)
					}
				}
			}
		}
	}
}
