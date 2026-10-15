package api

// issue723_724_redirect_test.go: falsifiers for #723 and #724. A client that
// carries a credential snowplow holds must never follow a redirect.
//
//   - R1 (#723): the branch-E fetch (httpFetchAllowingNonJSON) on an SA-marked
//     dial, where the "apiserver" answers 302/307 to another host. Nothing may
//     reach the other host, and the fetch must report the 3xx (Failure, with
//     the redirect's own code) with no body accepted.
//   - R2 (#724): the branch-C client (internalClientFor) built from the #574
//     impersonating *rest.Config, with the same 302/307. No Authorization and
//     no Impersonate-* may reach the other host, and the Get must fail with the
//     redirect's code. Through dispatchViaInternalRESTConfig the redirect must
//     not be served as the object.
//   - Same-host: the policy refuses EVERY redirect, so a redirect back to the
//     same origin is not followed either.
//
// RED on main eb055b77 (redirects followed); GREEN with internal/credhttp.
// The fixture credentials are synthetic strings, never real tokens.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

const redirectForgedBody = `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x","namespace":"n"},"data":{"forged":"yes"}}`

type hit struct {
	path, auth, impUser string
	impGroups           []string
}

type recorder struct {
	mu   sync.Mutex
	hits []hit
}

func (r *recorder) record(req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hits = append(r.hits, hit{
		path: req.URL.Path, auth: req.Header.Get("Authorization"),
		impUser: req.Header.Get("Impersonate-User"), impGroups: req.Header.Values("Impersonate-Group"),
	})
}

func (r *recorder) all() []hit {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]hit(nil), r.hits...)
}

// redirectRig: "other" is a foreign host that serves a forged ConfigMap on any
// path. "apiserver" answers every request under /api/ with `code` to
// other/stolen (crossHost) or to its own /same/... (same origin), whose
// handler also serves the forged ConfigMap.
type redirectRig struct {
	apiserver, other *httptest.Server
	otherHits        *recorder
	sameHits         *recorder
}

func newRedirectRig(t *testing.T, code int, crossHost bool) *redirectRig {
	t.Helper()
	rig := &redirectRig{otherHits: &recorder{}, sameHits: &recorder{}}
	rig.other = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rig.otherHits.record(req)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(redirectForgedBody))
	}))
	t.Cleanup(rig.other.Close)
	mux := http.NewServeMux()
	mux.HandleFunc("/same/", func(w http.ResponseWriter, req *http.Request) {
		rig.sameHits.record(req)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(redirectForgedBody))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		loc := rig.other.URL + "/stolen"
		if !crossHost {
			loc = "/same" + req.URL.Path
		}
		http.Redirect(w, req, loc, code)
	})
	rig.apiserver = httptest.NewServer(mux)
	t.Cleanup(rig.apiserver.Close)
	return rig
}

var redirectCodes = []int{http.StatusFound, http.StatusTemporaryRedirect}

// R1 (#723) — the SA-endpoint fetch behind every branch-E dial.
func TestIssue723_R1_SAEndpointFetchNeverFollowsRedirect(t *testing.T) {
	for _, code := range redirectCodes {
		for _, cross := range []bool{true, false} {
			rig := newRedirectRig(t, code, cross)
			ctx := cache.WithServiceAccountDial(context.Background())
			res, body, _, _ := httpFetchAllowingNonJSON(ctx, httpcall.RequestOptions{
				RequestInfo: httpcall.RequestInfo{
					Path: "/api/v1/namespaces/n/configmaps/x/status",
					Verb: ptr.To(http.MethodGet),
				},
				Endpoint: &endpoints.Endpoint{ServerURL: rig.apiserver.URL, Token: "sa-token-R1"},
			})
			if hits := rig.otherHits.all(); len(hits) != 0 {
				t.Errorf("code=%d cross=%v: RED — %d request(s) reached the other host, first auth-present=%v", code, cross, len(hits), hits[0].auth != "")
			}
			if hits := rig.sameHits.all(); len(hits) != 0 {
				t.Errorf("code=%d cross=%v: RED — the same-origin redirect was followed (%d request(s), auth present=%v)", code, cross, len(hits), hits[0].auth != "")
			}
			if res == nil || res.Status != response.StatusFailure || res.Code != code {
				t.Errorf("code=%d cross=%v: want a Failure carrying code %d, got %+v", code, cross, code, res)
			}
			if body != nil {
				t.Errorf("code=%d cross=%v: RED — the redirected body was accepted: %s", code, cross, body)
			}
		}
	}
}

func impersonatingRC(host string) *rest.Config {
	return &rest.Config{
		Host:        host,
		BearerToken: "sa-token-R2",
		Impersonate: rest.ImpersonationConfig{UserName: "cohort-user-R2", Groups: []string{"cohort-group-R2"}},
	}
}

func redirectCode(err error) int {
	var st apierrors.APIStatus
	if errors.As(err, &st) {
		return int(st.Status().Code)
	}
	return 0
}

// R2 (#724) — the branch-C client built from the #574 impersonating config.
func TestIssue724_R2_ImpersonatingRESTConfigNeverFollowsRedirect(t *testing.T) {
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	for _, code := range redirectCodes {
		for _, cross := range []bool{true, false} {
			resetInternalClientCacheForTest()
			rig := newRedirectRig(t, code, cross)
			cli, err := internalClientFor(impersonatingRC(rig.apiserver.URL))
			if err != nil {
				t.Fatal(err)
			}
			obj, err := cli.Resource(gvr).Namespace("n").Get(context.Background(), "x", metav1.GetOptions{})
			for _, h := range rig.otherHits.all() {
				t.Errorf("code=%d cross=%v: RED — the other host received path=%s auth-present=%v Impersonate-User-present=%v Impersonate-Group-count=%d",
					code, cross, h.path, h.auth != "", h.impUser != "", len(h.impGroups))
			}
			if hits := rig.sameHits.all(); len(hits) != 0 {
				t.Errorf("code=%d cross=%v: RED — the same-origin redirect was followed (%d request(s))", code, cross, len(hits))
			}
			if err == nil {
				t.Errorf("code=%d cross=%v: RED — err=nil, the redirected body was accepted as the object (data=%v)", code, cross, obj.Object["data"])
				continue
			}
			if got := redirectCode(err); got != code {
				t.Errorf("code=%d cross=%v: want a status error carrying %d, got code %d (%v)", code, cross, code, got, err)
			}
		}
	}
	resetInternalClientCacheForTest()
}

// R2 through the dispatcher: a refused redirect is a dispatch error, never a
// served object (and so never a cell).
func TestIssue724_R2_DispatchDoesNotServeARedirect(t *testing.T) {
	for _, code := range redirectCodes {
		resetInternalClientCacheForTest()
		rig := newRedirectRig(t, code, true)
		ctx := cache.WithInternalRESTConfig(context.Background(), impersonatingRC(rig.apiserver.URL))
		raw, served, err := dispatchViaInternalRESTConfig(ctx, httpcall.RequestOptions{
			RequestInfo: httpcall.RequestInfo{Path: "/api/v1/namespaces/n/configmaps/x", Verb: ptr.To(http.MethodGet)},
		})
		if len(rig.otherHits.all()) != 0 {
			t.Errorf("code=%d: RED — the dispatch reached the other host", code)
		}
		if served || raw != nil {
			t.Errorf("code=%d: RED — the redirect target's body was served (served=%v, %d bytes)", code, served, len(raw))
		}
		if err == nil || redirectCode(err) != code {
			t.Errorf("code=%d: want a dispatch error carrying %d, got %v", code, code, err)
		}
	}
	resetInternalClientCacheForTest()
}
