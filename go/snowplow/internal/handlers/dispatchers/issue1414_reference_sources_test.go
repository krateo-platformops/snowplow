// issue1414_reference_sources_test.go — #1257 / #1414: a reference that is not
// an apiserver path is refused whatever its source and whatever the credential.
//
// A /call query is one source of the reference objects.Get reads. The others
// carry it from CR content or from a cache entry's inputs, and read under
// snowplow's own ServiceAccount: the prewarm walk's child fetch, a nested
// call, a widget's apiRef, the refresher's re-fetch. objects.Get refuses the
// reference at its entry, so every one of them is covered by the one check;
// these arms drive one caller of each class and assert the apiserver sees NO
// request at all (no discovery, no dial). The test apiserver serves every
// target such a reference would reach, so an escape would succeed and be seen.
//
// They also pin the credential of the mapper-miss route under the
// ServiceAccount: every request of a read, the miss route's included, carries
// the same Authorization and the same impersonation as its discovery.
package dispatchers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	"github.com/krateo-platformops/snowplow/internal/testsupport/unservedapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
)

// i1414EscapingRefs are references whose resource, version or group would
// leave the object they name if joined into a path as given.
func i1414EscapingRefs() map[string]templatesv1.ObjectReference {
	ref := func(apiVersion, resource, name string) templatesv1.ObjectReference {
		return templatesv1.ObjectReference{
			Reference: templatesv1.Reference{Name: name, Namespace: "ns"}, APIVersion: apiVersion, Resource: resource,
		}
	}
	return map[string]templatesv1.ObjectReference{
		"another_namespaces_configmap": ref(unservedapi.GV, "../../../../../api/v1/namespaces/other/configmaps", "x"),
		"pod_log_subresource":          ref("v1", "pods/p", "log"),
		"service_proxy":                ref("v1", "services/svc:80/proxy", "metrics"),
		"non_resource_path":            ref("v1", "../../../../metrics", "x"),
		"version_dot_dot":              ref(unservedapi.Group+"/..", "buttons", "x"),
	}
}

func i1414BadRequest(err error) bool {
	var se *apierrors.StatusError
	return errors.As(err, &se) && se.ErrStatus.Code == http.StatusBadRequest
}

func TestIssue1414_ANonPathReferenceIsRefusedFromEverySourceUnderTheServiceAccount(t *testing.T) {
	for _, cacheOn := range []string{"true", "false"} {
		for name, ref := range i1414EscapingRefs() {
			// Each caller class gets its own apiserver, so "no request" is per caller.
			newCtx := func(t *testing.T) (context.Context, *unservedapi.Server, *httptest.Server) {
				t.Helper()
				t.Setenv("CACHE_ENABLED", cacheOn)
				a := &unservedapi.Server{}
				srv := httptest.NewServer(a)
				t.Cleanup(srv.Close)
				return f6cSACtx(srv.URL), a, srv
			}
			nothingSent := func(t *testing.T, a *unservedapi.Server) {
				t.Helper()
				if got := a.Paths(); len(got) != 0 {
					t.Fatalf("nothing may be sent for such a reference, the apiserver saw %v", got)
				}
			}
			prefix := "cache=" + cacheOn + "/" + name

			// The prewarm walk's child fetch: objects.Get under the walk's ServiceAccount context.
			t.Run(prefix+"/walk_child", func(t *testing.T) {
				ctx, a, _ := newCtx(t)
				got := objects.Get(ctx, ref)
				nothingSent(t, a)
				if got.Err == nil || got.Err.Code != http.StatusBadRequest || got.Unstructured != nil {
					t.Fatalf("want a 400 and no object, got err=%+v object=%v", got.Err, got.Unstructured != nil)
				}
				// This reference did not come from a query: the refusal names the reference's
				// field, and neither the value nor the object.
				m := got.Err.Message
				if !strings.HasPrefix(m, `invalid reference field "resource": `) && !strings.HasPrefix(m, `invalid reference field "apiVersion": `) {
					t.Errorf("the 400 must name the reference's field, got %q", m)
				}
				for _, part := range []string{"query", "configmaps", "other", "metrics", "svc", "pods", "buttons", ref.Name + `"`} {
					if strings.Contains(m, part) {
						t.Errorf("the 400 carries %q: %q", part, m)
					}
				}
				// And the walk does not take the refusal for a deleted child.
				w, _ := f6cWalker(t)
				if w.forgetChildIfGone(ctx, ref, got.Err) {
					t.Fatalf("a refused reference was treated as a deleted child")
				}
			})
			t.Run(prefix+"/nested_call", func(t *testing.T) {
				ctx, a, _ := newCtx(t)
				_, err := ResolveNestedCall(ctx, ref, 0, 0, nil)
				nothingSent(t, a)
				if !i1414BadRequest(err) {
					t.Fatalf("want the read's 400, got %v", err)
				}
			})
			t.Run(prefix+"/widget_apiref", func(t *testing.T) {
				ctx, a, srv := newCtx(t)
				_, err := apiref.Resolve(ctx, apiref.ResolveOptions{RC: &rest.Config{Host: srv.URL}, ApiRef: ref})
				nothingSent(t, a)
				if !i1414BadRequest(err) {
					t.Fatalf("want the read's 400, got %v", err)
				}
			})
		}
	}
}

// The refresher's re-fetch reads the coordinates of a cache entry's inputs.
func TestIssue1414_ANonPathReferenceIsRefusedOnTheRefreshersRefetch(t *testing.T) {
	for name, ref := range i1414EscapingRefs() {
		t.Run(name, func(t *testing.T) {
			a := &unservedapi.Server{}
			srv := &i187APIServer{Server: httptest.NewServer(a)}
			t.Cleanup(srv.Close)
			_, inputs, _, saEP, saRC := i187Fixture(t, srv, true)
			inputs.Resource = ref.Resource
			if ref.APIVersion == "v1" {
				inputs.Group, inputs.Version = "", "v1"
			} else if name == "version_dot_dot" {
				inputs.Version = ".."
			}
			inputs.Namespace, inputs.Name = ref.Namespace, ref.Name

			err := resolveAndPopulateL1(context.Background(), inputs, saEP, saRC)
			if got := a.Paths(); len(got) != 0 {
				t.Fatalf("nothing may be sent for such inputs, the apiserver saw %v", got)
			}
			if !i1414BadRequest(err) {
				t.Fatalf("want the re-fetch's 400, got %v", err)
			}
		})
	}
}

// Under the ServiceAccount, with and without impersonation: every request of a
// read carries the same credential and the same impersonation as its
// discovery, on the mapped route and on the mapper-miss route (the group's own
// document and the object GET).
func TestIssue1414_TheMissRouteCarriesTheReadsCredentialUnderTheServiceAccount(t *testing.T) {
	const token = "i1414-sa-token"
	ref := templatesv1.ObjectReference{
		Reference: templatesv1.Reference{Name: "x", Namespace: "ns"}, APIVersion: unservedapi.GV, Resource: "buttons",
	}
	for _, c := range []struct {
		name        string
		impersonate rest.ImpersonationConfig
		miss        bool
	}{
		{"sa_mapped", rest.ImpersonationConfig{}, false},
		{"sa_miss", rest.ImpersonationConfig{}, true},
		{"sa_impersonating_mapped", rest.ImpersonationConfig{UserName: "cohort-representative", Groups: []string{"devs"}}, false},
		{"sa_impersonating_miss", rest.ImpersonationConfig{UserName: "cohort-representative", Groups: []string{"devs"}}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("CACHE_ENABLED", "false")
			a := &unservedapi.Server{Token: token, WidgetsDiscoveryLags: c.miss}
			srv := httptest.NewServer(a)
			t.Cleanup(srv.Close)
			ctx := withPhase1SAContext(context.Background(),
				endpoints.Endpoint{ServerURL: srv.URL},
				&rest.Config{Host: srv.URL, BearerToken: token, Impersonate: c.impersonate})

			got := objects.Get(ctx, ref)
			if got.Err != nil || got.Unstructured == nil || got.Unstructured.GetName() != "x" {
				t.Fatalf("premise: the served object must be read, got %+v; requests: %v", got.Err, a.Paths())
			}
			reqs := a.Requests()
			var docReads, objectGets int
			for _, r := range reqs {
				switch {
				case r.Path == "/apis/"+unservedapi.GV:
					docReads++
				case !unservedapi.IsDiscovery(r.Path):
					objectGets++
				}
				if r.Auth != "bearer-match" {
					t.Errorf("%s: Authorization=%s, want the read's bearer token", r.Path, r.Auth)
				}
				if r.Impersonates != reqs[0].Impersonates {
					t.Errorf("%s: impersonates=%v, the read's discovery (%s) impersonates=%v", r.Path, r.Impersonates, reqs[0].Path, reqs[0].Impersonates)
				}
			}
			if wantImp := c.impersonate.UserName != ""; reqs[0].Impersonates != wantImp {
				t.Errorf("premise: discovery impersonates=%v, want %v", reqs[0].Impersonates, wantImp)
			}
			if objectGets != 1 || (c.miss && docReads != 1) || (!c.miss && docReads != 0) {
				t.Fatalf("premise: object GETs=%d group document reads=%d (miss route=%v); requests: %v", objectGets, docReads, c.miss, a.Paths())
			}
		})
	}
}
