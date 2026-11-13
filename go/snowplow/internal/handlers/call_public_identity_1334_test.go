package handlers

// call_public_identity_1334_test.go — #1334 MN3 + MN4 on the raw /call
// passthrough (reads, lists and every write verb), driven end to end: the REAL
// auth middleware (UserConfigOrPublic) in front of the REAL Call() handler, a
// token-less request, and a fake apiserver that authenticates the
// ServiceAccount bearer, honours exactly the impersonation the chart grants, and
// authorizes with the independent RBAC model (testsupport/k8sauthz) over the
// identity the impersonation names.
//
//   - MN3: every request the apiserver receives for the public identity carries
//     Impersonate-User krateo:public and exactly Impersonate-Group
//     system:unauthenticated. The fake treats a non-impersonated ServiceAccount
//     request as the ServiceAccount (which may do anything), so a dial that
//     dropped the impersonation would make a refused write SUCCEED here.
//   - MN4: a read and a write succeed only where RBAC grants them to the USER
//     krateo:public; a grant to system:authenticated does not reach it. (The
//     passthrough names one object; a LIST is a RESTAction stage, whose
//     impersonated refusal is chartcrd's kind arm and whose in-process verdict
//     is #1342's evaluator parity.)
//   - Audit: a write is recorded by the existing /call audit writer, as any
//     write is; a read is not.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/testsupport/k8sauthz"
)

const (
	i1334CallSAToken = "sa-token-call-1334"
	i1334CallNS      = "krateo-public"
)

type i1334Hit struct {
	method, path, user, authz string
	groups                    []string
}

// i1334FakeAPIServer is the apiserver for the passthrough. It records every
// request and answers per the RBAC model over the impersonated identity.
func i1334FakeAPIServer(t *testing.T, authz *k8sauthz.Authorizer) (*httptest.Server, func() []i1334Hit) {
	t.Helper()
	var mu sync.Mutex
	var hits []i1334Hit
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := i1334Hit{method: r.Method, path: r.URL.Path, user: r.Header.Get("Impersonate-User"),
			groups: r.Header.Values("Impersonate-Group"), authz: r.Header.Get("Authorization")}
		mu.Lock()
		hits = append(hits, h)
		mu.Unlock()
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		status := func(code int, reason string) {
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"` + reason + `","code":` +
				strconv.Itoa(code) + `}`))
		}
		if h.authz != "Bearer "+i1334CallSAToken {
			status(http.StatusUnauthorized, "Unauthorized")
			return
		}
		user, groups := "system:serviceaccount:krateo-system:snowplow", []string(nil)
		if h.user != "" || len(h.groups) > 0 {
			// The chart's grant: exactly these principals, by name.
			if h.user != "krateo:public" || !slices.Equal(h.groups, []string{"system:unauthenticated"}) {
				status(http.StatusForbidden, "Forbidden")
				return
			}
			user, groups = h.user, h.groups
		}
		// /api/v1/namespaces/<ns>/configmaps[/<name>]
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 5 || parts[0] != "api" || parts[4] != "configmaps" {
			status(http.StatusNotFound, "NotFound")
			return
		}
		ns, name := parts[3], ""
		if len(parts) > 5 {
			name = parts[5]
		}
		verb := map[string]string{http.MethodPost: "create", http.MethodPut: "update", http.MethodPatch: "patch", http.MethodDelete: "delete"}[r.Method]
		if r.Method == http.MethodGet {
			verb = "list"
			if name != "" {
				verb = "get"
			}
		}
		if user != "system:serviceaccount:krateo-system:snowplow" && !authz.Allowed(k8sauthz.Attributes{
			User: user, Groups: groups, Verb: verb, Resource: "configmaps", Namespace: ns, Name: k8sauthz.RequestName(verb, name),
		}) {
			status(http.StatusForbidden, "Forbidden")
			return
		}
		code := http.StatusOK
		if verb == "create" {
			code = http.StatusCreated
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"` + ns + `","uid":"uid-1334"}}`))
	}))
	t.Cleanup(srv.Close)
	return srv, func() []i1334Hit {
		mu.Lock()
		defer mu.Unlock()
		return append([]i1334Hit(nil), hits...)
	}
}

// i1334World: the page's Role, bound to the USER krateo:public, grants get on
// public-page and create on configmaps; a second Role, bound to Group
// system:authenticated only, grants get on authenticated-only.
func i1334World() *k8sauthz.Authorizer {
	g := rbacv1.GroupName
	return k8sauthz.New(
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: i1334CallNS, Name: "page"}, Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{"public-page"}, Verbs: []string{"get"}},
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"create"}},
		}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: i1334CallNS, Name: "page"},
			Subjects: []rbacv1.Subject{{Kind: "User", APIGroup: g, Name: "krateo:public"}},
			RoleRef:  rbacv1.RoleRef{APIGroup: g, Kind: "Role", Name: "page"}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: i1334CallNS, Name: "authn-only"}, Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{"authenticated-only"}, Verbs: []string{"get", "update", "patch", "delete"}},
		}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: i1334CallNS, Name: "authn-only"},
			Subjects: []rbacv1.Subject{{Kind: "Group", APIGroup: g, Name: "system:authenticated"}},
			RoleRef:  rbacv1.RoleRef{APIGroup: g, Kind: "Role", Name: "authn-only"}},
	)
}

func i1334Grants(t *testing.T) {
	t.Helper()
	cache.ResetImpersonationCapabilityForTest()
	t.Cleanup(cache.ResetImpersonationCapabilityForTest)
	t.Cleanup(cache.SetImpersonationAvailableForTest(true))
	cache.SetImpersonationReviewer(func(_ context.Context, resource, _, name string) (bool, error) {
		return (resource == cache.ImpersonateResourceUsers && name == middleware.PublicUsername) ||
			(resource == cache.ImpersonateResourceGroups && name == middleware.PublicGroup), nil
	})
}

func TestIssue1334_PublicPassthrough_ImpersonatedAndDecidedByRBAC(t *testing.T) {
	prev := xenv.TestMode()
	xenv.SetTestMode(true)
	t.Cleanup(func() { xenv.SetTestMode(prev) })
	i1334Grants(t)
	api, hits := i1334FakeAPIServer(t, i1334World())
	rec := recorder957(t)

	public := middleware.NewPublicIdentity(
		func() (*endpoints.Endpoint, error) { return &endpoints.Endpoint{ServerURL: api.URL, Token: i1334CallSAToken}, nil },
		func() (*rest.Config, error) { return &rest.Config{Host: api.URL, BearerToken: i1334CallSAToken}, nil })
	// The real middleware: any JWT is refused (the key source holds no key), so
	// only the token-less path can reach Call().
	h := use.NewChain(middleware.UserConfigOrPublic(jwtutil.NewStaticKeySource(nil), "krateo-system", public)).Then(Call())

	cm := func(name string) string {
		return "/call?apiVersion=v1&resource=configmaps&namespace=" + i1334CallNS + "&name=" + name
	}
	body := `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"form-entry","namespace":"` + i1334CallNS + `"}}`
	for _, tc := range []struct {
		name, method, target, body string
		want                       int
	}{
		{"get granted to the user", http.MethodGet, cm("public-page"), "", http.StatusOK},
		{"get granted only to system:authenticated", http.MethodGet, cm("authenticated-only"), "", http.StatusForbidden},
		{"get in another namespace", http.MethodGet, "/call?apiVersion=v1&resource=configmaps&namespace=default&name=public-page", "", http.StatusForbidden},
		{"create granted to the user", http.MethodPost, cm("form-entry"), body, http.StatusOK},
		{"create in another namespace", http.MethodPost, "/call?apiVersion=v1&resource=configmaps&namespace=default&name=form-entry",
			strings.Replace(body, i1334CallNS, "default", 1), http.StatusForbidden},
		{"update granted only to system:authenticated", http.MethodPut, cm("authenticated-only"),
			strings.Replace(body, "form-entry", "authenticated-only", 1), http.StatusForbidden},
		{"patch granted only to system:authenticated", http.MethodPatch, cm("authenticated-only"), `{"data":{"k":"v"}}`, http.StatusForbidden},
		{"delete granted only to system:authenticated", http.MethodDelete, cm("authenticated-only"), "", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := len(hits())
			var rdr io.Reader
			if tc.body != "" {
				rdr = bytes.NewReader([]byte(tc.body))
			}
			req := httptest.NewRequest(tc.method, tc.target, rdr)
			if tc.body != "" {
				ct := "application/json"
				if tc.method == http.MethodPatch {
					ct = "application/merge-patch+json"
				}
				req.Header.Set("Content-Type", ct)
			}
			// Identity hints on the request change nothing (MN2).
			req.Header.Set("Impersonate-User", "alice")
			req.Header.Set("Impersonate-Group", "system:masters")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("MUST NEVER: %s %s answered %d, want %d (RBAC decides for krateo:public). body=%s",
					tc.method, tc.target, w.Code, tc.want, w.Body.String())
			}
			got := hits()[before:]
			if len(got) != 1 {
				t.Fatalf("want exactly one apiserver request, got %d: %+v", len(got), got)
			}
			if got[0].user != "krateo:public" || !slices.Equal(got[0].groups, []string{"system:unauthenticated"}) {
				t.Fatalf("MUST NEVER: the apiserver received %s %s as user=%q groups=%v; every request for the public identity is impersonated as krateo:public [system:unauthenticated]",
					got[0].method, got[0].path, got[0].user, got[0].groups)
			}
		})
	}

	// Audit: the five writes are recorded by the /call audit writer, the three
	// reads are not.
	recs := records957(rec)
	if len(recs) != 5 {
		t.Fatalf("audit: want 5 write records (1 success, 4 refused), got %d: %v", len(recs), recs)
	}
	ok := 0
	for _, r := range recs {
		if r["user.name"] != "krateo:public" {
			t.Errorf("audit: a public write is recorded as user.name=%q, want krateo:public", r["user.name"])
		}
		if r["outcome"] == "success" {
			ok++
		}
	}
	if ok != 1 {
		t.Fatalf("audit: want exactly one successful write (the granted create), got %d: %v", ok, recs)
	}
}
