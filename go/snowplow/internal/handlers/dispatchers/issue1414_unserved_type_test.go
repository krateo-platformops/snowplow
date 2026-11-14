// issue1414_unserved_type_test.go — #1414: a /call for an object whose type the
// apiserver does not serve.
//
// THE DEFECT. The read's dynamic client resolves the resource through a
// discovery RESTMapper before dialling. With no CRD the mapper has no mapping,
// its error is not a Status, and the handler answered 500.
//
// THE ANSWER. 424 Failed Dependency, as a JSON Status with reason
// FailedDependency and a message naming the type and nothing else. Not 404: a
// 404 says an object is not there, which the portal retries as a cold start
// and reads, on a refetch, as a confirmed delete.
//
// These arms go through the REAL handlers (Widgets(), RESTAction()) and the
// real read (fetchObject → objects.Get → the requester's client config →
// dynamic.NewClient) against internal/testsupport/unservedapi, which answers
// as kube-apiserver does and records every request.
//
// They also pin the three places where "type not installed" must not read as
// "object deleted": the refresher's re-fetch of a cell's own object
// (resolve_populate.go, self-object-gone eviction), the Phase-1 walk's child
// fetch (forgetChildIfGone) and the widget resolve's dependency answer
// (encodeWidgetResolveError).
package dispatchers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	jwtutil "github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/testsupport/unservedapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const i1414Token = "i1414-requester-token"

// i1414Call sends GET /call with the given query to handler, as a requester
// whose endpoint is the test apiserver (a bearer token, the one credential it
// accepts), with the cache off so the read is the requester's own.
func i1414Call(t *testing.T, handler http.Handler, a *unservedapi.Server, query url.Values) *httptest.ResponseRecorder {
	t.Helper()
	prev := env.TestMode()
	env.SetTestMode(true) // keep the endpoint's server URL as given
	t.Cleanup(func() { env.SetTestMode(prev) })
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("DEBUG", "false")

	a.Token = i1414Token
	srv := httptest.NewTLSServer(a)
	t.Cleanup(srv.Close)
	ca := base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "requester", Groups: []string{"devs"}}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, CertificateAuthorityData: ca, Username: "requester", Token: a.Token}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/call?"+query.Encode(), nil).WithContext(ctx))
	return rec
}

func i1414Query(apiVersion, resource, namespace, name string) url.Values {
	return url.Values{"apiVersion": {apiVersion}, "resource": {resource}, "namespace": {namespace}, "name": {name}}
}

// i1414Status decodes the response as the JSON Status every /call error is,
// and fails when it is not one.
func i1414Status(t *testing.T, rec *httptest.ResponseRecorder) metav1.Status {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json; body: %s", ct, rec.Body.String())
	}
	var st metav1.Status
	dec := json.NewDecoder(strings.NewReader(rec.Body.String()))
	if err := dec.Decode(&st); err != nil {
		t.Fatalf("the body is not JSON: %v; body: %s", err, rec.Body.String())
	}
	if st.Kind != "Status" || st.APIVersion != "v1" {
		t.Fatalf("the body is not a Status: kind=%q apiVersion=%q; body: %s", st.Kind, st.APIVersion, rec.Body.String())
	}
	if int(st.Code) != rec.Code {
		t.Fatalf("the Status code %d differs from the HTTP status %d", st.Code, rec.Code)
	}
	return st
}

func i1414Handlers() map[string]http.Handler {
	return map[string]http.Handler{"widgets": Widgets(), "restactions": RESTAction()}
}

func TestIssue1414_CallForAnUnservedTypeIs424FailedDependency(t *testing.T) {
	for hname, h := range i1414Handlers() {
		for _, c := range []struct {
			name, apiVersion, resource, want string
			legacy                           bool
		}{
			{"crd_absent", unservedapi.GV, "flexes",
				"The type flexes (widgets.templates.krateo.io/v1beta1) is not installed on this cluster.", false},
			{"crd_absent_legacy_discovery", unservedapi.GV, "flexes",
				"The type flexes (widgets.templates.krateo.io/v1beta1) is not installed on this cluster.", true},
			{"group_absent", "absent.example.org/v1", "things",
				"The type things (absent.example.org/v1) is not installed on this cluster.", false},
			{"core_group", "v1", "things",
				"The type things (v1) is not installed on this cluster.", false},
		} {
			t.Run(hname+"/"+c.name, func(t *testing.T) {
				a := &unservedapi.Server{LegacyDiscovery: c.legacy}
				rec := i1414Call(t, h, a, i1414Query(c.apiVersion, c.resource, "ns", "header-context"))
				t.Logf("status=%d content-type=%q body=%s", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
				if rec.Code != http.StatusFailedDependency {
					t.Fatalf("status = %d, want 424; body: %s", rec.Code, rec.Body.String())
				}
				st := i1414Status(t, rec)
				if st.Status != metav1.StatusFailure {
					t.Errorf("status = %q, want Failure", st.Status)
				}
				if st.Code != http.StatusFailedDependency {
					t.Errorf("code = %d, want 424", st.Code)
				}
				if st.Reason != "FailedDependency" {
					t.Errorf("reason = %q, want FailedDependency", st.Reason)
				}
				if st.Message != c.want {
					t.Errorf("message = %q, want exactly %q", st.Message, c.want)
				}
				for _, leak := range []string{"header-context", "requester", "devs", i1414Token} {
					if strings.Contains(rec.Body.String(), leak) {
						t.Errorf("the body carries %q: %s", leak, rec.Body.String())
					}
				}
				if all, _ := a.ObjectPaths(); len(all) != 0 {
					t.Errorf("an unserved type must not be requested from the apiserver, saw %v", all)
				}
			})
		}
	}
}

// A served type keeps its answers through the same handlers: a missing object
// is the apiserver's 404, a group whose backend is down is a 5xx. Neither is
// ever a 424.
func TestIssue1414_CallForAServedTypeKeepsItsAnswers(t *testing.T) {
	for hname, h := range i1414Handlers() {
		t.Run(hname+"/missing_object_is_404", func(t *testing.T) {
			a := &unservedapi.Server{}
			rec := i1414Call(t, h, a, i1414Query(unservedapi.GV, "buttons", "ns", "gone"))
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404; body: %s", rec.Code, rec.Body.String())
			}
			st := i1414Status(t, rec)
			if st.Reason != metav1.StatusReasonNotFound || st.Message != `buttons.widgets.templates.krateo.io "gone" not found` {
				t.Errorf("want the apiserver's NotFound, got reason=%q message=%q", st.Reason, st.Message)
			}
			all, malformed := a.ObjectPaths()
			if want := "/apis/" + unservedapi.GV + "/namespaces/ns/buttons/gone"; len(all) != 1 || all[0] != want || len(malformed) != 0 {
				t.Errorf("the apiserver saw object paths %v, want exactly [%s]", all, want)
			}
		})
		t.Run(hname+"/missing_object_through_a_mapper_miss_is_404", func(t *testing.T) {
			a := &unservedapi.Server{WidgetsDiscoveryLags: true}
			rec := i1414Call(t, h, a, i1414Query(unservedapi.GV, "buttons", "ns", "gone"))
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404; body: %s", rec.Code, rec.Body.String())
			}
			if st := i1414Status(t, rec); st.Reason != metav1.StatusReasonNotFound {
				t.Errorf("reason = %q, want NotFound", st.Reason)
			}
		})
		t.Run(hname+"/backend_down_is_5xx", func(t *testing.T) {
			for _, resource := range []string{"buttons", "flexes"} {
				a := &unservedapi.Server{WidgetsBackendOff: true}
				rec := i1414Call(t, h, a, i1414Query(unservedapi.GV, resource, "ns", "x"))
				if rec.Code < 500 || rec.Code > 599 {
					t.Fatalf("%s: status = %d, want 5xx; body: %s", resource, rec.Code, rec.Body.String())
				}
				i1414Status(t, rec)
			}
		})
	}
}

// #1257 on the resolve route: a query whose group, version or resource is not
// one apiserver path segment is a 400 before anything is sent. The first four
// are the review's reproducers; the test apiserver serves each target, so an
// escape would succeed and be seen.
func TestIssue1414_AQueryThatIsNotOnePathIsA400AndNothingIsSent(t *testing.T) {
	for hname, h := range i1414Handlers() {
		for _, c := range []struct {
			name string
			q    url.Values
		}{
			{"traverse_to_another_namespaces_configmap", i1414Query(unservedapi.GV, "../../../../../api/v1/namespaces/other/configmaps", "ns", "x")},
			{"pod_log_subresource", i1414Query("v1", "pods/p", "ns", "log")},
			{"service_proxy", i1414Query("v1", "services/svc:80/proxy", "ns", "metrics")},
			{"traverse_to_metrics", i1414Query("v1", "../../../../metrics", "ns", "x")},
			{"version_dot_dot", i1414Query(unservedapi.Group+"/..", "buttons", "ns", "x")},
			{"percent_encoded_slash", i1414Query(unservedapi.GV, "buttons%2Fx", "ns", "x")},
		} {
			t.Run(hname+"/"+c.name, func(t *testing.T) {
				a := &unservedapi.Server{}
				rec := i1414Call(t, h, a, c.q)
				if got := a.Paths(); len(got) != 0 {
					t.Fatalf("nothing may be sent for such a query (no discovery, no dial), the apiserver saw %v", got)
				}
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want 400; body: %s", rec.Code, rec.Body.String())
				}
				st := i1414Status(t, rec)
				// The refusal names the parameter and the rule, never the value.
				for _, part := range []string{"configmaps", "other", "metrics", "svc", "pods", "buttons"} {
					if strings.Contains(st.Message, part) {
						t.Errorf("the 400 echoes the value (%q): %q", part, st.Message)
					}
				}
			})
		}
	}
}

// Every accepted read, through the real handlers: the only non-discovery path
// the apiserver sees is /api[s/<group>]/<version>/namespaces/<ns>/<resource>/<name>,
// for the object the query names, under the requester's credential.
func TestIssue1414_AnAcceptedReadDialsOnlyItsObjectsPath(t *testing.T) {
	for hname, h := range i1414Handlers() {
		for _, c := range []struct {
			name string
			a    *unservedapi.Server
			q    url.Values
			want []string
		}{
			{"mapped", &unservedapi.Server{}, i1414Query(unservedapi.GV, "buttons", "ns", "gone"),
				[]string{"/apis/" + unservedapi.GV + "/namespaces/ns/buttons/gone"}},
			{"mapped_core", &unservedapi.Server{}, i1414Query("v1", "configmaps", "ns", "gone"),
				[]string{"/api/v1/namespaces/ns/configmaps/gone"}},
			{"mapper_miss", &unservedapi.Server{WidgetsDiscoveryLags: true}, i1414Query(unservedapi.GV, "buttons", "ns", "gone"),
				[]string{"/apis/" + unservedapi.GV + "/namespaces/ns/buttons/gone"}},
			{"mapper_miss_singular_upper_case", &unservedapi.Server{WidgetsDiscoveryLags: true}, i1414Query(unservedapi.GV, "Button", "ns", "gone"),
				[]string{"/apis/" + unservedapi.GV + "/namespaces/ns/buttons/gone"}},
			{"unserved", &unservedapi.Server{}, i1414Query(unservedapi.GV, "flexes", "ns", "x"), nil},
		} {
			t.Run(hname+"/"+c.name, func(t *testing.T) {
				i1414Call(t, h, c.a, c.q)
				all, malformed := c.a.ObjectPaths()
				if len(malformed) != 0 || fmt.Sprint(all) != fmt.Sprint(c.want) {
					t.Fatalf("object paths = %v (malformed %v), want exactly %v", all, malformed, c.want)
				}
				reqs := c.a.Requests()
				if len(reqs) == 0 {
					t.Fatalf("PRECONDITION: the read never reached the apiserver")
				}
				for _, r := range reqs {
					if r.Auth != "bearer-match" || r.Impersonates {
						t.Errorf("%s %s: Authorization=%s impersonates=%v, want the requester's bearer token and no impersonation",
							r.Method, r.Path, r.Auth, r.Impersonates)
					}
				}
			})
		}
	}
}

// --- "type not installed" is never "object deleted" --------------------------

// i1414UnservedSA returns an i187 apiserver handle over unservedapi, which does
// not serve the fixture's type (flexes): its discovery lists the group without
// it, and its path is the apiserver's 404.
func i1414UnservedSA(t *testing.T) (*i187APIServer, *unservedapi.Server) {
	t.Helper()
	a := &unservedapi.Server{LegacyDiscovery: true}
	s := &i187APIServer{Server: httptest.NewServer(a)}
	t.Cleanup(s.Close)
	return s, a
}

// Site 1 — the refresher's re-fetch of a cell's own object. A definite 404 is
// a deletion: after the requeue budget the cell is evicted (#187). An
// uninstalled type is not one: the failure is classed type_not_installed at
// the refetch stage, keeps the budget (a CRD re-registration clears inside
// it), and at the drop point the cell stays RESIDENT with its last body, known
// stale, so the next read revalidates it and a later install finds it.
func TestIssue1414_RefresherTypeNotInstalledIsNotSelfObjectGone(t *testing.T) {
	i187RefresherEnv(t)
	srv, a := i1414UnservedSA(t)
	store, inputs, key, saEP, saRC := i187Fixture(t, srv, true)
	before, ok := store.Get(key)
	if !ok {
		t.Fatalf("premise: the fixture cell is not resident")
	}
	lastBody := string(before.RawJSON)

	err := resolveAndPopulateL1(context.Background(), inputs, saEP, saRC)
	if err == nil {
		t.Fatalf("premise: want an error from the re-fetch of an unserved type")
	}
	if len(a.Paths()) == 0 {
		t.Fatalf("premise: the re-fetch never reached the apiserver")
	}
	var se *apierrors.StatusError
	if !errors.As(err, &se) || se.ErrStatus.Code != http.StatusFailedDependency {
		t.Fatalf("the re-fetch error must carry objects.Get's 424 Status, got %v", err)
	}
	if errors.Is(err, cache.ErrSelfObjectGone) {
		t.Fatalf("an uninstalled type was classified as a deleted object (self-object-gone): %v", err)
	}
	if c, s := cache.ClassifyRefreshError(err); c != cache.RefreshErrTypeNotInstalled || s != cache.ResolveStageRefetch {
		t.Errorf("ClassifyRefreshError = (%s, %s), want (type_not_installed, refetch)", c, s)
	}
	if cache.IsNonRetryableRefreshError(err) {
		t.Errorf("an uninstalled type must keep the requeue budget")
	}

	// The whole loop, to the drop point.
	cache.ResetRefreshFailedByClassForTest()
	t.Cleanup(cache.ResetRefreshFailedByClassForTest)
	evictsBefore := cache.RefresherSelfNotFoundEvictTotal()
	n := i187RunRefreshCycle(t, inputs, key, saEP, saRC, store)
	if n != i187MaxRequeues+1 {
		t.Errorf("attempts = %d, want the whole budget (%d)", n, i187MaxRequeues+1)
	}
	after, resident := store.Get(key)
	if !resident || string(after.RawJSON) != lastBody {
		t.Fatalf("an uninstalled type removed the cell (resident=%v): a later install would find nothing to refresh", resident)
	}
	if got := cache.RefresherSelfNotFoundEvictTotal(); got != evictsBefore {
		t.Errorf("self_notfound_evict_total moved %d → %d: an uninstalled type took the deleted-object route", evictsBefore, got)
	}
	byClass := map[string]uint64{}
	for _, c := range cache.RefreshFailedByClassSnapshot() {
		byClass[c.Class+"/"+c.Stage] = c.N
	}
	if byClass["type_not_installed/refetch"] != uint64(n) || len(byClass) != 1 {
		t.Errorf("refresh_failed by class = %v, want exactly {type_not_installed/refetch: %d}", byClass, n)
	}
}

// Site 2 — the Phase-1 walk's child fetch. A confirmed 404 forgets the child's
// harvested copy; an uninstalled type keeps it.
func TestIssue1414_WalkTypeNotInstalledKeepsTheHarvestedChild(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	srv, a := i1414UnservedSA(t)
	ctx := f6cSACtx(srv.URL)

	got := objects.Get(ctx, f6cRef())
	if got.Err == nil || got.Err.Code != http.StatusFailedDependency {
		t.Fatalf("premise: want objects.Get's 424 for an unserved type, got %+v", got.Err)
	}
	if len(a.Paths()) == 0 {
		t.Fatalf("premise: the read never reached the apiserver")
	}
	w, nav := f6cWalker(t)
	if gone := w.forgetChildIfGone(ctx, f6cRef(), got.Err); gone {
		t.Fatalf("an uninstalled type was treated as a deleted child")
	}
	if !f6bHarvestedStillHolds(nav, f6cGVR(), i187NS, i187Name) {
		t.Fatalf("an uninstalled type dropped the harvested copy of %s/%s", i187NS, i187Name)
	}
}

// Site 3 — the widget resolve's dependency answer. A dependency's 404 is
// answered 424 with the dependency's message (#853). A dependency whose TYPE
// is not installed already is that answer: it keeps its 424, its reason and
// its message as a JSON Status.
func TestIssue1414_ResolveErrorKeepsADependencysTypeNotInstalled(t *testing.T) {
	const msg = "The type restactions (templates.krateo.io/v1) is not installed on this cluster."
	dep := &apierrors.StatusError{ErrStatus: metav1.Status{
		Status: metav1.StatusFailure, Code: http.StatusFailedDependency, Reason: "FailedDependency", Message: msg,
	}}
	rec := httptest.NewRecorder()
	encodeWidgetResolveError(h1ReqCtx(h1User), rec, fmt.Errorf("apiref resolve templates.krateo.io/restactions/x: %w", dep))
	if rec.Code != http.StatusFailedDependency {
		t.Fatalf("status = %d, want 424; body: %s", rec.Code, rec.Body.String())
	}
	st := i1414Status(t, rec)
	if st.Status != metav1.StatusFailure || st.Reason != "FailedDependency" || st.Message != msg {
		t.Errorf("want Failure/FailedDependency/%q, got %q/%q/%q", msg, st.Status, st.Reason, st.Message)
	}
}
