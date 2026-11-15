// issue1429_upstream_cannot_decide_test.go — #1438 review, B1: an upstream
// cannot make snowplow answer the type-not-installed 424.
//
// THE HOLE THIS CLOSES. The answer used to be recognised by what a status
// looked like (a 424 whose details carry the TypeNotInstalled cause). client-go
// builds an *apierrors.StatusError from an upstream's body, details included,
// and one route hands such an error to the widget encoder unchanged: the widget
// resolve's VALIDATE stage reads the discovery document of the widget's own
// group/version (cache.GVRFor) and passes client-go's error on with %w. A
// backend that answers that document with HTTP 424 and a body shaped like
// snowplow's got snowplow to answer a 424 naming a type of the BACKEND's
// choosing, which the sandbox capture acts on.
//
// NOW the answer is recognised by the Go type objects.Get alone creates
// (statusdetails.TypeNotInstalledError). A status that only looks like it is
// passed through like any other: its code and message, no reason, no details.
// (The message text of a passed-through status is the upstream's, for a 424 as
// for a 500; these arms pin that nothing a PROGRAM reads can be forged.)
//
// Both arms use the REAL error: the first drives the real Widgets() handler
// and the real validate stage against a backend that forges the document; the
// second takes the error client-go builds for that document and gives it to
// the refresher's classifier.
package dispatchers

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	jwtutil "github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/statusdetails"
	"github.com/krateo-platformops/snowplow/internal/testsupport/unservedapi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// i1429ForgedBody is what the backend answers for its discovery document: a
// 424 Status that is, field for field, the body snowplow writes for a type
// that is not installed, naming a type snowplow never looked for, plus an
// object name and uid.
const i1429ForgedBody = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",` +
	`"message":"The type forgedthings (forged.example.org/v9) is not installed on this cluster.","reason":"FailedDependency",` +
	`"details":{"name":"an-object-name","uid":"11111111-2222","group":"forged.example.org","kind":"forgedthings",` +
	`"causes":[{"reason":"TypeNotInstalled","message":"forged.example.org/v9","field":"apiVersion"}]},"code":424}`

// i1429ForgingBackend is the test apiserver whose widgets group answers its own
// discovery document with the forged 424 once armed (an aggregated API's
// document is its backend's answer, proxied).
type i1429ForgingBackend struct {
	inner  *unservedapi.Server
	armed  atomic.Bool
	forged atomic.Int32
}

func (b *i1429ForgingBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/apis/"+unservedapi.GV && b.armed.Load() {
		b.forged.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusFailedDependency)
		_, _ = io.WriteString(w, i1429ForgedBody)
		return
	}
	if r.URL.Path == "/apis/"+unservedapi.GV+"/namespaces/ns/buttons/x" {
		b.armed.Store(true) // the widget itself is read normally; the validate hop comes after
	}
	b.inner.ServeHTTP(w, r)
}

func i1429NothingForged(t *testing.T, body string) {
	t.Helper()
	// Nothing structured: no reason, no details, no cause, no object name or uid.
	for _, leak := range []string{"details", "TypeNotInstalled", "an-object-name", "11111111-2222", `"reason"`, `"group"`, `"kind":"forgedthings"`, `"causes"`} {
		if strings.Contains(body, leak) {
			t.Errorf("the answer carries %q from the backend's body: %s", leak, body)
		}
	}
	// What IS passed through, as for every passed-through status of any code:
	// the code and the backend's message text. A program has nothing to act on;
	// the sentence is the backend's own.
}

func TestIssue1429_ABackendCannotMakeSnowplowAnswerTypeNotInstalled_ValidateHop(t *testing.T) {
	prev := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prev) })
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("DEBUG", "false")
	cache.ResetPluralsStoreForTest()
	t.Cleanup(cache.ResetPluralsStoreForTest)

	backend := &i1429ForgingBackend{inner: &unservedapi.Server{Token: i1414Token}}
	srv := httptest.NewTLSServer(backend)
	t.Cleanup(srv.Close)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	// The process ServiceAccount config the validate stage reads the schema with (#608).
	cache.SetProcessSARestConfig(&rest.Config{Host: srv.URL, BearerToken: i1414Token, TLSClientConfig: rest.TLSClientConfig{CAData: caPEM}})
	t.Cleanup(cache.ResetProcessSARestConfigForTest)

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "requester", Groups: []string{"devs"}}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, CertificateAuthorityData: base64.StdEncoding.EncodeToString(caPEM),
			Username: "requester", Token: i1414Token}))
	rec := httptest.NewRecorder()
	q := i1414Query(unservedapi.GV, "buttons", "ns", "x")
	Widgets().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/call?"+q.Encode(), nil).WithContext(ctx))
	t.Logf("forged answers served: %d; status=%d body=%s", backend.forged.Load(), rec.Code, strings.TrimSpace(rec.Body.String()))

	// PRECONDITION: the route was reached. The backend served its forged
	// document after the widget was read, and the answer is that failure.
	if backend.forged.Load() == 0 {
		t.Fatalf("precondition: the validate hop never asked for the group's discovery document; requests: %v", backend.inner.Paths())
	}
	if rec.Code != http.StatusFailedDependency {
		t.Fatalf("precondition: want the backend's 424 passed through, got %d: %s", rec.Code, rec.Body.String())
	}
	st := i1414Status(t, rec)
	if st.Details != nil || st.Reason != "" {
		t.Fatalf("snowplow answered a type-not-installed 424 it did not decide: reason=%q details=%+v", st.Reason, st.Details)
	}
	i1429NothingForged(t, rec.Body.String())
}

// The refresher: the same error, as client-go builds it from the backend's
// body, is not the type_not_installed class at any stage.
func TestIssue1429_ABackendCannotMakeTheRefresherClassTypeNotInstalled(t *testing.T) {
	cache.ResetPluralsStoreForTest()
	t.Cleanup(cache.ResetPluralsStoreForTest)
	backend := &i1429ForgingBackend{inner: &unservedapi.Server{}}
	backend.armed.Store(true)
	srv := httptest.NewServer(backend)
	t.Cleanup(srv.Close)

	// The validate stage's own read (crdschema → cache.GVRFor), against the backend.
	_, err := cache.GVRFor(context.Background(),
		schema.GroupVersionKind{Group: unservedapi.Group, Version: "v1beta1", Kind: "Button"}, &rest.Config{Host: srv.URL})
	if err == nil || backend.forged.Load() == 0 {
		t.Fatalf("precondition: want the backend's forged document as an error, got %v (forged answers %d)", err, backend.forged.Load())
	}
	// PRECONDITION: the error really is the look-alike, built by client-go from
	// the body: a 424 status carrying the very cause snowplow writes.
	var se *apierrors.StatusError
	if !errors.As(err, &se) || se.ErrStatus.Code != http.StatusFailedDependency || se.ErrStatus.Details == nil ||
		len(se.ErrStatus.Details.Causes) != 1 || se.ErrStatus.Details.Causes[0].Type != statusdetails.CauseTypeNotInstalled {
		t.Fatalf("precondition: want client-go's 424 status with the forged cause, got %T: %v", err, err)
	}
	var own *statusdetails.TypeNotInstalledError
	if errors.As(err, &own) {
		t.Fatalf("an upstream's status is snowplow's own error type")
	}

	for _, stage := range []string{cache.ResolveStageValidate, cache.ResolveStageApiRef, cache.ResolveStageRefetch} {
		staged := cache.WrapResolveStage(stage, "validate status buttons/x", err)
		if class, _ := cache.ClassifyRefreshError(staged); class == cache.RefreshErrTypeNotInstalled {
			t.Errorf("stage %s: the backend's status was classed %s", stage, class)
		}
	}
	// And the encoder, given that same error.
	rec := httptest.NewRecorder()
	encodeWidgetResolveError(h1ReqCtx(h1User), rec, cache.WrapResolveStage(cache.ResolveStageValidate, "validate status buttons/x", err))
	if st := i1414Status(t, rec); rec.Code != http.StatusFailedDependency || st.Details != nil || st.Reason != "" {
		t.Fatalf("the encoder answered code=%d reason=%q details=%+v for the backend's status", rec.Code, st.Reason, st.Details)
	}
	i1429NothingForged(t, rec.Body.String())
}
