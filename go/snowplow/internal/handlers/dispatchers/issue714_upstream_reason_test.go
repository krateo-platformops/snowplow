package dispatchers

// issue714_upstream_reason_test.go — #714: an upstream's Status body cannot
// steer snowplow's own classification of a failed stage.
//
// Driven through the REAL chain on the #443 ps harness: the requester's own
// dial to a fake apiserver, restactions.Resolve, then every internal consumer
// of the resulting error — the refresher classifier, the seed classifier, the
// denial-only Put predicate, the inline stage-outcomes header, the /call
// answer — plus the failure log level.
//
// Shapes:
//
//	LieReason        GET,  HTTP 409, body {"reason":"Forbidden","code":409}
//	LieCode          GET,  HTTP 404, body {"code":403}  (no reason)
//	Post500Forbidden POST, HTTP 500, body {"reason":"Forbidden","code":403}
//
// A 5xx body is parsed only for a NON-idempotent method: plumbing's retry
// client (http/util/retry.go) retries GET/HEAD/OPTIONS/PUT/DELETE and turns an
// exhausted 5xx into a transport error before the body is read, but hands a
// POST/PATCH response straight back (architect-623, gate of #714). So the
// issue's literal "500 + reason=Forbidden" is reachable on a POST stage; the
// GET 5xx case is the idempotent control.
//
// On main before #714 all three read as FORBIDDEN internally (non-retryable
// refresh, seed RBAC deny, outcome "Forbidden"). After: the status line decides.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

const i714Path = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/lies.example.io"

type i714Logs struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *i714Logs) Enabled(context.Context, slog.Level) bool { return true }
func (h *i714Logs) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.recs = append(h.recs, r.Clone())
	h.mu.Unlock()
	return nil
}
func (h *i714Logs) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *i714Logs) WithGroup(string) slog.Handler      { return h }

func (h *i714Logs) failureLevels() []slog.Level {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []slog.Level
	for _, r := range h.recs {
		if r.Message == "api call response failure" {
			out = append(out, r.Level)
		}
	}
	return out
}

func i714RA(verb string) map[string]any {
	// No continueOnError, and a filter that yields nothing without the stage,
	// so restactions.Resolve returns the stage's typed failure (#709).
	return in443RA("lie", `.lie.spec // empty`, map[string]any{"name": "lie", "verb": verb, "path": i714Path})
}

func i714Serve(f *in443API, verb string, code int, body string) {
	f.extra[verb+" "+i714Path] = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = fmt.Fprint(w, body)
	}
}

// resolve714 runs the RESTAction through the real restactions.Resolve under the
// requester's ctx with the Put-gate sinks installed, and returns the error and
// the sinks.
func resolve714(t *testing.T, f *in443API, verb string) (error, *cache.StageErrorSink, *cache.PerIdentityForbiddenSink, *cache.ExternalTouchedSink, *i714Logs) {
	t.Helper()
	var cr templatesv1.RESTAction
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(i714RA(verb), &cr); err != nil {
		t.Fatal(err)
	}
	logs := &i714Logs{}
	ctx := xcontext.BuildContext(in443Ctx(f, psAlice), xcontext.WithLogger(slog.New(logs)))
	ctx, stage := cache.WithStageErrorSink(ctx)
	ctx, forb := cache.WithPerIdentityForbiddenSink(ctx)
	ctx, ext := cache.WithExternalTouchedSink(ctx)
	_, err := restactions.Resolve(ctx, restactions.ResolveOptions{In: &cr, SArc: &rest.Config{Host: f.srv.URL}, AuthnNS: psAuthnNS})
	return err, stage, forb, ext, logs
}

func TestIssue714_UpstreamCannotSteerClassification(t *testing.T) {
	cases := []struct {
		name     string
		verb     string
		code     int
		body     string
		wantCode int
		// wantOutcome is the inline (dry-run) outcome: what the status line
		// says for a read; a write stage is never executed in a dry run (#443 k).
		wantOutcome string
	}{
		{"LieReason", http.MethodGet, http.StatusConflict, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":409,"message":"x"}`, http.StatusConflict, api.StageReasonError},
		{"LieCode", http.MethodGet, http.StatusNotFound, `{"code":403,"message":"x"}`, http.StatusNotFound, api.StageReasonNotFound},
		{"Post500Forbidden", http.MethodPost, http.StatusInternalServerError, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,"message":"x"}`, http.StatusInternalServerError, api.StageReasonNotExecuted},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := in443Setup(t)
			i714Serve(f, c.verb, c.code, c.body)
			err, stage, forb, ext, logs := resolve714(t, f, c.verb)
			if err == nil {
				t.Fatal("precondition: the stage failure must be returned (#709)")
			}
			var se *apierrors.StatusError
			if !errors.As(err, &se) || int(se.Status().Code) != c.wantCode {
				t.Fatalf("#714 RED: the typed error must carry the HTTP status line's code %d, got %v", c.wantCode, err)
			}
			if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
				t.Fatalf("#714 RED: an upstream claim made the failure read as forbidden/unauthorized: %+v", se.Status())
			}
			if class, _ := cache.ClassifyRefreshError(err); class == cache.RefreshErrForbidden {
				t.Fatalf("#714 RED: the refresher classifies it %q", class)
			}
			if cache.IsNonRetryableRefreshError(err) {
				t.Fatal("#714 RED: the refresher would not retry it")
			}
			if got := classifySeedErr(err); got != seedFailOperational {
				t.Fatalf("#714 RED: the seed classifies it %v, want operational (re-enqueued)", got)
			}
			if cache.DenialOnly(stage, forb, ext, "restactions") {
				t.Fatal("#714 RED: the result reads as denial-only (would be Put as a per-identity denial)")
			}
			for _, lvl := range logs.failureLevels() {
				if lvl != slog.LevelError {
					t.Fatalf("the failure must log at ERROR, got %v", lvl)
				}
			}
			if len(logs.failureLevels()) == 0 {
				t.Fatal("precondition: the failure line must be logged")
			}

			// The /call answer and the inline outcomes header.
			rec := serveStored(t, f, in443Ctx(f, psAlice), i714RA(c.verb))
			if rec.Code != c.wantCode {
				t.Fatalf("/call answered %d, want %d: %s", rec.Code, c.wantCode, rec.Body.String())
			}
			rec = serveInline(t, f, in443Ctx(f, psAlice), i714RA(c.verb))
			if h := rec.Header().Get("X-Snowplow-Stage-Outcomes"); !strings.Contains(h, `"reason":"`+c.wantOutcome+`"`) {
				t.Fatalf("#714 RED: the outcomes header must say %s, got %s", c.wantOutcome, h)
			}
		})
	}
}

// Control, the IDEMPOTENT case: for a GET the retry client retries a 5xx and
// returns a transport error before any body is read, so "500 + Forbidden" on a
// GET was already a plain retryable 500. (A POST/PATCH 5xx IS parsed — see
// Post500Forbidden above.)
func TestIssue714_Get5xxBodyIsNotParsed(t *testing.T) {
	f := in443Setup(t)
	i714Serve(f, http.MethodGet, http.StatusInternalServerError, `{"kind":"Status","status":"Failure","reason":"Forbidden","code":403}`)
	err, _, _, _, _ := resolve714(t, f, http.MethodGet)
	if err == nil || apierrors.IsForbidden(err) || cache.IsNonRetryableRefreshError(err) {
		t.Fatalf("a GET 5xx claiming Forbidden must read as a retryable 500, got %v", err)
	}
}

// The Kubernetes apiserver's own answers are unchanged: a real 403 Forbidden,
// 404 NotFound and 409 AlreadyExists keep their reason.
func TestIssue714_AgreeingReasonsKept(t *testing.T) {
	for _, c := range []struct {
		code   int
		reason string
		is     func(error) bool
	}{
		{http.StatusForbidden, "Forbidden", apierrors.IsForbidden},
		{http.StatusNotFound, "NotFound", apierrors.IsNotFound},
		{http.StatusConflict, "AlreadyExists", apierrors.IsAlreadyExists},
	} {
		f := in443Setup(t)
		i714Serve(f, http.MethodGet, c.code, fmt.Sprintf(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":%q,"code":%d,"message":"x"}`, c.reason, c.code))
		err, _, _, _, _ := resolve714(t, f, http.MethodGet)
		if err == nil || !c.is(err) {
			t.Fatalf("%d %s: an agreeing reason must be kept, got %v", c.code, c.reason, err)
		}
	}
}
