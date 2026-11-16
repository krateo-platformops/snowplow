package dispatchers

// issue1481_reason_classes_test.go — #1481 (plumbing#195): a required stage
// that failed with 502, 503, 504 or 429 (or a refused redirect, reported 502),
// through the REAL chain on the #443 harness (the requester's own clientconfig
// pointing at a fake apiserver), as each reader of it sees it:
//
//   A  the RESTAction /call answer to a customer (inside its page budget): the
//      code, and the reason that agrees with it (a 502 answers none, never
//      plumbing's InternalError);
//   W  the widget apiRef frame resolved in the background (apiref.Resolve with
//      no page budget, as the refresher and the seed run it): the error's code,
//      the refresher's class (cache.ClassifyRefreshError), IsInternalError and
//      the seed's verdict (classifySeedErr: every one is operational, so it is
//      retried). A refused redirect is a 502 with no reason, "unclassified".
//
// Every expectation is a literal (credhttp/reason.go has the decision).

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

const i1481Path = "/api/v1/namespaces/" + psTargetNS + "/configmaps/fail1481"

type i1481Case struct {
	name   string
	answer int // what the fake apiserver answers the stage's GET with
	// A — the customer's /call, inside its page budget: what it answers.
	code   int
	reason any // the answer's "reason" (nil: none)
	// W — a background resolve (no page budget), as the refresher and the seed
	// run it: the error's code, the refresher's class, IsInternalError.
	bgCode   int
	class    string
	internal bool
}

// A background read of a 5xx or a 429 is reported 500 InternalError: plumbing's
// RetryClient gives up with an error ("server error after N retries: 502") and
// the code is lost before any reason is derived. Unchanged by #1481; pinned so
// that changing it is a decision.
var i1481Cases = []i1481Case{
	{"502", http.StatusBadGateway, http.StatusBadGateway, nil, http.StatusInternalServerError, cache.RefreshErrUnavailable, true},
	{"503", http.StatusServiceUnavailable, http.StatusServiceUnavailable, "ServiceUnavailable", http.StatusInternalServerError, cache.RefreshErrUnavailable, true},
	{"504", http.StatusGatewayTimeout, http.StatusGatewayTimeout, "Timeout", http.StatusInternalServerError, cache.RefreshErrUnavailable, true},
	{"429", http.StatusTooManyRequests, http.StatusTooManyRequests, "TooManyRequests", http.StatusInternalServerError, cache.RefreshErrUnavailable, true},
	// Never retried: the 502 is snowplow's own, and carries no reason.
	{"302 refused redirect", http.StatusFound, http.StatusBadGateway, nil, http.StatusBadGateway, cache.RefreshErrRedirectRefused, false},
}

func i1481Setup(t *testing.T, c i1481Case) (*in443API, map[string]any) {
	t.Helper()
	ra := in443RA("fail-1481", `.crd.data.k // empty | {k: .}`, map[string]any{"name": "crd", "verb": "GET", "path": i1481Path})
	f := in443Setup(t, &unstructured.Unstructured{Object: ra})
	f.extra["GET "+i1481Path] = func(w http.ResponseWriter, _ *http.Request) {
		if c.answer >= 300 && c.answer < 400 {
			w.Header().Set("Location", "/elsewhere")
			w.WriteHeader(c.answer)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(c.answer)
		_, _ = fmt.Fprint(w, "<html><body>upstream failed</body></html>")
	}
	return f, ra
}

func TestIssue1481_A_RESTActionAnswerCarriesTheAgreeingReason(t *testing.T) {
	for _, c := range i1481Cases {
		t.Run(c.name, func(t *testing.T) {
			f, ra := i1481Setup(t, c)
			// The /call middleware gives a customer request its page budget
			// (#1115): a read's last attempt is then the answer.
			rec := serveStored(t, f, pagebudget.WithCustomer(in443Ctx(f, psAlice), time.Now()), ra)
			if rec.Code != c.code {
				t.Fatalf("PRE: code=%d, want %d: %s", rec.Code, c.code, rec.Body.String())
			}
			st := i709Status(t, rec)
			if st["kind"] != "Status" || st["status"] != "Failure" || st["code"] != float64(c.code) {
				t.Fatalf("PRE: want a %d Failure Status, got %s", c.code, rec.Body.String())
			}
			if st["reason"] != c.reason {
				t.Errorf("RED: the answer's reason is %v, want %v: %s", st["reason"], c.reason, rec.Body.String())
			}
		})
	}
}

func TestIssue1481_W_WidgetApiRefClassAndSeedVerdict(t *testing.T) {
	for _, c := range i1481Cases {
		t.Run(c.name, func(t *testing.T) {
			f, _ := i1481Setup(t, c)
			if rw := cache.Global(); rw != nil {
				if _, ch := rw.EnsureResourceType(h1RAGVR); ch != nil {
					select {
					case <-ch:
					case <-time.After(5 * time.Second):
						t.Fatal("restactions informer did not sync")
					}
				}
			}
			_, err := apiref.Resolve(in443Ctx(f, psAlice), apiref.ResolveOptions{
				RC: &rest.Config{Host: f.srv.URL}, ApiRef: i431ApiRef("fail-1481"), AuthnNS: psAuthnNS,
			})
			var se *apierrors.StatusError
			if !errors.As(err, &se) || int(se.Status().Code) != c.bgCode {
				t.Fatalf("PRE: apiref.Resolve: want a typed %d, got %T: %v", c.bgCode, err, err)
			}
			if class, _ := cache.ClassifyRefreshError(err); class != c.class {
				t.Errorf("RED: the refresher classifies it %q (reason %q), want %q", class, se.Status().Reason, c.class)
			}
			if got := apierrors.IsInternalError(err); got != c.internal {
				t.Errorf("RED: IsInternalError=%v (reason %q), want %v", got, se.Status().Reason, c.internal)
			}
			if v := classifySeedErr(err); v != seedFailOperational {
				t.Errorf("RED: the seed's verdict is %v, want operational (retried)", v)
			}
		})
	}
}
