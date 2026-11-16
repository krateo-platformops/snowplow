package api

// issue1481_reason_classes_test.go — #1481 (plumbing#195): what a stage that
// failed with 502, 503, 504 or 429 is classified as, on every path a stage
// failure takes to a classifier (credhttp/reason.go has the decision).
//
//   E  a named endpointRef, on both dial chains (the owned client-go chain for
//      a bearer token, plumbing's for a password-only Secret), for every body:
//      empty, text, JSON that is not a Status, a Status claiming InternalError,
//      a Status naming the code's own reason. The stage failure's code and
//      reason, the refresher's class (cache.ClassifyRefreshError), the stage
//      outcome header's reason.
//   X  a redirect the dial refused: reported 502, no reason, not "unavailable".
//   G  client-go's own status for a 5xx answer that is not a Status
//      (apierrors.NewGenericServerResponse, the internal-rest-config branch):
//      the same reason and class as E.
//
// Every expectation is a literal. Before #1481, with plumbing v1.84.0, a 502
// read InternalError, so the refresher's class was "unavailable".

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type x1481Want struct {
	reason  string // the stage failure's reason
	class   string // cache.ClassifyRefreshError
	outcome string // X-Snowplow-Stage-Outcomes
}

var x1481Wants = map[int]x1481Want{
	http.StatusBadGateway:         {"", cache.RefreshErrUnclassified, StageReasonError},
	http.StatusServiceUnavailable: {"ServiceUnavailable", cache.RefreshErrUnavailable, StageReasonError},
	http.StatusGatewayTimeout:     {"Timeout", cache.RefreshErrTimeout, StageReasonTimeout},
	http.StatusTooManyRequests:    {"TooManyRequests", cache.RefreshErrUnavailable, StageReasonError},
}

// x1481Check asserts f's code and reason, the class the refresher assigns to
// the error a resolve returns for it, and that it never reads InternalError.
func x1481Check(t *testing.T, f *StageFailure, code int, w x1481Want) {
	t.Helper()
	if f == nil {
		t.Fatal("PRE: the resolve reported no stage failure")
	}
	if f.Code != code {
		t.Fatalf("PRE: the stage failure's code is %d, want %d", f.Code, code)
	}
	if f.Reason != w.reason {
		t.Errorf("RED: the stage failure's reason is %q, want %q", f.Reason, w.reason)
	}
	err := cache.WrapResolveStage(cache.ResolveStageApiRef, "apiref x/y", f.Err("ns", "ra"))
	if class, _ := cache.ClassifyRefreshError(err); class != w.class {
		t.Errorf("RED: the refresher classifies %d (reason %q) as %q, want %q", code, f.Reason, class, w.class)
	}
	if code != http.StatusInternalServerError && apierrors.IsInternalError(err) {
		t.Errorf("RED: %d reads IsInternalError", code)
	}
}

func TestIssue1481_E_EndpointStageClassPerCode(t *testing.T) {
	bodies := map[string]func(code int) (ct, body string){
		"empty":           func(int) (string, string) { return "", "" },
		"text":            func(int) (string, string) { return "text/plain", "upstream failed" },
		"json not Status": func(int) (string, string) { return "application/json", `{"error":"upstream failed"}` },
		"Status claiming InternalError": func(code int) (string, string) {
			return "application/json", fmt.Sprintf(`{"kind":"Status","apiVersion":"v1","status":"Failure","message":"m","reason":"InternalError","code":%d}`, code)
		},
		"Status with no reason": func(code int) (string, string) {
			return "application/json", fmt.Sprintf(`{"kind":"Status","apiVersion":"v1","status":"Failure","message":"m","code":%d}`, code)
		},
	}
	secrets := map[string]func(url string) map[string]string{
		"owned chain (token)": func(url string) map[string]string { return x1106Secret(url) },
		"plumbing chain (password only)": func(url string) map[string]string {
			return map[string]string{"server-url": url, "password": x1106Token}
		},
	}
	for code, w := range x1481Wants {
		for bname, body := range bodies {
			for sname, secret := range secrets {
				t.Run(fmt.Sprintf("%d/%s/%s", code, bname, sname), func(t *testing.T) {
					ct, b := body(code)
					ep := newX1106Srv(t, false, func(rw http.ResponseWriter, _ *http.Request) {
						if ct != "" {
							rw.Header().Set("Content-Type", ct)
						}
						rw.WriteHeader(code)
						_, _ = rw.Write([]byte(b))
					})
					x1106Env(t, "", map[string]map[string]string{x1106Ref: secret(ep.srv.URL)})
					o := x1106Resolve(t, x1106Extras, x1106Stage("read", http.MethodGet, "/zd/tickets/42"))
					if len(ep.all()) == 0 {
						t.Fatal("PRE: the endpoint was not dialled")
					}
					x1481Check(t, o.failure, code, w)
					rows := x1106Rows(t, o.outcomes)
					if len(rows) != 1 || rows[0].OK || rows[0].Reason != w.outcome {
						t.Errorf("RED: the stage outcome is %+v, want reason %q", rows, w.outcome)
					}
				})
			}
		}
	}
}

func TestIssue1481_X_RefusedRedirectIsAReasonless502(t *testing.T) {
	ep := newX1106Srv(t, false, func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Location", "/elsewhere")
		rw.WriteHeader(http.StatusFound)
	})
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL)})
	o := x1106Resolve(t, x1106Extras, x1106Stage("read", http.MethodGet, "/zd/tickets/42"))
	x1481Check(t, o.failure, http.StatusBadGateway, x1481Wants[http.StatusBadGateway])
}

func TestIssue1481_G_ClientGoGenericStatusClassPerCode(t *testing.T) {
	gr := schema.GroupResource{Resource: "configmaps"}
	for code, w := range x1481Wants {
		t.Run(http.StatusText(code), func(t *testing.T) {
			itemErr := apierrors.NewGenericServerResponse(code, http.MethodGet, gr, "cm", "upstream failed", 0, false)
			if code == http.StatusBadGateway && !apierrors.IsInternalError(itemErr) {
				t.Fatal("PRE: client-go's own status for a 502 is InternalError")
			}
			f := newStageItemFailure("read", itemErr.Error(), itemErr).failure
			x1481Check(t, &f, code, w)
		})
	}
}
