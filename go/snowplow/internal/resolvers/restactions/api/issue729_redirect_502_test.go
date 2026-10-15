package api

// issue729_redirect_502_test.go: a required stage whose credentialed dial got
// a refused redirect (#723/#724) reports 502 Bad Gateway with a canonical
// message naming the stage and the 3xx, never a bare 302/307. Both carriers
// are driven from REAL refused redirects (the redirect rig of
// issue723_724_redirect_test.go): the branch-E fetch's recorded Status map,
// and branch C's client-go StatusError.

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue729_RefusedRedirectReportsBadGateway(t *testing.T) {
	path := "/api/v1/namespaces/n/configmaps/x"
	calls := []httpcall.RequestOptions{{RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(http.MethodGet)}}}
	for _, code := range redirectCodes {
		want := fmt.Sprintf(`configmaps "x" in namespace "n": upstream answered %d %s, a redirect snowplow does not follow`, code, http.StatusText(code))

		// Branch E: the fetch's Failure envelope, recorded under errorKey.
		rig := newRedirectRig(t, code, true)
		res, _, _, _ := httpFetchAllowingNonJSON(cache.WithServiceAccountDial(context.Background()), httpcall.RequestOptions{
			RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(http.MethodGet)},
			Endpoint:    &endpoints.Endpoint{ServerURL: rig.apiserver.URL, Token: "sa-token-729"},
		})
		if res.Code != code {
			t.Fatalf("fixture: the fetch must hand back the %d, got %+v", code, res)
		}
		m, _ := response.AsMap(res)
		f := firstStageFailure([]error{newStageItemFailure("cm", m, fmt.Errorf("api cm item 0 failed: %s", res.Message))}, calls)
		check(t, "branch E", code, f, want)

		// Branch C: client-go's StatusError from the refused redirect.
		resetInternalClientCacheForTest()
		cli, err := internalClientFor(impersonatingRC(rig.apiserver.URL))
		if err != nil {
			t.Fatal(err)
		}
		_, getErr := cli.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace("n").Get(context.Background(), "x", metav1.GetOptions{})
		if redirectCode(getErr) != code {
			t.Fatalf("fixture: branch C must surface the %d, got %v", code, getErr)
		}
		f = firstStageFailure([]error{newStageItemFailure("cm", getErr.Error(), fmt.Errorf("w: %w", getErr))}, calls)
		check(t, "branch C", code, f, want)
	}
	resetInternalClientCacheForTest()
}

func check(t *testing.T, carrier string, code int, f *StageFailure, want string) {
	t.Helper()
	if f == nil || f.Code != http.StatusBadGateway || f.Message != want {
		t.Fatalf("%s %d: want 502 %q, got %+v", carrier, code, want, f)
	}
	se := f.StatusError("ns", "ra")
	if se.Status().Code != http.StatusBadGateway || apierrors.IsNotFound(se) || apierrors.IsForbidden(se) {
		t.Fatalf("%s %d: StatusError %+v", carrier, code, se.Status())
	}
	if got := se.Status().Message; got != `restaction ns/ra stage "cm" failed: `+want {
		t.Fatalf("%s %d: StatusError message %q", carrier, code, got)
	}
}

// A non-redirect code is untouched by the mapping.
func TestIssue729_NonRedirectCodesUnchanged(t *testing.T) {
	calls := []httpcall.RequestOptions{{RequestInfo: httpcall.RequestInfo{Path: "/x", Verb: ptr.To(http.MethodGet)}}}
	for _, code := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable} {
		m, _ := response.AsMap(failureStatus(code, nil))
		f := firstStageFailure([]error{newStageItemFailure("s", m, fmt.Errorf("e"))}, calls)
		if f.Code != code || f.Message != fmt.Sprintf("upstream answered %d %s", code, http.StatusText(code)) {
			t.Fatalf("%d changed: %+v", code, f)
		}
	}
}
