package api

// page_budget_1115_test.go — #1115 A4 at the resolver: a stage whose exchange
// the request's page budget ended reports a clear error, never a hang and never
// "upstream answered": its envelope is 504 Timeout, its stage outcome reason is
// Timeout, and (without continueOnError) the resolve's StageFailure is 504 with
// an identity-free message. Driven through ResolveWithFailure against an
// httptest upstream reached through an endpointRef Secret (#904's harness).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestBudget1115_A4_StageEndedByTheBudgetIsA504Timeout(t *testing.T) {
	release := make(chan struct{})
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/v1/namespaces/"+u904AuthnNS+"/secrets/"+u904Secret:
			b, _ := json.Marshal(corev1.Secret{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
				ObjectMeta: metav1.ObjectMeta{Name: u904Secret, Namespace: u904AuthnNS},
				Data:       map[string][]byte{"server-url": []byte(srv.URL)},
			})
			_, _ = w.Write(b)
		case r.URL.Path == "/hang":
			<-release // connected, never answers
		default:
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "cyberjoker", Groups: []string{"devs"}}))
	ctx = pagebudget.WithCustomer(ctx, time.Now().Add(2*time.Second-pagebudget.PageMaxGoal)) // room for the fast stage on a loaded CI host
	ctx, outcomes := WithStageOutcomes(ctx)
	stages := []*templates.API{
		u904Stage("fast", http.MethodGet, "/fast"),
		u904Stage("slow", http.MethodGet, "/hang"),
	}
	start := time.Now()
	dict, failure := ResolveWithFailure(ctx, ResolveOptions{
		RC: &rest.Config{Host: srv.URL}, AuthnNS: u904AuthnNS, Items: stages,
		RESTActionNamespace: "ns-1115", RESTActionName: "ra-1115",
	})
	if took := time.Since(start); took > 6*time.Second {
		t.Fatalf("the resolve took %v; the budget left 2s", took)
	}
	if failure == nil || failure.Stage != "slow" || failure.Code != http.StatusGatewayTimeout || failure.Reason != credhttp.BudgetStatusReason {
		t.Fatalf("StageFailure = %+v, want stage slow, 504 Timeout", failure)
	}
	if !strings.Contains(failure.Message, "budget ran out") || strings.Contains(failure.Message, "upstream answered") || strings.Contains(failure.Message, "127.0.0.1") {
		t.Fatalf("StageFailure message %q: want the budget named, no upstream claim, no host", failure.Message)
	}
	if se := failure.StatusError("ns-1115", "ra-1115"); se.ErrStatus.Code != http.StatusGatewayTimeout {
		t.Fatalf("StatusError code %d, want 504", se.ErrStatus.Code)
	}
	l, _ := dict["slowErr"].([]any)
	v, _ := func() (map[string]any, bool) {
		if len(l) != 1 {
			return nil, false
		}
		m, ok := l[0].(map[string]any)
		return m, ok
	}()
	if v == nil || statusCodeOf(v["code"]) != http.StatusGatewayTimeout || v["reason"] != credhttp.BudgetStatusReason || v["status"] != "Failure" {
		t.Fatalf("slow's errorKey value = %#v, want one 504 Timeout Failure Status", dict["slowErr"])
	}
	raw := outcomes.HeaderValue()
	if !strings.Contains(raw, `"name":"slow","ok":false,"reason":"Timeout"`) {
		t.Fatalf("stage outcomes %s: want slow failed with reason Timeout", raw)
	}
	if strings.Contains(raw, `"name":"fast"`) {
		t.Fatalf("the fast stage failed too: %s", raw)
	}
}

// A3 (fill leader) — the #710 PR-1 fill leader's detached ctx leaves the
// reader's page budget: its joiners are never cut by the leader's deadline.
func TestBudget1115_A3_FillLeaderLeavesTheReadersBudget(t *testing.T) {
	ctx := pagebudget.WithCustomer(context.Background(), time.Now().Add(-2*pagebudget.PageMaxGoal))
	fctx, cancel := detachedFillContext(ctx)
	defer cancel()
	if _, ok := pagebudget.From(fctx); ok {
		t.Fatal("the fill leader's detached ctx carries the reader's page budget")
	}
}
