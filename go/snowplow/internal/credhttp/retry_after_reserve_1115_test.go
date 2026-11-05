package credhttp

// retry_after_reserve_1115_test.go — #1122 re-review R38: a Retry-After that
// fits the remaining budget but not the remaining budget minus the retry
// reserve is withheld from client-go (the retry it buys could not answer
// inside the budget), so the 429 is answered at once; with the reserve fitting
// too it is kept (TestReviewer1122b-style control below).

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func retryAfterOnceServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x","namespace":"ns"}}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func TestBudget1115_R38_RetryAfterMustLeaveTheReserve(t *testing.T) {
	cm := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

	// 2 s left, Retry-After 1 s, reserve 1.5 s: 1 + 1.5 > 2 → withheld.
	srv, n := retryAfterOnceServer(t)
	seedReserve(t, 1500*time.Millisecond)
	dc, err := Dynamic(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = dc.Resource(cm).Namespace("ns").Get(budgetLeft(2*time.Second), "x", metav1.GetOptions{})
	if took := time.Since(start); err == nil || n.Load() != 1 || took >= time.Second {
		t.Fatalf("err %v, %d request(s), %v: want the 429 at once (Retry-After 1 s + reserve 1.5 s > 2 s left)", err, n.Load(), took)
	}

	// Control: 5 s left → 1 + 1.5 fits → kept, retried once.
	srv2, n2 := retryAfterOnceServer(t)
	dc2, err := Dynamic(&rest.Config{Host: srv2.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dc2.Resource(cm).Namespace("ns").Get(budgetLeft(5*time.Second), "x", metav1.GetOptions{}); err != nil || n2.Load() != 2 {
		t.Fatalf("control: err %v, %d requests; want the fitting Retry-After honoured", err, n2.Load())
	}
}
