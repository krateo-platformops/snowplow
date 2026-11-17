package credhttp

// bind_context_1318_test.go — #1318 review F1: a client built from
// BindContext(ctx, rc) ends every round trip when ctx does, including one sent
// on context.TODO() (what client-go's discovery does), and a round trip that
// answers in time is unaffected.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"k8s.io/client-go/rest"
)

func TestBindContext1318_TODORoundTripEndsWithTheUnit(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hang" {
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) }) // runs first: a failed arm does not hang Close

	unit, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c, err := HTTPClientFor(BindContext(unit, &rest.Config{Host: srv.URL}))
	if err != nil {
		t.Fatal(err)
	}

	// In time: answered, and the body reads whole.
	req, _ := http.NewRequestWithContext(context.TODO(), http.MethodGet, srv.URL+"/fast", nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("fast round trip: %v", err)
	}
	b, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || string(b) != "ok" {
		t.Fatalf("fast body %q, %v", b, err)
	}

	// Hanging, on context.TODO(): ends with the unit.
	done := make(chan error, 1)
	t0 := time.Now()
	go func() {
		req, _ := http.NewRequestWithContext(context.TODO(), http.MethodGet, srv.URL+"/hang", nil)
		resp, err := c.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("hanging round trip: %v; want the unit's context.DeadlineExceeded", err)
		}
		if d := time.Since(t0); d < 150*time.Millisecond {
			t.Fatalf("ended after %v, before the unit's deadline", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("a context.TODO() round trip outlived the unit by 5 s")
	}
}

func TestBindContext1318_NeverEndingCtxLeavesTheConfig(t *testing.T) {
	rc := &rest.Config{Host: "https://example.invalid"}
	if got := BindContext(context.Background(), rc); got != rc {
		t.Fatalf("BindContext(Background) copied the config; want it unchanged")
	}
}
