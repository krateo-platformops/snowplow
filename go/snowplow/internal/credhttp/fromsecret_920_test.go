package credhttp

// fromsecret_920_test.go — #920 F7: plumbing's endpoints.FromSecret builds its
// own REST client from the config it is given. credhttp.FromSecret hands it a
// hardened copy. (The redirect arm is in credhttp_test.go,
// TestIssue723_724_EveryConstructorRefusesRedirects.)

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

// secretServer answers any GET with a Secret carrying a server-url, the shape
// plumbing's endpoints.FromSecret reads, and counts the requests that reach it.
func secretServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		// server-url = base64("https://example.invalid")
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Secret","metadata":{"name":"s","namespace":"n"},"data":{"server-url":"aHR0cHM6Ly9leGFtcGxlLmludmFsaWQ="}}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// A config whose limiter is already exhausted: the next Wait blocks ~1 s.
func drainedConfig(t *testing.T, host string) (*rest.Config, flowcontrol.RateLimiter) {
	t.Helper()
	rl := flowcontrol.NewTokenBucketRateLimiter(1, 1)
	if !rl.TryAccept() {
		t.Fatal("precondition: a fresh burst-1 bucket grants its first token")
	}
	return &rest.Config{Host: host, QPS: 1, Burst: 1, RateLimiter: rl}, rl
}

// The caller's config carries an exhausted limiter. Plumbing's own FromSecret
// (control) waits on it and misses a 50 ms deadline before any request is
// sent. credhttp.FromSecret reads the Secret at once and leaves the caller's
// config as it was.
func TestIssue920_FromSecretHasNoClientRateLimit(t *testing.T) {
	t.Run("control", func(t *testing.T) {
		srv, hits := secretServer(t)
		rc, _ := drainedConfig(t, srv.URL)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := endpoints.FromSecret(ctx, rc, "s", "n"); err == nil || !strings.Contains(err.Error(), "rate limiter") {
			t.Fatalf("control: plumbing's FromSecret on a drained limiter should deadline in the limiter, got %v", err)
		}
		if hits.Load() != 0 {
			t.Fatalf("control: the request left the client although the limiter should have held it")
		}
	})
	t.Run("credhttp", func(t *testing.T) {
		srv, hits := secretServer(t)
		rc, rl := drainedConfig(t, srv.URL)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		ep, err := FromSecret(ctx, rc, "s", "n")
		if err != nil {
			t.Fatalf("RED — credhttp.FromSecret waited on a client-side limiter: %v", err)
		}
		if ep.ServerURL != "https://example.invalid" || hits.Load() != 1 {
			t.Fatalf("FromSecret read %q with %d request(s); want the served server-url with 1", ep.ServerURL, hits.Load())
		}
		if rc.QPS != 1 || rc.Burst != 1 || rc.RateLimiter != rl || rc.WrapTransport != nil {
			t.Errorf("FromSecret changed the caller's config: QPS %v Burst %d RateLimiter %v WrapTransport set %v",
				rc.QPS, rc.Burst, rc.RateLimiter, rc.WrapTransport != nil)
		}
	})
}
