// phase1_roots_unthrottled_test.go — #120 falsifier for the boot-critical
// navigation-roots read, on its #920 home.
//
// ROOT CAUSE (#120): the roots-read dynamic client was built from main.go's
// in-cluster config, which carried no QPS / RateLimiter, so client-go gave it
// its default 5-QPS / 10-burst token bucket. On a slow-apiserver boot the
// very first roots read (the frontend-config ConfigMap Get) could lose its
// tryThrottle Wait to the startupProbe ctx deadline BEFORE the request was
// sent: no nav roots, no recursive walk, proactive informer registration
// starved.
//
// FIX: #120 copied the config and cleared the limiter for this one read
// (rootsReadRESTConfig). #920 removed that per-site copy: credhttp builds
// every client with no client-side limiter, so Phase1Warmup builds the roots
// client with credhttp.Dynamic(rc) directly.
//
// These are HERMETIC unit tests: they drive the REAL client-go throttle path
// (Request.tryThrottle → RateLimiter.Wait) against an httptest apiserver. No
// cluster.
//
// ARMS:
//   - RED (pre-fix posture): a client-go dynamic client built straight from a
//     config whose token bucket is already drained (the boot condition), under
//     a tight ctx: the read DEADLINES in tryThrottle, proving the limiter is
//     the cause.
//   - GREEN (with fix): the SAME drained-bucket config handed to
//     credhttp.Dynamic, the builder Phase1Warmup uses: the client has no
//     limiter, so the SAME tight-ctx read COMPLETES.

package dispatchers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sdynamic "k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

// configMapReplyServer is a minimal apiserver that answers the roots
// ConfigMap Get with a valid (empty) ConfigMap object. It records whether the
// HTTP handler was ever reached — the discriminator between "throttle
// deadlined before the request left client-go" and "the request actually ran".
type configMapReplyServer struct {
	*httptest.Server
	reached bool
}

func newConfigMapReplyServer(t *testing.T) *configMapReplyServer {
	t.Helper()
	s := &configMapReplyServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.reached = true
		w.Header().Set("Content-Type", "application/json")
		// A well-formed ConfigMap so the dynamic Get decodes cleanly. The
		// test only cares that the read COMPLETES vs deadlines, not the body.
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x","namespace":"krateo-system"},"data":{"config.json":"{}"}}`))
	}))
	t.Cleanup(s.Close)
	return s
}

// drainedBucketConfig builds a *rest.Config pointed at srv whose RateLimiter
// is a real token bucket with its single token already consumed — the
// slow-boot condition where the next Wait must block for a refill. This is
// the client-go default-limiter posture (a bucket that gates requests), made
// deterministic by pre-draining rather than depending on wall-clock QPS.
func drainedBucketConfig(t *testing.T, srv *configMapReplyServer) *rest.Config {
	t.Helper()
	rl := flowcontrol.NewTokenBucketRateLimiter(1, 1)
	if !rl.TryAccept() {
		t.Fatalf("precondition: fresh burst-1 bucket should grant its first token")
	}
	// Bucket now empty; the next Wait blocks ~1s for a refill at QPS=1.
	return &rest.Config{
		Host:        srv.URL,
		RateLimiter: rl,
	}
}

// getRootsConfigMap performs the SAME dynamic ConfigMap Get the roots read
// uses (readFrontendConfig → dynCli.Resource(configMapGVR).Get), through the
// real client-go throttle path, under the given ctx, with a client from build.
func getRootsConfigMap(ctx context.Context, build func(*rest.Config) (k8sdynamic.Interface, error), rc *rest.Config) error {
	cli, err := build(rc)
	if err != nil {
		return err
	}
	_, err = cli.Resource(configMapGVR).Namespace("krateo-system").Get(ctx, "x", metav1.GetOptions{})
	return err
}

func rawClientGoDynamic(rc *rest.Config) (k8sdynamic.Interface, error) {
	return k8sdynamic.NewForConfig(rc)
}

// Phase1Warmup's builder.
func credhttpDynamic(rc *rest.Config) (k8sdynamic.Interface, error) { return credhttp.Dynamic(rc) }

// TestRootsRead_DrainedLimiter_DeadlinesWithoutFix is the RED arm: the pre-fix
// posture (a client-go client over a drained bucket) under a tight ctx
// DEADLINES in the client-side throttle before the request is sent.
func TestRootsRead_DrainedLimiter_DeadlinesWithoutFix(t *testing.T) {
	srv := newConfigMapReplyServer(t)
	rc := drainedBucketConfig(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := getRootsConfigMap(ctx, rawClientGoDynamic, rc)
	if err == nil {
		t.Fatalf("RED arm expected the roots read to fail (throttle should deadline), got nil error")
	}
	// client-go wraps the throttle-time ctx-deadline as a rate-limiter error.
	if !strings.Contains(err.Error(), "rate limiter Wait") {
		t.Fatalf("RED arm expected a client rate-limiter Wait deadline error, got: %v", err)
	}
	if srv.reached {
		t.Fatalf("RED arm: the HTTP request should NEVER have left client-go (throttle deadlined first), but the apiserver handler was reached")
	}
}

// TestRootsRead_Unthrottled_CompletesWithFix is the GREEN arm: the SAME
// drained-bucket config through credhttp.Dynamic (Phase1Warmup's builder) has
// no limiter, so the SAME tight-ctx read COMPLETES and reaches the apiserver.
func TestRootsRead_Unthrottled_CompletesWithFix(t *testing.T) {
	srv := newConfigMapReplyServer(t)
	rc := drainedBucketConfig(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := getRootsConfigMap(ctx, credhttpDynamic, rc); err != nil {
		t.Fatalf("GREEN arm expected the roots read to complete under the fix, got: %v", err)
	}
	if !srv.reached {
		t.Fatalf("GREEN arm: the apiserver handler should have been reached (no throttle to deadline)")
	}
}
