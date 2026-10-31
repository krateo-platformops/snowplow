// Package credhttp is the ONE place snowplow builds an HTTP client, a
// transport or a *rest.Config that carries a credential snowplow itself holds:
// the ServiceAccount bearer, the #574 impersonating config (SA bearer +
// Impersonate-*), and the per-user `<user>-clientconfig` credential snowplow
// attaches on the user's behalf (#723, #724).
//
// THE RULE: such a client NEVER follows an HTTP redirect — not cross-host, not
// same-host. Two layers enforce it:
//
//  1. CheckRedirect (on every *http.Client built here) returns
//     http.ErrUseLastResponse. The client never issues the second request; the
//     caller receives the 3xx response itself, so it is reported with its own
//     status code (client-go: a StatusError with Code 302/307; the branch-E
//     fetch: a Failure envelope with the same code). A 3xx is a non-2xx answer:
//     it is never fed to a JSON handler and never stored as a cell.
//
//  2. RefuseRedirectHops (a RoundTripper, installed on every *rest.Config that
//     leaves this package via rc.Wrap, and outermost on every *http.Client built
//     here) refuses any request whose Request.Response is set — net/http sets it
//     ONLY on a redirect hop — before a byte reaches the network. This is the
//     backstop for the clients snowplow does NOT construct itself: plumbing's
//     endpoints.FromSecret / kubeutil helpers, client-go internals, anything
//     built later from a copy of a hardened config (rest.CopyConfig copies
//     WrapTransport, so the #574 impersonating copy inherits it).
//
// WHY "REFUSE ALL" AND NOT "SAME-ORIGIN ONLY": the credential (bearer,
// Impersonate-*) is re-added by a RoundTripper on EVERY hop, so Go's own
// cross-domain Authorization stripping never applies; the kube-apiserver
// answers no resource read with a redirect, so the only sources of one are an
// aggregated-API backend or a proxy on the apiserver path — exactly the parties
// the issues name; a same-origin redirect would still re-send the credential
// to a path the redirecting party chose and accept that path's body as the
// requested object; and a 301/302 silently turns a POST into a GET. Refusing
// every redirect is the conservative choice and costs nothing that works today.
//
// A client that carries a THIRD PARTY's own credential (a named endpointRef)
// is not built here; see httpClientForEndpoint.
//
// The structural guard (guard_test.go) fails CI when any non-test file in the
// module builds a client, a transport or a *rest.Config by any other route.
//
// #920 — NO CLIENT-SIDE RATE LIMIT. client-go gives every client whose config
// leaves QPS at 0 and RateLimiter nil its own token bucket of 5 requests/s,
// burst 10 (rest.DefaultQPS / rest.DefaultBurst). Nobody chose that cap, and it
// silently paced every long-lived client snowplow built (the metadata client's
// store-verification re-check ran at exactly 5 reads/s in bench run 16). The
// apiserver's own Priority & Fairness is the authority on how fast snowplow may
// read, so every *rest.Config that leaves this package, and every client built
// here, carries no client-side limiter: noClientRateLimit is the one place that
// says so. Harden applies it to the configs (so a client built from one by code
// outside this package, such as plumbing's endpoints.FromSecret, inherits it),
// and every client constructor applies it to its own copy (so a client built
// here has no limiter whatever config it was handed).
//
// #988 — THE RESOLVE COLLECTION-LIST COUNTER. Because every config and client
// that carries a snowplow-held credential leaves this package, this is also
// where a resolve's collection LISTs are counted on the wire
// (snowplow_resolve_collection_lists, resolve_lists.go): Harden and Client
// install CountResolveCollectionLists wherever they install RefuseRedirectHops.
package credhttp

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/kubeconfig"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
)

// RedirectRefusedError is what a refused redirect hop returns. It carries the
// status code of the redirect that was refused and nothing else: never a URL
// (a Location can carry credentials), a token or an identity.
type RedirectRefusedError struct {
	StatusCode int
}

func (e *RedirectRefusedError) Error() string {
	return fmt.Sprintf("refused to follow a %d redirect: a client carrying snowplow-held credentials never follows a redirect (#723/#724)", e.StatusCode)
}

// CheckRedirect is the http.Client.CheckRedirect of every credentialed client:
// never follow, hand the caller the 3xx response itself.
func CheckRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// RefuseRedirectHops wraps rt so a redirect hop never reaches the network.
// Idempotent in effect (a double wrap refuses the same requests).
func RefuseRedirectHops(rt http.RoundTripper) http.RoundTripper {
	if rt == nil {
		rt = http.DefaultTransport
	}
	return &refuseRedirectHops{next: rt}
}

type refuseRedirectHops struct{ next http.RoundTripper }

func (t *refuseRedirectHops) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Response != nil {
		// Close the request body per the RoundTripper contract.
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, &RedirectRefusedError{StatusCode: req.Response.StatusCode}
	}
	return t.next.RoundTrip(req)
}

// WrappedRoundTripper lets client-go/utilnet walk the chain (e.g.
// CloseIdleConnectionsFor).
func (t *refuseRedirectHops) WrappedRoundTripper() http.RoundTripper { return t.next }

// Harden installs RefuseRedirectHops on rc, with the #988 resolve
// collection-LIST counter inside it, and removes its client-side rate limit
// (#920), in place, and returns it. Every client later built from rc or from a
// rest.CopyConfig of it inherits all three. nil-safe.
func Harden(rc *rest.Config) *rest.Config {
	if rc != nil {
		rc.Wrap(CountResolveCollectionLists)
		rc.Wrap(RefuseRedirectHops)
		noClientRateLimit(rc)
	}
	return rc
}

// noClientRateLimit removes client-go's client-side rate limit from rc, in
// place (#920). A negative QPS with a nil RateLimiter is client-go's own "no
// limiter" setting (rest.Config.QPS: "Setting this to a negative value will
// disable client-side ratelimiting"): rest.RESTClientForConfigAndClient builds
// a token bucket only when RateLimiter is nil and QPS > 0, and a QPS of 0 means
// rest.DefaultQPS (5/s). RateLimiter is cleared as well because a non-nil one
// is used verbatim whatever QPS says. Burst is read only by a bucket, so it is
// zeroed to say that no bucket exists. Idempotent.
func noClientRateLimit(rc *rest.Config) {
	rc.QPS = -1
	rc.Burst = 0
	rc.RateLimiter = nil
}

// Client applies both layers to c (in place), with the #988 resolve
// collection-LIST counter inside the hop refusal, and returns it.
func Client(c *http.Client) *http.Client {
	c.CheckRedirect = CheckRedirect
	c.Transport = RefuseRedirectHops(CountResolveCollectionLists(c.Transport))
	return c
}

// inClusterConfig is the source InClusterConfig hardens. rest.InClusterConfig
// reads fixed service-account paths, so this package's own tests replace it to
// build the in-cluster config against a test server.
var inClusterConfig = rest.InClusterConfig

// SetInClusterConfigForTest replaces the in-cluster config source and returns
// the restore func. TEST-ONLY: it lets another package's test drive a caller
// of InClusterConfig (the auth middleware's apiserver re-read, #994) against a
// test server. Not safe for concurrent use with InClusterConfig.
func SetInClusterConfigForTest(fn func() (*rest.Config, error)) (restore func()) {
	prev := inClusterConfig
	inClusterConfig = fn
	return func() { inClusterConfig = prev }
}

// InClusterConfig is rest.InClusterConfig, hardened.
func InClusterConfig() (*rest.Config, error) {
	rc, err := inClusterConfig()
	if err != nil {
		return nil, err
	}
	return Harden(rc), nil
}

// NewClientConfig is plumbing's kubeconfig.NewClientConfig (a per-user
// clientconfig endpoint → *rest.Config), hardened.
func NewClientConfig(ctx context.Context, ep endpoints.Endpoint) (*rest.Config, error) {
	rc, err := kubeconfig.NewClientConfig(ctx, ep)
	if err != nil {
		return nil, err
	}
	return Harden(rc), nil
}

// FromSecret is plumbing's endpoints.FromSecret over a hardened copy of rc.
// FromSecret builds its own REST client from the config it is given (one per
// call), so this is where that client gets both #723's redirect refusal and
// #920's absence of a client-side rate limit, whatever config the caller holds.
// The caller's rc is not changed. Errors are plumbing's, unwrapped, so callers
// keep telling a NotFound from any other failure.
func FromSecret(ctx context.Context, rc *rest.Config, name, namespace string) (endpoints.Endpoint, error) {
	if rc != nil {
		rc = Harden(rest.CopyConfig(rc))
	}
	return endpoints.FromSecret(ctx, rc, name, namespace)
}

// HTTPClientFor is rest.HTTPClientFor with both layers applied.
func HTTPClientFor(rc *rest.Config) (*http.Client, error) {
	hc, err := rest.HTTPClientFor(rc)
	if err != nil {
		return nil, err
	}
	return Client(hc), nil
}

// Dynamic is dynamic.NewForConfig through HTTPClientFor, with no client-side
// rate limit (#920).
func Dynamic(rc *rest.Config) (*dynamic.DynamicClient, error) {
	cfg := dynamic.ConfigFor(rc)
	noClientRateLimit(cfg)
	hc, err := HTTPClientFor(cfg)
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfigAndClient(cfg, hc)
}

// Metadata is metadata.NewForConfig through HTTPClientFor, with no client-side
// rate limit (#920).
func Metadata(rc *rest.Config) (metadata.Interface, error) {
	cfg := metadata.ConfigFor(rc)
	noClientRateLimit(cfg)
	hc, err := HTTPClientFor(cfg)
	if err != nil {
		return nil, err
	}
	return metadata.NewForConfigAndClient(cfg, hc)
}

// Kubernetes is kubernetes.NewForConfig through HTTPClientFor, with no client-side
// rate limit (#920).
func Kubernetes(rc *rest.Config) (*kubernetes.Clientset, error) {
	cfg := *rc
	noClientRateLimit(&cfg)
	if cfg.UserAgent == "" {
		cfg.UserAgent = rest.DefaultKubernetesUserAgent()
	}
	hc, err := HTTPClientFor(&cfg)
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfigAndClient(&cfg, hc)
}

// APIExtensions is the apiextensions clientset NewForConfig through
// HTTPClientFor, with no client-side rate limit (#920).
func APIExtensions(rc *rest.Config) (*apiextensionsclientset.Clientset, error) {
	cfg := *rc
	noClientRateLimit(&cfg)
	if cfg.UserAgent == "" {
		cfg.UserAgent = rest.DefaultKubernetesUserAgent()
	}
	hc, err := HTTPClientFor(&cfg)
	if err != nil {
		return nil, err
	}
	return apiextensionsclientset.NewForConfigAndClient(&cfg, hc)
}

// discoveryDefaultTimeout mirrors client-go discovery's defaultTimeout, which
// NewDiscoveryClientForConfig applies before building its http.Client.
const discoveryDefaultTimeout = 32 * time.Second

// Discovery is discovery.NewDiscoveryClientForConfig through HTTPClientFor,
// with no client-side rate limit (#920).
func Discovery(rc *rest.Config) (*discovery.DiscoveryClient, error) {
	cfg := *rc
	noClientRateLimit(&cfg)
	if cfg.Timeout == 0 {
		cfg.Timeout = discoveryDefaultTimeout
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = rest.DefaultKubernetesUserAgent()
	}
	hc, err := HTTPClientFor(&cfg)
	if err != nil {
		return nil, err
	}
	return discovery.NewDiscoveryClientForConfigAndClient(&cfg, hc)
}

type heldCredentialKey struct{}

// WithHeldCredential marks ctx: the dial it carries presents a credential
// snowplow holds and attaches itself (the ServiceAccount endpoint, or a
// per-user `<user>-clientconfig` snowplow resolved on the user's behalf), as
// opposed to a named endpointRef that carries a third party's OWN credential.
// A dial site that builds its client from an endpoint rather than from a
// *rest.Config (which is always hardened) applies Client() when the mark is
// present. PROVENANCE, never endpoint shape: a token-auth endpointRef is
// shape-identical to the SA endpoint.
func WithHeldCredential(ctx context.Context) context.Context {
	return WithHeldCredentialValue(ctx, true)
}

// WithHeldCredentialValue sets the mark to held, true OR false. The mark is
// per stage: a stage sets it to its own provenance every time, so a ctx a
// nested resolve inherits from an outer held stage never makes a nested
// named-ref stage read as held (#1019 review F1).
func WithHeldCredentialValue(ctx context.Context, held bool) context.Context {
	return context.WithValue(ctx, heldCredentialKey{}, held)
}

// HeldCredential reports whether ctx carries the WithHeldCredential mark.
func HeldCredential(ctx context.Context) bool {
	v, _ := ctx.Value(heldCredentialKey{}).(bool)
	return v
}

// RESTClient is rest.RESTClientFor through HTTPClientFor, with no client-side
// rate limit (#920).
func RESTClient(rc *rest.Config) (*rest.RESTClient, error) {
	cfg := *rc
	noClientRateLimit(&cfg)
	hc, err := HTTPClientFor(&cfg)
	if err != nil {
		return nil, err
	}
	return rest.RESTClientForConfigAndClient(&cfg, hc)
}
