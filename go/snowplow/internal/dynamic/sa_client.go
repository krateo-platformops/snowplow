// sa_client.go — Tag 0.30.9 Sub-scope A: snowplow ServiceAccount
// endpoint provider for the userAccessFilter dispatch path.
//
// When a RestAction API stage declares userAccessFilter, snowplow
// dispatches the inner K8s call using its OWN ServiceAccount token
// (cluster-wide read) instead of the per-user-clientconfig token.
// The returned result set is then in-process-refiltered through
// EvaluateRBAC so the caller only sees objects they are RBAC-permitted
// to read.
//
// Two design constraints (binding):
//   1. feedback_no_special_cases.md — no per-resource policy here.
//      The SA endpoint is a single, uniform fallback; the resolver
//      decides WHEN to use it based on userAccessFilter presence.
//   2. feedback_l1_invalidation_delete_only.md is unaffected — the
//      SA endpoint is the read path, not the dep-tracker.
//
// Concurrency: the singleton is built lazily on first call and
// cached process-wide. After construction the *Endpoint is immutable;
// callers MUST NOT mutate the returned pointer's fields.
//
// Memory: one *Endpoint per process. Negligible.

package dynamic

import (
	"fmt"
	"os"
	"sync"

	"github.com/krateo-platformops/plumbing/endpoints"
	"k8s.io/client-go/rest"
)

// Standard in-cluster ServiceAccount projected paths (see
// https://kubernetes.io/docs/tasks/run-application/access-api-from-pod/).
// These mirror the locations rest.InClusterConfig reads.
//
// They are package vars (not consts) ONLY so the credential-real
// falsifier test can point ServiceAccountEndpoint at a temp dir holding
// real-shaped credentials (a raw JWT token + a raw PEM CA) — the 0.30.102
// unit tests shipped the "illegal base64 data" bug precisely because they
// only exercised the no-files error path, never a real SA credential.
// Production NEVER reassigns these; the values are the fixed projected
// SA volume paths.
var (
	saTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	saCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

// saEndpointSingleton is the process-wide cached SA endpoint. Built
// lazily on first call to ServiceAccountEndpoint(); error path
// re-tries on every call (so a recoverable file-read failure at
// startup doesn't poison the lifetime of the process).
var (
	saEndpointMu       sync.Mutex
	saEndpointInstance *endpoints.Endpoint
)

// saRestConfigSingleton is the process-wide cached SA *rest.Config.
// Built lazily on first successful call to ServiceAccountRESTConfig();
// error path re-tries on every call (same contract as
// saEndpointInstance — a transient boot-time failure must not poison
// the lifetime of the process).
//
// Ship 0.30.167: the 0.30.166 dispatcher attach invokes
// ServiceAccountRESTConfig per request. rest.InClusterConfig() reads
// /var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt} +
// parses the CA PEM on every call — under concurrent /call load
// this serialises the cache-off request path through the disk + parse
// cost, defeating the per-request parallelism the cache-off mode is
// supposed to deliver. The singleton is structurally identical to
// saEndpointInstance and is the architect-recommended Option 1 fix
// (mirror the same prior-art pattern at sa_client.go:55-58).
//
// Concurrency: after construction the *rest.Config is immutable from
// the consumer's perspective; callers MUST NOT mutate the returned
// pointer's fields. rest.Config carries internal sync.Once state for
// its transport cache, so transport construction remains race-free
// regardless of how many goroutines share the pointer.
var (
	saRestConfigMu       sync.Mutex
	saRestConfigInstance *rest.Config
)

// ServiceAccountEndpoint returns the snowplow ServiceAccount-backed
// Endpoint used by the userAccessFilter dispatch path. The endpoint
// targets the in-cluster apiserver ("https://kubernetes.default.svc")
// and carries snowplow's projected SA token (cluster-wide read).
//
// Caches the result on first success — subsequent calls return the
// same pointer. On failure (e.g., running outside a cluster, missing
// token / CA files), returns an error and does NOT cache, so a later
// call can retry (intended for unit tests that synthesise the files
// after pod-init — never relevant in production where the SA volume
// is always mounted).
//
// The function is goroutine-safe.
//
// Per Revision 17 + plan §"Sub-scope A — UAF" binding: this is the
// ONLY way snowplow obtains cluster-wide-read credentials at
// dispatch time. The endpoint is NOT user-bound (deliberately) so
// the refilter step is the load-bearing security gate per
// feedback_no_shortcuts_or_workarounds.md.
func ServiceAccountEndpoint() (*endpoints.Endpoint, error) {
	saEndpointMu.Lock()
	defer saEndpointMu.Unlock()

	if saEndpointInstance != nil {
		return saEndpointInstance, nil
	}

	tokenBytes, err := os.ReadFile(saTokenPath)
	if err != nil {
		return nil, fmt.Errorf("dynamic.sa: read SA token at %s: %w", saTokenPath, err)
	}
	if len(tokenBytes) == 0 {
		return nil, fmt.Errorf("dynamic.sa: SA token at %s is empty", saTokenPath)
	}

	caBytes, err := os.ReadFile(saCAPath)
	if err != nil {
		return nil, fmt.Errorf("dynamic.sa: read SA CA at %s: %w", saCAPath, err)
	}

	ep := &endpoints.Endpoint{
		ServerURL:                "https://kubernetes.default.svc",
		Token:                    string(tokenBytes),
		CertificateAuthorityData: string(caBytes),
		Insecure:                 false,
	}
	saEndpointInstance = ep
	return ep, nil
}

// ServiceAccountRESTConfig returns a *rest.Config backed by snowplow's
// in-cluster ServiceAccount. Used by callers that need a real
// kubernetes.Clientset (e.g., the typed dynamic client) rather than
// the Endpoint shape that the httpcall resolver consumes.
//
// Caches the result on first success — subsequent calls return the
// SAME pointer. On failure (e.g., running outside a cluster, missing
// SA env / file), returns an error and does NOT cache, so a later
// call can retry. Identical shape + contract to ServiceAccountEndpoint
// above (the lines 79-108 prior art).
//
// Ship 0.30.167 memoisation rationale: prior to memoisation, the
// 0.30.166 dispatcher attach invoked rest.InClusterConfig() per
// /call request — re-reading the SA token/CA from disk and re-parsing
// the CA PEM on every dispatch, serialising the cache-off request
// path through the disk + parse cost under concurrent load (the
// parallelism regression). The singleton lifts that cost to once
// per process.
//
// The function is goroutine-safe. After construction the *rest.Config
// is immutable from the consumer's perspective; callers MUST NOT
// mutate the returned pointer's fields.
func ServiceAccountRESTConfig() (*rest.Config, error) {
	saRestConfigMu.Lock()
	defer saRestConfigMu.Unlock()

	if saRestConfigInstance != nil {
		return saRestConfigInstance, nil
	}

	rc, err := inClusterConfigFn()
	if err != nil {
		return nil, fmt.Errorf("dynamic.sa: rest.InClusterConfig: %w", err)
	}
	rc.QPS = -1 // disable client-side rate limiter (server-side P&F is authoritative); see client-go rest/config.go:117-122
	rc.Burst = 0
	saRestConfigInstance = rc
	return rc, nil
}

// inClusterConfigFn is the package-private indirection that lets unit
// tests swap rest.InClusterConfig for a synthetic builder. The real
// rest.InClusterConfig reads hardcoded paths
// /var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt}
// (client-go rest/config.go:544-547) and is not pointable from tests
// without this seam. Production code path is unchanged; the variable
// is initialized once to the real function and is only reassigned by
// test code wrapped in the resetSARestConfigForTest contract.
var inClusterConfigFn = rest.InClusterConfig

// resetSAEndpointForTest clears the singleton so each test sees a
// fresh state. Exported via the _test.go shim only; production code
// MUST NOT call this.
func resetSAEndpointForTest() {
	saEndpointMu.Lock()
	defer saEndpointMu.Unlock()
	saEndpointInstance = nil
}

// resetSARestConfigForTest clears the *rest.Config singleton so each
// test sees a fresh state. Same shape as resetSAEndpointForTest above;
// production code MUST NOT call this.
func resetSARestConfigForTest() {
	saRestConfigMu.Lock()
	defer saRestConfigMu.Unlock()
	saRestConfigInstance = nil
}

// ServiceAccountTokenFile returns the filesystem path of the projected SA token
// (#267 robust fix). The dial site (httpClientForEndpoint, package api) threads
// this into transport.Config.TokenFile, for a dispatch marked
// cache.WithServiceAccountDial, so client-go's NewCachedFileTokenSource re-reads
// it on expiry — self-adapting, no static snapshot, no timer. dynamic is the sole
// minter of the SA endpoint, so the path lives with the minter
// (feedback_self_adapt_no_magic_env_knobs).
//
// There is deliberately NO shape predicate (IsServiceAccountEndpoint): a token-auth
// per-user <user>-clientconfig is shape-identical to the SA endpoint (same apiserver
// ServerURL + cluster CAData + token-auth, !HasCertAuth), so the SA endpoint is
// recognised by PROVENANCE at the resolver (cache.WithServiceAccountDial), never by
// field shape (arch-268 safety ruling).
func ServiceAccountTokenFile() string {
	return saTokenPath
}

// SetServiceAccountTokenPathForTest overrides the projected SA token path so a
// cross-package test (the api dial-site wire-bearer arm) can plant + rotate a
// hermetic token file and observe the file-backed credential on the wire. Returns
// a restore. Exported test-support ONLY — mirrors the package's other *ForTest
// hooks (SetSecretsClientForTest, RebuildRBACSnapshotForTest, …); production never
// calls it. Also clears the SA endpoint singleton so the next mint reads the
// overridden path.
func SetServiceAccountTokenPathForTest(path string) func() {
	orig := saTokenPath
	saTokenPath = path
	resetSAEndpointForTest()
	return func() {
		saTokenPath = orig
		resetSAEndpointForTest()
	}
}
