// discovery_dispatch.go — Fix A1: a CA-bearing dispatch branch for a
// BARE group-discovery api-step path.
//
// THE DEFECT (TRACED — docs/troubleshoot-discovery-url-apistep-x509-2026-06-23.md):
//
//   A composition-resources RESTAction issues an api-step GET against a
//   bare group-discovery URL — /apis/<group>/<version> (no resource
//   segment, no endpointRef) — to enumerate the served resources of each
//   managed apiVersion (e.g. /apis/templates.krateo.io/v1). That path has
//   only 2 segments after "/apis/", so cache.ParseAPIServerPathToDep
//   declines it (inventory.go: needs ≥3 / a resource segment). It then
//   parse-fails EVERY CA-bearing dispatch branch (informer-pivot /
//   apistage / internal-rest-config) and falls through to the EXTERNAL
//   fetch (resolve.go), which builds a plumbing HTTP client from the
//   per-user `<user>-clientconfig` Endpoint. That endpoint is TOKEN-auth
//   (bearer JWT + caData, no client cert); plumbing's tlsConfigFor
//   installs the CA pool ONLY inside the HasCertAuth() branch, so the
//   cluster caData is DROPPED and the apiserver cert is verified against
//   the system root store:
//
//     Get "https://kubernetes.default.svc/apis/templates.krateo.io/v1":
//       tls: failed to verify certificate: x509: certificate signed by
//       unknown authority
//
//   This is the SAME plumbing TLS defect internal_dispatch.go documents
//   for the Phase-1 SA path (0.30.104) — here it bites a per-user request
//   because no CA-bearing dispatch branch claims the bare-discovery shape.
//
// THE FIX (Fix A1, snowplow-side):
//
//   Add a dispatch branch keyed on the discovery SHAPE
//   (cache.ParseAPIServerDiscoveryPath — /apis/<g>/<v> or /api/<v>, no
//   resource segment) ahead of the external fall-through. The branch
//   serves group discovery via client-go's discovery client
//   (ServerResourcesForGroupVersion) on a CA-BEARING SA transport sourced
//   from dynamic.ServiceAccountRESTConfig() — the process-wide in-cluster
//   SA *rest.Config singleton (the rest.InClusterConfig() value, which
//   carries the cluster CA verbatim; client-go's transport installs it
//   correctly, the load-bearing difference from plumbing's httpcall.Do).
//   The served body is the marshalled *metav1.APIResourceList.
//
//   RBAC POSTURE (load-bearing): the SA-serve exemption is for the
//   discovery SHAPE ONLY. Group discovery is anonymous-readable (the
//   system:discovery ClusterRole grants /apis, /apis/<g>/<v>, ... to
//   system:authenticated and system:unauthenticated) and carries NO
//   tenant data — only the apiserver's resource catalogue for a
//   GroupVersion — so serving it under the SA identity leaks nothing. A
//   RESOURCE path is NEVER routed here (ParseAPIServerDiscoveryPath
//   returns false for ≥3-segment paths), so it keeps its existing
//   per-user-token dispatch branch byte-unchanged: SA-serving a resource
//   path WOULD be a cross-user leak, and the shape predicate is what
//   structurally prevents it.
//
//   CACHE-OFF (AC5): the branch sources the SA rc from the process-wide
//   dynamic.ServiceAccountRESTConfig() singleton, which exists regardless
//   of CACHE_ENABLED — NOT the context-carried internal rc (Phase-1-only).
//   So under cache-off the branch STILL fires (transparent fallback,
//   project_cache_off_is_transparent_fallback): the same APIResourceList,
//   not a degraded mode.
//
// CRITICAL — like internal_dispatch.go, this unit/httptest falsifier is
// necessary but NOT sufficient: it runs against an httptest TLS server
// with a synthetic CA and cannot exercise the real cluster CA or a real
// apiserver discovery handshake. The on-cluster falsifier is a /call of
// the triggering RA returning 200 with no x509 in pod logs (tester /
// post-deploy).

package api

import (
	"context"
	"encoding/json"
	"sync"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
)

// discoveryClientCache memoises the client-go discovery client built from
// a given SA *rest.Config. Every /call's discovery-shaped api-step shares
// the SAME SA *rest.Config pointer (dynamic.ServiceAccountRESTConfig()
// returns a process-wide singleton), so rebuilding the discovery client —
// and its TLS transport — per call would be pure waste. Keyed on the
// *rest.Config pointer identity, exactly like internalClientCache
// (internal_dispatch.go). Guarded by discoveryClientMu; the cached
// discovery.DiscoveryInterface is safe for concurrent use.
var (
	discoveryClientMu    sync.Mutex
	discoveryClientCache = map[*rest.Config]discovery.DiscoveryInterface{}
)

// saRESTConfigForDiscoveryFn is the package-private seam over the SA
// *rest.Config source. Production wires dynamic.ServiceAccountRESTConfig
// (the CA-bearing in-cluster singleton); the falsifier swaps in a builder
// returning a *rest.Config pointed at an httptest TLS server with a
// synthetic CA, so the dispatch path can be exercised without a real
// apiserver. Production code path is unchanged — the variable is set once
// and only reassigned by test code. Mirrors inClusterConfigFn
// (dynamic/sa_client.go) and discoveryClientForConfigFn
// (dynamic/cached_client.go).
var saRESTConfigForDiscoveryFn = dynamic.ServiceAccountRESTConfig

// discoveryClientFor returns a memoised client-go discovery client for rc,
// building one on first use. rc is the SA in-cluster *rest.Config and
// carries the cluster CA + SA bearer token verbatim, so client-go's
// NewDiscoveryClientForConfig produces a transport that trusts the
// cluster CA — the load-bearing difference from plumbing's httpcall.Do
// path, whose tlsConfigFor drops the CA for token-auth endpoints.
func discoveryClientFor(rc *rest.Config) (discovery.DiscoveryInterface, error) {
	discoveryClientMu.Lock()
	defer discoveryClientMu.Unlock()
	if cli, ok := discoveryClientCache[rc]; ok {
		return cli, nil
	}
	cli, err := credhttp.Discovery(rc)
	if err != nil {
		return nil, err
	}
	discoveryClientCache[rc] = cli
	return cli, nil
}

// resetDiscoveryClientCacheForTest clears the memoised discovery-client
// map. TEST-ONLY — the production cache is set-once-per-config and never
// cleared. Exported within-package for the falsifier test.
func resetDiscoveryClientCacheForTest() {
	discoveryClientMu.Lock()
	defer discoveryClientMu.Unlock()
	discoveryClientCache = map[*rest.Config]discovery.DiscoveryInterface{}
}

// dispatchViaDiscovery attempts to serve `call` as a BARE group-discovery
// GET through a client-go discovery client built from the process-wide
// CA-bearing SA *rest.Config. It is the discovery-shape sibling of
// dispatchViaInternalRESTConfig and is wired into resolve.go's inner-call
// worker AHEAD of the external fall-through.
//
// Returns (rawBytes, true, nil) when the call was served — the caller
// feeds rawBytes through the same dictMu-protected handler chain the
// in-memory dispatch paths use. Returns (nil, false, nil) for every gate
// that must take the unchanged downstream path:
//
//   - non-GET verb (discovery is read-only; a write to a discovery-shaped
//     path is not a discovery probe and stays on the existing path) — F5;
//   - a non-discovery shape (any resource path — ≥3 segments / a resource
//     segment — and any external / ${...} / malformed path):
//     ParseAPIServerDiscoveryPath returns ok=false — F3/F4. This is the
//     code-enforced RBAC boundary: a resource path is NEVER SA-served here.
//
// Returns (nil, false, err) ONLY when the SA rc could not be built or the
// discovery call itself errored after the dispatcher committed to serving.
// resolve.go treats a non-nil err here exactly as it treats an
// httpcall.Do StatusFailure (it does NOT fall through to the external
// fetch, which would just re-hit the broken plumbing TLS path and mask
// the real error behind a second x509 failure).
//
// On the served path the body is the marshalled *metav1.APIResourceList —
// the exact wire shape the apiserver's group-discovery endpoint returns,
// so the downstream JQ pipeline is invariant
// (feedback_cache_must_not_constrain_jq.md).
func dispatchViaDiscovery(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool, error) {
	// Gate 1: verb. Discovery is read-only; a non-GET to a
	// discovery-shaped path is not a discovery probe (F5).
	if !isPlainGet(call.Verb) { // #1011: the one read-optimisation verb predicate
		return nil, false, nil
	}

	// Gate 2: shape. A discovery-shaped path is either a single-GroupVersion
	// path (/apis/<g>/<v> or /api/<v>) OR a bare discovery ROOT (/api | /apis,
	// #74 Class 1). Both are SA-served via the CA-bearing transport; both
	// reject every resource path (the RBAC boundary). A non-discovery path
	// (resource / external / ${...} / malformed) falls through here.
	gv, gvOK := cache.ParseAPIServerDiscoveryPath(call.Path)
	root, rootOK := cache.ParseAPIServerDiscoveryRoot(call.Path)
	if !gvOK && !rootOK {
		return nil, false, nil
	}

	// Source the CA-bearing SA *rest.Config from the process-wide
	// singleton — present regardless of CACHE_ENABLED (AC5: cache-off
	// transparent fallback) and NOT the per-user request rc. A build
	// failure is surfaced (we committed to serving once the shape
	// matched).
	rc, rcErr := saRESTConfigForDiscoveryFn()
	if rcErr != nil {
		return nil, false, rcErr
	}

	cli, cliErr := discoveryClientFor(rc)
	if cliErr != nil {
		return nil, false, cliErr
	}

	// #74 Class 1 — bare discovery ROOT (/api | /apis): serve the apiserver's
	// own multi-group index via the discovery client's RESTClient AbsPath,
	// over the SAME CA-bearing SA transport. DoRaw returns the raw wire bytes
	// (the apiserver's APIVersions / APIGroupList JSON), so the downstream JQ
	// pipeline is invariant (feedback_cache_must_not_constrain_jq). This is
	// the #18/#19 mechanism extended to the discovery root.
	if rootOK {
		raw, dErr := cli.RESTClient().Get().AbsPath(root).DoRaw(ctx)
		if dErr != nil {
			return nil, false, dErr
		}
		return raw, true, nil
	}

	// Single-GroupVersion discovery (/apis/<g>/<v> or /api/<v>) — the #18/#19
	// path, unchanged.
	list, dErr := cli.ServerResourcesForGroupVersion(gv)
	if dErr != nil {
		return nil, false, dErr
	}

	raw, mErr := json.Marshal(list)
	if mErr != nil {
		return nil, false, mErr
	}
	return raw, true, nil
}
