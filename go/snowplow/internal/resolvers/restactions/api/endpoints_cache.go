// endpoints_cache.go — Ship D.2 (0.30.143). Snowplow-local adapter
// over the AUTHN_NAMESPACE Secrets snapshot
// (internal/cache/secrets_snapshot.go) for the resolver mapper's
// `<user>-clientconfig` lookup. Drop-in sibling of plumbing's
// endpoints.FromSecret (plumbing@v0.9.3/endpoints/endpoints.go:14-89)
// — consulted FIRST on every resolveOne call; falls back to upstream
// on any miss.
//
// AC-D2.6 byte-equivalence (HARD GATE). extractEndpointFromSecret
// MUST extract the same 14 fields upstream extracts, with IDENTICAL
// semantics:
//   - server-url REQUIRED → on miss return the exact upstream error
//     verbatim: `"missed required attribute for endpoint: server-url"`.
//   - 11 optional string fields copied as-is.
//   - debug + insecure booleans use strconv.ParseBool(string(v));
//     parse error MUST be SILENTLY DROPPED (assign `_`) — mirrors
//     upstream verbatim. A test that strict-checks the parse error
//     would BREAK upstream parity.
//
// MIGRATION SITE (AC-D2.4). endpoints.go:67-68 consults this BEFORE
// the upstream plumbing call. On cache hit, the post-processing
// (isInternal+!env.TestMode → ServerURL override) MUST run on both
// the cache-hit and the fallback paths — kept in endpoints.go (NOT
// factored here) per PM-explicit `feedback_no_special_cases`: that
// override is the resolver's contextual decision, not a Secret-cache
// concern.
package api

import (
	"context"
	"errors"
	"strconv"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
)

// Label constants — snowplow-side mirror of plumbing's UNEXPORTED
// label set (plumbing@v0.9.3/endpoints/endpoints.go:124-139). Re-
// declared here because plumbing's are package-private; the design
// §2.5 references this fork.
//
// AC-D2.6 byte-equivalence GATE: TestFromInformerSecret_UpstreamFieldParity
// asserts these strings + the extraction logic produce a
// reflect.DeepEqual Endpoint to plumbing's FromSecret across the
// documented field permutations. A future upstream rename of any
// label would silently break parity; the test catches it.
const (
	clientCertLabel   = "client-certificate-data"
	clientKeyLabel    = "client-key-data"
	caLabel           = "certificate-authority-data"
	proxyUrlLabel     = "proxy-url"
	serverUrlLabel    = "server-url"
	debugLabel        = "debug"
	passwordLabel     = "password"
	usernameLabel     = "username"
	tokenLabel        = "token"
	insecureLabel     = "insecure"
	awsAccessKeyLabel = "aws-access-key"
	awsSecretKeyLabel = "aws-secret-key"
	awsRegionLabel    = "aws-region"
	awsServiceLabel   = "aws-service"
)

// FromInformerSecret is the snowplow-local sibling of plumbing's
// endpoints.FromSecret. It serves the per-user <user>-clientconfig
// Secret from the Ship D.2 in-process informer cache when servable;
// signals "not served" so the caller falls back to upstream
// FromSecret otherwise.
//
// Returns:
//   - (endpoint, true,  nil) — cache hit, content extracted.
//   - (zero,     false, nil) — soft miss: cache=off, informer not
//     servable (pre-sync, watch-broken, namespace mismatch), or
//     Secret absent from the snapshot. Caller falls back to upstream.
//     AC-D2.12 pre-readiness fallback: a nil snapshot pointer (the
//     window between StartSecretsInformer's return and the initial
//     publish landing) ALSO returns this — semantic-equivalent to
//     "not servable yet."
//   - (zero,     false, err) — hard error: Secret IS present in the
//     snapshot BUT field extraction failed (specifically server-url
//     missing). Returns the exact upstream error message verbatim
//     ("missed required attribute for endpoint: server-url") so a
//     caller checking err.Error() doesn't behave differently.
//     Reached by the Ship D.3 middleware site
//     (`internal/handlers/middleware/userconfig.go`) on cache
//     hard-error — the middleware treats this branch as upstream's
//     non-NotFound error arm (→ 500 InternalError with the verbatim
//     string). The resolver-internal site
//     (`endpoints.go:67-88`) reaches it only when the upstream
//     re-call subsequently produces the same error; on a plain cache
//     hard-error the resolver re-call ALSO returns the same string
//     (apiserver state is authoritative — if the Secret on the wire
//     is malformed, both call sites surface the same verbatim text).
//
// AC-D2.6 byte-equivalence: TestFromInformerSecret_UpstreamFieldParity
// asserts this returns a reflect.DeepEqual Endpoint to plumbing's
// FromSecret across every documented field permutation, INCLUDING
// the silent ParseBool error on debug/insecure.
//
// Identity-free at the cache layer: the function reads whatever
// (namespace, name) the caller asks for. RBAC is enforced upstream
// — the resolver only ever asks for the user derived from the
// authenticated request identity (same name plumbing would have
// asked for via endpoints.FromSecret). The `<user>-clientconfig`
// scope is preserved by construction.
func FromInformerSecret(ctx context.Context, namespace, name string) (endpoints.Endpoint, bool, error) {
	_ = ctx // reserved for future cancellation / logger; the snapshot read is non-blocking
	if cache.Disabled() {
		// Cache=off — invariant inert. Honors
		// project_caching_is_provisional.
		return endpoints.Endpoint{}, false, nil
	}
	if !cache.SecretsCacheServable() {
		// Soft miss: pre-sync window, watch-broken, or informer not
		// wired (AUTHN_NAMESPACE unset at boot). Fall back.
		return endpoints.Endpoint{}, false, nil
	}
	if namespace != cache.SecretsCacheNamespace() {
		// The cache is scoped to AUTHN_NAMESPACE; a request for a
		// different namespace is uncacheable by construction. Fall
		// back. The namespace-match guard is the architectural
		// boundary the WithNamespace informer option draws — design
		// §7 X2 documents this allowance.
		return endpoints.Endpoint{}, false, nil
	}

	snap := cache.SecretsSnapshotLoad()
	if snap == nil {
		// AC-D2.12 pre-readiness fallback: SecretsCacheServable
		// returned true (HasSynced flipped) but the initial publish
		// hasn't landed yet, OR the snapshot was reset for a test
		// after servability gate flipped. Either way, fall back to
		// upstream — the apiserver is the authoritative source.
		return endpoints.Endpoint{}, false, nil
	}

	sec, ok := snap.ByName[name]
	if !ok || sec == nil {
		// Soft miss: the Secret is absent in the snapshot. Upstream
		// FromSecret will then issue the apiserver GET and get the
		// authoritative answer (likely 404, indistinguishable from
		// a stale snapshot in our eventual-consistency model). We
		// do NOT synthesize a 404 — apiserver is source of truth
		// for absence (feedback_apiserver_is_source_of_truth).
		return endpoints.Endpoint{}, false, nil
	}

	ep, err := extractEndpointFromSecret(sec)
	if err != nil {
		// Hard error path: the Secret is present but its content
		// is malformed (server-url missing). Return the error
		// verbatim so the caller's err.Error() check sees the same
		// string upstream FromSecret would have produced.
		return endpoints.Endpoint{}, false, err
	}
	return ep, true, nil
}

// extractEndpointFromSecret mirrors plumbing's FromSecret field-
// extraction (endpoints.go:29-87) line-by-line. AC-D2.6 byte-
// equivalence is the hard contract: identical label set, identical
// optional-field handling, identical strconv.ParseBool with SILENT
// error (the `_` blank assign is intentional — upstream does it,
// we mirror).
func extractEndpointFromSecret(sec *corev1.Secret) (endpoints.Endpoint, error) {
	res := endpoints.Endpoint{}

	// server-url is REQUIRED. Upstream returns the verbatim error
	// "missed required attribute for endpoint: server-url" — we
	// match the string exactly. Tests grep for this constant in
	// err.Error() and a divergence breaks parity.
	if v, ok := sec.Data[serverUrlLabel]; ok {
		res.ServerURL = string(v)
	} else {
		return res, errors.New(msgEndpointSecretNoServerURL)
	}

	if v, ok := sec.Data[proxyUrlLabel]; ok {
		res.ProxyURL = string(v)
	}
	if v, ok := sec.Data[tokenLabel]; ok {
		res.Token = string(v)
	}
	if v, ok := sec.Data[usernameLabel]; ok {
		res.Username = string(v)
	}
	if v, ok := sec.Data[passwordLabel]; ok {
		res.Password = string(v)
	}
	if v, ok := sec.Data[caLabel]; ok {
		// Ship 0.30.165 — cache-on mirror of endpoints.go normalization
		// (HG-165-4 cache-on parity). For correctly-shaped single-base64
		// PEM bytes the call is a no-op passthrough; for double-base64
		// `<user>-clientconfig` Secrets it returns the inner single-base64
		// form that plumbing's transport can decode in one pass.
		//
		// 0.30.166: dormant — wire shape is single-base64 in-process; helper
		// kept as inert passthrough. The 0.30.165 pre-deploy proof was
		// captured via kubectl-jsonpath (auto-decodes wire-base64) which
		// silently double-decoded vs the in-process client-go shape; the
		// helper defends a wire shape that never occurs at this call site.
		// Retained per design §5 (zero-cost forward defence; deletion adds
		// churn) and `feedback_no_kubectl_jsonpath_for_in_process_reasoning`.
		res.CertificateAuthorityData = string(normalizeCAData(v))
	}
	if v, ok := sec.Data[clientKeyLabel]; ok {
		res.ClientKeyData = string(v)
	}
	if v, ok := sec.Data[clientCertLabel]; ok {
		res.ClientCertificateData = string(v)
	}

	// debug + insecure: strconv.ParseBool with SILENT parse error
	// (the blank `_` assign mirrors upstream verbatim — see
	// plumbing/endpoints/endpoints.go:63 and :67). A malformed
	// bool leaves the field at its zero value (false). AC-D2.6:
	// TestFromInformerSecret_UpstreamFieldParity covers the
	// debug-invalid-bool case explicitly.
	if v, ok := sec.Data[debugLabel]; ok {
		res.Debug, _ = strconv.ParseBool(string(v))
	}
	if v, ok := sec.Data[insecureLabel]; ok {
		res.Insecure, _ = strconv.ParseBool(string(v))
	}

	if v, ok := sec.Data[awsAccessKeyLabel]; ok {
		res.AwsAccessKey = string(v)
	}
	if v, ok := sec.Data[awsSecretKeyLabel]; ok {
		res.AwsSecretKey = string(v)
	}
	if v, ok := sec.Data[awsRegionLabel]; ok {
		res.AwsRegion = string(v)
	}
	if v, ok := sec.Data[awsServiceLabel]; ok {
		res.AwsService = string(v)
	}

	return res, nil
}
