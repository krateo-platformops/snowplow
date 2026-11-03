package api

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/url"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	"k8s.io/client-go/transport"
)

// endpoints_tls.go — snowplow#229 / audit A1: the CA bundle of a
// TOKEN-AUTH endpoint is silently dropped, so any non-GET api-step with
// no `endpointRef` fails TLS against a private CA.
//
// THE DEFECT, on the wire (same pod, same second, same SA endpoint, same
// host — the asymmetry IS the bug):
//
//	GET  /apis/none.krateo.io/v1/none                       -> honest 404, TLS fine
//	POST /apis/authorization.k8s.io/v1/subjectaccessreviews -> x509: certificate
//	                                                           signed by unknown authority
//
// MECHANISM, traced end to end:
//
//  1. snowplow's own ServiceAccount endpoint is built TOKEN-AUTH with a
//     RAW-PEM CA (`internal/dynamic/sa_client.go:126-131`): Token set,
//     CertificateAuthorityData set, ClientCertificateData/ClientKeyData
//     left EMPTY ⇒ `endpoints.Endpoint.HasCertAuth()` is false.
//  2. That endpoint is returned for ANY nil `endpointRef`
//     (`endpoints.go:69-71`) and is attached to every in-cluster `/call`
//     (`restactions.go:261`, `widgets.go:272`).
//  3. plumbing's `tlsConfigFor` RETURNS BEFORE THE CA INSTALL when the
//     endpoint is not cert-auth (`http/request/transport.go:37-39`,
//     plumbing v1.14.2 — the CA-install code lives at :56-72, INSIDE the
//     `HasCertAuth()` branch). A token-auth endpoint therefore verifies
//     the server against the SYSTEM root store, never its own CA.
//  4. A GET escapes only by INTERCEPTION: all four CA-bearing dispatch
//     branches gate on `verb == GET` (`apistage.go:481`,
//     `informer_dispatch.go:274`, `internal_dispatch.go:271`,
//     `discovery_dispatch.go:169`), so a GET never reaches plumbing. A
//     POST passes all four and falls through to `external_fetch.go:136`,
//     the single call site of `httpcall.HTTPClientForEndpoint`.
//
// SECOND, LATENT DEFECT AT THE SAME PLACE: even with the early return
// removed, plumbing base64-decodes the CA (`transport.go:59`) while this
// CA is raw PEM, whose leading '-' is not in the base64 alphabet ⇒
// "unable to decode certificate authority data". `normalizeCAData`
// (`endpoints_ca.go:30`) passes raw PEM straight through, so it does not
// cover this shape either. Both encodings must therefore be accepted, or
// the fix works on one cluster and not another — see caPEMBytes.
//
// IDENTITY-NEUTRAL BY CONSTRUCTION: the SA bearer token is ALREADY what
// is presented today (step 2); installing the CA changes the TLS trust
// store and nothing else — same credential, same identity, same RBAC
// posture. The SubjectAccessReview payload carries an explicit subject,
// so it intends to be sent by a privileged caller asking about another
// subject. Any change to WHICH credential is presented must be rejected.
//
// httpClientForEndpoint is the snowplow-owned replacement for
// `httpcall.HTTPClientForEndpoint`, covering exactly the shape plumbing
// mishandles and DELEGATING VERBATIM for every other shape — so every
// endpoint that works today is byte-identical (the whole live cert-auth
// `<user>-clientconfig` population goes through the unchanged path).
//
// The predicate is keyed on the endpoint's DECLARED CA and auth mode —
// not on a path, a resource, a verb, a user or an env flag — so it
// satisfies feedback_no_special_cases and
// feedback_self_adapt_no_magic_env_knobs. It fixes the same bug for a
// customer's token-auth webhook behind a private CA, which is broken
// today for the identical reason.
//
// PRIOR ART (feedback_check_k8s_clientgo_prior_art): the owned branch
// does NOT hand-roll an x509.CertPool. `k8s.io/client-go/transport.New`
// builds precisely this RoundTripper — TLS from `TLSConfig.CAData` plus
// the bearer/basic auth wrappers via `HTTPWrappersForConfig`. (Its
// `tlsCache` does NOT keep a transport between calls: since client-go v0.36
// the cache holds a weak pointer (ClientsAllowTLSCacheGC, default on), so a
// client built per call is re-dialled after every GC — measured: 5 requests,
// 5 dials. Reuse is httpClientForEndpoint's cache below, #1088.)
//
// #1088 — ONE CLIENT PER ENDPOINT IDENTITY, REUSED. The owned branch used to
// take only the token-auth-with-CA shape, and every other endpoint (the whole
// live cert-auth `<user>-clientconfig` population included) went to plumbing,
// whose tlsConfigFor builds a NEW http.Transport on every call: every stage
// fetch paid a fresh TCP connect and TLS handshake (run 20: one unanswered SYN
// held a page for the 30 s connect timeout). The owned branch now takes every
// shape client-go's transport.Config reproduces exactly (endpointOwnedShape),
// and httpClientForEndpoint keeps each such client in stageEndpointClients,
// keyed on every field of the endpoint plus the provenance marks, so one
// identity's fetches share one kept-alive HTTP/2 connection.
//
// WHY CLIENT-GO'S TRANSPORT AND NOT PLUMBING'S, KEPT: a shared connection must
// notice a peer that died silently, or every request multiplexed on it hangs
// until the kernel gives up on TCP. client-go's transport (utilnet.
// SetTransportDefaults) runs the HTTP/2 health check (read-idle ping); plumbing's
// has none and no response timeout either. That cost nothing while every fetch
// had its own connection; it would once fetches share one. So a plumbing-built
// client is never kept: it stays per fetch, exactly as before.
func httpClientForEndpoint(ctx context.Context, ep *endpoints.Endpoint, ri *httpcall.RequestInfo) (*http.Client, error) {
	held := credhttp.HeldCredential(ctx)
	saDial := cache.ServiceAccountDialFromContext(ctx)
	if !endpointOwnedShape(ep) {
		cli, _, err := buildEndpointClient(ctx, ep, ri)
		if err != nil {
			return cli, err
		}
		return wrapEndpointClient(cli, ep, held, saDial), nil
	}
	// The provenance inputs are everything besides the endpoint that changes
	// the chain buildEndpointClient and wrapEndpointClient build: a held
	// credential refuses redirects, and an SA dial presents the token it reads
	// from this file instead of ep.Token.
	heldIn, saTokenFile := "", ""
	if held {
		heldIn = "held"
	}
	if saDial {
		saTokenFile = "sa-token-file:" + dynamic.ServiceAccountTokenFile()
	}
	return stageEndpointClients.Get(credhttp.EndpointKeyOf(ep, heldIn, saTokenFile), func() (*http.Client, bool, error) {
		cli, keep, err := buildEndpointClient(ctx, ep, ri)
		if err != nil {
			return cli, false, err
		}
		return wrapEndpointClient(cli, ep, held, saDial), keep, nil
	})
}

// stageEndpointClients is the stage fetch's per-identity client cache (#1088).
var stageEndpointClients = credhttp.NewEndpointClients()

// StageEndpointClientsStats reads the stage client cache for the metrics:
// entries held, clients built (one per miss), entries dropped by the idle sweep.
func StageEndpointClientsStats() (entries int, builds, evictions uint64) {
	return stageEndpointClients.Len(), stageEndpointClients.Builds(), stageEndpointClients.Evictions()
}

// wrapEndpointClient applies the provenance policy to a freshly built client
// of ep, in place. Everything it installs is a function of ep and the two
// provenance marks, all three folded into the #1088 cache key, so a kept client
// carries exactly the policy a fresh build would.
func wrapEndpointClient(cli *http.Client, ep *endpoints.Endpoint, held, saDial bool) *http.Client {
	// #1112 D4 — an endpoint that asks for a wire dump gets snowplow's, which
	// logs metadata only: no credential, cookie or body (credhttp/wiredump.go).
	// Plumbing's own dump is never installed (buildEndpointClient hands it a
	// copy with Debug cleared). Inside the redirect policy below, so a refused
	// hop is not dumped either: it never leaves.
	if ep != nil && ep.Debug {
		cli.Transport = credhttp.WireDump(cli.Transport)
	}
	// #723/#724 — a dial that presents a credential snowplow holds (the SA
	// endpoint, marked by cache.WithServiceAccountDial, or any held credential,
	// marked by credhttp.WithHeldCredential: the per-user clientconfig) NEVER
	// follows a redirect. Both the bearer and the #615 Impersonate-* headers are
	// added by RoundTrippers on EVERY hop, so Go's cross-domain header stripping
	// never protects them; and a followed redirect's body would be accepted as
	// the object. credhttp.Client refuses at CheckRedirect (the caller sees the
	// 3xx with its own code) and at the transport (no hop reaches the network).
	//
	// #1106 — a named endpointRef's dial never leaves its origin either: a
	// redirect elsewhere is refused by credhttp.Send, the one place every
	// endpoint request leaves, outermost on this client, so nothing installed
	// here (the wire dump above, a plumbing-built chain) can displace it
	// (credhttp/endpoint_redirect.go).
	if held || saDial {
		cli = credhttp.Client(cli)
	}
	// #988 — whatever credential the stage dials with, its collection LISTs
	// are a resolve's, so every endpoint client counts them
	// (snowplow_resolve_collection_lists). Separate from the redirect policy
	// above: a named endpointRef has its own redirect policy, but a LIST
	// it sends to the apiserver — e.g. of a GVR whose store is being swapped
	// (#870) — is counted like any other. On a held dial the counter is already
	// in Client's chain and this adds none.
	return credhttp.CountResolveListsOn(cli)
}

// buildEndpointClient builds the client for ep and reports whether it may be
// kept across fetches (only a client-go-built one, see httpClientForEndpoint).
// It is the ONLY site in the module allowed to build an endpoint client without
// credhttp (the structural guard in internal/credhttp names it), because its
// single caller, httpClientForEndpoint, applies credhttp.Client by provenance
// (credhttp.EndpointClient otherwise, #1106). Plumbing is always handed a copy
// of ep without Debug, so it never installs its wire dump (#1112 D4).
func buildEndpointClient(ctx context.Context, ep *endpoints.Endpoint, ri *httpcall.RequestInfo) (*http.Client, bool, error) {
	if !endpointOwnedShape(ep) {
		cli, err := httpcall.HTTPClientForEndpoint(credhttp.WithoutPlumbingWireDump(ep), ri)
		return cli, false, err
	}

	tlsCfg, ok := endpointTLSConfig(ep)
	if !ok {
		cli, err := httpcall.HTTPClientForEndpoint(credhttp.WithoutPlumbingWireDump(ep), ri)
		return cli, false, err
	}

	// #1088 — Proxy is always set: client-go shares one transport across every
	// config with equal TLS material ONLY when Proxy is nil (tlsConfigKey), so
	// an explicit proxy keeps this transport this identity's own. Two bearer
	// tokens on one CA therefore never share a connection, and the idle sweep
	// closing one identity's connections never closes another's. When the
	// endpoint names no proxy, the one set is http.ProxyFromEnvironment: what
	// client-go and plumbing both use when none is given.
	cfg := &transport.Config{
		TLS:   tlsCfg,
		Proxy: http.ProxyFromEnvironment,
	}
	// #267 robust fix (design §4). When this dispatch dials snowplow's own SA
	// endpoint — recognised by PROVENANCE (cache.WithServiceAccountDial, stamped by
	// runStage when resolveStageEndpoint returned the ctx-carried internal endpoint
	// or the UAF SA endpoint), NEVER by field shape — thread the token FILE so
	// client-go's NewBearerAuthWithRefreshRoundTripper / NewCachedFileTokenSource
	// re-reads it on expiry (self-adapting, no timer). A read-once static ep.Token
	// goes stale → 401 (the #267 defect, the accident masking #268 branch E).
	//
	// Every unmarked dispatch keeps the static ep.Token — a per-user
	// <user>-clientconfig (token OR cert auth, resolved via FromSecret, never
	// marked) and any external endpoint are byte-identical to before. This is why
	// the marker is provenance, not shape: a token-auth clientconfig is
	// shape-identical to the SA endpoint (same apiserver ServerURL + cluster CAData
	// + token-auth) and would be misclassified by a field predicate, reintroducing
	// #268/#269.
	//
	// #1088: the SA dial now sets BOTH, as client-go's own rest.InClusterConfig
	// does (BearerToken + BearerTokenFile): client-go presents the file's token
	// whenever the file reads, and the endpoint's token only while it cannot
	// (bearerAuthRoundTripper). Before #1088 only the token-auth-with-CA shape
	// reached this branch and an SA-marked dial of any other shape went to
	// plumbing with ep.Token alone; every SA-marked shape is owned now, and the
	// endpoint's token stays its fallback rather than becoming a build failure.
	// The file refreshes the bearer credential the endpoint CARRIES: an
	// endpoint with no token has no bearer credential to refresh and presents
	// none, exactly as plumbing sent it (the SA endpoint always carries its
	// boot token, sa_client.go).
	cfg.BearerToken = ep.Token
	if cache.ServiceAccountDialFromContext(ctx) && ep.HasTokenAuth() {
		cfg.BearerTokenFile = dynamic.ServiceAccountTokenFile()
	}

	// THE TWO LIBRARIES DEFINE "basic auth" ON DIFFERENT FIELDS, and copying
	// both credentials unconditionally turns a working endpoint into a hard
	// failure:
	//
	//	plumbing  v1.14.2  endpoints/types.go:29-31    HasBasicAuth() = len(Password) != 0  ← PASSWORD
	//	client-go v0.33.0  transport/config.go:105-107 HasBasicAuth() = len(Username) != 0  ← USERNAME
	//
	// and client-go HARD-ERRORS on the overlap
	// (transport/round_trippers.go:50-51): "username/password or bearer token
	// may be set, but not both".
	//
	// So an endpoint carrying a Token AND a Username with an EMPTY Password
	// works today — plumbing sees no password, classifies it token-auth, and
	// succeeds — but would make transport.New refuse to build a client at all,
	// i.e. a total outage for that endpoint rather than a degradation.
	//
	// Gate the copy on PLUMBING's predicate so this function preserves
	// plumbing's classification exactly. Adopting client-go's would silently
	// change WHICH endpoints count as basic auth, which is a different (and
	// unrequested) behavioural change.
	if ep.HasBasicAuth() {
		cfg.Username = ep.Username
		cfg.Password = ep.Password
	}

	// Mirror plumbing's proxy handling (transport.go:28-35 + its
	// parseProxyURL scheme allow-list) so a proxied token-auth endpoint
	// keeps the same behaviour it would have had.
	if ep.ProxyURL != "" {
		u, err := url.Parse(ep.ProxyURL)
		if err != nil {
			return nil, false, fmt.Errorf("could not parse: %v", ep.ProxyURL)
		}
		switch u.Scheme {
		case "http", "https", "socks5":
		default:
			return nil, false, fmt.Errorf("unsupported scheme %q, must be http, https, or socks5", u.Scheme)
		}
		cfg.Proxy = http.ProxyURL(u)
	}

	rt, err := transport.New(cfg)
	if err != nil {
		return nil, false, err
	}

	// NOTE on the trace-id header: plumbing wraps every client in a
	// traceIdRoundTripper (transport.go:107-119) that sets
	// X-Krateo-TraceId when absent. It is not replicated here because
	// the SOLE call site (external_fetch.go:136) has already set that
	// header on the request in both its branches — the AWS branch via
	// opts.Headers, the non-AWS branch via call.Header.Set — before the
	// client is built. An AWS-auth endpoint never reaches this branch
	// anyway (see endpointOwnedShape).
	//
	// THE WIRE DUMP (#1112 D4): plumbing wraps its client in a
	// debuggingRoundTripper when ep.Debug is set, logging the Authorization
	// header and both bodies. No builder installs it any more; a Debug endpoint
	// on either branch gets credhttp.WireDump from wrapEndpointClient instead.
	//
	// CONNECTION REUSE (#1088): this client is kept per endpoint identity by
	// httpClientForEndpoint, and its transport is its own (Proxy above), so one
	// identity's fetches share its connections and no other identity's do.
	return &http.Client{Transport: rt}, true, nil
}

// endpointOwnedShape reports whether the owned client-go branch takes ep (and
// so whether its client is kept per identity, #1088). It is the cheap,
// declarative POLICY half: it reads flags and field presence only, never
// parses. endpointTLSConfig is the CAPABILITY half that parses.
//
// ONE PRINCIPLE GOVERNS EVERY EXCLUSION BELOW: the owned branch owns only the
// shapes it can reproduce EXACTLY. A shape client-go's transport.Config cannot
// express is delegated to plumbing unchanged, per fetch as before — better to
// leave a shape as it is today than to "fix" it into a different, quieter
// failure. Every exclusion is an instance of that rule:
//
//   - ep == nil                 → nothing to build from; delegate.
//   - HasAwsAuth()              → plumbing signs SigV4 at client BUILD time from
//     the request (ComputeAwsSignature), through an
//     unexported roundtripper with no client-go
//     equivalent: the client is bound to one request,
//     so it can be neither owned nor kept.
//   - HasBasicAuth() &&          → plumbing keys basic auth off PASSWORD and sends
//     Username == ""               `Authorization: Basic base64(":password")`;
//     client-go keys it off USERNAME, so the same
//     Config would send NO Authorization header at
//     all. A silent credential drop is worse than any
//     loud failure, so it is delegated, as AWS is.
//   - HasCertAuth() && Insecure → plumbing IGNORES Insecure on a cert-auth
//     endpoint (its cert branch replaces the
//     skip-verify tls.Config with one that verifies
//     against the CA pool); client-go would honour it.
//   - HasCertAuth() && !HasCA() → plumbing verifies a cert-auth endpoint against
//     the EMPTY pool a missing CA leaves; client-go
//     would use the system roots.
//
// Everything else is owned: the cert-auth `<user>-clientconfig` (the whole live
// per-user population), the token-auth SA endpoint and any token-auth endpoint
// with a CA (#229: plumbing drops that CA), CA-less endpoints (system roots, as
// plumbing), Insecure endpoints (skip-verify, as plumbing, which ignores the CA
// then), bearer, basic-with-username and a proxy. A Debug endpoint is owned
// like any other since #1112 D4: its wire dump is snowplow's own wrapper
// (credhttp.WireDump), installed on either branch, so it no longer needs
// plumbing's.
func endpointOwnedShape(ep *endpoints.Endpoint) bool {
	if ep == nil || ep.HasAwsAuth() || (ep.HasBasicAuth() && ep.Username == "") {
		return false
	}
	if ep.HasCertAuth() && (ep.Insecure || !ep.HasCA()) {
		return false
	}
	return true
}

// endpointTLSConfig is ep's TLS material as client-go takes it, read exactly as
// plumbing reads it, or ok=false when ep carries material the owned branch
// cannot read exactly — the caller then delegates to plumbing, which fails or
// succeeds on it as it does today. Requires endpointOwnedShape(ep).
func endpointTLSConfig(ep *endpoints.Endpoint) (transport.TLSConfig, bool) {
	if ep.HasCertAuth() {
		// plumbing's tlsConfigFor: cert, key and CA each base64-decoded once,
		// the CA appended as PEM, and a key pair that does not load is an error.
		cert, cerr := base64.StdEncoding.DecodeString(ep.ClientCertificateData)
		key, kerr := base64.StdEncoding.DecodeString(ep.ClientKeyData)
		ca, aerr := base64.StdEncoding.DecodeString(ep.CertificateAuthorityData)
		if cerr != nil || kerr != nil || aerr != nil {
			return transport.TLSConfig{}, false
		}
		if block, _ := pem.Decode(ca); block == nil {
			return transport.TLSConfig{}, false
		}
		if _, err := tls.X509KeyPair(cert, key); err != nil {
			return transport.TLSConfig{}, false
		}
		return transport.TLSConfig{CAData: ca, CertData: cert, KeyData: key}, true
	}
	if ep.Insecure {
		// plumbing: skip-verify, and the CA is never read without cert auth.
		return transport.TLSConfig{Insecure: true}, true
	}
	if !ep.HasCA() {
		return transport.TLSConfig{}, true // system roots, as plumbing
	}
	caPEM, ok := caPEMBytes([]byte(ep.CertificateAuthorityData))
	if !ok {
		// THE CAPABILITY DELEGATION, deliberately distinct from the policy
		// rejection in endpointOwnedShape — do NOT collapse the two. That one
		// is declarative and silent; this one is "this IS our shape, but we
		// cannot read its CA" — a misconfiguration worth surfacing.
		//
		// The CA is present but is neither raw PEM nor (double-)base64 PEM.
		// Delegate and let plumbing fail exactly as it does today rather than
		// silently building a client with an EMPTY root pool — an empty pool
		// would fall back to the system roots and turn a loud misconfiguration
		// into a confusing x509 error.
		//
		// #233: warnUnparseableCADelegation surfaces it: an uncapped OTLP
		// detector counter (snowplow_unparseable_ca_delegations_total) + a
		// BOUNDED one-shot WARN keyed by (ServerURL, CA sha256). Such a client
		// is not kept (#1088), so it still counts once per fetch.
		warnUnparseableCADelegation(ep)
		return transport.TLSConfig{}, false
	}
	return transport.TLSConfig{CAData: caPEM}, true
}

// maxCABase64Unwraps bounds the base64 unwrapping loop in caPEMBytes.
// Two is sufficient for every shape observed in the wild: the RAW PEM
// that sa_client.go builds (zero unwraps), the single-base64 PEM of a
// correctly-shaped client-go kubeconfig (one), and the double-base64 PEM
// some operators write into `<user>-clientconfig` Secrets — the shape
// normalizeCAData exists to absorb (two). The bound is what stops a
// crafted input driving an unbounded decode loop.
const maxCABase64Unwraps = 2

// caPEMBytes returns ca as the RAW PEM bytes client-go's
// transport.TLSConfig.CAData expects, accepting every CA encoding
// snowplow can be handed, and reports whether it recognised the shape.
//
// Shape-keyed detection, NOT source-keyed (feedback_no_special_cases):
// PEM text begins with "-----BEGIN" and '-' is not in the standard
// base64 alphabet, so a raw PEM can never be mistaken for base64 and the
// first check is unambiguous. Each further step peels exactly one
// base64 layer and re-tests for PEM.
//
// Returning ok=false for an unrecognised shape is load-bearing: the
// caller must NOT build a client with an empty root pool, because an
// empty pool silently falls back to the system roots — which is the very
// failure mode this file exists to remove.
func caPEMBytes(ca []byte) ([]byte, bool) {
	if len(ca) == 0 {
		return nil, false
	}
	cur := ca
	for i := 0; i <= maxCABase64Unwraps; i++ {
		if block, _ := pem.Decode(cur); block != nil {
			return cur, true
		}
		dec, err := base64.StdEncoding.DecodeString(string(cur))
		if err != nil {
			return nil, false
		}
		cur = dec
	}
	return nil, false
}
