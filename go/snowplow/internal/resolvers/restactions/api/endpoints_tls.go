package api

import (
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/url"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
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
// the bearer/basic auth wrappers via `HTTPWrappersForConfig` — and
// memoises the transport in client-go's `tlsCache`, so repeated calls
// share connections instead of building a fresh transport per request
// the way plumbing does.
func httpClientForEndpoint(ep *endpoints.Endpoint, ri *httpcall.RequestInfo) (*http.Client, error) {
	if !endpointNeedsOwnedCAClient(ep) {
		return httpcall.HTTPClientForEndpoint(ep, ri)
	}

	caPEM, ok := caPEMBytes([]byte(ep.CertificateAuthorityData))
	if !ok {
		// THE SECOND DELEGATION SITE, deliberately distinct from the predicate
		// rejection above — do NOT collapse the two. That one is a POLICY
		// decision ("this shape is not ours"): declarative, cheap, and the
		// normal path for the entire live cert-auth population, where silence
		// is correct. This one is a CAPABILITY failure ("this IS our shape, but
		// we cannot read its CA") — a misconfiguration worth surfacing. Folding
		// caPEMBytes into the routing predicate would make a declarative
		// predicate do parsing work and erase which of the two occurred.
		//
		// The CA is present but is neither raw PEM nor (double-)base64
		// PEM. We cannot improve on plumbing for an unrecognised shape,
		// so delegate and let it fail exactly as it does today rather
		// than silently building a client with an EMPTY root pool —
		// an empty pool would fall back to the system roots and turn a
		// loud misconfiguration into a confusing x509 error.
		//
		// KNOWN GAP, tracked as snowplow#233: this delegation is itself SILENT,
		// so the operator sees plumbing's x509 error with nothing saying
		// snowplow read the bundle and could not parse it. The fix needs a
		// BOUNDED warning — the endpoint resolves per stage per /call, so an
		// unconditional one floods — and that decision belongs with #233, not
		// with this diff.
		return httpcall.HTTPClientForEndpoint(ep, ri)
	}

	cfg := &transport.Config{
		TLS:         transport.TLSConfig{CAData: caPEM},
		BearerToken: ep.Token,
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
			return nil, fmt.Errorf("could not parse: %v", ep.ProxyURL)
		}
		switch u.Scheme {
		case "http", "https", "socks5":
		default:
			return nil, fmt.Errorf("unsupported scheme %q, must be http, https, or socks5", u.Scheme)
		}
		cfg.Proxy = http.ProxyURL(u)
	}

	rt, err := transport.New(cfg)
	if err != nil {
		return nil, err
	}

	// NOTE on the trace-id header: plumbing wraps every client in a
	// traceIdRoundTripper (transport.go:107-119) that sets
	// X-Krateo-TraceId when absent. It is not replicated here because
	// the SOLE call site (external_fetch.go:136) has already set that
	// header on the request in both its branches — the AWS branch via
	// opts.Headers, the non-AWS branch via call.Header.Set — before the
	// client is built. An AWS-auth endpoint never reaches this branch
	// anyway (see endpointNeedsOwnedCAClient).
	//
	// KNOWN DIVERGENCE, diagnostic-only: plumbing also wraps the client in a
	// debuggingRoundTripper when ep.Debug is set (client.go:19-23), dumping
	// each request via httputil.DumpRequestOut. A Debug:true token-auth
	// endpoint with a CA loses that dump on this branch. Named here rather
	// than left to be rediscovered as a bug; it affects no request semantics.
	//
	// CONNECTION REUSE: transport.New memoises in client-go's tlsCache, which
	// keys on the CA material and EXCLUDES the bearer token (and returns an
	// uncacheable transport when a proxy is set). So two endpoints sharing a
	// CA share TCP/TLS connections while each still presents its OWN token
	// per request — no cross-identity credential reuse, and strictly better
	// than plumbing, which builds a fresh transport on every call.
	return &http.Client{Transport: rt}, nil
}

// endpointNeedsOwnedCAClient reports whether ep is the exact shape
// plumbing mishandles: it declares a CA that plumbing will DISCARD.
//
// ONE PRINCIPLE GOVERNS EVERY EXCLUSION BELOW: the owned branch owns only the
// shapes it can reproduce EXACTLY. A shape client-go's transport.Config cannot
// express is delegated to plumbing unchanged — better to leave a shape as
// broken as it is today than to "fix" it into a different, quieter failure.
// Two shapes meet that test: AWS auth (SigV4 unexported upstream) and
// basic-auth-with-no-username. They are instances of the same rule, not two
// unrelated carve-outs.
//
// Every conjunct is a deliberate exclusion, not an accident:
//
//   - ep == nil                 → nothing to do; delegate.
//   - !HasCA()                  → no CA to install; plumbing's system-root
//     client is already correct.
//   - HasCertAuth()             → plumbing DOES take its CA branch
//     (transport.go:37 falls through), so it already
//     works. This is the entire live
//     `<user>-clientconfig` population — all 7
//     Secrets on 057 are cert-auth — and it MUST
//     stay byte-identical.
//   - Insecure                  → the caller asked to skip verification;
//     installing a root pool would silently
//     override an explicit operator choice.
//   - HasAwsAuth()              → the AWS SigV4 roundtripper is UNEXPORTED
//     in plumbing and has no client-go
//     equivalent. Delegating leaves an AWS+CA
//     token-less endpoint exactly as broken as it
//     is today — status quo, no regression — and
//     is strictly better than hand-rolling SigV4
//     here. Out of scope for #229.
//   - HasBasicAuth() &&          → THE MIRROR IMAGE of the basic-auth gating
//     Username == ""               above, and the reason it must be an
//     EXCLUSION rather than a gate. plumbing keys
//     basic auth off PASSWORD, so a
//     password-set/username-EMPTY endpoint is basic
//     auth to plumbing and it sends
//     `Authorization: Basic base64(":password")`.
//     client-go keys off USERNAME, so the same
//     Config yields NO basic wrapper — and with no
//     token there is no bearer wrapper either, so
//     the request would go out with NO Authorization
//     header AT ALL.
//
// A SILENT CREDENTIAL DROP IS WORSE THAN THE LOUD FAILURE THIS FILE REMOVES: a
// build error is immediate and traceable, whereas a well-formed unauthenticated
// request is rejected server-side and sends the next engineer chasing a
// permissions problem that does not exist. Choosing the other library's
// predicate does not help — the shape is simply NOT EXPRESSIBLE in
// transport.Config, because the two disagree on which field defines it. So it
// is delegated, exactly as AWS is, and keeps the behaviour it has today.
func endpointNeedsOwnedCAClient(ep *endpoints.Endpoint) bool {
	if ep == nil {
		return false
	}
	return ep.HasCA() && !ep.HasCertAuth() && !ep.Insecure && !ep.HasAwsAuth() &&
		!(ep.HasBasicAuth() && ep.Username == "")
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
