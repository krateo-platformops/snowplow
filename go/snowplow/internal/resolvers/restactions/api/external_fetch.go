package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"

	xcontext "github.com/krateo-platformops/plumbing/context"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/http/util"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/tracing"
	"sigs.k8s.io/yaml"
)

// maxUnstructuredResponseTextBytes mirrors plumbing v0.9.3
// http/request/request.go:19 — the 2048-byte cap on the non-2xx body
// snippet folded into the error envelope. Kept verbatim so the
// truncation (falsifier (j)) is byte-identical.
const maxUnstructuredResponseTextBytes = 2048

// httpFetchAllowingNonJSON is the snowplow-owned external-GET fetch for
// the EXTERNAL api-step branch (resolve.go:822). It is a FAITHFUL
// transcription of plumbing v0.9.3 http/request/request.go:35-130
// (`request.Do`) MINUS the 406 JSON content-type gate at :118-120, with
// two deliberate deltas:
//
//  1. It does NOT invoke opts.ResponseHandler. Instead it reads the 2xx
//     body, detects JSON-vs-YAML, converts YAML→JSON (a no-op for JSON,
//     since sigs.k8s.io/yaml.YAMLToJSON round-trips valid JSON
//     losslessly), and RETURNS the JSON bytes. The caller feeds those
//     bytes into the EXISTING unchanged handler chain (feedBytes →
//     jsonHandlerBytes → jsonHandlerCore), so the JSON-passthrough,
//     internal-rest-config, and in-process-nested-call paths are
//     untouched by construction (AC3/AC4).
//
//  2. The non-2xx body is shaped into a *response.Status envelope
//     BYTE-IDENTICALLY to request.go:102-116 (LimitReader(2048) →
//     json.Unmarshal into Status, else raw-string response.New) and
//     returned so the caller's existing StatusFailure path
//     (resolve.go:823-856 → recordItemError) honours
//     ContinueOnError/ErrorKey identically (AC5).
//
// The TLS/CA/client-cert/proxy/timeout/auth apparatus
// (HTTPClientForEndpoint, the bearer/basic/AWS roundtrippers,
// util.NewRetryClient, ComputeAwsHeaders) is REUSED verbatim from
// plumbing — the only hand-written part is the request assembly and the
// 2xx body conversion (AC7; falsifier (k) asserts the auth header on the
// wire).
//
// Return contract:
//   - On transport / request-build failure: (StatusFailure envelope,
//     nil, "", goErr) — the goErr mirrors request.go's
//     response.New(500, err) cases; the caller treats a non-nil err as a
//     hard fault exactly as before.
//   - On a non-2xx response: (StatusFailure envelope, nil, contentType,
//     nil) — surfaced through recordItemError like an httpcall.Do
//     failure; no go error so the resolve is not truncated.
//   - On a 2xx response that does NOT convert (malformed YAML / HTML
//     garbage): (StatusFailure envelope, nil, contentType, nil) — same
//     recordItemError path, no panic, no creds leak (the envelope
//     message is the conversion error, never the endpoint auth) (AC6;
//     falsifiers (e),(g)).
//   - On a 2xx response that converts: (Success envelope, jsonBytes,
//     contentType, nil).
func httpFetchAllowingNonJSON(ctx context.Context, opts httpcall.RequestOptions) (*response.Status, []byte, string, error) {
	// --- request assembly (transcribed from request.go:36-95) ---
	uri := strings.TrimSuffix(opts.Endpoint.ServerURL, "/")
	if len(opts.Path) > 0 {
		uri = fmt.Sprintf("%s/%s", uri, strings.TrimPrefix(opts.Path, "/"))
	}

	u, err := url.Parse(uri)
	if err != nil {
		return response.New(http.StatusInternalServerError, err), nil, "", err
	}

	verb := ptr.Deref(opts.Verb, http.MethodGet)

	var body io.Reader
	if s := ptr.Deref(opts.Payload, ""); len(s) > 0 {
		body = strings.NewReader(s)
	}

	call, err := http.NewRequestWithContext(ctx, verb, u.String(), body)
	if err != nil {
		return response.New(http.StatusInternalServerError, err), nil, "", err
	}

	// Additional headers for AWS Signature 4 algorithm — transcribed
	// verbatim from request.go:57-72 so AWS-signed external GETs keep
	// working through the owned fetch (AC7).
	if opts.Endpoint.HasAwsAuth() {
		headers, _, _, _, _, _ := httpcall.ComputeAwsHeaders(opts.Endpoint, &opts.RequestInfo)
		opts.Headers = append(opts.Headers, headers...)
		opts.Headers = append(opts.Headers, xcontext.LabelKrateoTraceId+":"+xcontext.TraceId(ctx, true))
		// D19a: the audit session correlation id is NO LONGER forwarded as a
		// bespoke X-Krateo-Correlation-Id header. It rides W3C baggage
		// (session.id) and is serialized into the outbound `baggage` header
		// by the global propagation.Baggage propagator on the otelhttp
		// transport, so a downstream adapter links its own AuditEvents to the
		// originating action via trace-context, not an ad-hoc header.
		// Set all headers to lower case for AWS signature
		for i := range opts.Headers {
			hParts := strings.Split(opts.Headers[i], ":")
			opts.Headers[i] = strings.ToLower(strings.Trim(hParts[0], " ")) + ":" + strings.Trim(hParts[1], " ")
		}
		sort.Strings(opts.Headers)
	} else {
		call.Header.Set(xcontext.LabelKrateoTraceId, xcontext.TraceId(ctx, true))
		// D19a: audit session id rides W3C baggage (session.id) and is
		// injected as the `baggage` header by the propagator on the otelhttp
		// transport — no bespoke X-Krateo-Correlation-Id header here.
	}

	if len(opts.Headers) > 0 {
		for _, el := range opts.Headers {
			idx := strings.Index(el, ":")
			if idx <= 0 {
				continue
			}
			key := el[:idx]
			val := strings.TrimSpace(el[idx+1:])
			call.Header.Set(key, val)
		}
	}

	// Builds the HTTP client (TLS/CA/client-certs/proxy/timeouts +
	// bearer/basic/AWS auth roundtrippers) from the Endpoint.
	//
	// snowplow#229 / audit A1: this was `httpcall.HTTPClientForEndpoint`
	// verbatim, which DROPS the CA bundle of a TOKEN-AUTH endpoint
	// (plumbing transport.go:37 returns before the CA install when
	// !HasCertAuth()). That is snowplow's own SA endpoint — the default
	// for every api-step with no `endpointRef` — so every non-GET such
	// step failed TLS with x509-unknown-authority while GETs escaped by
	// being intercepted upstream. httpClientForEndpoint covers exactly
	// that shape and DELEGATES to plumbing verbatim for every other one,
	// so the live cert-auth population is byte-identical. See
	// endpoints_tls.go for the full trace.
	cli, err := httpClientForEndpoint(opts.Endpoint, &opts.RequestInfo)
	if err != nil {
		werr := fmt.Errorf("unable to create HTTP Client for endpoint: %w", err)
		return response.New(http.StatusInternalServerError, werr), nil, "", werr
	}

	// OTel outbound instrumentation (ADDITIVE + default-OFF). When tracing
	// is enabled (OTEL_TRACING_ENABLED, defaulting to OTEL_ENABLED), wrap
	// the client transport so this
	// external GET to authn / api-steps emits a client span and injects a
	// W3C `traceparent` (the request already carries ctx via
	// NewRequestWithContext above, so the active server span is the
	// parent). When tracing is disabled WrapTransport returns the transport
	// unchanged, so this branch is a no-op and the outbound path stays
	// byte-identical. The existing shortid X-Krateo-TraceId header set
	// above is UNTOUCHED — both correlation ids ride the same request.
	cli.Transport = tracing.WrapTransport(cli.Transport)

	// REUSED verbatim: retry wrapper.
	retryCli := util.NewRetryClient(cli)

	respo, err := retryCli.Do(call)
	if err != nil {
		return response.New(http.StatusInternalServerError, err), nil, "", err
	}
	defer respo.Body.Close()

	ct := respo.Header.Get("Content-Type")

	// --- non-2xx shaping (transcribed BYTE-IDENTICALLY from
	// request.go:98-116) --- (AC5; falsifier (j)).
	statusOK := respo.StatusCode >= 200 && respo.StatusCode < 300
	if !statusOK {
		dat, rerr := io.ReadAll(io.LimitReader(respo.Body, maxUnstructuredResponseTextBytes))
		if rerr != nil {
			return response.New(http.StatusInternalServerError, rerr), nil, ct, rerr
		}

		res := &response.Status{}
		if jerr := json.Unmarshal(dat, res); jerr != nil {
			res = response.New(respo.StatusCode, fmt.Errorf("%s", string(dat)))
			return res, nil, ct, nil
		}
		return res, nil, ct, nil
	}

	// --- 2xx body: detect + convert (the new, snowplow-owned step;
	// request.go:118-120 406 gate DELETED) ---
	dat, rerr := io.ReadAll(respo.Body)
	if rerr != nil {
		return response.New(http.StatusInternalServerError, rerr), nil, ct, rerr
	}

	jsonBytes, cerr := toJSONBytes(ct, dat)
	if cerr != nil {
		// Conversion failed (malformed YAML / HTML garbage). Surface as a
		// StatusFailure envelope so the caller's recordItemError honours
		// ContinueOnError/ErrorKey — NO go error (do not truncate the
		// resolve), NO panic. The envelope message is the conversion
		// error only; the endpoint auth never appears in it (AC6;
		// falsifiers (e),(g)).
		return response.New(http.StatusUnprocessableEntity, cerr), nil, ct, nil
	}

	return response.New(http.StatusOK, nil), jsonBytes, ct, nil
}

// toJSONBytes returns JSON bytes for a 2xx external body, converting from
// YAML when the Content-Type sniff (convert.go:50 pattern) says YAML, OR
// as a body-shape fallback when the sniff is inconclusive. YAMLToJSON
// round-trips valid JSON losslessly, so the JSON fast-path is safe and
// value-preserving (AC3; falsifier (d)).
//
// Detection order:
//  1. Explicit YAML content-type → YAMLToJSON.
//  2. Otherwise try the body as JSON first (the dominant, byte-identical
//     path). If it parses, return it unchanged.
//  3. JSON parse failed → last-resort YAMLToJSON (covers YAML served
//     under text/plain, the Helm index.yaml case (AC1; falsifier (a))).
//     If that also fails, return the error (malformed / non-structured
//     body → falsifiers (e),(g)).
func toJSONBytes(contentType string, dat []byte) ([]byte, error) {
	if isYAMLContentType(contentType) {
		out, err := yaml.YAMLToJSON(dat)
		if err != nil {
			return nil, fmt.Errorf("failed to convert YAML response to JSON: %w", err)
		}
		// Architect note A — apply the structured-shape guard SYMMETRICALLY
		// with the last-resort path below. A bare top-level YAML scalar
		// (e.g. a body that is just `"v1.2.3"`) round-trips to a bare JSON
		// scalar, which is NOT a shape a stage filter consumes. Rejecting it
		// here too keeps the convert contract uniform regardless of whether
		// YAML was detected by content-type or by body-shape fallback — no
		// asymmetry where an explicit text/yaml scalar slips through but the
		// same scalar under text/plain is rejected. Strictly additive: such
		// a body was 406'd pre-ship, so "still fails, cleaner error" is no
		// regression.
		if !isJSONObjectOrArray(out) {
			return nil, fmt.Errorf("YAML response converted to a non-structured (scalar) value (content-type %q)", contentType)
		}
		return out, nil
	}

	// JSON fast-path: valid JSON passes through byte-identical in VALUE
	// (json.Valid avoids a full unmarshal; the bytes are returned as-is).
	if json.Valid(dat) {
		return dat, nil
	}

	// Inconclusive content-type and not valid JSON: last-resort YAML.
	// (Helm repos commonly serve index.yaml as text/plain.)
	out, err := yaml.YAMLToJSON(dat)
	if err != nil {
		return nil, fmt.Errorf("response body is neither JSON nor convertible YAML: %w", err)
	}
	// YAMLToJSON happily turns a bare HTML/garbage scalar into a JSON
	// string ("<!DOCTYPE...>"), which is NOT a usable structured stage
	// result. Require the converted result to be a JSON object or array —
	// the only shapes a stage filter consumes — so HTML garbage surfaces
	// as a clean conversion failure (AC6; falsifier (g)) rather than a
	// silently-wrapped string.
	if !isJSONObjectOrArray(out) {
		return nil, fmt.Errorf("response body is neither JSON nor structured YAML (content-type %q)", contentType)
	}
	return out, nil
}

// isYAMLContentType mirrors handlers/convert.go:50 — the explicit YAML
// media types the convert endpoint recognises.
func isYAMLContentType(contentType string) bool {
	return strings.Contains(contentType, "application/x-yaml") ||
		strings.Contains(contentType, "text/yaml")
}

// isJSONObjectOrArray reports whether b's first non-whitespace byte is
// '{' or '[' — i.e. a structured JSON value a stage filter can consume.
func isJSONObjectOrArray(b []byte) bool {
	s := strings.TrimLeft(string(b), " \t\r\n")
	return strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[")
}
