package credhttp

// do.go — #726: plumbing's request.Do for a dial that presents a credential
// snowplow holds on the caller's behalf (the /call raw passthrough's
// `<user>-clientconfig`).
//
// request.Do builds its *http.Client internally (HTTPClientForEndpoint) and
// offers no seam to refuse a redirect, so a token-auth clientconfig's bearer
// (re-added by plumbing's bearerAuthRoundTripper on EVERY hop) would follow a
// 3xx to whatever host the upstream named, and the redirected body would be
// returned as the answer. Do is request.Do transcribed (plumbing v1.15.1
// http/request/request.go) with exactly two differences:
//
//  1. the client is the endpoint client through Client(): it never follows a
//     redirect (CheckRedirect) and no redirect hop reaches the network
//     (RefuseRedirectHops);
//  2. the refused 3xx is surfaced as a Failure envelope carrying the
//     redirect's own status code and the fixed RedirectRefusedError text —
//     never the redirect's body, which can carry the target URL.
//
// Everything else (headers, the AWS-signature branch, the payload, the retry
// client, the non-2xx envelope, the JSON content-type check, the
// ResponseHandler contract) is unchanged, so a caller swapping request.Do for
// Do sees the same envelopes on every non-redirect path.

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
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/http/util"
	"github.com/krateo-platformops/plumbing/ptr"
)

// maxUnstructuredResponseTextBytes mirrors request.Do's bound on a non-2xx
// body it echoes.
const maxUnstructuredResponseTextBytes = 2048

// Do is request.Do with a client that never follows a redirect (see do.go).
func Do(ctx context.Context, opts request.RequestOptions) *response.Status {
	uri := strings.TrimSuffix(opts.Endpoint.ServerURL, "/")
	if len(opts.Path) > 0 {
		uri = fmt.Sprintf("%s/%s", uri, strings.TrimPrefix(opts.Path, "/"))
	}

	u, err := url.Parse(uri)
	if err != nil {
		return response.New(http.StatusInternalServerError, err)
	}

	verb := ptr.Deref(opts.Verb, http.MethodGet)

	var body io.Reader
	if s := ptr.Deref(opts.Payload, ""); len(s) > 0 {
		body = strings.NewReader(s)
	}

	call, err := http.NewRequestWithContext(ctx, verb, u.String(), body)
	if err != nil {
		return response.New(http.StatusInternalServerError, err)
	}
	// Additional headers for AWS Signature 4 algorithm.
	if opts.Endpoint.HasAwsAuth() {
		headers, _, _, _, _, _ := request.ComputeAwsHeaders(opts.Endpoint, &opts.RequestInfo)
		opts.Headers = append(opts.Headers, headers...)
		opts.Headers = append(opts.Headers, xcontext.LabelKrateoTraceId+":"+xcontext.TraceId(ctx, true))
		// Set all headers to lower case for AWS signature.
		for i := range opts.Headers {
			hParts := strings.Split(opts.Headers[i], ":")
			opts.Headers[i] = strings.ToLower(strings.Trim(hParts[0], " ")) + ":" + strings.Trim(hParts[1], " ")
		}
		sort.Strings(opts.Headers)
	} else {
		call.Header.Set(xcontext.LabelKrateoTraceId, xcontext.TraceId(ctx, true))
	}

	for _, el := range opts.Headers {
		idx := strings.Index(el, ":")
		if idx <= 0 {
			continue
		}
		call.Header.Set(el[:idx], strings.TrimSpace(el[idx+1:]))
	}

	cli, err := request.HTTPClientForEndpoint(opts.Endpoint, &opts.RequestInfo)
	if err != nil {
		return response.New(http.StatusInternalServerError,
			fmt.Errorf("unable to create HTTP Client for endpoint: %w", err))
	}

	respo, err := util.NewRetryClient(Client(cli)).Do(call)
	if err != nil {
		return response.New(http.StatusInternalServerError, err)
	}
	defer respo.Body.Close()

	// #726 — the refused redirect itself. As #729 ruled for the resolver path,
	// a 3xx is not an answer a client can act on (and carries no Location
	// here): report a Failure with 502 Bad Gateway; the fixed message names the
	// redirect's own code and never its target.
	if respo.StatusCode >= 300 && respo.StatusCode < 400 {
		st := response.New(http.StatusBadGateway, &RedirectRefusedError{StatusCode: respo.StatusCode})
		st.Status = response.StatusFailure
		return st
	}

	if respo.StatusCode < 200 || respo.StatusCode >= 300 {
		dat, err := io.ReadAll(io.LimitReader(respo.Body, maxUnstructuredResponseTextBytes))
		if err != nil {
			return response.New(http.StatusInternalServerError, err)
		}
		res := &response.Status{}
		if err := json.Unmarshal(dat, res); err != nil {
			return response.New(respo.StatusCode, fmt.Errorf("%s", string(dat)))
		}
		return res
	}

	if ct := respo.Header.Get("Content-Type"); !strings.Contains(ct, "json") {
		return response.New(http.StatusNotAcceptable, fmt.Errorf("content type %q is not allowed", ct))
	}

	if opts.ResponseHandler != nil {
		if err := opts.ResponseHandler(respo.Body); err != nil {
			return response.New(http.StatusInternalServerError, err)
		}
		return response.New(http.StatusOK, nil)
	}

	return response.New(http.StatusNoContent, nil)
}
