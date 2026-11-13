package credhttp

// do.go — #726: plumbing's request.Do for a dial that presents a credential
// snowplow holds on the caller's behalf (the /call raw passthrough's
// `<user>-clientconfig`).
//
// request.Do builds its *http.Client internally (HTTPClientForEndpoint) and
// offers no seam to refuse a redirect, so a token-auth clientconfig's bearer
// (re-added by plumbing's bearerAuthRoundTripper on EVERY hop) would follow a
// 3xx to whatever host the upstream named, and the redirected body would be
// returned as the answer. Do is request.Do transcribed (plumbing v1.28.1
// http/request/request.go) with exactly three differences:
//
//  1. the client is the endpoint client through Client(): it never follows a
//     redirect (CheckRedirect) and no redirect hop reaches the network
//     (RefuseRedirectHops);
//  2. the refused 3xx is surfaced as a Failure envelope carrying the
//     redirect's own status code and the fixed RedirectRefusedError text —
//     never the redirect's body, which can carry the target URL.
//
//  3. (#1088) it sends through Send (send.go): a write is sent once, where
//     request.Do's RetryClient re-sent a PUT or a DELETE.
//
//  4. (#1112 D4) a Debug endpoint's wire dump is WireDump (metadata only),
//     never plumbing's, which logs the Authorization header and the bodies.
//
//  5. (#1131) a refused destination is reported as itself: DestinationStatus.
//
//  6. (#1298) what the authority answered is observed for the audit record
//     (answer.go: Send records the status line; a 2xx body not used is noted
//     here), because the envelope's code is often snowplow's own.
//
// Everything else (headers, the AWS-signature branch, the payload, a read's
// retry client, the non-2xx envelope, the JSON content-type check, the
// ResponseHandler contract) is unchanged, so a caller swapping request.Do for
// Do sees the same envelopes on every non-redirect path.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"k8s.io/client-go/rest"
)

// maxUnstructuredResponseTextBytes mirrors request.Do's bound on a non-2xx
// body it echoes.
const maxUnstructuredResponseTextBytes = 2048

// Do is request.Do with a client that never follows a redirect (see do.go).
func Do(ctx context.Context, opts request.RequestOptions) *response.Status {
	return do(ctx, opts, func() (*http.Client, error) {
		// #1112 D4 — plumbing never installs its wire dump (it logs the
		// Authorization header and the bodies); a Debug endpoint gets WireDump,
		// which logs metadata only (wiredump.go).
		cli, err := request.HTTPClientForEndpoint(WithoutPlumbingWireDump(opts.Endpoint), &opts.RequestInfo)
		if err != nil {
			return nil, err
		}
		if opts.Endpoint != nil && opts.Endpoint.Debug {
			cli.Transport = WireDump(cli.Transport)
		}
		return cli, nil
	})
}

// DoAs is Do sent with rc's credential instead of opts.Endpoint's (#1334):
// the request goes to rc.Host, through the client rest.HTTPClientFor builds
// for rc (its TLS, its credential and its Impersonate-* headers), with Do's
// redirect refusal, send policy and envelopes. opts.Endpoint is replaced by an
// endpoint naming only rc.Host, so nothing of the endpoint it carried is sent.
//
// It exists for a caller whose only credential is an impersonating config (the
// public identity): there is no clientconfig endpoint to give Do.
func DoAs(ctx context.Context, opts request.RequestOptions, rc *rest.Config) *response.Status {
	if rc == nil || rc.Host == "" {
		return response.New(http.StatusInternalServerError, fmt.Errorf("unable to create HTTP Client: no rest config"))
	}
	opts.Endpoint = &endpoints.Endpoint{ServerURL: rc.Host}
	return do(ctx, opts, func() (*http.Client, error) {
		return rest.HTTPClientFor(rc)
	})
}

func do(ctx context.Context, opts request.RequestOptions, client func() (*http.Client, error)) *response.Status {
	u, err := url.Parse(RequestURL(opts.Endpoint.ServerURL, opts.Path))
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

	cli, err := client()
	if err != nil {
		return response.New(http.StatusInternalServerError,
			fmt.Errorf("unable to create HTTP Client for endpoint: %w", err))
	}

	// #1088 — the endpoint send policy (send.go): a write is sent once, never
	// re-sent once its bytes may have reached the apiserver (plumbing's
	// RetryClient re-sent a PUT or DELETE); a read keeps the RetryClient, with
	// every retry counted and logged with its cause.
	respo, err := Send(SendSiteCall, Client(cli), call)
	if err != nil {
		if st, ok := BudgetStatus(err); ok {
			return st // #1115 — 504 Timeout
		}
		if st, ok := DestinationStatus(err); ok {
			return st // #1131 — 502 DestinationRefused
		}
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
		return nonOKEnvelope(respo.StatusCode, dat)
	}

	// #1298 — the write was accepted (Send recorded its 2xx on the Answer); a
	// body snowplow does not use is noted, never reported as the answer.
	if ct := respo.Header.Get("Content-Type"); !strings.Contains(ct, "json") {
		AnswerFrom(ctx).Note(audit.NoteBodyNotJSON)
		return response.New(http.StatusNotAcceptable, fmt.Errorf("content type %q is not allowed", ct))
	}

	if opts.ResponseHandler != nil {
		if err := opts.ResponseHandler(respo.Body); err != nil {
			AnswerFrom(ctx).Note(audit.NoteBodyUnread)
			return response.New(http.StatusInternalServerError, err)
		}
		return response.New(http.StatusOK, nil)
	}

	return response.New(http.StatusNoContent, nil)
}

// RequestURL is the URL a call to path on the endpoint at serverURL is sent to,
// as request.Do builds it (plumbing v1.28.1 http/request/request.go): the
// server URL without its trailing "/", then exactly one "/", then path without
// its leading "/". Both HTTP dials build their URL here, this one and the
// resolver's own fetch (restactions/api httpFetchConverting), and so does the
// plan's check of what the apiserver will read (#1137), so the check cannot
// build that URL another way. The resolver's internal-rest-config branch does
// not send a path: client-go joins the parsed coordinates with path.Join, and
// the plan's check refuses every path that cleaning would change.
//
// A path that does not start with "/" is sent as if it did, and the server
// percent-decodes what it receives: the request a server reads is the decoded
// path of url.Parse(RequestURL(...)).
func RequestURL(serverURL, path string) string {
	uri := strings.TrimSuffix(serverURL, "/")
	if len(path) > 0 {
		uri = fmt.Sprintf("%s/%s", uri, strings.TrimPrefix(path, "/"))
	}
	return uri
}

// BudgetStatusReason is the reason of the envelope a page-budget exhaustion is
// reported with: the apiserver's own reason for 504 (metav1.StatusReasonTimeout).
const BudgetStatusReason = "Timeout"

// BudgetStatus shapes an exchange its page budget ended (#1115) into the
// envelope a stage or a /call passthrough reports: 504 Gateway Timeout, a
// Failure, reason Timeout, the identity-free ExhaustedError text. ok=false for
// any other error. (plumbing's response.New(504, …) would read as a Success:
// 504 is not in its list.)
func BudgetStatus(err error) (*response.Status, bool) {
	var e *pagebudget.ExhaustedError
	if !errors.As(err, &e) {
		return nil, false
	}
	st := response.New(http.StatusGatewayTimeout, e)
	st.Status = response.StatusFailure
	st.Reason = response.StatusReason(BudgetStatusReason)
	return st, true
}

// WriteBudgetError writes the 504 Timeout envelope (BudgetStatus) for an
// exchange the request's page budget ended and reports true; false (nothing
// written) for any other error. For handlers that answer an error directly.
func WriteBudgetError(w http.ResponseWriter, err error) bool {
	st, ok := BudgetStatus(err)
	if !ok {
		return false
	}
	_ = response.Encode(w, st)
	return true
}

// nonOKEnvelope shapes a non-2xx answer into the envelope Do returns. A non-2xx
// answer is ALWAYS a Failure carrying the status line's code (#1088). Before
// #1088 two shapes read as a success: a JSON body that is not a Status (an
// object, which decodes into a Status with no "Failure" and no code) and a code
// plumbing's response.New does not list (502, 504, ...). They hid behind the
// RetryClient for PUT and DELETE, which turned a 5xx into a transport error;
// with a write now sent once (send.go), every write meets this shaping, and an
// audited write that the apiserver refused must never be recorded as a success.
// A Status body keeps its message and reason, as before.
func nonOKEnvelope(code int, dat []byte) *response.Status {
	res := &response.Status{}
	if err := json.Unmarshal(dat, res); err != nil {
		res = response.New(code, fmt.Errorf("%s", string(dat)))
	}
	res.Code = code
	res.Status = response.StatusFailure
	// A body that names no reason gets the one plumbing derives from the code
	// (response.New), as every other non-2xx envelope here does: an object-
	// bodied 500 reads InternalError. A code plumbing does not map (502, 504,
	// 429) keeps no reason: before #1088 those read InternalError only because
	// the RetryClient's give-up error was re-wrapped as a 500, which also hid
	// the real code. Inventing one would claim what the upstream never said.
	if res.Reason == "" {
		res.Reason = response.New(code, nil).Reason
	}
	if res.Kind == "" {
		res.Kind, res.APIVersion = "Status", "v1"
	}
	if res.Message == "" {
		if len(dat) > 0 {
			res.Message = string(dat)
		} else {
			res.Message = fmt.Sprintf("upstream answered %d %s with an empty body", code, http.StatusText(code))
		}
	}
	return res
}
