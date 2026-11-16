package credhttp

// redact_canary_1481_test.go — #1481 (#1112 D4 kept): the /call passthrough's
// outbound dials, credhttp.Do (an endpoint, its debug wire dump on) and
// credhttp.DoAs (a rest.Config), never put the request's credential, its query
// or a body into snowplow's logs, at any level, whatever the dial ends in: a
// 2xx, a refused redirect, a 5xx, a refused connection.
//
// THE DECISION IT PINS. plumbing v1.84.0's debug round tripper logs metadata
// only ("http request" / "http request: failed", plumbing#160), but it sits
// UNDER plumbing's credential round trippers, so every request it sees carries
// the Authorization header: whether a credential reaches a log would rest on
// code snowplow does not own. Snowplow keeps WithoutPlumbingWireDump, so that
// round tripper is never installed in a client snowplow builds, and an endpoint
// that asks for a dump gets WireDump, snowplow's own metadata-only dump. So the
// logs carry snowplow's "request details" and never plumbing's "http request".
//
// Canaries: the bearer token and the basic password (and its base64), a query
// value, the request body, and the 2xx response body and its Set-Cookie. The
// log sink is both the request's logger and slog's default, at DEBUG. What a
// /call logs of a failure is the envelope's message (handlers/call.go), so that
// is checked too, for every outcome but a non-2xx answer: an upstream's own
// error body is echoed there by design (nonOKEnvelope), so none of these
// fixtures puts a canary in one.

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"k8s.io/client-go/rest"
)

const (
	x1481Token    = "AUTHTOKEN-canary-1481"
	x1481Password = "BASICPW-canary-1481"
	x1481Query    = "QUERY-canary-1481"
	x1481ReqBody  = "REQBODY-canary-1481"
	x1481RespBody = "RESPBODY-canary-1481"
	x1481Cookie   = "RESPCOOKIE-canary-1481"
)

type x1481Sink struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *x1481Sink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *x1481Sink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// x1481Logs is a DEBUG logger on the request's context and as slog's default,
// both into one sink.
func x1481Logs(t *testing.T) (context.Context, *x1481Sink) {
	t.Helper()
	sink := &x1481Sink{}
	lg := slog.New(slog.NewJSONHandler(sink, &slog.HandlerOptions{Level: slog.LevelDebug}))
	prev := slog.Default()
	slog.SetDefault(lg)
	t.Cleanup(func() { slog.SetDefault(prev) })
	return xcontext.BuildContext(context.Background(), xcontext.WithLogger(lg)), sink
}

// x1481Upstream answers every request the way outcome says.
func x1481Upstream(t *testing.T, outcome string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		switch outcome {
		case "2xx":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Set-Cookie", "sid="+x1481Cookie)
			_, _ = io.WriteString(w, `{"kind":"ConfigMap","data":{"k":"`+x1481RespBody+`"}}`)
		case "redirect":
			w.Header().Set("Location", "/elsewhere?sig="+x1481Query)
			w.WriteHeader(http.StatusFound)
		case "5xx":
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "upstream failed")
		}
	}))
	url := srv.URL
	if outcome == "refused" {
		srv.Close() // nothing listens: the connection is refused
	} else {
		t.Cleanup(srv.Close)
	}
	return url
}

func x1481Canaries() []string {
	return []string{x1481Token, x1481Password, base64.StdEncoding.EncodeToString([]byte("u:" + x1481Password)),
		x1481Query, x1481ReqBody, x1481RespBody, x1481Cookie}
}

// x1481RequireClean fails on any canary in logs or in the message a /call logs.
func x1481RequireClean(t *testing.T, logs, message string) {
	t.Helper()
	for _, c := range x1481Canaries() {
		if strings.Contains(logs, c) {
			t.Errorf("RED: %q reached the logs:\n%s", c, strings.ReplaceAll(logs, c, "<CANARY>"))
		}
		if strings.Contains(message, c) {
			t.Errorf("RED: %q is in the message a /call logs: %s", c, strings.ReplaceAll(message, c, "<CANARY>"))
		}
	}
	if strings.Contains(logs, `"msg":"http request"`) || strings.Contains(logs, `"msg":"http request: failed"`) {
		t.Errorf("RED: plumbing's debug round tripper is installed (its line is in the logs): WithoutPlumbingWireDump no longer holds:\n%s", logs)
	}
}

var x1481Outcomes = []string{"2xx", "redirect", "5xx", "refused"}

func TestIssue1481_DoLogsNoCredentialQueryOrBody(t *testing.T) {
	eps := map[string]func(url string) *endpoints.Endpoint{
		"bearer": func(url string) *endpoints.Endpoint {
			return &endpoints.Endpoint{ServerURL: url, Token: x1481Token, Debug: true}
		},
		"basic": func(url string) *endpoints.Endpoint {
			return &endpoints.Endpoint{ServerURL: url, Username: "u", Password: x1481Password, Debug: true}
		},
	}
	for ename, ep := range eps {
		for _, outcome := range x1481Outcomes {
			for _, verb := range []string{http.MethodGet, http.MethodPut} {
				t.Run(ename+"/"+outcome+"/"+verb, func(t *testing.T) {
					t.Setenv("CLIENT_MAX_RETRIES", "0")
					ctx, sink := x1481Logs(t)
					opts := request.RequestOptions{
						RequestInfo: request.RequestInfo{
							Path: "/api/v1/namespaces/default/configmaps/cm-x?token=" + x1481Query,
							Verb: ptr.To(verb), Headers: []string{"Content-Type: application/json"},
						},
						Endpoint:        ep(x1481Upstream(t, outcome)),
						ResponseHandler: func(rc io.ReadCloser) error { _, err := io.Copy(io.Discard, rc); return err },
					}
					if verb == http.MethodPut {
						opts.Payload = ptr.To(`{"data":{"k":"` + x1481ReqBody + `"}}`)
					}
					st := Do(ctx, opts)
					logs := sink.String()
					if !strings.Contains(logs, `"msg":"request details"`) {
						t.Fatalf("PRE: the debug wire dump must be on (snowplow's WireDump); logs:\n%s", logs)
					}
					if (outcome == "2xx") != (st.Code < 300) {
						t.Fatalf("PRE: outcome %s answered %d %s", outcome, st.Code, st.Message)
					}
					msg := st.Message
					if outcome == "5xx" {
						msg = "" // the upstream's own error body, echoed by design (no canary in it)
					}
					x1481RequireClean(t, logs, msg)
				})
			}
		}
	}
}

func TestIssue1481_DoAsLogsNoCredentialQueryOrBody(t *testing.T) {
	for _, outcome := range x1481Outcomes {
		for _, verb := range []string{http.MethodGet, http.MethodPut} {
			t.Run(outcome+"/"+verb, func(t *testing.T) {
				t.Setenv("CLIENT_MAX_RETRIES", "0")
				ctx, sink := x1481Logs(t)
				opts := request.RequestOptions{
					RequestInfo: request.RequestInfo{
						Path: "/api/v1/namespaces/default/configmaps/cm-x?token=" + x1481Query,
						Verb: ptr.To(verb), Headers: []string{"Content-Type: application/json"},
					},
					ResponseHandler: func(rc io.ReadCloser) error { _, err := io.Copy(io.Discard, rc); return err },
				}
				if verb == http.MethodPut {
					opts.Payload = ptr.To(`{"data":{"k":"` + x1481ReqBody + `"}}`)
				}
				st := DoAs(ctx, opts, &rest.Config{Host: x1481Upstream(t, outcome), BearerToken: x1481Token})
				if (outcome == "2xx") != (st.Code < 300) {
					t.Fatalf("PRE: outcome %s answered %d %s", outcome, st.Code, st.Message)
				}
				msg := st.Message
				if outcome == "5xx" {
					msg = ""
				}
				x1481RequireClean(t, sink.String(), msg)
			})
		}
	}
}
