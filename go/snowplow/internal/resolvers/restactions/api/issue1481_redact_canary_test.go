package api

// issue1481_redact_canary_test.go — #1481 (#1112 D4 kept): a RESTAction stage's
// outbound dial to a named endpoint never puts the endpoint's credential, the
// stage path's query or a body into snowplow's logs, at any level, on every
// dial path a stage takes: the owned client-go chain (a bearer token), the
// plumbing-built chain (a password-only Secret), each with the endpoint
// Secret's debug wire dump, and the owned chain with the resolver's verbose
// wire dump (RESOLVER_VERBOSE_WIRE_DUMP=true + a verbose resolve); and for
// every way the dial ends: a 2xx, a refused redirect, a 5xx, a refused
// connection.
//
// It also pins the #1481 decision on WithoutPlumbingWireDump: plumbing's debug
// round tripper (v1.84.0: "http request" / "http request: failed") is never
// installed, so its line is never in the logs, and snowplow's own dump
// ("request details") is.
//
// The sink is the resolve's logger and slog's default, at DEBUG
// (x1106ResolveWith). An upstream's own non-2xx body is recorded and logged by
// design (#709), so the 5xx fixture carries no canary.

import (
	"encoding/base64"
	"io"
	"net/http"
	"strings"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

const x1481QueryCanary = "QUERY-canary-1481"

func x1481Server(t *testing.T, outcome string) *x1106Srv {
	t.Helper()
	ep := newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch outcome {
		case "2xx":
			w.Header().Set("Set-Cookie", "sid=RESPCOOKIE-canary")
			x1106OK(w, r) // echoes x1106Body
		case "redirect":
			// Another origin: never followed (#1106), reported 502 (#729).
			w.Header().Set("Location", x1481OtherOrigin(r)+"/elsewhere?sig="+x1481QueryCanary)
			w.WriteHeader(http.StatusFound)
		case "5xx":
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "upstream failed")
		}
	})
	if outcome == "refused" {
		ep.srv.Close() // nothing listens: the connection is refused
	}
	return ep
}

// x1481Stage is a write and a read carrying every request canary: the query
// in the path, the body marker in the payload, a cookie and an API key header.
func x1481Stages() []*templates.API {
	w := x1106Write("put", http.MethodPut, `${ "/zd/tickets/" + (.ticketId|tostring) + "?apikey=`+x1481QueryCanary+`" }`)
	w.Headers = append(x1106DumpStage(), "Content-Type: application/json")
	w.ContinueOnError = ptrTrue()
	r := x1106Stage("get", http.MethodGet, `${ "/zd/tickets/" + (.ticketId|tostring) + "?apikey=`+x1481QueryCanary+`" }`)
	r.Headers = x1106DumpStage()
	r.ContinueOnError = ptrTrue()
	return []*templates.API{w, r}
}

func ptrTrue() *bool { b := true; return &b }

// x1481OtherOrigin is r's server under the hostname "localhost": another host
// than 127.0.0.1, so a redirect there leaves the origin.
func x1481OtherOrigin(r *http.Request) string {
	return "http://" + strings.Replace(r.Host, "127.0.0.1", "localhost", 1)
}

func TestIssue1481_StageDialLogsNoCredentialQueryOrBody(t *testing.T) {
	type chain struct {
		secret  func(url string) map[string]string
		verbose bool
	}
	chains := map[string]chain{
		"owned, secret debug": {secret: func(url string) map[string]string { return x1106Secret(url, "debug", "true") }},
		"plumbing, secret debug": {secret: func(url string) map[string]string {
			return map[string]string{"server-url": url, "password": x1106Token, "debug": "true"}
		}},
		"owned, verbose wire dump": {secret: func(url string) map[string]string { return x1106Secret(url) }, verbose: true},
	}
	for cname, c := range chains {
		for _, outcome := range []string{"2xx", "redirect", "5xx", "refused"} {
			t.Run(cname+"/"+outcome, func(t *testing.T) {
				if c.verbose {
					t.Setenv(envVerboseWireDump, "true")
				}
				ep := x1481Server(t, outcome)
				x1106Env(t, "", map[string]map[string]string{x1106Ref: c.secret(ep.srv.URL)})
				o := x1106ResolveWith(t, x1106Opts{verbose: c.verbose}, x1106Extras, x1481Stages()...)
				if outcome != "refused" && len(ep.all()) == 0 {
					t.Fatal("PRE: the endpoint was not dialled")
				}
				if !strings.Contains(o.logs, `"msg":"request details"`) {
					t.Fatalf("PRE: snowplow's wire dump must be on; logs:\n%.3000s", o.logs)
				}
				secrets := []string{x1106Token, x1481QueryCanary, x1106Body, "REQCOOKIE-canary", "RESPCOOKIE-canary", "APIKEY-canary"}
				// The credential exactly as it went out (a basic one is base64).
				for _, h := range ep.all() {
					if v := strings.TrimPrefix(strings.TrimPrefix(h.Auth, "Basic "), "Bearer "); v != "" {
						if dec, _ := base64.StdEncoding.DecodeString(v); v == x1106Token || strings.Contains(string(dec), x1106Token) {
							secrets = append(secrets, v)
							continue
						}
						t.Fatalf("PRE: fixture: the endpoint received a credential without the canary: %q", h.Auth)
					}
				}
				if outcome != "refused" && len(secrets) == 6 {
					t.Fatal("PRE: no request carried the endpoint's credential")
				}
				for _, s := range secrets {
					if i := strings.Index(o.logs, s); i >= 0 {
						lo := strings.LastIndex(o.logs[:i], "\n") + 1
						line := o.logs[lo:]
						if j := strings.Index(line, "\n"); j >= 0 {
							line = line[:j]
						}
						t.Errorf("RED: %q reached the logs, line (masked): %.400s", s, strings.ReplaceAll(line, s, "<CANARY>"))
					}
				}
				if strings.Contains(o.logs, `"msg":"http request"`) || strings.Contains(o.logs, `"msg":"http request: failed"`) {
					t.Errorf("RED: plumbing's debug round tripper is installed on a stage client (WithoutPlumbingWireDump no longer holds)")
				}
			})
		}
	}
}
