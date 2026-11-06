package credhttp

// redirect_dump_1138_test.go — the #1120 review's arms, adopted (#1138), and
// the backstop's text. Every arm runs on a fake RoundTripper: no httptest, no
// loopback, so the host comparison never depends on 127.0.0.1 collapsing
// names.
//
//   - the hop table: what Origin and refusesHop call another origin;
//   - Send on a fake transport: a cross-origin hop is never handed to the
//     transport, whatever the verb and the 3xx; a same-origin one is followed;
//   - the transport backstop alone (CheckRedirect replaced), and the
//     credential its error names (#1138);
//   - #1112 D1: a write sent once with its full body;
//   - #1112 D4: no header value, body, userinfo or query value in the wire
//     dump (the URL arm is the one that pins redact.URL there, #1138), and the
//     body handed onward intact.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
)

type fakeHit struct {
	Method, URL, Auth, Key string
	Body                   []byte
}

// fakeTransport answers per URL and records every request it is handed,
// simulating plumbing's bearer round-tripper (re-adds the token every hop).
type fakeTransport struct {
	mu     sync.Mutex
	hits   []fakeHit
	answer func(*http.Request) (*http.Response, error)
}

func (t *fakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer FAKETOKEN") // the credential RT re-adds on every hop
	var b []byte
	if req.Body != nil {
		b, _ = io.ReadAll(req.Body)
		_ = req.Body.Close()
	}
	t.mu.Lock()
	t.hits = append(t.hits, fakeHit{Method: req.Method, URL: req.URL.String(), Auth: req.Header.Get("Authorization"), Key: req.Header.Get("X-Api-Key"), Body: b})
	t.mu.Unlock()
	return t.answer(req)
}

func (t *fakeTransport) all() []fakeHit {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]fakeHit(nil), t.hits...)
}

func fakeResp(req *http.Request, code int, loc string) *http.Response {
	h := http.Header{}
	if loc != "" {
		h.Set("Location", loc)
	}
	return &http.Response{StatusCode: code, Status: http.StatusText(code), Header: h,
		Body: io.NopCloser(strings.NewReader("")), Request: req, ProtoMajor: 1, ProtoMinor: 1}
}

func TestIssue1138_HopTable(t *testing.T) {
	own := Origin(mustURL(t, "https://api.example.com/v1"))
	if own != "https://api.example.com:443" {
		t.Fatalf("own origin = %q", own)
	}
	for _, tc := range []struct {
		u      string
		refuse bool
	}{
		{"https://api.example.com/v2/x", false},
		{"HTTPS://Api.Example.Com:443/x", false},
		{"https://api.example.com:/x", false}, // empty port = default
		{"https://api.example.com.attacker.net/x", true},
		{"https://api.example.com.attacker.net:443/x", true},
		{"https://attacker.net/api.example.com", true},
		{"https://attacker.net/?h=api.example.com", true},
		{"https://xapi.example.com/x", true},
		{"https://example.com/x", true},
		{"https://api.example.co/x", true},
		{"https://api.example.com:8443/x", true},
		{"https://api.example.com:0443/x", true}, // fail-safe: not normalised
		{"https://api.example.com:80/x", true},
		{"http://api.example.com/x", true},
		{"http://api.example.com:443/x", true},
		{"ws://api.example.com:443/x", true},
		{"https://api.example.com@evil.example.net/x", true},
		{"https://user:pw@api.example.com@evil.example.net/x", true},
		{"https://evil.example.net@api.example.com/x", false}, // the host IS api.example.com
		{"https://evil.example.net#@api.example.com/x", true},
		{"https://evil.example.net?@api.example.com/x", true},
		{"https://api.example.com../x", true},
		{"https://[::1]/x", true},
		{"https://127.0.0.1/x", true},
		{"https://api.example.com%2eattacker.net/x", true},
	} {
		u, err := url.Parse(tc.u)
		if err != nil {
			// an unparseable Location is never followed by net/http either
			continue
		}
		if got := refusesHop(own, u); got != tc.refuse {
			t.Errorf("%s: refused=%v want %v (Origin=%q)", tc.u, got, tc.refuse, Origin(u))
		}
	}
	// IPv6 default port
	if Origin(mustURL(t, "https://[::1]/x")) != Origin(mustURL(t, "https://[::1]:443/y")) {
		t.Error("IPv6 default port")
	}
}

// Send, on a fake transport: a cross-origin Location is never handed to the
// transport (zero hits at the other origin, so the credential never reaches
// it), the caller gets the 3xx itself; a same-origin hop is followed.
func TestIssue1138_SendNeverHandsACrossOriginHopToTheTransport(t *testing.T) {
	for _, loc := range []string{
		"https://api.example.com.attacker.net/stolen",
		"https://api.example.com:8443/stolen",
		"http://api.example.com/stolen",
		"https://api.example.com@evil.example.net/stolen",
		"https://evil.api.example.com/stolen",
		"//evil.example.net/stolen", // scheme-relative
	} {
		for _, verb := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPost} {
			for _, code := range []int{301, 302, 303, 307, 308} {
				tr := &fakeTransport{}
				tr.answer = func(r *http.Request) (*http.Response, error) {
					if r.URL.Host == "api.example.com" && r.URL.Scheme == "https" && r.URL.Path == "/x" {
						return fakeResp(r, code, loc), nil
					}
					return fakeResp(r, 200, ""), nil
				}
				var body io.Reader
				if verb != http.MethodGet {
					body = strings.NewReader(`{"b":"BODY"}`)
				}
				req, _ := http.NewRequest(verb, "https://api.example.com/x", body)
				req.Header.Set("X-Api-Key", "FAKEKEY")
				resp, err := Send(SendSiteStage, &http.Client{Transport: tr}, req)
				if err != nil {
					t.Fatalf("%s %d %s: err %v", verb, code, loc, err)
				}
				_ = resp.Body.Close()
				hits := tr.all()
				if len(hits) != 1 || resp.StatusCode != code {
					t.Fatalf("RED %s %d %s: %d hits (%+v), status %d", verb, code, loc, len(hits), hits, resp.StatusCode)
				}
			}
		}
	}
	// control: same origin is followed, body re-sent on 307
	tr := &fakeTransport{}
	tr.answer = func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/x" {
			return fakeResp(r, 307, "https://API.example.com:443/y"), nil
		}
		return fakeResp(r, 200, ""), nil
	}
	req, _ := http.NewRequest(http.MethodPut, "https://api.example.com/x", strings.NewReader("FULLBODY"))
	resp, err := Send(SendSiteStage, &http.Client{Transport: tr}, req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("control: %v %v", err, resp)
	}
	hits := tr.all()
	if len(hits) != 2 || string(hits[1].Body) != "FULLBODY" {
		t.Fatalf("control: same-origin 307 followed with the body; hits %+v", hits)
	}
}

// The backstop alone (CheckRedirect replaced on the copy): the hop never
// reaches the transport.
func TestIssue1138_BackstopAloneRefusesTheHop(t *testing.T) {
	tr := &fakeTransport{}
	tr.answer = func(r *http.Request) (*http.Response, error) {
		if r.URL.Host == "api.example.com" {
			return fakeResp(r, 302, "https://api.example.com.attacker.net/s"), nil
		}
		return fakeResp(r, 200, ""), nil
	}
	c := SameOriginOnly(&http.Client{Transport: tr}, Origin(mustURL(t, "https://api.example.com/x")))
	c.CheckRedirect = nil // a copy whose policy was replaced
	req, _ := http.NewRequest(http.MethodGet, "https://api.example.com/x", nil)
	_, err := c.Do(req)
	var refused *RedirectRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("RED: backstop did not refuse: err=%v", err)
	}
	if n := len(tr.all()); n != 1 {
		t.Fatalf("RED: %d hits, the hop reached the transport", n)
	}
	// same-origin hop passes the backstop
	tr2 := &fakeTransport{}
	tr2.answer = func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/x" {
			return fakeResp(r, 302, "/y"), nil
		}
		return fakeResp(r, 200, ""), nil
	}
	c2 := SameOriginOnly(&http.Client{Transport: tr2}, Origin(mustURL(t, "https://api.example.com/x")))
	c2.CheckRedirect = nil
	req2, _ := http.NewRequest(http.MethodGet, "https://api.example.com/x", nil)
	resp, err := c2.Do(req2)
	if err != nil || resp.StatusCode != 200 || len(tr2.all()) != 2 {
		t.Fatalf("backstop must pass a same-origin hop: %v %v %d", err, resp, len(tr2.all()))
	}
}

// #1138 — the backstop's error names the credential the refused hop carried,
// by the dial's provenance mark: a named endpoint's own (and its stage's
// headers), never "snowplow-held"; a held dial's, the held rule. The status
// code is the redirect's, and no URL is in the text.
func TestIssue1138_BackstopNamesTheCredentialTheHopCarried(t *testing.T) {
	const loc = "https://api.example.com.attacker.net/s?sig=LOCSIG-zq1138"
	for _, tc := range []struct {
		name    string
		ctx     context.Context
		carried RedirectCredential
		want    string
		notWant string
	}{
		{"named_endpoint", WithHeldCredentialValue(context.Background(), false), CarriedEndpoint,
			"a named endpoint's own credential, and any header its stage set, never leave the origin", "snowplow-held"},
		{"unmarked", context.Background(), CarriedEndpoint,
			"a named endpoint's own credential", "snowplow-held"},
		{"held", WithHeldCredential(context.Background()), CarriedHeld,
			"a client carrying snowplow-held credentials never follows a redirect", "named endpoint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := &fakeTransport{}
			tr.answer = func(r *http.Request) (*http.Response, error) {
				if r.URL.Host == "api.example.com" {
					return fakeResp(r, http.StatusTemporaryRedirect, loc), nil
				}
				return fakeResp(r, 200, ""), nil
			}
			c := SameOriginOnly(&http.Client{Transport: tr}, Origin(mustURL(t, "https://api.example.com/x")))
			c.CheckRedirect = nil // the backstop alone answers
			req, _ := http.NewRequestWithContext(tc.ctx, http.MethodGet, "https://api.example.com/x", nil)
			_, err := c.Do(req)
			var refused *RedirectRefusedError
			if !errors.As(err, &refused) {
				t.Fatalf("PRE: the backstop must refuse the hop, err=%v", err)
			}
			if n := len(tr.all()); n != 1 {
				t.Fatalf("PRE: %d hits, the hop reached the transport", n)
			}
			if refused.StatusCode != http.StatusTemporaryRedirect || refused.Carried != tc.carried {
				t.Fatalf("RED: refused %+v, want status 307 carried %d", *refused, tc.carried)
			}
			text := refused.Error()
			if !strings.Contains(text, "refused to follow a 307 redirect") || !strings.Contains(text, tc.want) ||
				strings.Contains(text, tc.notWant) {
				t.Fatalf("RED: the text names the wrong credential: %q (want %q, never %q)", text, tc.want, tc.notWant)
			}
			// (net/http's *url.Error around it names the hop; the stage
			// redacts that with redact.ErrorURL, #523.)
			if strings.Contains(text, "LOCSIG-zq1138") || strings.Contains(text, "attacker") {
				t.Fatalf("RED: the refusal's own text carries the Location: %q", text)
			}
		})
	}
}

// #1138 — a hop the held-credential transport refuses names the held rule,
// whatever the ctx says: that transport is installed only on a held client.
func TestIssue1138_HeldTransportNamesTheHeldCredential(t *testing.T) {
	tr := &fakeTransport{}
	tr.answer = func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/x" {
			return fakeResp(r, http.StatusFound, "/y"), nil
		}
		return fakeResp(r, 200, ""), nil
	}
	c := &http.Client{Transport: RefuseRedirectHops(tr)}
	req, _ := http.NewRequestWithContext(WithHeldCredentialValue(context.Background(), false), http.MethodGet, "https://api.example.com/x", nil)
	_, err := c.Do(req)
	var refused *RedirectRefusedError
	if !errors.As(err, &refused) || refused.Carried != CarriedHeld ||
		!strings.Contains(refused.Error(), "snowplow-held credentials never follows a redirect") {
		t.Fatalf("RED: the held transport's refusal must name the held rule, err=%v", err)
	}
}

// D1: a PUT/DELETE answered 502, or dropped, is sent exactly once, with the
// full body.
func TestIssue1138_D1_WriteSentOnceWithItsBody(t *testing.T) {
	for _, verb := range []string{http.MethodPut, http.MethodDelete, http.MethodPost, http.MethodPatch} {
		for _, mode := range []string{"502", "429", "neterr"} {
			tr := &fakeTransport{}
			tr.answer = func(r *http.Request) (*http.Response, error) {
				switch mode {
				case "502":
					return fakeResp(r, 502, ""), nil
				case "429":
					return fakeResp(r, 429, ""), nil
				}
				return nil, errors.New("connection reset by peer")
			}
			req, _ := http.NewRequest(verb, "https://api.example.com/x", strings.NewReader("FULLBODY-0123456789"))
			resp, _ := Send(SendSiteStage, &http.Client{Transport: tr}, req)
			if resp != nil {
				_ = resp.Body.Close()
			}
			hits := tr.all()
			if len(hits) != 1 || string(hits[0].Body) != "FULLBODY-0123456789" {
				t.Fatalf("RED %s/%s: %d attempts, bodies %+v", verb, mode, len(hits), hits)
			}
		}
	}
}

// D4: WireDump never logs a header value, a body, userinfo or a query value.
func TestIssue1138_D4_WireDumpHoldsNoValueOfAnyKind(t *testing.T) {
	var buf bytes.Buffer
	lg := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(lg))
	inner := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		h := http.Header{}
		h.Set("Set-Cookie", "sid=RESPCOOKIE-canary")
		h.Set("X-Echo-Token", "RESPHDR-canary")
		h.Set("Location", "https://h/x?sig=RESPLOC-canary")
		return &http.Response{StatusCode: 200, Header: h, ContentLength: 20,
			Body: io.NopCloser(strings.NewReader(`{"t":"RESPBODY-canary"}`)), Request: r}, nil
	})
	rt := WireDump(inner)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://USER-canary:PASS-canary@api.example.com/x/PATHSEG?api_key=QUERY-canary&token=QTOKEN-canary",
		strings.NewReader(`{"b":"REQBODY-canary"}`))
	req.Header.Set("Authorization", "Bearer AUTH-canary")
	req.Header.Set("X-Api-Key", "APIKEY-canary")
	req.Header.Set("Private-Token", "PRIV-canary")
	req.Header.Set("Cookie", "sid=COOKIE-canary")
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "RESPBODY-canary") {
		t.Fatal("PRE: the response body must still reach the caller")
	}
	// the request body must still be fully readable by the next RT
	logs := buf.String()
	if !strings.Contains(logs, "request details") || !strings.Contains(logs, "response details") {
		t.Fatalf("PRE: both dump lines present: %s", logs)
	}
	for _, c := range []string{"AUTH-canary", "APIKEY-canary", "PRIV-canary", "COOKIE-canary", "REQBODY-canary",
		"RESPBODY-canary", "RESPCOOKIE-canary", "RESPHDR-canary", "RESPLOC-canary", "PASS-canary", "QUERY-canary", "QTOKEN-canary"} {
		if strings.Contains(logs, c) {
			t.Errorf("RED: %s in the wire dump: %s", c, logs)
		}
	}
}

// the request body handed onward by WireDump is intact
func TestIssue1138_D4_WireDumpHandsTheBodyOnIntact(t *testing.T) {
	var got string
	inner := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})
	req, _ := http.NewRequest(http.MethodPut, "https://api.example.com/x", strings.NewReader("FULLBODY"))
	if _, err := WireDump(inner).RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if got != "FULLBODY" {
		t.Fatalf("RED: body onward = %q", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
