// Package authn is a small client for the Kubernetes intra-service auth login
// strategy: snowplow presents its own projected (audience-bound) ServiceAccount
// token to authn's /serviceaccount/login endpoint and receives an authn-issued
// JWT (signed by the Krateo authn issuer's jwt-sign-key). That JWT is the
// Bearer the PREWARM SEED uses to authenticate a nested loopback `/call`
// against snowplow's OWN auth-gated `/call` endpoint — the only credential
// snowplow's middleware (jwtutil.Validate against jwt-sign-key) will accept
// (the projected apiserver SA token has a different issuer and is rejected;
// see docs/rca-prewarm-nested-loopback-jwt-2026-06-26.md §4).
//
// Ported VERBATIM from composition-dynamic-controller/internal/authn (an
// internal/ package, not cross-module importable) — the established cdc→authn
// pattern. Self-contained: stdlib plus internal/credhttp (#723/#724).
//
// Token() reads the projected SA token fresh on every cache miss (projected
// tokens rotate on disk), exchanges it for a JWT, and caches the JWT until
// shortly before its own expiry so the exchange is amortised across seeds.
package authn

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// DefaultTokenPath is the conventional mount path of the projected
// ServiceAccount token. snowplow's Deployment (chart) projects a token bound to
// the authn audience at this path (mirrors the cdc Deployment).
const DefaultTokenPath = "/var/run/secrets/krateo.io/serviceaccount/token"

// refreshSkew is how long before a cached JWT's expiry we proactively
// re-exchange, so a token is never handed out moments before it would be
// rejected downstream.
const refreshSkew = 60 * time.Second

// Client exchanges a projected ServiceAccount token for an authn-issued JWT and
// caches it.
type Client struct {
	server     string
	tokenPath  string
	httpClient *http.Client

	now func() time.Time // injectable clock for tests

	mu     sync.Mutex
	cached string
	expiry time.Time
}

// New returns an authn client for the given base URL (e.g.
// http://authn.krateo-system.svc.cluster.local:8082). tokenPath is the
// projected SA token file; if empty, DefaultTokenPath is used.
func New(server, tokenPath string) *Client {
	if tokenPath == "" {
		tokenPath = DefaultTokenPath
	}
	return &Client{
		server:    server,
		tokenPath: tokenPath,
		// #723/#724: this client presents snowplow's projected SA token; it
		// never follows a redirect (a same-host redirect would re-send it).
		httpClient: credhttp.Client(&http.Client{Timeout: 30 * time.Second}),
		now:        time.Now,
	}
}

// WithHTTPClient overrides the HTTP client (tests inject a stub transport).
func (c *Client) WithHTTPClient(h *http.Client) *Client { c.httpClient = h; return c }

// tokenLock / tokenUnlock are the (var-seam) critical-section entry/exit for
// Token's exchange-and-cache. Production ALWAYS uses c.mu — these are never
// reassigned outside tests; they exist only so the concurrency falsifier can
// transiently neuter the mutual exclusion (observe >1 exchange under -race)
// and restore it, without reflectively mutating a sync.Mutex. Repo idiom:
// resolveOnceFn / nestedCallResolver / saRESTConfigForInspectFn.
var (
	tokenLock   = func(c *Client) { c.mu.Lock() }
	tokenUnlock = func(c *Client) { c.mu.Unlock() }
)

// Token returns a valid authn JWT, exchanging the projected SA token when the
// cache is empty or near expiry.
func (c *Client) Token(ctx context.Context) (string, error) {
	tokenLock(c)
	defer tokenUnlock(c)

	if c.cached != "" && c.now().Before(c.expiry.Add(-refreshSkew)) {
		return c.cached, nil
	}

	jwt, exp, err := c.exchange(ctx)
	if err != nil {
		return "", err
	}
	c.cached = jwt
	c.expiry = exp
	return jwt, nil
}

// exchange reads the projected SA token and POSTs it to authn's
// /serviceaccount/login, returning the issued JWT and its expiry.
func (c *Client) exchange(ctx context.Context) (string, time.Time, error) {
	saToken, err := os.ReadFile(c.tokenPath)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("reading projected service account token %q: %w", c.tokenPath, err)
	}

	u, err := url.JoinPath(c.server, "/serviceaccount/login")
	if err != nil {
		return "", time.Time{}, fmt.Errorf("joining authn url: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+string(saToken))
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("calling authn: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("authn returned %d: %s", resp.StatusCode, string(body))
	}

	var out struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", time.Time{}, fmt.Errorf("decoding authn response: %w", err)
	}
	if out.AccessToken == "" {
		return "", time.Time{}, fmt.Errorf("authn response carried no accessToken")
	}

	// Derive expiry from the JWT's exp claim so caching tracks the real
	// lifetime; fall back to a conservative window if the token is unparseable
	// (still usable, just refreshed sooner).
	exp := jwtExpiry(out.AccessToken)
	if exp.IsZero() {
		exp = c.now().Add(5 * time.Minute)
	}
	return out.AccessToken, exp, nil
}

// jwtExpiry decodes (without verifying) the JWT payload and returns its exp
// claim as a time. The signature is irrelevant here — snowplow's seed only
// CONSUMES the token to present it; snowplow's /call middleware is what
// VALIDATES it. A zero time is returned when the token has no parseable exp.
func jwtExpiry(token string) time.Time {
	parts := splitJWT(token)
	if len(parts) != 3 {
		return time.Time{}
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == 0 {
		return time.Time{}
	}
	return time.Unix(claims.Exp, 0)
}

func splitJWT(token string) []string {
	out := make([]string, 0, 3)
	start := 0
	for i := 0; i < len(token); i++ {
		if token[i] == '.' {
			out = append(out, token[start:i])
			start = i + 1
		}
	}
	out = append(out, token[start:])
	return out
}
