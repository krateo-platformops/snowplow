// main_identity_logs_1192_test.go — #1192: no log line of a request carries the
// caller's name or groups, at any level.
//
// PRODUCTION FUNCTIONS UNDER TEST: newRequestChain and buildLogHandler, the same
// two main() calls (F10 and TestS1192_MainBuildsTheChainFromNewRequestChain pin
// that). Only UserConfig is a stand-in, because the real one needs a cluster for
// the clientconfig Secret; it attaches the sentinel identity to the request
// exactly as the real one does, after the chain, and it logs nothing.
//
// #1141's arms could not see this: x1106Resolve hands the resolver a logger of
// its own, so plumbing's use.Logger never ran. Here the bearer token is on the
// request and the resolver's logger comes off the chain.
//
// The refresher half is issue1192_refresher_identity_logs_test.go.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/plumbing/server/use"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"k8s.io/client-go/rest"
)

const (
	sentinelUser1192  = "sentinel-user-zq1192@tenant.example"
	sentinelGroup1192 = "sentinel-group-zq1192"
	traceID1192       = "trace-zq1192"
	probeMsg1192      = "s1192.probe"
)

// lockedBuffer is the capture a background emitter can write to while the test
// reads it (#392 class): the handler serialises its own writes, not this read.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// bearer1192 is a token of the sentinel identity. Its signature is not checked
// here: use.Logger read a token without verifying it, and the stand-in below
// does not read it at all.
func bearer1192() string {
	seg := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	claims, _ := json.Marshal(map[string]any{"sub": sentinelUser1192, "groups": []string{sentinelGroup1192}})
	return "Bearer " + seg(`{"alg":"none"}`) + "." + seg(string(claims)) + ".sig"
}

// userConfig1192 stands in for middleware.UserConfig.
func userConfig1192(ep endpoints.Endpoint) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := xcontext.BuildContext(r.Context(),
				xcontext.WithAccessToken("test-token"),
				xcontext.WithUserInfo(jwtutil.UserInfo{Username: sentinelUser1192, Groups: []string{sentinelGroup1192}}),
				xcontext.WithUserConfig(ep))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// forbiddingAPI1192 is an apiserver that refuses everything the way a real one
// does: its Status names the requester, and the impersonation form names a
// group. It returns the endpoint a stage dials and the number of refused calls.
func forbiddingAPI1192(t *testing.T) (endpoints.Endpoint, func() int) {
	t.Helper()
	prev := xenv.TestMode()
	xenv.SetTestMode(true)
	var mu sync.Mutex
	calls := 0
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// The caller's clientconfig Secret: the one read that succeeds, so a
		// stage gets as far as its own dial.
		if strings.Contains(r.URL.Path, "/secrets/") {
			b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
			_ = json.NewEncoder(w).Encode(map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"data": map[string]string{"server-url": b64(srv.URL), "token": b64("tok"), "insecure": b64("true")},
			})
			return
		}
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": "Forbidden", "code": 403,
			"message": `pods is forbidden: User "` + sentinelUser1192 + `" cannot list resource "pods"; ` +
				`groups "` + sentinelGroup1192 + `" is forbidden; ` +
				`secrets "` + sentinelUser1192 + `-clientconfig" not found`,
		})
	}))
	t.Cleanup(func() {
		srv.Close()
		xenv.SetTestMode(prev)
	})
	return endpoints.Endpoint{ServerURL: srv.URL}, func() int {
		mu.Lock()
		defer mu.Unlock()
		return calls
	}
}

// resolveHandler1192 runs a request's work the way a dispatcher does: the real
// resolver on the request context, plus one line at each level from the request
// logger itself, so a level the resolver does not reach is still on the arm.
func resolveHandler1192(ep endpoints.Endpoint) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := xcontext.Logger(r.Context())
		log.Debug(probeMsg1192)
		log.Info(probeMsg1192)
		log.Warn(probeMsg1192)
		log.Error(probeMsg1192)
		restactionsapi.ResolveWithFailure(r.Context(), restactionsapi.ResolveOptions{
			RC:      &rest.Config{Host: ep.ServerURL},
			AuthnNS: "authn-ns",
			Items: []*templates.API{
				// A read snowplow refuses: the WARN #1141 calls identity-free.
				{Name: "refused", Verb: ptr.To(http.MethodGet), Path: `/api/v1/namespaces/${ .nothing }/pods/${ .nothing }`, ErrorKey: ptr.To("refusedErr")},
				// A read the apiserver refuses, naming the caller.
				{Name: "denied", Verb: ptr.To(http.MethodGet), Path: "/api/v1/namespaces/default/pods", ErrorKey: ptr.To("deniedErr")},
			},
			RESTActionNamespace: "ns-1192",
			RESTActionName:      "ra-1192",
		})
		w.WriteHeader(http.StatusOK)
	})
}

// run1192 serves one request through chain + the stand-in, on the logger main
// builds, and returns what that logger wrote. It installs the logger as the
// default too, as main does (slog.SetDefault): the per-call line and every
// slog.Warn site write there.
func run1192(t *testing.T, chain func(*slog.Logger) use.Chain, ep endpoints.Endpoint) []string {
	t.Helper()
	capture := &lockedBuffer{}
	log := slog.New(buildLogHandler(false, slog.LevelDebug, capture))
	prev := slog.Default()
	slog.SetDefault(log)
	t.Cleanup(func() { slog.SetDefault(prev) })

	h := chain(log).Append(userConfig1192(ep)).Then(resolveHandler1192(ep))
	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	req.Header.Set("Authorization", bearer1192())
	req.Header.Set(xcontext.LabelKrateoTraceId, traceID1192)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("the request must reach the handler: code=%d body=%s", rec.Code, rec.Body.String())
	}
	return strings.Split(strings.TrimSpace(capture.String()), "\n")
}

// keys1192 collects every key of a decoded record, nested ones included.
func keys1192(v any, out map[string]bool) {
	switch x := v.(type) {
	case map[string]any:
		for k, sub := range x {
			out[k] = true
			keys1192(sub, out)
		}
	case []any:
		for _, sub := range x {
			keys1192(sub, out)
		}
	}
}

// TestS1192_RequestPath_NoIdentityOnAnyLine is the arm. Its controls are part of
// it: without a line at each level, the resolver's two real lines and the
// apiserver's text on one of them, an empty capture would pass.
func TestS1192_RequestPath_NoIdentityOnAnyLine(t *testing.T) {
	ep, calls := forbiddingAPI1192(t)
	lines := run1192(t, newRequestChain, ep)

	levels := map[string]bool{}
	messages := map[string]bool{}
	apiserverTextSeen := false
	for _, line := range lines {
		for _, tok := range []string{sentinelUser1192, sentinelGroup1192} {
			if strings.Contains(line, tok) {
				t.Errorf("#1192: identity on a log line: %s", line)
			}
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("a line is not one JSON object (%v): %s", err, line)
		}
		keys := map[string]bool{}
		keys1192(rec, keys)
		for _, k := range []string{"user", "username", "groups", "identity"} {
			if keys[k] {
				t.Errorf("#1192: identity attribute %q on a log line: %s", k, line)
			}
		}
		msg, _ := rec["msg"].(string)
		messages[msg] = true
		if msg == probeMsg1192 {
			levels[rec["level"].(string)] = true
		}
		if msg == probeMsg1192 || msg == "read stage not sent" || msg == "api call response failure" {
			if rec["traceId"] != traceID1192 {
				t.Errorf("a request line must keep its traceId: %s", line)
			}
		}
		if strings.Contains(line, "cannot list resource") {
			apiserverTextSeen = true
		}
	}

	// POSITIVE CONTROL: the capture holds what the assertions above claim to
	// have looked at.
	for _, l := range []string{"DEBUG", "INFO", "WARN", "ERROR"} {
		if !levels[l] {
			t.Errorf("CONTROL: no %s line reached the capture; lines=%v", l, lines)
		}
	}
	for _, m := range []string{"read stage not sent", "api call response failure"} {
		if !messages[m] {
			t.Errorf("CONTROL: the resolver's %q line reached no capture; lines=%v", m, lines)
		}
	}
	if !apiserverTextSeen {
		t.Errorf("CONTROL: the apiserver's refusal never reached a line, so its text was not scrubbed but absent; lines=%v", lines)
	}
	if calls() == 0 {
		t.Error("CONTROL: the denied stage never dialled the apiserver")
	}
}

// TestS1192_ControlPlumbingLoggerPutsIdentityOnTheLine is the arm above against
// the wiring it replaced: the same request through use.Logger carries both
// sentinels. It proves the token on the request is read, the sentinels would be
// seen, and #1192 was real; it fails the day plumbing stops attaching them, which
// is when newRequestChain can go back to use.Logger.
func TestS1192_ControlPlumbingLoggerPutsIdentityOnTheLine(t *testing.T) {
	ep, _ := forbiddingAPI1192(t)
	lines := run1192(t, func(log *slog.Logger) use.Chain {
		return use.NewChain(use.TraceId(), use.Logger(log), audit.Middleware())
	}, ep)
	out := strings.Join(lines, "\n")
	for _, tok := range []string{sentinelUser1192, sentinelGroup1192} {
		if !strings.Contains(out, tok) {
			t.Errorf("CONTROL: use.Logger's identity %q never reached the capture, so the arm above proves nothing; lines=%v", tok, lines)
		}
	}
}

// TestS1192_UnauthenticatedRequestKeepsItsTraceId: the traceId is on the request
// logger whether or not a token parsed, and once.
func TestS1192_UnauthenticatedRequestKeepsItsTraceId(t *testing.T) {
	capture := &lockedBuffer{}
	log := slog.New(buildLogHandler(false, slog.LevelDebug, capture))
	h := newRequestChain(log).Then(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		xcontext.Logger(r.Context()).Info(probeMsg1192)
		if got := xcontext.TraceId(r.Context(), false); got != traceID1192 {
			t.Errorf("the context's traceId = %q", got)
		}
	}))
	req := httptest.NewRequest(http.MethodGet, "/capabilities", nil)
	req.Header.Set(xcontext.LabelKrateoTraceId, traceID1192)
	h.ServeHTTP(httptest.NewRecorder(), req)
	if strings.Count(capture.String(), `"traceId":"`+traceID1192+`"`) != 1 {
		t.Fatalf("the line must carry the request's traceId once: %s", capture.String())
	}
}

// TestS1192_MainBuildsTheChainFromNewRequestChain: the arm above is about the
// pod only if main's chain is the one it drives.
func TestS1192_MainBuildsTheChainFromNewRequestChain(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	var calls, built int
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "main" || fd.Recv != nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fn := c.Fun.(type) {
			case *ast.Ident:
				if fn.Name == "newRequestChain" {
					calls++
				}
			case *ast.SelectorExpr:
				if id, ok := fn.X.(*ast.Ident); ok && id.Name == "use" && fn.Sel.Name == "NewChain" {
					built++
				}
			}
			return true
		})
	}
	if calls != 1 || built != 0 {
		t.Fatalf("main() must build its chain with newRequestChain once and use.NewChain never: %d, %d", calls, built)
	}
}
