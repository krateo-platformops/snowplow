package api

// issue1112_d4_wire_dump_test.go — #1112 D4: a wire dump never holds a
// credential, a cookie or a body.
//
//   A  an endpoint Secret with debug: "true" (owned client-go branch): the dump
//      is present, and carries no token, no cookie, no request or response body.
//   B  RESOLVER_VERBOSE_WIRE_DUMP=true + a verbose resolve: the same.
//   C  a plumbing-built endpoint (basic auth with a password only, which
//      plumbing keeps): plumbing's own dump is never installed.
//   D  the control: no debug, no dump.

import (
	"context"
	"encoding/base64"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	utilnet "k8s.io/apimachinery/pkg/util/net"
)

func x1106DumpArm(t *testing.T, label string, o x1106Out, secrets ...string) {
	t.Helper()
	if !strings.Contains(o.logs, `"msg":"request details"`) || !strings.Contains(o.logs, `"msg":"response details"`) {
		t.Fatalf("%s: PRE: the wire dump must be present (debug asked for it); logs:\n%.2000s", label, o.logs)
	}
	// The dict carries the answer by design (it is the stage's value); the
	// debug log is the surface under test.
	for _, sec := range secrets {
		if i := strings.Index(o.logs, sec); i >= 0 {
			lo := strings.LastIndex(o.logs[:i], "\n") + 1
			line := o.logs[lo:]
			if j := strings.Index(line, "\n"); j >= 0 {
				line = line[:j]
			}
			t.Errorf("%s: RED — %q is in the debug log, line (masked): %.300s", label, sec, strings.ReplaceAll(line, sec, "<CANARY>"))
		}
	}
}

func x1106DumpServer(t *testing.T) *x1106Srv {
	return newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Set-Cookie", "sid=RESPCOOKIE-canary")
		x1106OK(w, r)
	})
}

func x1106DumpStage() []string {
	return []string{"Content-Type: application/json", "Cookie: sid=REQCOOKIE-canary", "X-Api-Key: APIKEY-canary"}
}

func TestIssue1112_D4_A_SecretDebugDumpHoldsNoCredentialOrBody(t *testing.T) {
	ep := x1106DumpServer(t)
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL, "debug", "true")})
	s := x1106Write("put", http.MethodPut, `${ "/zd/tickets/" + (.ticketId|tostring) }`)
	s.Headers = x1106DumpStage()
	o := x1106Resolve(t, x1106Extras, s)
	if len(ep.all()) != 1 || o.outcomes != "[]" {
		t.Fatalf("PRE: the write is sent once and succeeds: %d sent, header %s", len(ep.all()), o.outcomes)
	}
	x1106DumpArm(t, "secret debug", o, x1106Token, x1106Body, "REQCOOKIE-canary", "RESPCOOKIE-canary", "APIKEY-canary")
}

func TestIssue1112_D4_B_VerboseWireDumpHoldsNoCredentialOrBody(t *testing.T) {
	t.Setenv(envVerboseWireDump, "true")
	ep := x1106DumpServer(t)
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL)})
	s := x1106Write("put", http.MethodPut, `${ "/zd/tickets/" + (.ticketId|tostring) }`)
	s.Headers = x1106DumpStage()
	o := x1106ResolveWith(t, x1106Opts{verbose: true}, x1106Extras, s)
	x1106DumpArm(t, "verbose wire dump", o, x1106Token, x1106Body, "REQCOOKIE-canary", "RESPCOOKIE-canary", "APIKEY-canary")
}

func TestIssue1112_D4_C_PlumbingBuiltClientNeverInstallsPlumbingDump(t *testing.T) {
	ep := x1106DumpServer(t)
	sec := map[string]string{"server-url": ep.srv.URL, "password": x1106Token, "debug": "true"}
	x1106Env(t, "", map[string]map[string]string{x1106Ref: sec})
	o := x1106Resolve(t, x1106Extras, x1106Write("put", http.MethodPut, `${ "/zd/tickets/" + (.ticketId|tostring) }`))
	hits := ep.all()
	if len(hits) != 1 || !strings.HasPrefix(hits[0].Auth, "Basic ") {
		t.Fatalf("PRE: the plumbing branch sends its basic credential: %v", hits)
	}
	basic := strings.TrimPrefix(hits[0].Auth, "Basic ")
	if dec, _ := base64.StdEncoding.DecodeString(basic); !strings.Contains(string(dec), x1106Token) {
		t.Fatalf("PRE: fixture: the basic credential carries the canary")
	}
	x1106DumpArm(t, "plumbing branch", o, basic, x1106Body)
}

// E — the #1101 crossing for the dump: a Debug endpoint and a non-Debug one,
// field-identical otherwise, are two kept clients; only the Debug one dumps,
// interleaved in either order, and the Debug client's chain still unwraps to
// its *http.Transport (the idle sweep closes what it evicts).
func TestIssue1112_D4_E_DebugAndPlainClientsNeverCross(t *testing.T) {
	srv := x1106DumpServer(t)
	plain := &endpoints.Endpoint{ServerURL: srv.srv.URL, Token: x1106Token}
	debug := *plain
	debug.Debug = true
	w := &x1106Writer{}
	lg := slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(lg))
	dumps := func() int { return strings.Count(w.String(), `"msg":"request details"`) }
	for i, ep := range []*endpoints.Endpoint{plain, &debug, plain, &debug, plain} {
		before := dumps()
		res, _, _, err := httpFetchAllowingNonJSON(ctx, httpcall.RequestOptions{
			RequestInfo: httpcall.RequestInfo{Path: "/zd/x", Verb: ptr.To(http.MethodGet)}, Endpoint: ep})
		if err != nil || res == nil || res.Code >= 300 {
			t.Fatalf("fetch %d: %+v %v", i, res, err)
		}
		if got, want := dumps()-before, map[bool]int{true: 1, false: 0}[ep.Debug]; got != want {
			t.Fatalf("RED: fetch %d (debug=%v) dumped %d request(s), want %d: the two identities crossed", i, ep.Debug, got, want)
		}
	}
	cli, err := httpClientForEndpoint(context.Background(), &debug, &httpcall.RequestInfo{})
	if err != nil {
		t.Fatal(err)
	}
	rt := cli.Transport
	for {
		if _, ok := rt.(*http.Transport); ok {
			break
		}
		wr, ok := rt.(utilnet.RoundTripperWrapper)
		if !ok {
			t.Fatalf("the Debug client's chain stops at %T: the idle sweep could not close its connections", rt)
		}
		rt = wr.WrappedRoundTripper()
	}
}

func TestIssue1112_D4_D_NoDebugNoDump(t *testing.T) {
	ep := x1106DumpServer(t)
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL)})
	o := x1106Resolve(t, x1106Extras, x1106Write("put", http.MethodPut, `${ "/zd/tickets/" + (.ticketId|tostring) }`))
	if strings.Contains(o.logs, `"msg":"request details"`) {
		t.Fatal("a dial that did not ask for the dump must not dump")
	}
}
