package credhttp

// connections_847_test.go — #847 arms for the opened and handover counters.
//
// Every arm drives real connections through the real send path and compares the
// counters with what the SERVER saw (http.Server.ConnState, the requests it
// served), which nothing in the client can influence: the bound does not come
// from the instrument under test. Every arm asserts the exact delta over all
// cells, so a double count, or a count in the wrong cell, fails it too.
//
// The arms on a body closed early are in connections_earlyclose_*_847_test.go:
// which one compiles depends on the toolchain, because that is the behaviour the
// counters exist to read. The arms on concurrent pools, and on connections whose
// dial and whose first request are not the same event, are in
// connections_pool_847_test.go.

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/rest"
)

// connCells reads every cell as "egress/protocol/opened" and
// "egress/protocol/handovers".
func connCells() map[string]uint64 {
	m := map[string]uint64{}
	for _, c := range EgressConnectionsSnapshot() {
		m[c.Egress+"/"+c.Protocol+"/opened"] = c.Opened
		m[c.Egress+"/"+c.Protocol+"/handovers"] = c.Handovers
	}
	return m
}

// assertConnDelta fails unless the cells moved by exactly want since before.
func assertConnDelta(t *testing.T, before, want map[string]uint64) {
	t.Helper()
	got := map[string]uint64{}
	for k, v := range connCells() {
		if d := v - before[k]; d != 0 {
			got[k] = d
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("connection cells moved by %v, want %v", got, want)
	}
}

type wireMode int

const (
	wirePlain wireMode = iota // http://: HTTP/1 with no TLS
	wireTLSH1                 // https:// that offers only HTTP/1.1 over ALPN
	wireH2                    // https:// that negotiates HTTP/2
)

// wireServer counts, on the server's side of the wire, the connections it
// accepted and the requests it served.
type wireServer struct {
	*httptest.Server
	conns, reqs atomic.Int64
}

func newWireServer(t *testing.T, mode wireMode, h http.HandlerFunc) *wireServer {
	t.Helper()
	s := &wireServer{}
	if h == nil {
		h = func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	}
	s.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.reqs.Add(1)
		h(w, r)
	}))
	s.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateNew {
			s.conns.Add(1)
		}
	}
	switch mode {
	case wirePlain:
		s.Start()
	case wireTLSH1:
		s.StartTLS()
	case wireH2:
		s.EnableHTTP2 = true
		s.StartTLS()
	}
	t.Cleanup(s.Close)
	return s
}

// assertWire fails unless the server accepted conns connections and served reqs.
func (s *wireServer) assertWire(t *testing.T, conns, reqs int64) {
	t.Helper()
	if c, r := s.conns.Load(), s.reqs.Load(); c != conns || r != reqs {
		t.Fatalf("the server saw %d connections and %d requests, want %d and %d", c, r, conns, reqs)
	}
}

// sendOpen sends one request through Send and leaves its body to the caller.
func sendOpen(t *testing.T, ctx context.Context, site string, cli *http.Client, method, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := Send(site, cli, req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// send sends one request through Send and reads its body to the end.
func send(t *testing.T, ctx context.Context, site string, cli *http.Client, method, url string) *http.Response {
	t.Helper()
	resp := sendOpen(t, ctx, site, cli, method, url)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}

// sendUnread sends one GET through Send and closes its body without reading a
// byte, as every path in #847's table does.
func sendUnread(t *testing.T, ctx context.Context, cli *http.Client, url string) {
	t.Helper()
	_ = sendOpen(t, ctx, EgressStage, cli, http.MethodGet, url).Body.Close()
}

func get(t *testing.T, site string, cli *http.Client, url string) *http.Response {
	t.Helper()
	return send(t, context.Background(), site, cli, http.MethodGet, url)
}

// One connection serves three requests: one opened, three handovers.
func TestConnections847_ReusedConnectionIsOneOpenedManyHandovers(t *testing.T) {
	s := newWireServer(t, wirePlain, nil)
	cli := s.Client()
	before := connCells()
	for range 3 {
		get(t, EgressStage, cli, s.URL)
	}
	s.assertWire(t, 1, 3)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 1, "stage/http1/handovers": 3})
}

// Keep-alives off: every request opens its own connection.
func TestConnections847_NewConnectionPerRequest(t *testing.T) {
	s := newWireServer(t, wirePlain, nil)
	cli := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	before := connCells()
	for range 3 {
		get(t, EgressStage, cli, s.URL)
	}
	s.assertWire(t, 3, 3)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 3, "stage/http1/handovers": 3})
}

// HTTP/2 reuse is counted: one connection carries every stream. Over the stage
// path and over a real client-go client (whose HTTP/2 is x/net's, not net/http's
// bundled copy).
func TestConnections847_HTTP2ReuseIsCounted(t *testing.T) {
	t.Run("stage", func(t *testing.T) {
		s := newWireServer(t, wireH2, nil)
		cli := s.Client()
		before := connCells()
		for range 3 {
			if resp := get(t, EgressStage, cli, s.URL); resp.ProtoMajor != 2 {
				t.Fatalf("the arm spoke HTTP/%d, want HTTP/2", resp.ProtoMajor)
			}
		}
		s.assertWire(t, 1, 3)
		assertConnDelta(t, before, map[string]uint64{"stage/http2/opened": 1, "stage/http2/handovers": 3})
	})
	t.Run("apiserver", func(t *testing.T) {
		s := newWireServer(t, wireH2, nil)
		hc := hardenedClient(t, s)
		before := connCells()
		for range 3 {
			clientGet(t, hc, s.URL, 2)
		}
		s.assertWire(t, 1, 3)
		assertConnDelta(t, before, map[string]uint64{"apiserver/http2/opened": 1, "apiserver/http2/handovers": 3})
	})
}

// HTTP/2 does not lose a connection to a body closed early: only that stream
// ends. On any toolchain, which is why the protocol is a label.
func TestConnections847_EarlyClosedBodyOnHTTP2KeepsItsConnection(t *testing.T) {
	s := newWireServer(t, wireH2, nil)
	cli := s.Client()
	before := connCells()
	sendUnread(t, context.Background(), cli, s.URL)
	get(t, EgressStage, cli, s.URL)
	s.assertWire(t, 1, 2)
	assertConnDelta(t, before, map[string]uint64{"stage/http2/opened": 1, "stage/http2/handovers": 2})
}

// TLS alone is not HTTP/2, and neither is ALPN: the client offers h2 and
// http/1.1, as client-go does, and the server picks http/1.1. The negotiated
// protocol is asserted, so the arm cannot quietly stop testing that boundary.
func TestConnections847_HTTP1NegotiatedOverALPNCountsAsHTTP1(t *testing.T) {
	s := newWireServer(t, wireTLSH1, nil)
	cli := s.Client()
	cli.Transport.(*http.Transport).ForceAttemptHTTP2 = true
	before := connCells()
	for range 2 {
		resp := get(t, EgressStage, cli, s.URL)
		if resp.ProtoMajor != 1 || resp.TLS == nil || resp.TLS.NegotiatedProtocol != "http/1.1" {
			t.Fatalf("the arm spoke HTTP/%d with ALPN %+v, want http/1.1 negotiated", resp.ProtoMajor, resp.TLS)
		}
	}
	s.assertWire(t, 1, 2)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 1, "stage/http1/handovers": 2})
}

// hardenedClient is a real client-go http.Client for s, built the way every
// client in the module is: Harden, then HTTPClientFor.
func hardenedClient(t *testing.T, s *wireServer) *http.Client {
	t.Helper()
	hc, err := HTTPClientFor(Harden(restConfigFor(s)))
	if err != nil {
		t.Fatal(err)
	}
	return hc
}

func restConfigFor(s *wireServer) *rest.Config {
	return &rest.Config{Host: s.URL, TLSClientConfig: rest.TLSClientConfig{
		CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}),
	}}
}

func clientGet(t *testing.T, hc *http.Client, url string, proto int) {
	t.Helper()
	resp, err := hc.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.ProtoMajor != proto {
		t.Fatalf("the arm spoke HTTP/%d, want HTTP/%d", resp.ProtoMajor, proto)
	}
}

// One hook per request, whichever layers it crosses (the file comment of
// connections.go). Harden on a copy of a hardened config is FromSecret's chain,
// which carries the transport twice.
func TestConnections847_CountsOnceThroughADoubleChain(t *testing.T) {
	t.Run("hardened twice", func(t *testing.T) {
		s := newWireServer(t, wireH2, nil)
		hc, err := HTTPClientFor(Harden(rest.CopyConfig(Harden(restConfigFor(s)))))
		if err != nil {
			t.Fatal(err)
		}
		before := connCells()
		for range 2 {
			clientGet(t, hc, s.URL, 2)
		}
		s.assertWire(t, 1, 2)
		assertConnDelta(t, before, map[string]uint64{"apiserver/http2/opened": 1, "apiserver/http2/handovers": 2})
	})
	t.Run("Send on a hardened client: the site is the egress", func(t *testing.T) {
		s := newWireServer(t, wireH2, nil)
		hc := hardenedClient(t, s)
		before := connCells()
		for range 2 {
			get(t, EgressStage, hc, s.URL)
		}
		s.assertWire(t, 1, 2)
		assertConnDelta(t, before, map[string]uint64{"stage/http2/opened": 1, "stage/http2/handovers": 2})
	})
}

// Every path through Send counts, once: a safe request and a write, with and
// without a page budget. Each is its own branch of Send, with its own trace.
func TestConnections847_EveryPathThroughSendCounts(t *testing.T) {
	seedReserve(t, 0)
	for _, tc := range []struct {
		name   string
		method string
		ctx    func() context.Context
	}{
		{"read", http.MethodGet, context.Background},
		{"write", http.MethodPut, context.Background},
		{"budgeted read", http.MethodGet, func() context.Context { return budgetLeft(9 * time.Second) }},
		{"budgeted write", http.MethodPut, func() context.Context { return budgetLeft(9 * time.Second) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newWireServer(t, wirePlain, nil)
			cli := s.Client()
			before := connCells()
			for range 2 {
				send(t, tc.ctx(), EgressStage, cli, tc.method, s.URL)
			}
			s.assertWire(t, 1, 2)
			assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 1, "stage/http1/handovers": 2})
		})
	}
}

// A retry takes a connection of its own: both attempts of a request that failed
// with a 503 are counted. The 503 has no body, so the connection survives
// whatever the toolchain (a retry after a body left unread is the early-close
// arm); what this pins is that the attempts count one each.
func TestConnections847_EachAttemptCounts(t *testing.T) {
	fastRetries(t, "1")
	var n atomic.Int64
	s := newWireServer(t, wirePlain, func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	cli := s.Client()
	before := connCells()
	if resp := get(t, EgressStage, cli, s.URL); resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want the retry's 200", resp.StatusCode)
	}
	s.assertWire(t, 1, 2)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 1, "stage/http1/handovers": 2})
}

// The /call passthrough counts under its own egress. Do builds a client per
// call, so two calls are two connections: the arm pins that reading too, since a
// reuse share of 0 on this egress is the transport's doing and no toolchain's.
func TestConnections847_CallEgress(t *testing.T) {
	s := newWireServer(t, wirePlain, nil)
	before := connCells()
	for range 2 {
		st := Do(context.Background(), request.RequestOptions{
			RequestInfo: request.RequestInfo{Path: "/x", Verb: ptr.To(http.MethodGet)},
			Endpoint:    &endpoints.Endpoint{ServerURL: s.URL},
			ResponseHandler: func(rc io.ReadCloser) error {
				_, err := io.Copy(io.Discard, rc)
				return err
			},
		})
		if st.Code != http.StatusOK {
			t.Fatalf("Do answered %d: %s", st.Code, st.Message)
		}
	}
	s.assertWire(t, 2, 2)
	assertConnDelta(t, before, map[string]uint64{"call/http1/opened": 2, "call/http1/handovers": 2})
}

// A client that names its egress counts under it. The authn and RUM constructors
// are driven through their own packages' arms.
func TestConnections847_CountConnectionsOn(t *testing.T) {
	s := newWireServer(t, wirePlain, nil)
	cli := CountConnectionsOn(EgressAuthn, Client(&http.Client{Transport: s.Client().Transport}))
	before := connCells()
	for range 2 {
		clientGet(t, cli, s.URL, 1)
	}
	s.assertWire(t, 1, 2)
	assertConnDelta(t, before, map[string]uint64{"authn/http1/opened": 1, "authn/http1/handovers": 2})
}

// The counting layer stays walkable: the idle sweep (#1101 review B2) reaches
// the transport through it. Otherwise the sweep closes nothing and the second
// request reuses a connection that should be gone.
func TestConnections847_CloseIdleConnectionsReachesThroughTheLayer(t *testing.T) {
	for name, build := range map[string]func(s *wireServer) *http.Client{
		"CountConnectionsOn": func(s *wireServer) *http.Client {
			return CountConnectionsOn(EgressAuthn, Client(&http.Client{Transport: s.Client().Transport}))
		},
		"Harden": func(s *wireServer) *http.Client { return hardenedClient(t, s) },
	} {
		t.Run(name, func(t *testing.T) {
			s := newWireServer(t, wireTLSH1, nil)
			cli := build(s)
			clientGet(t, cli, s.URL, 1)
			utilnet.CloseIdleConnectionsFor(cli.Transport)
			clientGet(t, cli, s.URL, 1)
			s.assertWire(t, 2, 2)
		})
	}
}

// The label set is closed: every cell exists from the start, nothing else ever
// does, and a name outside the set folds into "other" instead of becoming a
// label. Whatever a request carries (host, path, query, identity), no label
// holds it.
func TestConnections847_LabelSetIsClosed(t *testing.T) {
	want := map[string]bool{}
	for _, e := range []string{"stage", "call", "apiserver", "authn", "rum_forward", "other"} {
		for _, p := range []string{"http1", "http2"} {
			for _, c := range []string{"opened", "handovers"} {
				want[e+"/"+p+"/"+c] = true
			}
		}
	}
	s := newWireServer(t, wirePlain, nil)
	host := strings.TrimPrefix(s.URL, "http://")
	req, err := http.NewRequest(http.MethodGet, s.URL+"/tenant-path?user=alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	before := connCells()
	resp, err := Send(host, s.Client(), req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	assertConnDelta(t, before, map[string]uint64{"other/http1/opened": 1, "other/http1/handovers": 1})

	got := connCells()
	if len(got) != len(want) {
		t.Fatalf("%d cells, want %d: %v", len(got), len(want), got)
	}
	for k := range got {
		if !want[k] {
			t.Errorf("cell %q is outside the closed set", k)
		}
	}
	out, err := json.Marshal(EgressConnectionsSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{host, "tenant-path", "alice"} {
		if strings.Contains(string(out), leak) {
			t.Errorf("the snapshot carries %q: %s", leak, out)
		}
	}
}

// What a connection's protocol is, by itself. The arms above go through net/http;
// this names the case it never reaches them with (no connection in the report).
func TestConnections847_ProtocolOfAConnection(t *testing.T) {
	for negotiated, want := range map[string]string{"h2": ConnProtocolHTTP2, "http/1.1": ConnProtocolHTTP1, "": ConnProtocolHTTP1} {
		if got := alpnProtocol(negotiated); got != want {
			t.Errorf("ALPN %q reads %s, want %s", negotiated, got, want)
		}
	}
	if got := connProtocol(nil); got != ConnProtocolHTTP1 {
		t.Errorf("no connection reads %s, want %s", got, ConnProtocolHTTP1)
	}
}
