package credhttp

// destination_1131_test.go — #1131, at credhttp's edge: every arm goes through
// Send (or Do, or Client) on the two chains that carry a named endpoint, the
// owned one (EndpointDialHolder) and plumbing's. The stage-level arms are in
// internal/resolvers/restactions/api/issue1131_destination_test.go; the structural
// guard is destination_guard_1131_test.go.
//
// No arm sends a packet to a denied or private address: the policy refuses in the
// dialer's control hook, before the connect, and the private-address arm stops
// the dial there too. Names are answered in-process (fakedns).

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/krateo-platformops/plumbing/dialguard"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/testsupport/fakedns"
	"k8s.io/client-go/transport"
)

// x1131Clients are the chains a named endpoint is sent on, built as production
// builds them.
func x1131Clients(t *testing.T) map[string]*http.Client {
	t.Helper()
	owned, err := transport.New(&transport.Config{Proxy: http.ProxyFromEnvironment, DialHolder: EndpointDialHolder})
	if err != nil {
		t.Fatal(err)
	}
	plumbing, err := request.HTTPClientForEndpoint(&endpoints.Endpoint{ServerURL: "http://x", Password: "p"}, &request.RequestInfo{})
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*http.Client{"owned": {Transport: owned}, "plumbing": plumbing}
}

// x1131Spellings is every denied range, at its edges, in its v4, v6 and
// v4-mapped spellings.
var x1131Spellings = []string{
	"169.254.169.254", "169.254.0.1", "169.254.255.254", // metadata and link-local
	"[::ffff:169.254.169.254]", "[::ffff:a9fe:a9fe]",
	"[fe80::1]", "[febf:ffff::1]",
	"0.0.0.0", "[::]", "[::ffff:0.0.0.0]", // unspecified
	"224.0.0.1", "239.255.255.255", "[::ffff:224.0.0.1]", // multicast
	"[ff02::1]", "[ff0e::1]",
}

// x1131RequireRefused requires the policy's refusal, and that what a stage
// reports of it holds neither the path nor the query.
func x1131RequireRefused(t *testing.T, err error) {
	t.Helper()
	if !DestinationRefused(err) {
		t.Fatalf("RED: want the destination policy's refusal, got %v", err)
	}
	st, ok := DestinationStatus(err)
	if !ok || st.Code != http.StatusBadGateway || st.Reason != DestinationRefusedReason {
		t.Fatalf("the refusal's envelope: %+v (ok=%v)", st, ok)
	}
	if strings.Contains(st.Message, "x1131-path") || strings.Contains(st.Message, "x1131-query") {
		t.Fatalf("the refusal names the request's URL: %q", st.Message)
	}
	if !strings.Contains(st.Message, "destination policy") {
		t.Fatalf("the refusal does not say it is the policy: %q", st.Message)
	}
}

func x1131Get(t *testing.T, base string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/x1131-path?x=x1131-query", nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

// x1131Put is a request that gets exactly one attempt (a write), for an arm about
// a failure that RetryClient would otherwise back off on.
func x1131Put(t *testing.T, base string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, base+"/x1131-path?x=x1131-query", nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestIssue1131_EveryDeniedSpellingIsRefused(t *testing.T) {
	for name, cli := range x1131Clients(t) {
		for _, host := range x1131Spellings {
			for _, verb := range []string{http.MethodGet, http.MethodPut} {
				t.Run(name+"/"+verb+"/"+host, func(t *testing.T) {
					req, err := http.NewRequest(verb, "http://"+host+"/x1131-path?x=x1131-query", nil)
					if err != nil {
						t.Fatal(err)
					}
					resp, err := Send(SendSiteStage, cli, req)
					if err == nil {
						_ = resp.Body.Close()
						t.Fatalf("RED: %s answered %d", host, resp.StatusCode)
					}
					x1131RequireRefused(t, err)
				})
			}
		}
	}
}

// A name is resolved first and the ADDRESS is what is checked, on both chains.
func TestIssue1131_ANameResolvingToADeniedAddressIsRefused(t *testing.T) {
	fakedns.Install(t, func(name string) []net.IP {
		switch name {
		case "metadata.x1131.test":
			return []net.IP{net.ParseIP("169.254.169.254")}
		case "linklocal6.x1131.test":
			return []net.IP{net.ParseIP("fe80::1")}
		case "multicast.x1131.test":
			return []net.IP{net.ParseIP("224.0.0.1"), net.ParseIP("ff02::1")}
		}
		return nil
	})
	for name, cli := range x1131Clients(t) {
		for _, host := range []string{"metadata.x1131.test", "linklocal6.x1131.test", "multicast.x1131.test"} {
			t.Run(name+"/"+host, func(t *testing.T) {
				resp, err := Send(SendSiteStage, cli, x1131Get(t, "http://"+host))
				if err == nil {
					_ = resp.Body.Close()
					t.Fatalf("RED: %s answered %d", host, resp.StatusCode)
				}
				x1131RequireRefused(t, err)
			})
		}
	}
}

// The check is per candidate address: a name with a denied address and an allowed
// one reaches the allowed one, and the denied one gets nothing.
func TestIssue1131_ADeniedCandidateIsSkippedForAnAllowedOne(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	t.Cleanup(srv.Close)
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	fakedns.Install(t, func(name string) []net.IP {
		if name == "mixed.x1131.test" {
			return []net.IP{net.ParseIP("169.254.169.254"), net.ParseIP("127.0.0.1")}
		}
		return nil
	})
	for name, cli := range x1131Clients(t) {
		t.Run(name, func(t *testing.T) {
			before := hits.Load()
			resp, err := Send(SendSiteStage, cli, x1131Get(t, "http://mixed.x1131.test:"+port))
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if hits.Load() != before+1 {
				t.Fatalf("the allowed candidate got %d request(s)", hits.Load()-before)
			}
		})
	}
}

// Rebinding: a name that answered an allowed address and then answers a denied
// one. The check runs on the second answer, because it runs on each dial.
func TestIssue1131_ANameThatRebindsToADeniedAddressIsRefused(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	t.Cleanup(srv.Close)
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	var answer atomic.Value
	fakedns.Install(t, func(name string) []net.IP {
		if name == "rebind.x1131.test" {
			return []net.IP{net.ParseIP(answer.Load().(string))}
		}
		return nil
	})
	for name, cli := range x1131Clients(t) {
		t.Run(name, func(t *testing.T) {
			answer.Store("127.0.0.1")
			before := hits.Load()
			first := x1131Get(t, "http://rebind.x1131.test:"+port)
			first.Close = true // no connection to reuse: the second send has to dial
			resp, err := Send(SendSiteStage, cli, first)
			if err != nil {
				t.Fatalf("PRE: the allowed answer is sent: %v", err)
			}
			_ = resp.Body.Close()

			answer.Store("169.254.169.254")
			resp, err = Send(SendSiteStage, cli, x1131Get(t, "http://rebind.x1131.test:"+port))
			if err == nil {
				_ = resp.Body.Close()
				t.Fatalf("RED: the rebound name was sent (status %d)", resp.StatusCode)
			}
			x1131RequireRefused(t, err)
			if got := hits.Load() - before; got != 1 {
				t.Fatalf("the server got %d request(s), want the first alone", got)
			}
		})
	}
}

// x1131StopAtControl replaces the owned dial with one that keeps the production
// Control and records what it said, then stops before the connect, so an allowed
// address gets no packet from a test. Returns what Control was called with.
func x1131StopAtControl(t *testing.T) *atomic.Value {
	t.Helper()
	var seen atomic.Value
	seen.Store(map[string]error{})
	d := *endpointDialer
	production := endpointDialer.Control
	d.Control = func(network, address string, c syscall.RawConn) error {
		verdict := production(network, address, c)
		m := map[string]error{address: verdict}
		for k, v := range seen.Load().(map[string]error) {
			m[k] = v
		}
		seen.Store(m)
		if verdict != nil {
			return verdict
		}
		return x1131Stopped
	}
	prev := endpointDialFn
	endpointDialFn = d.DialContext
	t.Cleanup(func() { endpointDialFn = prev })
	return &seen
}

var x1131Stopped = errors.New("x1131: stopped after the control hook")

// Loopback, RFC-1918, ULA and svc.cluster.local stay allowed: in-cluster services
// and on-prem endpoints live there, and a policy that refused them would break
// every installation. This is the policy's verdict on the owned chain, stopped
// before the connect.
func TestIssue1131_PrivateAddressesAreNotRefused(t *testing.T) {
	seen := x1131StopAtControl(t)
	fakedns.Install(t, func(name string) []net.IP {
		switch name {
		case "svc.ns.svc.cluster.local":
			return []net.IP{net.ParseIP("10.96.0.10")}
		case "gke.ns.svc.cluster.local":
			return []net.IP{net.ParseIP("34.118.224.10")} // GKE's default Service range
		}
		return nil
	})
	owned := x1131Clients(t)["owned"]
	for _, tc := range []struct{ host, addr string }{
		{"127.0.0.1", "127.0.0.1:80"}, {"127.255.0.1", "127.255.0.1:80"}, {"[::1]", "[::1]:80"},
		{"10.0.0.1", "10.0.0.1:80"}, {"172.16.0.1", "172.16.0.1:80"}, {"172.31.255.1", "172.31.255.1:80"},
		{"192.168.0.1", "192.168.0.1:80"}, {"[fd00::1]", "[fd00::1]:80"}, {"[fc00::1]", "[fc00::1]:80"},
		{"[::ffff:10.0.0.1]", "10.0.0.1:80"},
		{"svc.ns.svc.cluster.local", "10.96.0.10:80"},
		{"gke.ns.svc.cluster.local", "34.118.224.10:80"},
		{"93.184.216.34", "93.184.216.34:80"}, // and a public address
	} {
		t.Run(tc.host, func(t *testing.T) {
			resp, err := Send(SendSiteStage, owned, x1131Put(t, "http://"+tc.host))
			if err == nil {
				_ = resp.Body.Close()
				t.Fatal("PRE: the dial stops at the hook")
			}
			if DestinationRefused(err) {
				t.Fatalf("RED: the policy refused an allowed address: %v", err)
			}
			if !errors.Is(err, x1131Stopped) {
				t.Fatalf("the dial never reached the hook: %v", err)
			}
			verdict, ok := seen.Load().(map[string]error)[tc.addr]
			if !ok || verdict != nil {
				t.Fatalf("control saw %v, want a nil verdict on %s", seen.Load(), tc.addr)
			}
		})
	}
	// plumbing's chain has no seam to stop at, so its verdict is read from the
	// function its dialer runs. The loopback arm below sends on it for real.
	for _, a := range []string{"127.0.0.1:80", "[::1]:80", "10.0.0.1:80", "172.16.0.1:80", "192.168.0.1:80", "[fd00::1]:80"} {
		if err := dialguard.DenyAddr(a); err != nil {
			t.Errorf("RED: the policy refuses %s: %v", a, err)
		}
	}
}

func TestIssue1131_LoopbackIsSentOnBothChains(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	t.Cleanup(srv.Close)
	for name, cli := range x1131Clients(t) {
		t.Run(name, func(t *testing.T) {
			before := hits.Load()
			resp, err := Send(SendSiteStage, cli, x1131Get(t, srv.URL))
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if hits.Load() != before+1 {
				t.Fatal("loopback was not sent")
			}
		})
	}
}

// An unresolvable host fails closed: no address, no dial, and the failure is what
// it is, not a policy refusal. A name that reaches the deny function is refused.
func TestIssue1131_AnUnresolvableHostIsNeverDialled(t *testing.T) {
	seen := x1131StopAtControl(t)
	fakedns.Install(t, func(string) []net.IP { return nil })
	for name, cli := range x1131Clients(t) {
		t.Run(name, func(t *testing.T) {
			resp, err := Send(SendSiteStage, cli, x1131Put(t, "http://nowhere.x1131.test"))
			if err == nil {
				_ = resp.Body.Close()
				t.Fatal("RED: an unresolvable host was answered")
			}
			var dnsErr *net.DNSError
			if !errors.As(err, &dnsErr) || DestinationRefused(err) {
				t.Fatalf("want the DNS failure, got %v", err)
			}
		})
	}
	if got := seen.Load().(map[string]error); len(got) != 0 {
		t.Fatalf("an address was dialled: %v", got)
	}
	var denied *dialguard.ErrDenied
	if err := dialguard.DenyAddr("metadata.google.internal:80"); !errors.As(err, &denied) {
		t.Fatalf("RED: a name at the deny function must be refused, got %v", err)
	}
}

// A refusal cannot change by asking again: each send dials once, and no retry is
// counted, on the background path (RetryClient), the customer path (the page
// budget) and a write.
func TestIssue1131_RefusalIsSentOnce(t *testing.T) {
	var dials atomic.Int64
	d := *endpointDialer
	production := endpointDialer.Control
	d.Control = func(network, address string, c syscall.RawConn) error {
		dials.Add(1)
		return production(network, address, c)
	}
	prev := endpointDialFn
	endpointDialFn = d.DialContext
	t.Cleanup(func() { endpointDialFn = prev })
	owned := x1131Clients(t)["owned"]

	retries := func() (n uint64) {
		for _, c := range EndpointRetriesSnapshot() {
			n += c.Count
		}
		return n
	}
	for _, tc := range []struct {
		name string
		ctx  context.Context
		verb string
	}{
		{"background_read", context.Background(), http.MethodGet},
		{"customer_read", budgetLeft(pagebudget.PageMaxGoal), http.MethodGet},
		{"background_write", context.Background(), http.MethodPut},
		{"customer_write", budgetLeft(pagebudget.PageMaxGoal), http.MethodPut},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dials.Store(0)
			before := retries()
			req, err := http.NewRequestWithContext(tc.ctx, tc.verb, "http://169.254.169.254/x1131-path", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := Send(SendSiteStage, owned, req)
			if err == nil {
				_ = resp.Body.Close()
				t.Fatal("RED: the address answered")
			}
			x1131RequireRefused(t, err)
			if got := dials.Load(); got != 1 {
				t.Fatalf("RED: %d dials of a refused destination, want 1", got)
			}
			if got := retries() - before; got != 0 {
				t.Fatalf("RED: %d retries counted", got)
			}
		})
	}
}

// A client built here that names no transport is guarded too: the authn exchange
// and the RUM forwarder, and whatever reaches Send with a nil Transport.
func TestIssue1131_AClientWithNoTransportIsGuarded(t *testing.T) {
	t.Run("Client", func(t *testing.T) {
		resp, err := Client(&http.Client{}).Get("http://169.254.169.254/x1131-path")
		if err == nil {
			_ = resp.Body.Close()
			t.Fatal("RED: the address answered")
		}
		x1131RequireRefused(t, err)
	})
	t.Run("Send", func(t *testing.T) {
		resp, err := Send(SendSiteStage, &http.Client{}, x1131Get(t, "http://169.254.169.254"))
		if err == nil {
			_ = resp.Body.Close()
			t.Fatal("RED: the address answered")
		}
		x1131RequireRefused(t, err)
	})
	for name, wrap := range map[string]func(http.RoundTripper) http.RoundTripper{
		"RefuseRedirectHops":          RefuseRedirectHops,
		"BudgetTransport":             BudgetTransport,
		"CountResolveCollectionLists": CountResolveCollectionLists,
		"WireDump":                    WireDump,
	} {
		t.Run(name, func(t *testing.T) {
			resp, err := (&http.Client{Transport: wrap(nil)}).Do(x1131Get(t, "http://169.254.169.254"))
			if err == nil {
				_ = resp.Body.Close()
				t.Fatal("RED: the address answered")
			}
			x1131RequireRefused(t, err)
		})
	}
}

// /call: a refused destination is its own Failure, not a 500 that reads as a
// server fault, and no handler runs.
func TestIssue1131_DoReportsTheRefusal(t *testing.T) {
	handled := false
	st := Do(context.Background(), request.RequestOptions{
		RequestInfo:     request.RequestInfo{Path: "/x1131-path", Verb: ptr.To(http.MethodPost)},
		Endpoint:        &endpoints.Endpoint{ServerURL: "http://169.254.169.254", Token: "tok"},
		ResponseHandler: func(io.ReadCloser) error { handled = true; return nil },
	})
	if handled {
		t.Fatal("RED: a handler ran on a refused destination")
	}
	if st == nil || st.Code != http.StatusBadGateway || st.Reason != DestinationRefusedReason {
		t.Fatalf("RED: want 502 %s, got %+v", DestinationRefusedReason, st)
	}
	if strings.Contains(st.Message, "x1131-path") {
		t.Fatalf("the refusal names the path: %q", st.Message)
	}
}

// A network failure is not a refusal: it keeps its retry and its 500.
func TestIssue1131_ANetworkFailureIsNotARefusal(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	addr := srv.URL
	srv.Close() // nothing listens on it any more
	req, _ := http.NewRequest(http.MethodPut, addr+"/x", nil)
	resp, err := Send(SendSiteStage, x1131Clients(t)["owned"], req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("PRE: a closed port answers nothing")
	}
	if DestinationRefused(err) {
		t.Fatalf("RED: a connection failure read as a refusal: %v", err)
	}
	if _, ok := DestinationStatus(err); ok {
		t.Fatal("RED: a connection failure got the refusal's envelope")
	}
}
