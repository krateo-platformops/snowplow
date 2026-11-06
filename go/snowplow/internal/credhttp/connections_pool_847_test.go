package credhttp

// connections_pool_847_test.go — #847 arms for the case the counters were
// redesigned around: a connection's dial and its first request are separate
// events. GotConnInfo.Reused reads true for a dial whose waiter was served by
// something else, so a counter built on it undercounts new connections as soon
// as a pool is concurrent. Each arm's bound is what the server accepted.

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// eventually waits for cond: a connection the client has finished dialling is
// not yet one the server has accepted, and the reverse.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("never happened: %s", what)
}

// wireAttempts bounds untilWire. net/http can lose an HTTP/1 connection by itself:
// it keeps one only if the request's write status arrives within 50 ms of the
// response ending (persistConn.wroteRequest, maxWriteWaitBeforeConnReuse), and a
// thread that loses the CPU between its write and that status misses it. Measured
// on plain net/http at load 75–115, both toolchains: 9 misses in 18,070 pairs of
// requests, and 0 in 17,528 with only that timer raised, while 92 waits ran past
// 50 ms. No test can prevent it, so an arm that needs a connection to survive may
// have to try again. The count is of attempts, not of time.
const wireAttempts = 8

// untilWire runs sequence until the server saw exactly conns connections and reqs
// requests, on a fresh server each attempt. Every attempt must count exactly what
// its server saw, reached or not; only the literal conns can take a second try. It
// fails if no attempt reaches it, so a toolchain or transport that stops keeping
// connections cannot pass.
func untilWire(t *testing.T, conns, reqs int64, cells string, sequence func(t *testing.T) *wireServer) {
	t.Helper()
	for range wireAttempts {
		before := connCells()
		s := sequence(t)
		s.assertCounted(t, before, cells, reqs)
		if s.conns.Load() == conns {
			return
		}
	}
	t.Fatalf("no attempt in %d reached %d connections for %d requests", wireAttempts, conns, reqs)
}

// watchPool returns ctx with a report of whether the transport kept this
// request's connection, which net/http makes before a body read to its end
// returns.
func watchPool(ctx context.Context) (context.Context, *atomic.Bool) {
	kept := new(atomic.Bool)
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		PutIdleConn: func(err error) {
			if err == nil {
				kept.Store(true)
			}
		},
	}), kept
}

// decided waits until the transport has decided about a connection whose body was
// closed early, and reports whether it kept it: it says so (kept) or the server
// sees the connection close. A request sent after this cannot race the drain.
func decided(t *testing.T, s *wireServer, kept *atomic.Bool) bool {
	t.Helper()
	eventually(t, "the transport keeps or closes the connection", func() bool { return kept.Load() || s.closed.Load() > 0 })
	return kept.Load()
}

func tryGet(ctx context.Context, cli *http.Client, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := Send(EgressStage, cli, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

// The pool's normal way of replacing a connection, made deterministic. A request
// starts a dial; before the dial lands, the busy connection comes free and serves
// that request. The dial then lands with nobody waiting and parks. net/http marks
// it reused, so a counter built on GotConnInfo.Reused never sees it, while the
// server accepted two connections.
func TestConnections847_DialServedByAnotherConnectionIsStillOpened(t *testing.T) {
	for range wireAttempts {
		if parkedDialAttempt(t) {
			return
		}
	}
	t.Fatalf("the busy connection never served the waiting request in %d attempts", wireAttempts)
}

// parkedDialAttempt reports whether the attempt reached that shape; the counters
// must match the server either way.
func parkedDialAttempt(t *testing.T) bool {
	t.Helper()
	entered, finish := make(chan struct{}), make(chan struct{})
	s := newWireServer(t, wirePlain, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/busy" {
			close(entered)
			<-finish
		}
		_, _ = w.Write([]byte("ok"))
	})
	dialing, land := make(chan struct{}), make(chan struct{})
	var once sync.Once
	letLand := func() { once.Do(func() { close(land) }) }
	t.Cleanup(letLand)
	var dials atomic.Int32
	tr := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if dials.Add(1) == 2 {
			close(dialing)
			<-land
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
	t.Cleanup(tr.CloseIdleConnections)
	cli := &http.Client{Transport: tr}
	before := connCells()

	busyCtx, kept := watchPool(context.Background())
	busy := make(chan error, 1)
	go func() { busy <- tryGet(busyCtx, cli, s.URL+"/busy") }()
	<-entered

	var reused atomic.Bool
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(i httptrace.GotConnInfo) { reused.Store(i.Reused) },
	})
	second := make(chan error, 1)
	go func() { second <- tryGet(ctx, cli, s.URL) }()
	<-dialing // the second request is queued for a connection, and its dial is out
	close(finish)
	if err := <-busy; err != nil {
		t.Fatal(err)
	}
	// By now the transport has either handed the busy connection on or dropped it.
	// If it dropped it, the second request is waiting for the dial.
	shape := kept.Load()
	if !shape {
		letLand()
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if shape && !reused.Load() {
		t.Fatal("the transport kept the busy connection, and the waiting request did not get it")
	}
	letLand()
	eventually(t, "the server accepts the dial", func() bool { return s.conns.Load() == 2 })
	eventually(t, "the dial is counted", func() bool { return connCells()["stage/http1/opened"]-before["stage/http1/opened"] == 2 })
	s.assertWire(t, 2, 2)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 2, "stage/http1/handovers": 2})
	return shape
}

// A busy pool, one in twenty bodies closed unread: opened is what the server
// accepted and handovers is what it served, exactly, on every protocol and through
// client-go's own dialer. The dial takes longer than the handler, so dials land
// parked, as they do on a real pool.
func TestConnections847_ConcurrentPoolMatchesTheServer(t *testing.T) {
	const workers, perWorker = 16, 25
	handler := func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Millisecond)
		_, _ = w.Write([]byte("ok"))
	}
	run := func(t *testing.T, s *wireServer, send func(unread bool) error, opened, handovers string) {
		t.Helper()
		before := connCells()
		var wg sync.WaitGroup
		errs := make(chan error, workers*perWorker)
		for w := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range perWorker {
					if err := send((w*perWorker+i)%20 == 0); err != nil {
						errs <- err
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		eventually(t, "opened settles on what the server accepted", func() bool {
			return connCells()[opened]-before[opened] == uint64(s.conns.Load())
		})
		if r := s.reqs.Load(); r != workers*perWorker {
			t.Fatalf("the server served %d requests, want %d", r, workers*perWorker)
		}
		assertConnDelta(t, before, map[string]uint64{opened: uint64(s.conns.Load()), handovers: uint64(s.reqs.Load())})
	}
	slowDial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		time.Sleep(5 * time.Millisecond)
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	for _, tc := range []struct {
		name  string
		mode  wireMode
		cells string
	}{{"http", wirePlain, "stage/http1"}, {"https http1", wireTLSH1, "stage/http1"}, {"https http2", wireH2, "stage/http2"}} {
		t.Run(tc.name, func(t *testing.T) {
			s := newWireServer(t, tc.mode, handler)
			tr := s.Client().Transport.(*http.Transport).Clone()
			tr.ForceAttemptHTTP2 = true
			tr.MaxIdleConnsPerHost = 25
			tr.DialContext = slowDial
			t.Cleanup(tr.CloseIdleConnections)
			cli := &http.Client{Transport: tr}
			run(t, s, func(unread bool) error {
				req, err := http.NewRequest(http.MethodGet, s.URL, nil)
				if err != nil {
					return err
				}
				resp, err := Send(EgressStage, cli, req)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				if !unread {
					_, err = io.Copy(io.Discard, resp.Body)
				}
				return err
			}, tc.cells+"/opened", tc.cells+"/handovers")
		})
	}
	t.Run("client-go's dialer", func(t *testing.T) {
		s := newWireServer(t, wireH2, handler)
		hc := hardenedClient(t, s)
		run(t, s, func(bool) error {
			resp, err := hc.Get(s.URL)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			_, err = io.Copy(io.Discard, resp.Body)
			return err
		}, "apiserver/http2/opened", "apiserver/http2/handovers")
	})
}

// #1115: an attempt is cut, its dial goes on, and the retry is handed that
// connection. One connection on the wire, opened for this request and carrying
// its first request. GotConnInfo.Reused reads true for it.
func TestConnections847_ConnectCutRetryHandedItsOwnDial(t *testing.T) {
	s := newWireServer(t, wirePlain, nil)
	seedReserve(t, 150*time.Millisecond)
	d := newScriptedDialer(t, s.Listener.Addr().String(), "slow:800ms", "hang")
	before := connCells()
	if err := tryGet(budgetLeft(900*time.Millisecond), clientOn(d), s.URL); err != nil {
		t.Fatal(err)
	}
	s.assertWire(t, 1, 1)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 1, "stage/http1/handovers": 1})
}

// A dial that produced no connection counts as nothing. Every case sends a PUT,
// which is sent once, so the arm sees one dial.
func TestConnections847_FailedDialIsNotAConnection(t *testing.T) {
	put := func(t *testing.T, cli *http.Client, url string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPut, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		if resp, err := Send(EgressStage, cli, req); err == nil {
			_ = resp.Body.Close()
			t.Fatal("the send succeeded: the arm did not reach its shape")
		}
	}
	t.Run("connection refused", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		_ = ln.Close()
		before := connCells()
		put(t, &http.Client{Transport: &http.Transport{}}, "http://"+addr)
		assertConnDelta(t, before, map[string]uint64{})
	})
	t.Run("TLS handshake refused", func(t *testing.T) {
		s := newWireServer(t, wireH2, nil)
		before := connCells()
		put(t, &http.Client{Transport: &http.Transport{}}, s.URL) // does not trust the server's certificate
		eventually(t, "the TCP connect reached the server", func() bool { return s.conns.Load() == 1 })
		assertConnDelta(t, before, map[string]uint64{})
	})
	t.Run("TLS handshake given up", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		var accepted atomic.Int32
		go func() { // accepts, and never says a word
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				defer c.Close()
				accepted.Add(1)
			}
		}()
		before := connCells()
		put(t, &http.Client{Transport: &http.Transport{TLSHandshakeTimeout: 50 * time.Millisecond}}, "https://"+ln.Addr().String())
		if accepted.Load() != 1 {
			t.Fatalf("the server accepted %d connections, want 1: the handshake must have started", accepted.Load())
		}
		assertConnDelta(t, before, map[string]uint64{})
	})
}

// heldConn holds the client's first read, which is the TLS handshake's wait for
// the server's answer, until release returns.
type heldConn struct {
	net.Conn
	once sync.Once
	hold func()
}

func (c *heldConn) Read(p []byte) (int, error) {
	c.once.Do(c.hold)
	return c.Conn.Read(p)
}

// The other side of that line: a dial that comes up after its request left is a
// connection. The server accepted it and the cost is paid, so it is opened, and
// no request was handed it. https is its own hook, and the shape of every per-user
// client, so it is pinned there too: the request leaves during the connect, and
// during the handshake.
func TestConnections847_DialThatLandsAfterItsRequestLeftIsOpened(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      wireMode
		cell      string
		handshake bool
	}{
		{"http, during the connect", wirePlain, "stage/http1/opened", false},
		{"https http1, during the connect", wireTLSH1, "stage/http1/opened", false},
		{"https http2, during the connect", wireH2, "stage/http2/opened", false},
		{"https http1, during the handshake", wireTLSH1, "stage/http1/opened", true},
		{"https http2, during the handshake", wireH2, "stage/http2/opened", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newWireServer(t, tc.mode, nil)
			dialing, land := make(chan struct{}), make(chan struct{})
			var once sync.Once
			letLand := func() { once.Do(func() { close(land) }) }
			t.Cleanup(letLand)
			hold := func() {
				close(dialing)
				<-land
			}
			tr := s.Client().Transport.(*http.Transport).Clone()
			tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				if !tc.handshake {
					hold()
				}
				c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
				if err != nil || !tc.handshake {
					return c, err
				}
				return &heldConn{Conn: c, hold: hold}, nil
			}
			t.Cleanup(tr.CloseIdleConnections)
			ctx, cancel := context.WithCancel(context.Background())
			before := connCells()
			done := make(chan error, 1)
			go func() { done <- tryGet(ctx, &http.Client{Transport: tr}, s.URL) }()
			<-dialing
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("the request ended with %v, want its cancellation", err)
			}
			if d := connCells()[tc.cell] - before[tc.cell]; d != 0 {
				t.Fatalf("%s moved by %d before the dial landed: the arm did not reach its shape", tc.cell, d)
			}
			letLand()
			eventually(t, "the server accepts the dial", func() bool { return s.conns.Load() == 1 })
			eventually(t, "the dial is counted", func() bool { return connCells()[tc.cell]-before[tc.cell] == 1 })
			s.assertWire(t, 1, 0)
			assertConnDelta(t, before, map[string]uint64{tc.cell: 1})
		})
	}
}

// A redirect hop takes a connection of its own.
func TestConnections847_RedirectHops(t *testing.T) {
	t.Run("same origin: a second handover on the same connection", func(t *testing.T) {
		untilWire(t, 1, 2, "stage/http1", func(t *testing.T) *wireServer {
			s := newWireServer(t, wirePlain, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/a" {
					http.Redirect(w, r, "/b", http.StatusFound)
					return
				}
				_, _ = w.Write([]byte("ok"))
			})
			if resp := get(t, EgressStage, s.Client(), s.URL+"/a"); resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d, want the hop's 200", resp.StatusCode)
			}
			return s
		})
	})
	t.Run("another origin: refused, nothing opened there", func(t *testing.T) {
		foreign := newWireServer(t, wirePlain, nil)
		s := newWireServer(t, wirePlain, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, foreign.URL, http.StatusFound)
		})
		before := connCells()
		if resp := get(t, EgressStage, s.Client(), s.URL); resp.StatusCode != http.StatusFound {
			t.Fatalf("status %d, want the refused 302", resp.StatusCode)
		}
		s.assertWire(t, 1, 1)
		foreign.assertWire(t, 0, 0)
		assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 1, "stage/http1/handovers": 1})
	})
}

// A pooled connection that turned out dead: net/http takes it, finds it dead
// after the request was read, and retries by itself on a new one. The caller sees
// one request; the wire saw two connections and three requests. The server is
// raw, because it has to drop a connection at exactly that point.
func TestConnections847_DeadPooledConnection(t *testing.T) {
	for range wireAttempts {
		if deadPoolAttempt(t) {
			return
		}
	}
	t.Fatalf("the first connection was never reused in %d attempts", wireAttempts)
}

// deadPoolAttempt reports whether the second request really took the pooled
// connection (three requests on the wire); the counters must match the server
// either way.
func deadPoolAttempt(t *testing.T) bool {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var accepted, served atomic.Int32
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			first := accepted.Add(1) == 1
			go func() {
				defer c.Close()
				br := bufio.NewReader(c)
				for i := 0; ; i++ {
					if _, err := http.ReadRequest(br); err != nil {
						return
					}
					served.Add(1)
					if first && i == 1 {
						return // the second request on the first connection dies here
					}
					if _, err := c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")); err != nil {
						return
					}
				}
			}()
		}
	}()
	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	cli := &http.Client{Transport: tr}
	url := "http://" + ln.Addr().String()
	before := connCells()
	for range 2 {
		if err := tryGet(context.Background(), cli, url); err != nil {
			t.Fatal(err)
		}
	}
	a, r := accepted.Load(), served.Load()
	if a != 2 {
		t.Fatalf("the server saw %d connections, want 2", a)
	}
	assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": uint64(a), "stage/http1/handovers": uint64(r)})
	return r == 3
}
