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

	busy := make(chan error, 1)
	go func() { busy <- tryGet(context.Background(), cli, s.URL+"/busy") }()
	<-entered

	var servedByTheBusyConnection atomic.Bool
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(i httptrace.GotConnInfo) { servedByTheBusyConnection.Store(i.Reused) },
	})
	second := make(chan error, 1)
	go func() { second <- tryGet(ctx, cli, s.URL) }()
	<-dialing // the second request is queued for a connection, and its dial is out
	close(finish)
	for _, c := range []chan error{busy, second} {
		if err := <-c; err != nil {
			t.Fatal(err)
		}
	}
	if !servedByTheBusyConnection.Load() {
		t.Fatal("the second request did not get the busy connection: the arm did not reach its shape")
	}

	letLand()
	eventually(t, "the server accepts the parked dial", func() bool { return s.conns.Load() == 2 })
	eventually(t, "the parked dial is counted", func() bool { return connCells()["stage/http1/opened"]-before["stage/http1/opened"] == 2 })
	s.assertWire(t, 2, 2)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 2, "stage/http1/handovers": 2})
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

// The other side of that line: a dial that comes up after its request left is a
// connection. The server accepted it and the cost is paid, so it is opened, and
// no request was handed it.
func TestConnections847_DialThatLandsAfterItsRequestLeftIsOpened(t *testing.T) {
	s := newWireServer(t, wirePlain, nil)
	dialing, land := make(chan struct{}), make(chan struct{})
	var once sync.Once
	letLand := func() { once.Do(func() { close(land) }) }
	t.Cleanup(letLand)
	tr := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		close(dialing)
		<-land
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
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
	letLand()
	eventually(t, "the server accepts the dial", func() bool { return s.conns.Load() == 1 })
	eventually(t, "the dial is counted", func() bool { return connCells()["stage/http1/opened"]-before["stage/http1/opened"] == 1 })
	s.assertWire(t, 1, 0)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 1})
}

// A redirect hop takes a connection of its own.
func TestConnections847_RedirectHops(t *testing.T) {
	t.Run("same origin: a second handover on the same connection", func(t *testing.T) {
		s := newWireServer(t, wirePlain, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/a" {
				http.Redirect(w, r, "/b", http.StatusFound)
				return
			}
			_, _ = w.Write([]byte("ok"))
		})
		before := connCells()
		if resp := get(t, EgressStage, s.Client(), s.URL+"/a"); resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d, want the hop's 200", resp.StatusCode)
		}
		s.assertWire(t, 1, 2)
		assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 1, "stage/http1/handovers": 2})
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
	if a, r := accepted.Load(), served.Load(); a != 2 || r != 3 {
		t.Fatalf("the server saw %d connections and %d requests, want 2 and 3", a, r)
	}
	assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 2, "stage/http1/handovers": 3})
}
