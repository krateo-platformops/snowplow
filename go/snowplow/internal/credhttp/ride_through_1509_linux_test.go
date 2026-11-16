package credhttp

// ride_through_1509_linux_test.go — #1509 acceptance arm, on the real Linux
// kernel (no fake dialer, no fake server):
//
//  1. A TLS HTTP/2 server serves a warm, pooled connection.
//  2. It leaves: its next answer is 429, Retry-After: 5, Connection: close
//     (kube-apiserver's shutdown answer; Go's server sends it as a GOAWAY), and
//     at the same moment it stops accepting. Its listener has a backlog of 0
//     and one connection queued, so the kernel drops every further SYN, as for
//     an endpoint being replaced: a client socket resends its SYN only at
//     +1 s, +3 s, +7 s after it started.
//  3. At comeback (3.3 s after the request started) it accepts again.
//
// The customer request (page budget 9 s) must complete by comeback + 1 s:
//   - with (a) reverted it waits the 5 s Retry-After first: done at ≈5 s;
//   - with (b) reverted its one socket's next SYN is at +7 s: done at ≈7 s;
//   - with both, a raced socket started after the 1 s/3 s instants sends a
//     SYN shortly after comeback.
// And no socket leaks: when it is done the server holds only the request's
// connection.

import (
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"k8s.io/client-go/transport"
)

// gatedListener's Accept waits while the gate is shut.
type gatedListener struct {
	net.Listener
	mu   sync.Mutex
	cond *sync.Cond
	shut bool
}

func (g *gatedListener) Accept() (net.Conn, error) {
	g.mu.Lock()
	for g.shut {
		g.cond.Wait()
	}
	g.mu.Unlock()
	return g.Listener.Accept()
}

func (g *gatedListener) setShut(v bool) {
	g.mu.Lock()
	g.shut = v
	g.mu.Unlock()
	g.cond.Broadcast()
}

// backlogZeroListener listens on 127.0.0.1 with listen(2) backlog 0: the
// kernel queues at most one connection nobody accepted, and drops SYNs past it.
func backlogZeroListener(t *testing.T) net.Listener {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Listen(fd, 0); err != nil {
		t.Fatal(err)
	}
	f := os.NewFile(uintptr(fd), "backlog0")
	ln, err := net.FileListener(f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func TestIssue1509_RideThroughAReplicaChange(t *testing.T) {
	resetConnectTimes(t)
	seedReserve(t, 50*time.Millisecond)

	var leaving atomic.Bool
	var open atomic.Int32
	gl := &gatedListener{Listener: backlogZeroListener(t)}
	gl.cond = sync.NewCond(&gl.mu)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if leaving.CompareAndSwap(true, false) {
			w.Header().Set("Connection", "close")
			w.Header().Set("Retry-After", "5")
			http.Error(w, "the apiserver is shutting down", http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	srv.Listener = gl
	srv.EnableHTTP2 = true
	srv.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		switch st {
		case http.StateNew:
			open.Add(1)
		case http.StateClosed, http.StateHijacked:
			open.Add(-1)
		}
	}
	srv.StartTLS()
	t.Cleanup(func() { gl.setShut(false); srv.Close() })
	addr := gl.Addr().String()

	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	rt, err := transport.New(&transport.Config{
		TLS:        transport.TLSConfig{CAData: ca},
		Proxy:      http.ProxyFromEnvironment,
		DialHolder: EndpointDialHolder, // the owned dialer: endpointDial → raceDial → the guarded dialer
	})
	if err != nil {
		t.Fatal(err)
	}
	cli := &http.Client{Transport: rt}

	// 1. Warm: a pooled HTTP/2 connection, and this destination's connect time measured.
	warm, err := Send(SendSiteStage, cli, getReq(t, budgetLeft(9*time.Second), srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	_ = warm.Body.Close()
	if warm.ProtoMajor != 2 {
		t.Fatalf("warm request over %s; the arm needs HTTP/2", warm.Proto)
	}

	// 2. Leave: next answer is the shutdown 429; stop accepting and fill the
	// one-slot queue, so further SYNs are dropped by the kernel.
	leaving.Store(true)
	gl.setShut(true)
	var fillers []net.Conn
	t.Cleanup(func() {
		for _, f := range fillers {
			_ = f.Close()
		}
	})
	// The server's accept loop may already sit in Accept and take one; a probe
	// that cannot connect proves the queue is full.
	for range 4 {
		c, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
		if err != nil {
			break
		}
		fillers = append(fillers, c)
	}
	if c, err := net.DialTimeout("tcp", addr, 300*time.Millisecond); err == nil {
		_ = c.Close()
		t.Fatal("the listener still accepts SYNs; the arm needs them dropped")
	}

	// 3. The customer request; the server comes back 3.3 s after it started.
	start := time.Now()
	const comeback = 3300 * time.Millisecond
	go func() {
		time.Sleep(comeback)
		for _, f := range fillers {
			_ = f.Close()
		}
		gl.setShut(false)
	}()
	resp, err := Send(SendSiteStage, cli, getReq(t, budgetLeft(9*time.Second), srv.URL))
	took := time.Since(start)
	if err != nil {
		t.Fatalf("after %v: %v", took, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d after %v", resp.StatusCode, took)
	}
	if took > comeback+time.Second {
		t.Fatalf("done %v after start, %v after the server came back; want ≤ 1 s after it came back "+
			"(≈5 s = the leaving server's Retry-After was waited; ≈7 s = the connect waited for its own SYN retransmit)",
			took, took-comeback)
	}
	t.Logf("done %v after start (%v after comeback)", took, took-comeback)

	// No leaked socket: every losing connect is closed; the server is left with
	// the request's connection only (the fillers were closed at comeback).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && open.Load() > 1 {
		time.Sleep(20 * time.Millisecond)
	}
	if n := open.Load(); n != 1 {
		t.Fatalf("server holds %d connections after the request; want 1 (losing connects must be closed)", n)
	}
}
