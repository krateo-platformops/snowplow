package credhttp

// handshake_completed_1544_test.go — #1544: a raced TLS setup whose handshake COMPLETED just as its attempt's ctx ended
// (its chain finished: the last waiter served by another attempt) was counted by the server and by nobody else:
// crypto/tls's HandshakeContext returns the ctx's error for a completed handshake, and handshake's error branch
// closed the connection without counting it. Now it is counted opened with the chain's counting (closeLoser).
//
// The arm makes that moment deterministic: the client's connection cancels the attempt's ctx right after writing the
// client's Finished (in TLS 1.3, its first application-data record, outer type 0x17) and waits for crypto/tls's ctx
// watcher to act before returning, so the handshake completes with the ctx already ended. Judged against the
// server's own count of completed handshakes (mutation: the completed branch removed leaves opened 0 against the
// server's 1).

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func egressCells1544() map[string]uint64 {
	out := map[string]uint64{}
	for _, c := range EgressConnectionsSnapshot() {
		out[c.Egress+"/"+c.Protocol+"/opened"] = c.Opened
		out[c.Egress+"/"+c.Protocol+"/handovers"] = c.Handovers
	}
	return out
}

func cellDeltas1544(before, after map[string]uint64) map[string]uint64 {
	out := map[string]uint64{}
	for k, v := range after {
		if d := v - before[k]; d != 0 {
			out[k] = d
		}
	}
	return out
}

// cancelAtFinishedConn cancels `cancel` right after the write that matches (atFinished: the first write carrying an
// application-data record, TLS 1.3's client Finished; else the first write, the ClientHello), then waits `settle` so
// crypto/tls's ctx watcher sees the ctx done before the handshake goes on.
type cancelAtFinishedConn struct {
	net.Conn
	cancel     context.CancelFunc
	settle     time.Duration
	atFinished bool
	once       sync.Once
}

func hasAppDataRecord(p []byte) bool {
	for len(p) >= 5 {
		typ, n := p[0], int(p[3])<<8|int(p[4])
		if typ == 0x17 {
			return true
		}
		if len(p) < 5+n {
			return false
		}
		p = p[5+n:]
	}
	return false
}

func (c *cancelAtFinishedConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err == nil && (!c.atFinished || hasAppDataRecord(p)) {
		c.once.Do(func() {
			c.cancel()
			time.Sleep(c.settle)
		})
	}
	return n, err
}

// TestIssue1544_AHandshakeThatCompletesAsItsCtxEndsIsCountedOpened: completed, then its ctx ended: counted opened
// once, in stage, as the server counted it. And the guard: a handshake whose ctx ended BEFORE it completed (at the
// ClientHello) is no connection: counted nowhere, and the server completed none (mutation: counting every errored
// handshake reads opened 1 there).
func TestIssue1544_AHandshakeThatCompletesAsItsCtxEndsIsCountedOpened(t *testing.T) {
	for _, tc := range []struct {
		name       string
		atFinished bool
		want       int32
	}{{"completed", true, 1}, {"cut before it completed", false, 0}} {
		t.Run(tc.name, func(t *testing.T) { handshake1544(t, tc.atFinished, tc.want) })
	}
}

func handshake1544(t *testing.T, atFinished bool, want int32) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.StartTLS() // for its certificate only: the handshakes below are served by our own listener
	t.Cleanup(srv.Close)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var completed atomic.Int32
	go func() {
		for {
			raw, err := ln.Accept()
			if err != nil {
				return
			}
			go func(raw net.Conn) {
				defer raw.Close()
				sc := tls.Server(raw, &tls.Config{Certificates: srv.TLS.Certificates, MinVersion: tls.VersionTLS13})
				if sc.HandshakeContext(context.Background()) == nil {
					completed.Add(1) // the server's own count
				}
			}(raw)
		}
	}()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEMOf(srv))
	addr := ln.Addr().String()
	c := &setupChain{k: chainKey{network: "tcp", addr: addr,
		tlsCfg: &tls.Config{RootCAs: pool, ServerName: "example.com", MinVersion: tls.VersionTLS13}},
		countCtx: countConnections(httptest.NewRequest(http.MethodGet, "https://"+addr+"/", nil), EgressStage)}
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	before := egressCells1544()
	wrapped := &cancelAtFinishedConn{Conn: raw, cancel: cancel, settle: 100 * time.Millisecond, atFinished: atFinished}
	conn, err, tlsErr := c.handshake(ctx, wrapped)
	if conn != nil || !tlsErr || !errors.Is(err, context.Canceled) {
		t.Fatalf("handshake: conn %v, err %v, tlsErr %v; want the ctx's error (the arm did not reproduce its moment)",
			conn, err, tlsErr)
	}
	time.Sleep(300 * time.Millisecond) // the server has finished (or abandoned) its side
	if completed.Load() != want {
		t.Fatalf("the server completed %d handshakes; want %d (the arm's premise)", completed.Load(), want)
	}
	d := cellDeltas1544(before, egressCells1544())
	var opened uint64
	for k, v := range d {
		if len(k) > 7 && k[len(k)-7:] == "/opened" {
			opened += v
		}
	}
	if opened != uint64(completed.Load()) || d["stage/http1/opened"] != uint64(want) || len(d) > int(want) {
		t.Fatalf("the cells moved by %v for the server's %d completed handshake(s); want stage/http1 opened %d and "+
			"nothing else", d, completed.Load(), want)
	}
}
