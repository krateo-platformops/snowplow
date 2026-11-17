//go:build linux && syndrop1509

package credhttp

// ride_through_syndrop_1509_linux_test.go — #1509 (b)'s proof on a real
// kernel (owner ruling on #1521: "real SYN drop in CI"). Built only with the
// syndrop1509 tag, in its own CI job (.github/workflows/syndrop-1509.yaml),
// which can run iptables; there it never skips: an arm that cannot drop SYNs
// FAILS.
//
//  1. A TLS HTTP/2 server serves a warm, pooled connection.
//  2. It leaves: its next answer is 429, Retry-After: 5, Connection: close
//     (kube-apiserver's shutdown answer; a GOAWAY on HTTP/2), and at that moment
//     every new SYN to its port is DROPPED (iptables INPUT --syn -j DROP): the
//     destination does not answer, as an endpoint mid-replacement does not.
//     A client socket then resends its SYN only at +1 s, +3 s, +7 s.
//  3. At comeback (3.3 s after the request started) the rule is removed.
//
// The customer request (page budget 9 s) must complete by comeback + 1 s, on a
// connection the RACE opened (not the chain's first socket), and nothing leaks.
// With the race reverted (M-b, applied by the job's mutant leg) the one socket's
// next SYN is at +7 s: done at about 7 s, RED.

import (
	"context"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/transport"
)

func iptablesSYN(t *testing.T, op string, port int) {
	t.Helper()
	out, err := exec.Command("sudo", "-n", "iptables", op, "INPUT", "-p", "tcp", "--syn",
		"--dport", strconv.Itoa(port), "-j", "DROP").CombinedOutput()
	if err != nil {
		t.Fatalf("iptables %s: %v: %s (this arm must drop SYNs; it runs only in the syndrop-1509 CI job)", op, err, out)
	}
}

func TestIssue1509_SYNDrop_RaceRidesThroughADroppingDestination(t *testing.T) {
	resetConnectTimes(t)
	seedReserve(t, 50*time.Millisecond)

	var leaving atomic.Bool
	var open atomic.Int32
	var remoteMu sync.Mutex
	var lastRemote string
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if leaving.CompareAndSwap(true, false) {
			w.Header().Set("Connection", "close")
			w.Header().Set("Retry-After", "5")
			http.Error(w, "the apiserver is shutting down", http.StatusTooManyRequests)
			return
		}
		remoteMu.Lock()
		lastRemote = r.RemoteAddr
		remoteMu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	}))
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
	t.Cleanup(srv.Close)
	_, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)

	rt, err := NewEndpointTransport(&transport.Config{
		TLS:   transport.TLSConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})},
		Proxy: http.ProxyFromEnvironment,
	})
	if err != nil {
		t.Fatal(err)
	}
	cli := &http.Client{Transport: rt}
	warm, err := Send(SendSiteStage, cli, getReq(t, budgetLeft(9*time.Second), srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	_ = warm.Body.Close()
	if warm.ProtoMajor != 2 {
		t.Fatalf("warm request over %s; the arm needs HTTP/2", warm.Proto)
	}

	// Every connect attempt, through the real guarded dialer: when it started
	// and which local address it got, so the winner can be named.
	start := time.Now()
	var attemptsMu sync.Mutex
	type attempt struct {
		at    time.Duration
		local string
	}
	var attempts []attempt
	prev := endpointDialFn
	endpointDialFn = func(ctx context.Context, network, a string) (net.Conn, error) {
		at := time.Since(start)
		c, err := prev(ctx, network, a)
		attemptsMu.Lock()
		local := ""
		if c != nil {
			local = c.LocalAddr().String()
		}
		attempts = append(attempts, attempt{at.Round(time.Millisecond), local})
		attemptsMu.Unlock()
		return c, err
	}
	t.Cleanup(func() { endpointDialFn = prev })

	// 2. Leave, and drop SYNs: the rule goes in before the request, and the
	// probe proves the drop works (a connect that cannot complete).
	leaving.Store(true)
	iptablesSYN(t, "-I", port)
	var removed atomic.Bool
	t.Cleanup(func() {
		if !removed.Load() {
			iptablesSYN(t, "-D", port)
		}
	})
	if c, err := net.DialTimeout("tcp", srv.Listener.Addr().String(), 300*time.Millisecond); err == nil {
		_ = c.Close()
		t.Fatal("a connect completed with the SYN-drop rule in place; the arm needs SYNs dropped")
	}

	// 3. The request; the rule is removed 3.3 s after it started.
	start = time.Now()
	const comeback = 3300 * time.Millisecond
	go func() {
		time.Sleep(comeback)
		iptablesSYN(t, "-D", port)
		removed.Store(true)
	}()
	resp, err := Send(SendSiteStage, cli, getReq(t, budgetLeft(9*time.Second), srv.URL))
	took := time.Since(start)
	attemptsMu.Lock()
	log := fmt.Sprint(attempts)
	attemptsMu.Unlock()
	t.Logf("connect attempts (start, local address): %s", log)
	if err != nil {
		t.Fatalf("after %v: %v", took, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d after %v", resp.StatusCode, took)
	}
	if took > comeback+time.Second {
		t.Fatalf("done %v after start, %v after the destination answered again; want ≤ 1 s after it "+
			"(≈7 s = the one socket waited for its own SYN retransmit: no race)", took, took-comeback)
	}

	// The connection that answered is one the RACE opened: not the chain's
	// first socket.
	remoteMu.Lock()
	winner := lastRemote
	remoteMu.Unlock()
	attemptsMu.Lock()
	index := -1
	for i, a := range attempts {
		if a.local != "" && strings.EqualFold(a.local, winner) {
			index = i
		}
	}
	attemptsMu.Unlock()
	if index < 1 {
		t.Fatalf("the answering connection (%s) was attempt %d; want a raced one (index ≥ 1) — attempts %s", winner, index, log)
	}
	t.Logf("done %v after start (%v after comeback) on attempt %d", took, took-comeback, index)

	// No leaked socket: the server is left with the request's connection only.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && open.Load() > 1 {
		time.Sleep(20 * time.Millisecond)
	}
	if n := open.Load(); n != 1 {
		t.Fatalf("server holds %d connections after the request; want 1 (losing connects must be closed)", n)
	}
}
