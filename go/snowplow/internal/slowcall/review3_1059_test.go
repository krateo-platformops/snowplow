package slowcall

// The #1059 round-3 (delta) reviewer's arms (issuecomment-6602189006), adopted as written: settle()
// for a cut in each step, and a cut after the connection is up.

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/http/util"
)

// cutAttempt runs one dial on a /call request whose context ends at cut, with
// client cli and URL u, and returns its record.
func cutAttempt(t *testing.T, cut time.Duration, cli *http.Client, u string) map[string]any {
	t.Helper()
	m, buf := testMonitor(t, 50*time.Millisecond)
	serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
		cctx, cancel := context.WithTimeout(r.Context(), cut)
		defer cancel()
		ctx, dial := StartDial(cctx, BranchExternalFetch, "GET", "")
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		resp, err := util.NewRetryClient(cli).Do(req)
		if err == nil {
			resp.Body.Close()
		}
		dial.EndErr(cctx, err)
	})
	recs := buf.records(t, "slowcall.request")
	if len(recs) != 1 {
		t.Fatalf("PRECONDITION: %d records", len(recs))
	}
	t.Logf("detail: %v", recs[0]["wait_attempt_detail"])
	return recs[0]
}

// A TLS handshake the server never answers (it accepts and stays silent),
// cut at 400 ms: cause tls_handshake, tls ≈ 400 ms, connect done and small.
func TestReview3_CutMidTLSNamesTheHandshake(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	tr := &http.Transport{TLSHandshakeTimeout: 10 * time.Second}
	r := cutAttempt(t, 400*time.Millisecond, &http.Client{Transport: Transport(tr)}, "https://"+ln.Addr().String()+"/x")
	d, _ := r["wait_attempt_detail"].(string)
	if !strings.HasPrefix(d, "1 tls_handshake ") {
		t.Errorf("TLS-CUT RED: detail %q, want cause tls_handshake", d)
	}
	if got := num(t, r, "wait_tls_ms"); got < 300 {
		t.Errorf("TLS-CUT RED: wait_tls_ms = %d, want ≈ 400", got)
	}
	if got := num(t, r, "wait_connect_ms"); got > 200 {
		t.Errorf("TLS-CUT RED: wait_connect_ms = %d, want small (the connect finished)", got)
	}
}

// A cut while the server is still answering (connection up, request written):
// no step is in progress, so settle claims nothing: cause connection, connect
// as measured, never dial.
func TestReview3_CutAfterConnectIsNotADialCause(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	}))
	defer srv.Close()
	r := cutAttempt(t, 400*time.Millisecond, &http.Client{Transport: Transport(&http.Transport{})}, srv.URL+"/x")
	d, _ := r["wait_attempt_detail"].(string)
	if !strings.HasPrefix(d, "1 connection ") {
		t.Errorf("POSTCONNECT RED: detail %q, want cause connection (the connection was up)", d)
	}
	if got := num(t, r, "wait_connect_ms"); got > 200 {
		t.Errorf("POSTCONNECT RED: wait_connect_ms = %d, want the real (small) connect, not the time to the cut", got)
	}
}

// A DNS lookup that never answers, cut at 400 ms: cause dns, dns ≈ 400 ms.
func TestReview3_CutMidDNSNamesTheLookup(t *testing.T) {
	res := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	d := &net.Dialer{Timeout: 10 * time.Second, Resolver: res}
	tr := &http.Transport{DialContext: d.DialContext}
	r := cutAttempt(t, 400*time.Millisecond, &http.Client{Transport: Transport(tr)}, "http://s1054-review.invalid-zone-for-test/x")
	det, _ := r["wait_attempt_detail"].(string)
	if !strings.HasPrefix(det, "1 dns ") {
		t.Errorf("DNS-CUT RED: detail %q, want cause dns", det)
	}
	if got := num(t, r, "wait_dns_ms"); got < 300 {
		t.Errorf("DNS-CUT RED: wait_dns_ms = %d, want ≈ 400", got)
	}
}
