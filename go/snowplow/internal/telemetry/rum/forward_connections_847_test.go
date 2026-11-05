package rum

// forward_connections_847_test.go — #847: the forwarder the constructor builds
// counts its connections under its own egress.

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

func TestForwarder847_CountsUnderItsEgress(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	cells := func() map[string]uint64 {
		m := map[string]uint64{}
		for _, c := range credhttp.EgressConnectionsSnapshot() {
			m[c.Egress+"/"+c.Protocol+"/"+c.Connection] = c.Count
		}
		return m
	}
	before := cells()
	f := NewForwarder(srv.URL)
	for range 2 {
		f.post(context.Background(), batch{body: []byte("spans"), spans: 1})
	}
	if c := conns.Load(); c != 1 {
		t.Fatalf("the server saw %d connections, want 1", c)
	}
	for k, v := range cells() {
		want := uint64(0)
		switch k {
		case "rum_forward/http1/new", "rum_forward/http1/reused":
			want = 1
		}
		if d := v - before[k]; d != want {
			t.Errorf("cell %s moved by %d, want %d", k, d, want)
		}
	}
}
