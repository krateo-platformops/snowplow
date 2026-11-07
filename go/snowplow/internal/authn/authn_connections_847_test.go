package authn

// authn_connections_847_test.go — #847: the client New builds counts its
// connections under its own egress.

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

func TestNew847_CountsUnderItsEgress(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": makeJWT(time.Now().Add(time.Hour).Unix())})
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
			m[c.Egress+"/"+c.Protocol+"/opened"] = c.Opened
			m[c.Egress+"/"+c.Protocol+"/handovers"] = c.Handovers
		}
		return m
	}
	before := cells()
	if _, err := New(srv.URL, writeTokenFile(t, "sa-token")).Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c := conns.Load(); c != 1 {
		t.Fatalf("the server saw %d connections, want 1", c)
	}
	for k, v := range cells() {
		want := uint64(0)
		if k == "authn/http1/opened" || k == "authn/http1/handovers" {
			want = 1
		}
		if d := v - before[k]; d != want {
			t.Errorf("cell %s moved by %d, want %d", k, d, want)
		}
	}
}
