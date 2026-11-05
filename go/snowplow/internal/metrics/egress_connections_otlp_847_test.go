package metrics

// egress_connections_otlp_847_test.go — #847's counter reaches OTLP, hand-wired
// in metrics.go (no expvar->OTLP bridge), observed via the REAL callback over a
// ManualReader. The connections are real, and the server counts its own.

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

func TestEgressConnections847_ReachOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	const name = "snowplow_egress_connections_total"
	cells := func() map[string]int64 {
		t.Helper()
		m := map[string]int64{}
		for _, e := range credhttp.Egresses {
			for _, p := range credhttp.ConnProtocols {
				for _, c := range credhttp.ConnStates {
					attrs := map[string]string{"egress": e, "protocol": p, "connection": c}
					v, ok := find643(flatten(collectViaRealCallback(t, "deadbeef")), name, attrs)
					if !ok {
						t.Fatalf("no %s%v series: every cell is observed from the start", name, attrs)
					}
					m[e+"/"+p+"/"+c] = v
				}
			}
		}
		return m
	}
	before := cells()
	cli := srv.Client()
	for range 3 {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := credhttp.Send(credhttp.EgressStage, cli, req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	if c := conns.Load(); c != 1 {
		t.Fatalf("the server saw %d connections, want 1", c)
	}
	after := cells()
	for k, v := range after {
		want := int64(0)
		switch k {
		case "stage/http1/new":
			want = 1
		case "stage/http1/reused":
			want = 2
		}
		if d := v - before[k]; d != want {
			t.Errorf("%s{%s} moved by %d, want %d", name, k, d, want)
		}
	}
}
