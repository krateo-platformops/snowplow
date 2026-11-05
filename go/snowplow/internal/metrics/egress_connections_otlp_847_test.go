package metrics

// egress_connections_otlp_847_test.go — #847's counters reach OTLP, hand-wired
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

	const (
		openedName    = "snowplow_egress_connections_opened_total"
		handoversName = "snowplow_egress_connection_handovers_total"
	)
	cells := func() map[string]int64 {
		t.Helper()
		all := flatten(collectViaRealCallback(t, "deadbeef"))
		m := map[string]int64{}
		for _, e := range credhttp.Egresses {
			for _, p := range credhttp.ConnProtocols {
				for _, name := range []string{openedName, handoversName} {
					attrs := map[string]string{"egress": e, "protocol": p}
					v, ok := find643(all, name, attrs)
					if !ok {
						t.Fatalf("no %s%v series: every cell is observed from the start", name, attrs)
					}
					m[name+"/"+e+"/"+p] = v
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
	for k, v := range cells() {
		want := int64(0)
		switch k {
		case openedName + "/stage/http1":
			want = 1
		case handoversName + "/stage/http1":
			want = 3
		}
		if d := v - before[k]; d != want {
			t.Errorf("%s moved by %d, want %d", k, d, want)
		}
	}
}
