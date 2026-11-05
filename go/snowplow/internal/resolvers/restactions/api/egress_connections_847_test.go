package api

// egress_connections_847_test.go — #847 at the stage level: a real stage fetch
// counts under the stage egress, and the expvar key the paired run reads carries
// it. The credhttp arms cover the counter itself (credhttp/connections_847_test.go).

import (
	"context"
	"encoding/json"
	"expvar"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// stageServer847 is a plain http:// endpoint (always HTTP/1) that counts its
// connections on the server's side of the wire.
func stageServer847(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, &conns
}

func stageCells847() map[string]uint64 {
	m := map[string]uint64{}
	for _, c := range credhttp.EgressConnectionsSnapshot() {
		m[c.Egress+"/"+c.Protocol+"/"+c.Connection] = c.Count
	}
	return m
}

// Ten fetches of one external endpoint: one connection on the wire, one new and
// nine reused in the counter, under the stage egress and nowhere else.
func TestEgressConnections847_StageFetchCountsUnderStage(t *testing.T) {
	srv, conns := stageServer847(t)
	ep := &endpoints.Endpoint{ServerURL: srv.URL, Token: "third-party-token"}
	before := stageCells847()
	for range 10 {
		if err := fetch1088(t, context.Background(), ep, http.MethodGet); err != nil {
			t.Fatal(err)
		}
	}
	if c := conns.Load(); c != 1 {
		t.Fatalf("the server saw %d connections, want 1", c)
	}
	for k, v := range stageCells847() {
		want := uint64(0)
		switch k {
		case "stage/http1/new":
			want = 1
		case "stage/http1/reused":
			want = 9
		}
		if d := v - before[k]; d != want {
			t.Errorf("cell %s moved by %d, want %d", k, d, want)
		}
	}
}

const egressExpvarChildEnv = "SNOWPLOW_847_EXPVAR_CHILD"

// The expvar key is published only when the cache is on, which is decided in
// init (CFG-1), before any test can set it. So the arm runs itself in a child
// process with the cache on, and the child reads the key the way /debug/vars does.
func TestEgressConnections847_Expvar(t *testing.T) {
	if os.Getenv(egressExpvarChildEnv) == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestEgressConnections847_Expvar$", "-test.v")
		cmd.Env = append(os.Environ(), egressExpvarChildEnv+"=1", "CACHE_ENABLED=true")
		out, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), "--- PASS: TestEgressConnections847_Expvar") {
			t.Fatalf("the child arm failed: %v\n%s", err, out)
		}
		return
	}
	read := func() map[string]map[string]map[string]uint64 {
		t.Helper()
		v := expvar.Get("snowplow_egress_connections_total")
		if v == nil {
			t.Fatal("snowplow_egress_connections_total is not published with the cache on")
		}
		var got map[string]map[string]map[string]uint64
		if err := json.Unmarshal([]byte(v.String()), &got); err != nil {
			t.Fatalf("not {egress: {protocol: {connection: n}}}: %v: %s", err, v.String())
		}
		return got
	}
	before := read()
	for _, e := range credhttp.Egresses {
		for _, p := range credhttp.ConnProtocols {
			for _, c := range credhttp.ConnStates {
				if _, ok := before[e][p][c]; !ok {
					t.Errorf("no cell %s/%s/%s: every cell is published from the start", e, p, c)
				}
			}
		}
	}
	srv, _ := stageServer847(t)
	ep := &endpoints.Endpoint{ServerURL: srv.URL, Token: "third-party-token"}
	for range 2 {
		if err := fetch1088(t, context.Background(), ep, http.MethodGet); err != nil {
			t.Fatal(err)
		}
	}
	after := read()
	if d := after["stage"]["http1"]["new"] - before["stage"]["http1"]["new"]; d != 1 {
		t.Errorf("new moved by %d, want 1", d)
	}
	if d := after["stage"]["http1"]["reused"] - before["stage"]["http1"]["reused"]; d != 1 {
		t.Errorf("reused moved by %d, want 1", d)
	}
}
