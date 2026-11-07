package api

// egress_connections_847_test.go — #847 at the stage level: a real stage fetch
// counts under the stage egress, and the expvar key the paired run reads carries
// it. The credhttp arms cover the counter itself (credhttp/connections_847_test.go).

import (
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
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
		m[c.Egress+"/"+c.Protocol+"/opened"] = c.Opened
		m[c.Egress+"/"+c.Protocol+"/handovers"] = c.Handovers
	}
	return m
}

// Ten fetches of one external endpoint: opened is what the server accepted and
// handovers is one per fetch, under the stage egress and nowhere else.
func TestEgressConnections847_StageFetchCountsUnderStage(t *testing.T) {
	srv, conns := stageServer847(t)
	ep := &endpoints.Endpoint{ServerURL: srv.URL, Token: "third-party-token"}
	before := stageCells847()
	for range 10 {
		if err := fetch1088(t, context.Background(), ep, http.MethodGet); err != nil {
			t.Fatal(err)
		}
	}
	for k, v := range stageCells847() {
		want := uint64(0)
		switch k {
		case "stage/http1/opened":
			want = uint64(conns.Load())
		case "stage/http1/handovers":
			want = 10
		}
		if d := v - before[k]; d != want {
			t.Errorf("cell %s moved by %d, want %d", k, d, want)
		}
	}
}

// The live clientconfig shape, concurrently: real stage fetches over the owned
// dialer (credhttp.EndpointDialHolder) and TLS. opened is what the server
// accepted, surplus dials included, and handovers is what it served.
func TestEgressConnections847_ConcurrentStageMatchesTheServer(t *testing.T) {
	const workers, perWorker = 16, 10
	p := newPKI1088(t)
	cs := newConnServer1088(t, p, false)
	ep := certEndpoint1088(t, p, cs.srv.URL, "user-847")
	before := stageCells847()
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				cp := *ep
				st, _, _, err := httpFetchAllowingNonJSON(context.Background(), httpcall.RequestOptions{
					RequestInfo: httpcall.RequestInfo{Path: "/api/v1/namespaces/ns/configmaps/cm", Verb: ptr.To(http.MethodGet)},
					Endpoint:    &cp,
				})
				if err == nil && st.Code != http.StatusOK {
					err = errors.New(st.Message)
				}
				if err != nil {
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
	if h2, r := cs.h2.Load(), cs.reqs.Load(); h2 != workers*perWorker || r != workers*perWorker {
		t.Fatalf("the server served %d requests, %d of them HTTP/2; want %d of both", r, h2, workers*perWorker)
	}
	opened := func() uint64 { return stageCells847()["stage/http2/opened"] - before["stage/http2/opened"] }
	for deadline := time.Now().Add(10 * time.Second); opened() != uint64(cs.connections()) && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	for k, v := range stageCells847() {
		want := uint64(0)
		switch k {
		case "stage/http2/opened":
			want = uint64(cs.connections())
		case "stage/http2/handovers":
			want = workers * perWorker
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
		v := expvar.Get("snowplow_egress_connections")
		if v == nil {
			t.Fatal("snowplow_egress_connections is not published with the cache on")
		}
		var got map[string]map[string]map[string]uint64
		if err := json.Unmarshal([]byte(v.String()), &got); err != nil {
			t.Fatalf("not {egress: {protocol: {opened, handovers}}}: %v: %s", err, v.String())
		}
		return got
	}
	before := read()
	for _, e := range credhttp.Egresses {
		for _, p := range credhttp.ConnProtocols {
			for _, c := range []string{"opened", "handovers"} {
				if _, ok := before[e][p][c]; !ok {
					t.Errorf("no cell %s/%s/%s: every cell is published from the start", e, p, c)
				}
			}
		}
	}
	srv, conns := stageServer847(t)
	ep := &endpoints.Endpoint{ServerURL: srv.URL, Token: "third-party-token"}
	for range 2 {
		if err := fetch1088(t, context.Background(), ep, http.MethodGet); err != nil {
			t.Fatal(err)
		}
	}
	after := read()
	if d, want := after["stage"]["http1"]["opened"]-before["stage"]["http1"]["opened"], uint64(conns.Load()); d != want {
		t.Errorf("opened moved by %d, want the %d the server accepted", d, want)
	}
	if d := after["stage"]["http1"]["handovers"] - before["stage"]["http1"]["handovers"]; d != 2 {
		t.Errorf("handovers moved by %d, want 2", d)
	}
}
