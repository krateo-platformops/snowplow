package api

// egress_setup_races_expvar_test.go — #1521's race per egress on /debug/vars (Diego 2026-11-18, "add counters"):
// snowplow_egress_connections_setup_races {egress: {races_fired, attempts_replaced, stuck_dropped}}, every closed
// egress present from the start, read the way /debug/vars serves it, next to snowplow_egress_connections whose own
// shape it leaves unchanged.

import (
	"encoding/json"
	"expvar"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

const setupRacesExpvarChildEnv = "SNOWPLOW_SETUP_RACES_EXPVAR_CHILD"

// The key is published only with the cache on (decided in init, CFG-1): the arm runs in a child with the cache on.
func TestEgressSetupRaces_ExpvarShape(t *testing.T) {
	if os.Getenv(setupRacesExpvarChildEnv) == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestEgressSetupRaces_ExpvarShape$", "-test.v")
		cmd.Env = append(os.Environ(), setupRacesExpvarChildEnv+"=1", "CACHE_ENABLED=true")
		out, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), "--- PASS: TestEgressSetupRaces_ExpvarShape") {
			t.Fatalf("the child arm failed: %v\n%s", err, out)
		}
		return
	}
	v := expvar.Get("snowplow_egress_connections_setup_races")
	if v == nil {
		t.Fatal("snowplow_egress_connections_setup_races is not published with the cache on")
	}
	var got map[string]map[string]uint64
	if err := json.Unmarshal([]byte(v.String()), &got); err != nil {
		t.Fatalf("not {egress: {races_fired, attempts_replaced, stuck_dropped}}: %v: %s", err, v.String())
	}
	if len(got) != len(credhttp.Egresses) {
		t.Fatalf("%d egresses published, want the closed set's %d: %s", len(got), len(credhttp.Egresses), v.String())
	}
	for _, e := range credhttp.Egresses {
		if len(got[e]) != 3 {
			t.Fatalf("egress %s: %v, want exactly races_fired, attempts_replaced, stuck_dropped", e, got[e])
		}
		for _, k := range []string{"races_fired", "attempts_replaced", "stuck_dropped"} {
			if _, ok := got[e][k]; !ok {
				t.Fatalf("egress %s has no %s: every cell is published from the start", e, k)
			}
		}
	}
	// and snowplow_egress_connections keeps its own shape: {egress: {protocol: {opened, handovers}}}
	var conns map[string]map[string]map[string]uint64
	if err := json.Unmarshal([]byte(expvar.Get("snowplow_egress_connections").String()), &conns); err != nil {
		t.Fatalf("snowplow_egress_connections changed shape: %v", err)
	}
}
