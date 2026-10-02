// phase1_readiness_exit_expvar_407_test.go — #407: the phase-1 readiness-exit
// record is published as the expvar map snowplow_phase1_readiness_exit, so the
// success-path (outcome=latch, logged at INFO) timings and nav counts are
// readable on a LOG_LEVEL=warn production pod.
//
//	E0 before the exit the map is the empty object {}.
//	E1 latch exit (the REAL phase1WarmupWith + seedScopeYielding path, X1's
//	   fixture): the map holds EVERY field, each equal to the line's attribute
//	   and to the known nav counts.
//	E2 deadline exit (X2's wedged-worker fixture): likewise, with
//	   outcome=deadline, deadline_cause=phase1_timeout, nav_units_remaining=3.
//	E3 exact values: explicit step durations come back as exact *_ms values.
//	E4 set exactly once: keepwarm, a re-walk and a second record do not store a
//	   second record or move any value.
//	E5 boot_aborted (#401, the REAL releasePhase1BootAborted): abort_cause is
//	   set, deadline_cause is EMPTY (the #409 split survives into the map), and
//	   the map equals the line.
//
// RED without #407: snowplow_phase1_readiness_exit is not published (absent
// from /debug/vars) and the record is never stored.
//
// Serializes on engineLatchTestMu (shares the latch singleton, the seed seams
// and the default slog logger with the sibling #397 tests).

package dispatchers

import (
	"context"
	"encoding/json"
	"expvar"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// phase1ExitExpvarKeys is every field of the record (= every line attribute
// except the prose `effect`).
var phase1ExitExpvarKeys = []string{
	"outcome", "deadline_cause", "abort_cause", "latch_fired", "elapsed_ms", "since_process_start_ms",
	"nav_latch_armed", "nav_boot_passes", "nav_units_total", "nav_units_seeded",
	"nav_units_remaining", "cohorts", "nav_units_expected_deny",
	"nav_units_operational_failure", "nav_units_aborted",
	"walk_ms", "sync_wait_ms", "content_prewarm_ms", "cluster_list_prewarm_ms", "seed_ms",
}

// readExitExpvar reads snowplow_phase1_readiness_exit through the REAL
// /debug/vars handler, so the published key and its JSON are what an operator
// sees.
func readExitExpvar(t *testing.T) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	expvar.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/vars", nil))
	var all map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &all); err != nil {
		t.Fatalf("decode /debug/vars: %v", err)
	}
	raw, ok := all["snowplow_phase1_readiness_exit"]
	if !ok {
		t.Fatal("#407 RED: /debug/vars has no snowplow_phase1_readiness_exit key")
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("snowplow_phase1_readiness_exit is not a JSON object: %s (%v)", raw, err)
	}
	return m
}

// assertExpvarMatchesLine checks the map holds exactly the record's key set and
// that every value equals the readiness_exit line's attribute.
func assertExpvarMatchesLine(t *testing.T, m map[string]any, line exitRecord) {
	t.Helper()
	var got []string
	for k := range m {
		got = append(got, k)
	}
	sort.Strings(got)
	want := append([]string{}, phase1ExitExpvarKeys...)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("#407 RED: expvar key set = %v, want %v", got, want)
	}
	for _, k := range phase1ExitExpvarKeys {
		lv, ok := line.attrs[k]
		if !ok {
			t.Fatalf("line has no %q attribute", k)
		}
		switch v := m[k].(type) {
		case string:
			if v != lv.String() {
				t.Errorf("%s: expvar %q, line %q", k, v, lv.String())
			}
		case bool:
			if v != lv.Bool() {
				t.Errorf("%s: expvar %v, line %v", k, v, lv.Bool())
			}
		case float64:
			if int64(v) != lv.Int64() {
				t.Errorf("%s: expvar %v, line %d", k, v, lv.Int64())
			}
		default:
			t.Errorf("%s: unexpected expvar type %T", k, m[k])
		}
	}
}

func expInt(t *testing.T, m map[string]any, k string) int64 {
	t.Helper()
	v, ok := m[k].(float64)
	if !ok {
		t.Fatalf("expvar %s = %v (%T), want a number", k, m[k], m[k])
	}
	return int64(v)
}

func TestIssue407_E0_BeforeExit_EmptyObject(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	resetExitWorld(t)
	if m := readExitExpvar(t); len(m) != 0 {
		t.Fatalf("before the exit the map must be the empty object {}, got %v", m)
	}
	if _, ok := Phase1ReadinessExitTimings(); ok {
		t.Fatal("Phase1ReadinessExitTimings must report !ok before the exit")
	}
}

func TestIssue407_E1_LatchExit_ExpvarHoldsEveryField(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	rw := phase1TestWatcher(t)
	resetExitWorld(t)
	logs := captureExitLogs(t)

	widgets := exitFixture(t, func(_ context.Context, widget, cohort string) error {
		if widget == "w-b" && cohort == "group:ops" {
			return forbidden403()
		}
		return nil
	})
	ensureFirstNavLatch()
	seed := pipSeedFn(func(ctx context.Context) error {
		return seedScopeYielding(ctx, nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := phase1WarmupWith(ctx, rw, exitNoRoots, exitNoResolve, nil, nil, seed, nil); err != nil {
		t.Fatalf("phase1WarmupWith: %v", err)
	}
	ex := logs.exits()
	if len(ex) != 1 {
		t.Fatalf("want ONE readiness_exit line, got %d", len(ex))
	}
	m := readExitExpvar(t)
	assertExpvarMatchesLine(t, m, ex[0])
	if m["outcome"] != "latch" || m["deadline_cause"] != "" || m["abort_cause"] != "" || m["latch_fired"] != true {
		t.Errorf("E1: outcome/cause/latch_fired = %v/%v/%v, want latch/\"\"/true", m["outcome"], m["deadline_cause"], m["latch_fired"])
	}
	for k, want := range map[string]int64{
		"nav_units_total": 4, "nav_units_seeded": 4, "nav_units_remaining": 0, "cohorts": 2,
		"nav_units_expected_deny": 1, "nav_units_operational_failure": 0, "nav_units_aborted": 0,
		"nav_boot_passes": 1,
		// contentWarm / clusterListPrewarm are nil in this fixture: not run.
		"content_prewarm_ms": -1, "cluster_list_prewarm_ms": -1,
	} {
		if got := expInt(t, m, k); got != want {
			t.Errorf("E1: %s = %d, want %d", k, got, want)
		}
	}
	for _, k := range []string{"elapsed_ms", "since_process_start_ms", "walk_ms", "sync_wait_ms", "seed_ms"} {
		if expInt(t, m, k) < 0 {
			t.Errorf("E1: %s must be measured (>=0) on the seed path, got %d", k, expInt(t, m, k))
		}
	}
	if got := phase1ExitRecSets.Load(); got != 1 {
		t.Errorf("E1: record stored %d times, want 1", got)
	}
}

func TestIssue407_E2_DeadlineExit_ExpvarHoldsEveryField(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	rw := phase1TestWatcher(t)
	resetExitWorld(t)
	logs := captureExitLogs(t)

	release := make(chan struct{})
	secondEntered := make(chan struct{})
	var calls int
	var callsMu sync.Mutex
	widgets := exitFixture(t, func(ctx context.Context, _, _ string) error {
		callsMu.Lock()
		calls++
		n := calls
		callsMu.Unlock()
		if n == 1 {
			return nil
		}
		if n == 2 {
			close(secondEntered)
		}
		<-release
		return nil
	})
	latch := ensureFirstNavLatch()
	workerDone := make(chan struct{})
	seed := pipSeedFn(func(pctx context.Context) error {
		go func() {
			defer close(workerDone)
			_ = seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot)
		}()
		<-secondEntered
		select {
		case <-latch.wait():
			return nil
		case <-pctx.Done():
			return pctx.Err()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_ = phase1WarmupWith(ctx, rw, exitNoRoots, exitNoResolve, nil, nil, seed, nil)
	released := false
	releaseOnce := func() {
		if !released {
			released = true
			close(release)
		}
	}
	defer releaseOnce()
	if !cache.IsPhase1Done() {
		t.Fatal("readiness did not flip")
	}
	ex := logs.exits()
	if len(ex) != 1 {
		t.Fatalf("want ONE readiness_exit line, got %d", len(ex))
	}
	m := readExitExpvar(t)
	before, _ := json.Marshal(m)
	assertExpvarMatchesLine(t, m, ex[0])
	if m["outcome"] != "deadline" || m["deadline_cause"] != "phase1_timeout" || m["abort_cause"] != "" || m["latch_fired"] != false {
		t.Errorf("E2: outcome/cause/latch_fired = %v/%v/%v, want deadline/phase1_timeout/false", m["outcome"], m["deadline_cause"], m["latch_fired"])
	}
	for k, want := range map[string]int64{
		"nav_units_total": 4, "nav_units_seeded": 1, "nav_units_remaining": 3, "cohorts": 2,
	} {
		if got := expInt(t, m, k); got != want {
			t.Errorf("E2: %s = %d, want %d", k, got, want)
		}
	}
	// The PHASE1 ctx (1500ms) bounds the whole warmup: elapsed reaches it, and
	// the seed step ran (>0) until it.
	if got := expInt(t, m, "elapsed_ms"); got < 1400 {
		t.Errorf("E2: elapsed_ms = %d, want >= ~1500 (the warmup ran until the PHASE1 deadline)", got)
	}
	if got := expInt(t, m, "seed_ms"); got <= 0 {
		t.Errorf("E2: seed_ms = %d, want > 0 (the seed ran until the deadline)", got)
	}
	// content / cluster_list prewarm are nil in this fixture: not run → -1.
	if expInt(t, m, "content_prewarm_ms") != -1 || expInt(t, m, "cluster_list_prewarm_ms") != -1 {
		t.Errorf("E2: not-run steps must read -1; got content=%d cluster_list=%d",
			expInt(t, m, "content_prewarm_ms"), expInt(t, m, "cluster_list_prewarm_ms"))
	}

	// The late latch fire must not move the published record.
	releaseOnce()
	select {
	case <-workerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not finish after release")
	}
	after, _ := json.Marshal(readExitExpvar(t))
	if string(before) != string(after) {
		t.Errorf("E2: a late latch fire moved the record:\n before %s\n after  %s", before, after)
	}
	if got := phase1ExitRecSets.Load(); got != 1 {
		t.Errorf("E2: record stored %d times, want 1", got)
	}
}

func TestIssue407_E3_ExplicitTimings_ExactValues(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	resetExitWorld(t)
	_ = captureExitLogs(t)
	recordPhase1ReadinessExit(phase1ExitDeadline, "pip_global_timeout", 9500*time.Millisecond, phase1StepTimings{
		walk: 1100 * time.Millisecond, syncWait: 2200 * time.Millisecond, content: 3300 * time.Millisecond,
		clusterList: 440 * time.Millisecond, seed: 2460 * time.Millisecond,
	})
	m := readExitExpvar(t)
	for k, want := range map[string]int64{
		"elapsed_ms": 9500, "walk_ms": 1100, "sync_wait_ms": 2200, "content_prewarm_ms": 3300,
		"cluster_list_prewarm_ms": 440, "seed_ms": 2460,
	} {
		if got := expInt(t, m, k); got != want {
			t.Errorf("E3: %s = %d, want %d", k, got, want)
		}
	}
	if m["outcome"] != "deadline" || m["deadline_cause"] != "pip_global_timeout" {
		t.Errorf("E3: outcome/cause = %v/%v", m["outcome"], m["deadline_cause"])
	}
	tm, ok := Phase1ReadinessExitTimings()
	if !ok || tm != (Phase1ExitTimings{ElapsedMs: 9500, WalkMs: 1100, SyncWaitMs: 2200, ContentPrewarmMs: 3300, ClusterListPrewarmMs: 440, SeedMs: 2460}) {
		t.Errorf("E3: Phase1ReadinessExitTimings = %+v ok=%v", tm, ok)
	}
}

func TestIssue407_E4_SetExactlyOnce(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	rw := phase1TestWatcher(t)
	resetExitWorld(t)
	_ = captureExitLogs(t)

	widgets := exitFixture(t, func(context.Context, string, string) error { return nil })
	ensureFirstNavLatch()
	seed := pipSeedFn(func(ctx context.Context) error {
		return seedScopeYielding(ctx, nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := phase1WarmupWith(ctx, rw, exitNoRoots, exitNoResolve, nil, nil, seed, nil); err != nil {
		t.Fatalf("phase1WarmupWith: %v", err)
	}
	firstMap := readExitExpvar(t)
	first, _ := json.Marshal(firstMap)
	firstPtr := phase1ExitRec.Load()
	// Non-vacuity: the exit DID store a record. Without these, a recorder that
	// never stores passes the identity / unchanged-map checks below trivially
	// (nil == nil, {} == {}).
	if firstPtr == nil {
		t.Fatal("E4 RED: the latch exit stored no record")
	}
	if firstMap["outcome"] != "latch" {
		t.Fatalf("E4 RED: the first map has no latch outcome before the re-drive: %v", firstMap)
	}

	if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeKeepwarm); err != nil {
		t.Fatalf("keepwarm: %v", err)
	}
	if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("re-walk: %v", err)
	}
	recordPhase1ReadinessExit(phase1ExitDeadline, "phase1_timeout", time.Hour, phase1StepTimings{walk: time.Hour})
	RecordPhase1ReadinessExitNoSeed()

	if got := phase1ExitRecSets.Load(); got != 1 {
		t.Fatalf("E4 RED: record stored %d times, want exactly 1", got)
	}
	if phase1ExitRec.Load() != firstPtr {
		t.Fatal("E4: the stored record was replaced")
	}
	if again, _ := json.Marshal(readExitExpvar(t)); string(again) != string(first) {
		t.Errorf("E4: the map moved after the exit:\n first %s\n now   %s", first, again)
	}
}

func TestIssue407_E5_BootAborted_AbortCauseSet_DeadlineCauseEmpty(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	resetExitWorld(t)
	logs := captureExitLogs(t)

	for _, cause := range []string{phase1AbortNoSAEndpoint, phase1AbortNoDynClient} {
		t.Run(cause, func(t *testing.T) {
			resetExitWorld(t)
			releasePhase1BootAborted(cause)
			if !cache.IsPhase1Done() {
				t.Fatal("a boot abort must release readiness (#401)")
			}
			ex := logs.exits()
			if len(ex) == 0 {
				t.Fatal("no readiness_exit line")
			}
			m := readExitExpvar(t)
			assertExpvarMatchesLine(t, m, ex[len(ex)-1])
			if m["outcome"] != "boot_aborted" {
				t.Errorf("outcome = %v, want boot_aborted", m["outcome"])
			}
			if m["abort_cause"] != cause {
				t.Errorf("#407 RED: abort_cause = %v, want %q", m["abort_cause"], cause)
			}
			if m["deadline_cause"] != "" {
				t.Errorf("#407 RED: deadline_cause = %v on a boot abort, want empty (the abort cause leaked)", m["deadline_cause"])
			}
			for _, k := range []string{"walk_ms", "sync_wait_ms", "content_prewarm_ms", "cluster_list_prewarm_ms", "seed_ms"} {
				if got := expInt(t, m, k); got != -1 {
					t.Errorf("%s = %d on a pre-walk abort, want -1 (step not run)", k, got)
				}
			}
		})
	}
}
