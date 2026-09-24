// rbac_subgen_counters_test.go — #247 key-rotation vantage arms.
//
// WHAT THESE PIN. The L1 key folds RBACSubGenForSubject, which sums the
// requesting identity's PER-SUBJECT counters. Before #247 the only published
// RBAC counter was snowplow_rbac_publish_seq — GLOBAL, one bump per snapshot
// publish — one link too early on the chain publish -> per-subject bump -> key
// minted -> resident excess, so it cannot say whether any key rotated. Two
// counters close that second link: bumps_total (is the key rotating at all) and
// subjects_tracked (over how wide a blast radius).
//
// The arms and what each would catch:
//
//  1. Distinct accounting — N bumps over M subjects must read N and M. RED: a
//     bumps_total that counted CALLS instead of subjects (it would read the
//     number of BumpSubjectSubGens invocations, not N).
//  2. Re-bump — bumping an already-tracked subject raises bumps_total and NOT
//     subjects_tracked. RED: incrementing the subject counter on the Load fast
//     path, which would make subjects_tracked a duplicate of bumps_total and
//     destroy the blast-radius signal entirely.
//  3. Concurrent creation (-race) — G goroutines racing to create the SAME M
//     subjects must still read exactly M. RED: incrementing unconditionally on
//     the LoadOrStore branch instead of only when loaded==false; every racer
//     that lost the store would over-count, inflating the denominator.
//     Sequential arms cannot catch this — only the concurrent one can.
//  4. Zero-visibility — both expvar keys must be PRESENT at /debug/vars while
//     the values are 0. RED: any registration that only publishes once a bump
//     has happened; a missing key reads as "not instrumented" and reintroduces
//     exactly the vantage hole this work exists to close.

package cache

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// TestRBACSubGenCounters_DistinctSubjectsAndCumulativeBumps is arm (1)+(2):
// the two counters must move on different axes.
func TestRBACSubGenCounters_DistinctSubjectsAndCumulativeBumps(t *testing.T) {
	ResetRBACSubGenForTest()
	t.Cleanup(ResetRBACSubGenForTest)

	if got := RBACSubGenBumpsTotal(); got != 0 {
		t.Fatalf("setup: bumps_total = %d after reset; want 0 (the arm would not be measuring from a known floor)", got)
	}
	if got := RBACSubGenSubjectsTracked(); got != 0 {
		t.Fatalf("setup: subjects_tracked = %d after reset; want 0", got)
	}

	// M distinct subjects, spread over the three subject KINDS the fold reads,
	// so the accounting cannot be accidentally correct for users only.
	subjects := []subjectKey{
		{Kind: subjectKindUser, Name: "alice"},
		{Kind: subjectKindUser, Name: "bob"},
		{Kind: subjectKindGroup, Name: "devs"},
		{Kind: subjectKindGroup, Name: "on-call"},
		{Kind: subjectKindServiceAccount, Name: "snowplow", Namespace: "krateo-system"},
	}
	const wantSubjects = 5
	if len(subjects) != wantSubjects {
		t.Fatalf("setup: fixture has %d subjects, the arm asserts %d", len(subjects), wantSubjects)
	}

	// N bumps, delivered as several calls of DIFFERENT batch sizes — a
	// bumps_total that counted calls rather than subjects reads 3 here, not 8.
	BumpSubjectSubGens(subjects[:2]) // 2
	BumpSubjectSubGens(subjects[2:]) // 3
	BumpSubjectSubGens(subjects[:3]) // 3  (all three already tracked)
	const wantBumps = 8

	if got := RBACSubGenBumpsTotal(); got != wantBumps {
		t.Errorf("bumps_total = %d; want %d — the counter must add len(subjects) per call, "+
			"not one per call (a per-call counter cannot say how fast the key rotates)", got, wantBumps)
	}
	if got := RBACSubGenSubjectsTracked(); got != wantSubjects {
		t.Errorf("subjects_tracked = %d; want %d — the third call re-bumped three ALREADY-tracked "+
			"subjects and must not widen the blast radius", got, wantSubjects)
	}

	// Arm (2) isolated: one more bump of a subject that already has a counter.
	beforeBumps := RBACSubGenBumpsTotal()
	beforeSubjects := RBACSubGenSubjectsTracked()

	BumpSubjectSubGens([]subjectKey{subjects[0]})

	if got, want := RBACSubGenBumpsTotal(), beforeBumps+1; got != want {
		t.Errorf("after re-bumping a tracked subject: bumps_total = %d; want %d", got, want)
	}
	if got := RBACSubGenSubjectsTracked(); got != beforeSubjects {
		t.Errorf("after re-bumping a TRACKED subject: subjects_tracked = %d; want %d unchanged — "+
			"a subjects_tracked that tracks bumps_total is not a blast-radius signal, it is a copy",
			got, beforeSubjects)
	}

	// The counters must describe the same population the map holds.
	var mapEntries uint64
	rbacSubGen.Range(func(_, _ any) bool { mapEntries++; return true })
	if mapEntries != RBACSubGenSubjectsTracked() {
		t.Errorf("subjects_tracked = %d but rbacSubGen holds %d entries — the published "+
			"denominator has drifted from the thing it claims to count",
			RBACSubGenSubjectsTracked(), mapEntries)
	}
}

// TestRBACSubGenCounters_ConcurrentCreationCountsEachSubjectOnce is arm (3),
// the -race arm. G goroutines race to CREATE the same M subjects and then keep
// bumping them. Every goroutine but one loses the LoadOrStore for each subject;
// counting those losers would inflate subjects_tracked above M.
//
// This is a genuine concurrency arm, not a content-equivalence check: the
// defect it catches (dropping the `loaded` bool) is INVISIBLE to any
// sequential arm, because sequentially the Load fast path takes every repeat
// and the LoadOrStore branch is reached exactly once per subject.
func TestRBACSubGenCounters_ConcurrentCreationCountsEachSubjectOnce(t *testing.T) {
	ResetRBACSubGenForTest()
	t.Cleanup(ResetRBACSubGenForTest)

	const (
		goroutines     = 16
		subjectsPerRun = 24
		roundsPerG     = 8
	)

	subjects := make([]subjectKey, 0, subjectsPerRun)
	for i := range subjectsPerRun {
		switch i % 3 {
		case 0:
			subjects = append(subjects, subjectKey{Kind: subjectKindUser, Name: fmt.Sprintf("u%d", i)})
		case 1:
			subjects = append(subjects, subjectKey{Kind: subjectKindGroup, Name: fmt.Sprintf("g%d", i)})
		default:
			subjects = append(subjects, subjectKey{
				Kind: subjectKindServiceAccount, Name: fmt.Sprintf("sa%d", i), Namespace: "ns",
			})
		}
	}

	// A start barrier so every goroutine hits the FIRST round — the one where
	// the entries do not exist yet — at the same time. Without it the first
	// goroutine would create every subject before the others start and the
	// loaded==true branch would go unexercised, making the arm vacuous.
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	for range goroutines {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			for range roundsPerG {
				BumpSubjectSubGens(subjects)
			}
		}()
	}
	start.Done()
	done.Wait()

	const wantBumps = uint64(goroutines * roundsPerG * subjectsPerRun)
	if got := RBACSubGenBumpsTotal(); got != wantBumps {
		t.Errorf("bumps_total = %d; want %d (%d goroutines x %d rounds x %d subjects) — "+
			"a lost add means the cumulative counter under-reports rotation rate",
			got, wantBumps, goroutines, roundsPerG, subjectsPerRun)
	}
	if got := RBACSubGenSubjectsTracked(); got != subjectsPerRun {
		t.Errorf("subjects_tracked = %d; want %d — %d goroutines raced to create the SAME subjects; "+
			"counting a racer that LOST the LoadOrStore (loaded==true) inflates the blast-radius "+
			"denominator and makes a one-tenant churn look fleet-wide",
			got, subjectsPerRun, goroutines)
	}

	// Independent confirmation from the map itself, not from the counter.
	var mapEntries uint64
	rbacSubGen.Range(func(_, _ any) bool { mapEntries++; return true })
	if mapEntries != subjectsPerRun {
		t.Fatalf("rbacSubGen holds %d entries; want %d — the fixture, not the counter, is wrong", mapEntries, subjectsPerRun)
	}

	// Each subject's own counter must have taken every bump, so the cumulative
	// total is not merely arithmetic that happens to agree.
	const wantPerSubject = uint64(goroutines * roundsPerG)
	for _, s := range subjects {
		if got := subGenValue(s); got != wantPerSubject {
			t.Errorf("subject %v counter = %d; want %d", s, got, wantPerSubject)
		}
	}
}

// TestRBACSubGenExpvar_ZeroIsPublishedNotOmitted is arm (4) — the
// zero-readability constraint. Both keys must be present at the REAL
// /debug/vars handler with value 0 before anything has ever bumped.
//
// This is the arm that makes a `0` in a production capture admissible: without
// it, "the counter reads 0" and "the counter is not wired" are the same
// observation, which is precisely the ambiguity snowplow_rbac_publish_seq left
// behind for per-subject rotation.
func TestRBACSubGenExpvar_ZeroIsPublishedNotOmitted(t *testing.T) {
	// No CACHE_ENABLED here ON PURPOSE: these keys are registered
	// unconditionally from main.go (cache mode-agnostic, like
	// RegisterRBACSnapshotExpvar), so they must be readable with the cache off
	// too — a cache-off pod still needs a `0` that means "no rotation".
	RegisterRBACSubGenExpvar()
	RegisterRBACSubGenExpvar() // idempotent: expvar.Publish panics on a duplicate key.

	ResetRBACSubGenForTest()
	t.Cleanup(ResetRBACSubGenForTest)

	doc := scrapeDebugVarsForSubGenTest(t)

	for _, key := range []string{
		"snowplow_rbac_subgen_bumps_total",
		"snowplow_rbac_subgen_subjects_tracked",
	} {
		raw, present := doc[key]
		if !present {
			t.Fatalf("/debug/vars has NO %q key while the value is 0. A zero-valued observable that is "+
				"omitted from the payload makes \"no rotation occurred\" indistinguishable from "+
				"\"not instrumented\" — the exact vantage hole #247 exists to close", key)
		}
		var v float64
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatalf("%s is not a JSON number: %v (raw: %s)", key, err, raw)
		}
		if v != 0 {
			t.Errorf("%s = %v at the floor; want 0", key, v)
		}
	}

	// And the keys must TRACK, not merely exist — a Func that returned a
	// constant 0 would pass the presence check above.
	BumpSubjectSubGens([]subjectKey{
		{Kind: subjectKindUser, Name: "alice"},
		{Kind: subjectKindGroup, Name: "devs"},
	})
	BumpSubjectSubGens([]subjectKey{{Kind: subjectKindUser, Name: "alice"}})

	doc = scrapeDebugVarsForSubGenTest(t)
	for key, want := range map[string]float64{
		"snowplow_rbac_subgen_bumps_total":      3,
		"snowplow_rbac_subgen_subjects_tracked": 2,
	} {
		var v float64
		if err := json.Unmarshal(doc[key], &v); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if v != want {
			t.Errorf("%s = %v after 3 bumps over 2 subjects; want %v — the expvar.Func must read the "+
				"live counter at scrape time", key, v, want)
		}
	}
}

// scrapeDebugVarsForSubGenTest reads the whole expvar registry through the same
// expvar.Handler() main.go mounts at /debug/vars, so the arm exercises the
// operator's actual surface rather than the accessor functions.
func scrapeDebugVarsForSubGenTest(t *testing.T) map[string]json.RawMessage {
	t.Helper()

	mux := http.NewServeMux()
	mux.Handle("/debug/vars", expvar.Handler())
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/debug/vars")
	if err != nil {
		t.Fatalf("GET /debug/vars: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /debug/vars body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/debug/vars status = %d", resp.StatusCode)
	}

	var doc map[string]json.RawMessage
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("unmarshal /debug/vars: %v\nbody: %s", err, body)
	}
	return doc
}
