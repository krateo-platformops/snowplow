//go:build unit || integration

// smoke_test.go: the #765 harness at 1/50 of run 8 (1,000 objects, 20
// users), in the default unit/integration gate so the harness itself cannot rot.
// It proves every instrument the full run reads moves; it accepts nothing about
// A or B (that is full_test.go, build tag scale765).
package scale765_test

import (
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestScale765_Smoke(t *testing.T) {
	prev := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(prev)
	t0 := time.Now()

	h := s765Setup(t, s765Smoke())
	pop := h.populate()
	s765Emit(t, "smoke-populate", pop)
	for w, byCode := range pop.StatusByWidget {
		for code, n := range byCode.(map[int]int) {
			if code != http.StatusOK {
				t.Errorf("populate: widget %s answered HTTP %d to %d customer call(s)", w, code, n)
			}
		}
	}
	if pop.ResidentTotal == 0 || pop.ResidentByClass["widgets"] == 0 {
		t.Fatalf("populate built no widget cells: %v", pop.ResidentByClass)
	}
	if !pop.QuiesceReached {
		t.Errorf("the refresher never went quiet after the population")
	}

	for _, mode := range []s765Mode{s765Annotation, s765Status} {
		rep := h.run(s765Point{Mode: mode, Rate: 10, WarmUp: 2 * time.Second, Measure: 8 * time.Second,
			ProbeEvery: 2 * time.Second, PollEvery: 250 * time.Millisecond, CustEvery: 500 * time.Millisecond,
			Drain: 15 * time.Second, Platform: mode == s765Status})
		s765Emit(t, "smoke-"+string(mode), rep)
		arm := "smoke " + string(mode)
		if rep.EventsWritten < 50 || rep.WriteErrors != 0 {
			t.Errorf("%s: the generator wrote %d events (%d errors) in %s", arm, rep.EventsWritten, rep.WriteErrors, rep.Measure)
		}
		if rep.MarksTotal == 0 || rep.MarkEventsDelta == 0 {
			t.Errorf("%s: no dirty marks (marks=%d events=%d): the informer UPDATE never reached the dep tracker", arm, rep.MarksTotal, rep.MarkEventsDelta)
		}
		if rep.RefresherDelta["enqueue"] == 0 || rep.RefresherDelta["completed"] == 0 {
			t.Errorf("%s: the refresher did no work: %v", arm, rep.RefresherDelta)
		}
		if rep.BenchFillsPerSec == 0 {
			t.Errorf("%s: the bench list was never re-filled", arm)
		}
		// Convergence inside an 8 s window is load-sensitive (it is what the
		// full run measures); the smoke only proves the probe ran.
		if rep.ProbeSamples == 0 || len(rep.ProbeList) != rep.ProbeSamples {
			t.Errorf("%s: the probe recorded %d samples (%d listed)", arm, rep.ProbeSamples, len(rep.ProbeList))
		}
		if mode == s765Status && (rep.PlatformEvents == 0 || rep.MarksPerPlatformEvent == 0) {
			t.Errorf("%s: platform churn wrote %d events, %.1f marks each", arm, rep.PlatformEvents, rep.MarksPerPlatformEvent)
		}
		if rep.CustomerWidgetHit+rep.CustomerWidgetMiss == 0 {
			t.Errorf("%s: snowplow counted no customer L1 lookups for %d customer calls", arm, rep.Customer.N)
		}
		if rep.Customer.N == 0 || len(rep.CustomerNon200) != 0 {
			t.Errorf("%s: customer calls n=%d non-200=%v", arm, rep.Customer.N, rep.CustomerNon200)
		}
		if rep.CPUCoresRusage <= 0 || rep.CPUCoresRuntime <= 0 {
			t.Errorf("%s: CPU not measured (rusage %.2f, runtime/metrics %.2f)", arm, rep.CPUCoresRusage, rep.CPUCoresRuntime)
		}
		for p, n := range rep.UnservedReads {
			if strings.Contains(p, "/namespaces/") || strings.Count(p, "/") > 4 {
				t.Errorf("%s: %d object read(s) reached the fake apiserver at %s: the informer did not serve them", arm, n, p)
			}
		}
	}
	// The differential oracle the A/B arms use: after the drain, every served
	// body equals a cold resolve of the same request.
	diff := h.differential()
	s765Emit(t, "smoke-differential", diff)
	if diff.Compared == 0 || diff.Mismatched != 0 || diff.Errors != 0 {
		t.Errorf("smoke differential: %+v", diff)
	}
	t.Logf("scale765 smoke wall %s", time.Since(t0).Round(time.Millisecond))
}
