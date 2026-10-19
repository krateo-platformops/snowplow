//go:build scale765

// full_test.go: the #765 harness at run 8's scale (50,000 objects, 1,000
// users). Build tag scale765: it is out of the default CI and runs on demand for
// tens of minutes:
//
//	go test -tags scale765 -run 'TestScale765_H0' -timeout 3h -v ./internal/handlers/dispatchers/scale765/
//
// SCALE765_OUT=<dir> also writes every report there as JSON.
// SCALE765_WARMUP / SCALE765_MEASURE override the spec's 2 m / 10 m (a shorter
// run is a harness check, never an acceptance run: the report records both).
// SCALE765_PLATFORM=off drops the measured platform-composition churn (the
// default is on; a report says which, as platform_churn).
// Before a measured run, check the host: the load average must be below ~4
// (#765 ruling a); a starved run is flagged INVALID by s765Calibrate anyway.
package scale765_test

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestMain pins the harness to 4 cores, run 8's CPU limit (the spec:
// runtime.GOMAXPROCS(4) in the harness's own TestMain).
func TestMain(m *testing.M) {
	runtime.GOMAXPROCS(4)
	os.Exit(m.Run())
}

func s765Durations(t *testing.T) (warm, measure time.Duration) {
	t.Helper()
	warm, measure = 2*time.Minute, 10*time.Minute
	for env, d := range map[string]*time.Duration{"SCALE765_WARMUP": &warm, "SCALE765_MEASURE": &measure} {
		if v := os.Getenv(env); v != "" {
			p, err := time.ParseDuration(v)
			if err != nil {
				t.Fatalf("%s: %v", env, err)
			}
			*d = p
		}
	}
	return warm, measure
}

// s765RefuseBusyHost fails (never skips: a skip reads green) when the host's
// 1-minute load average is above SCALE765_MAX_LOAD (default 4, ruling a).
func s765RefuseBusyHost(t *testing.T) {
	t.Helper()
	limit := 4.0
	if v := os.Getenv("SCALE765_MAX_LOAD"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			limit = f
		}
	}
	la := strings.Trim(s765HostInfo().LoadAvg, "{} ")
	f, err := strconv.ParseFloat(strings.ReplaceAll(strings.Fields(la + " x")[0], ",", "."), 64)
	if err != nil {
		t.Fatalf("cannot read the host load average (%q): refusing to measure", la)
	}
	if f > limit {
		t.Fatalf("host 1-minute load average %.2f > %.1f: refusing to measure on a busy host", f, limit)
	}
}

func s765FullPoint(t *testing.T, mode s765Mode, rate float64) s765Point {
	warm, measure := s765Durations(t)
	return s765Point{Mode: mode, Rate: rate, WarmUp: warm, Measure: measure,
		ProbeEvery: measure / 60, PollEvery: 250 * time.Millisecond, CustEvery: time.Second,
		Drain: 10 * time.Minute, Platform: os.Getenv("SCALE765_PLATFORM") != "off"}
}

// TestScale765_H0Calibration is the calibration gate: on main, annotation churn
// at 10/s must reproduce run 8's shape within a factor of 2 before any A/B arm
// counts. It also MEASURES marks per event, which run 8 did not capture.
func TestScale765_H0Calibration(t *testing.T) {
	s765RefuseBusyHost(t)
	t0 := time.Now()
	h := s765Setup(t, s765Full())
	pop := h.populate()
	s765Emit(t, "h0-populate", pop)
	rep := h.run(s765FullPoint(t, s765Annotation, 10))
	checks := s765Calibrate(rep)
	s765Emit(t, "h0-annotation-10", rep)
	s765Emit(t, "h0-calibration", map[string]any{"checks": checks, "wall": time.Since(t0).Round(time.Second).String(),
		"host": rep.Host, "marks_per_event": rep.MarksPerEvent})
	for _, c := range checks {
		if c.Result != "PASS" {
			t.Errorf("CALIBRATION %s: got %.3g, run 8 band [%.3g, %.3g]", c.Name, c.Got, c.Lo, c.Hi)
		}
	}
}

// TestScale765_Point is one run of a PAIRED comparison (paired_ab.sh): the same
// population and one point (SCALE765_MODE, default annotation; SCALE765_RATE,
// default 10), its report written as SCALE765_LABEL.json. The driver runs this
// binary, built once per tree (main + harness, and each fix's branch + the same
// harness), alternating, and paired_summary.py compares them by ratios.
func TestScale765_Point(t *testing.T) {
	label := os.Getenv("SCALE765_LABEL")
	if label == "" {
		t.Fatalf("SCALE765_LABEL is required (paired_ab.sh sets it)")
	}
	mode := s765Mode(os.Getenv("SCALE765_MODE"))
	if mode == "" {
		mode = s765Annotation
	}
	if mode != s765Annotation && mode != s765Status {
		t.Fatalf("SCALE765_MODE: %q", mode)
	}
	rate := 10.0
	if v := os.Getenv("SCALE765_RATE"); v != "" {
		r, err := strconv.ParseFloat(v, 64)
		if err != nil {
			t.Fatalf("SCALE765_RATE: %v", err)
		}
		rate = r
	}
	s765RefuseBusyHost(t)
	h := s765Setup(t, s765Full())
	s765Emit(t, label+"-populate", h.populate())
	rep := h.run(s765FullPoint(t, mode, rate))
	s765Emit(t, label, rep)
}

// TestScale765_H0Matrix runs main at every point SCALE765_POINTS names
// ("mode:rate,..."; default: both modes at 1, 10 and 50/s), one harness, the
// points in order, each followed by a bounded drain.
func TestScale765_H0Matrix(t *testing.T) {
	points := os.Getenv("SCALE765_POINTS")
	if points == "" {
		points = "annotation:1,annotation:10,annotation:50,status:1,status:10,status:50"
	}
	s765RefuseBusyHost(t)
	h := s765Setup(t, s765Full())
	s765Emit(t, "h0m-populate", h.populate())
	for _, p := range strings.Split(points, ",") {
		mode, rateS, ok := strings.Cut(strings.TrimSpace(p), ":")
		rate, err := strconv.ParseFloat(rateS, 64)
		if !ok || err != nil || (mode != string(s765Annotation) && mode != string(s765Status)) {
			t.Fatalf("SCALE765_POINTS: bad point %q", p)
		}
		rep := h.run(s765FullPoint(t, s765Mode(mode), rate))
		s765Emit(t, fmt.Sprintf("h0m-%s-%g", mode, rate), rep)
	}
}
