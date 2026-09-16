// issue220_coverage_alarm_test.go — 1.12.7 / #220: the prewarm coverage
// regression alarm.
//
// #220 went 177 harvested widgets to 5 and nothing went red. These arms hold
// the alarm to the same standard as the rest of this commit: each drives the
// real event, asserts the counter moves, AND asserts it does NOT move on the
// neighbouring event it must not claim.
//
// THE THIRD ARM IS THE ONE THAT MATTERS. An alarm that fires on a partial or
// aborted walk is a flake generator, and someone silences it within a week —
// which lands us exactly back where #220 started, with a real outage hidden
// behind noise an operator has learned to ignore.
package dispatchers

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// covSyncBuf is a mutex-guarded slog sink (see the note in
// issue216_f6b_gone_forget_test.go: a process-lived ticker logs into whatever
// default is installed, so a bare bytes.Buffer sink races).
type covSyncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *covSyncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *covSyncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func covGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
}

// covPublish installs a harvester holding n distinct widgets.
func covPublish(t *testing.T, n int) {
	t.Helper()
	nav := newNavWidgetHarvester()
	for i := 0; i < n; i++ {
		u := &unstructured.Unstructured{}
		u.SetNamespace("krateo-system")
		u.SetName("flex-" + itoa(i))
		u.SetGroupVersionKind(schema.GroupVersionKind{
			Group: covGVR().Group, Version: covGVR().Version, Kind: "Flex",
		})
		nav.harvestNavWidget(u, covGVR(), -1, -1, -1, -1)
	}
	publishHarvestersForInspection(nav, newContentPrewarmHarvester())
}

// covCapture runs fn with slog captured and returns the emitted text.
func covCapture(t *testing.T, fn func()) string {
	t.Helper()
	var buf covSyncBuf
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)
	fn()
	return buf.String()
}

func covReset(t *testing.T) {
	t.Helper()
	ResetWalkCoverageForTest()
	ResetHarvestInspectionForTest()
	t.Cleanup(ResetWalkCoverageForTest)
	t.Cleanup(ResetHarvestInspectionForTest)
}

// TestIssue220_Coverage_CompletedRegressionAlarms — the #220 shape itself.
func TestIssue220_Coverage_CompletedRegressionAlarms(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	covReset(t)

	// A good pass establishes the baseline.
	covPublish(t, 177)
	if out := covCapture(t, func() { recordWalkCoverage(1, true) }); strings.Contains(out, "prewarm.coverage.regressed") {
		t.Fatalf("premise: the FIRST completed pass alarmed against an empty baseline:\n%s", out)
	}
	_, maxW, _, regressed := WalkCoverageForTest()
	if maxW != 177 || regressed != 0 {
		t.Fatalf("premise: baseline=%d regressed=%d after the first pass, want 177/0", maxW, regressed)
	}

	// The #220 regression: a COMPLETED pass reaching 5.
	covPublish(t, 5)
	out := covCapture(t, func() { recordWalkCoverage(1, true) })
	if !strings.Contains(out, "prewarm.coverage.regressed") {
		t.Fatalf("RED (#220): a completed walk that reached 5 widgets where this process had "+
			"reached 177 emitted NO alarm. This is the exact shape that took the portal cold with "+
			"nothing going red:\n%s", out)
	}
	if _, _, _, regressed = WalkCoverageForTest(); regressed != 1 {
		t.Fatalf("RED (#220): the regression counter moved by %d, want 1", regressed)
	}

	// BOTH numbers must be on the line — "regressed" alone does not tell an
	// operator whether this is a wobble or the portal going cold.
	var found bool
	for _, line := range strings.Split(out, "\n") {
		var rec map[string]any
		if line == "" || json.Unmarshal([]byte(line), &rec) != nil {
			continue
		}
		if rec["msg"] != "prewarm.coverage.regressed" {
			continue
		}
		found = true
		if rec["harvested_widgets"] != float64(5) {
			t.Fatalf("RED (#220): the alarm reports harvested_widgets=%v, want 5", rec["harvested_widgets"])
		}
		if rec["max_harvested_widgets"] != float64(177) {
			t.Fatalf("RED (#220): the alarm reports max_harvested_widgets=%v, want 177 — without the "+
				"baseline on the line the operator cannot see 177 → 5", rec["max_harvested_widgets"])
		}
	}
	if !found {
		t.Fatalf("RED (#220): no parseable prewarm.coverage.regressed record:\n%s", out)
	}

	// The baseline must NOT be lowered by the regression, or the alarm
	// silences itself on the next pass — the failure mode that would make it
	// fire exactly once and never again.
	if _, maxW, _, _ = WalkCoverageForTest(); maxW != 177 {
		t.Fatalf("RED (#220): the high-water mark dropped to %d after a regression. The baseline "+
			"would follow the outage down and the alarm would go quiet while the portal stays "+
			"cold", maxW)
	}
}

// TestIssue220_Coverage_EqualOrBetterDoesNotAlarm — the neighbouring event the
// counter must not claim.
func TestIssue220_Coverage_EqualOrBetterDoesNotAlarm(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	covReset(t)

	covPublish(t, 100)
	covCapture(t, func() { recordWalkCoverage(2, true) })

	// Equal.
	covPublish(t, 100)
	if out := covCapture(t, func() { recordWalkCoverage(2, true) }); strings.Contains(out, "regressed") {
		t.Fatalf("RED (#220): a pass matching the high-water mark alarmed:\n%s", out)
	}
	// Better — and it must RAISE the baseline.
	covPublish(t, 150)
	if out := covCapture(t, func() { recordWalkCoverage(2, true) }); strings.Contains(out, "regressed") {
		t.Fatalf("RED (#220): a pass that IMPROVED on the high-water mark alarmed:\n%s", out)
	}
	if _, maxW, _, regressed := WalkCoverageForTest(); maxW != 150 || regressed != 0 {
		t.Fatalf("RED (#220): baseline=%d regressed=%d after an improvement, want 150/0", maxW, regressed)
	}
}

// TestIssue220_Coverage_PartialPassNeverAlarms — THE arm that keeps this alarm
// alive. A walk cut short, or one that lost a root, legitimately harvests less.
// Alarming on it produces false WARNs, and a false WARN is how #220 stayed
// invisible in the first place.
func TestIssue220_Coverage_PartialPassNeverAlarms(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	covReset(t)

	covPublish(t, 177)
	covCapture(t, func() { recordWalkCoverage(3, true) })

	// A pass that reached far fewer widgets, but did NOT complete.
	covPublish(t, 5)
	out := covCapture(t, func() { recordWalkCoverage(1, false) })
	if strings.Contains(out, "prewarm.coverage.regressed") {
		t.Fatalf("RED (#220): an ABORTED/partial pass raised the regression alarm. A walk cut short "+
			"by a deadline or one that lost a root harvests less by definition; alarming on it "+
			"trains an operator to ignore the WARN, which is precisely how the real 177 → 5 stayed "+
			"invisible:\n%s", out)
	}
	if _, _, _, regressed := WalkCoverageForTest(); regressed != 0 {
		t.Fatalf("RED (#220): a partial pass moved the regression counter by %d, want 0", regressed)
	}
	// It must also not raise the baseline from a partial view...
	if _, maxW, _, _ := WalkCoverageForTest(); maxW != 177 {
		t.Fatalf("RED (#220): a partial pass changed the baseline to %d, want 177", maxW)
	}
	// ...but the figures it DID observe are still visible, so an operator
	// looking at /debug/vars sees what the last pass actually found.
	if lastW, _, roots, _ := WalkCoverageForTest(); lastW != 5 || roots != 1 {
		t.Fatalf("#220: a partial pass should still publish what it saw; got widgets=%d roots=%d",
			lastW, roots)
	}
}

// TestIssue220_Coverage_NoHarvesterIsInert — prewarm off must not alarm.
func TestIssue220_Coverage_NoHarvesterIsInert(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	covReset(t)

	if out := covCapture(t, func() { recordWalkCoverage(0, true) }); strings.Contains(out, "regressed") {
		t.Fatalf("RED (#220): the alarm fired with no harvester published (prewarm off):\n%s", out)
	}
	if _, _, _, regressed := WalkCoverageForTest(); regressed != 0 {
		t.Fatalf("RED (#220): prewarm-off moved the regression counter by %d", regressed)
	}
}
