package dispatchers

// issue904_stored_outcomes_test.go — #904: every restactions-class Put site
// that resolves keeps the resolve's stage outcomes on the entry, so an L1 hit
// of a cell the REFRESHER or the cohort SEED stored answers with the same
// X-Snowplow-Stage-Outcomes a cold resolve would. The cells are #681
// denial-only (the only stored cells that can carry a failed stage).
//
//   P1  refresher (the #681 R1 harness): the re-Put entry lists the denied
//       stage as Forbidden.
//   P2  cohort seed (the #681 G harness): the seeded entry lists it too, and a
//       customer L1 hit of the seeded cell answers with that header.

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
)

func TestIssue904_P1_RefresherPutKeepsStageOutcomes(t *testing.T) {
	e := i677Setup(t, i677DefaultRA())
	e.cycle(t)
	got, ok := e.store.GetNoTouch(e.key)
	if !ok {
		t.Fatal("PRE: the cell left the store")
	}
	if got.RawJSON == nil || string(got.RawJSON) == `{"prior":true}` {
		t.Fatal("PRE: the denial-only refresh must be re-Put (#681 R1)")
	}
	if !strings.Contains(got.StageOutcomes, `{"name":"catalog","ok":false,"reason":"Forbidden"}`) {
		t.Fatalf("#904 RED: the refresher's re-Put kept StageOutcomes=%q, want the Forbidden catalog stage", got.StageOutcomes)
	}
	if strings.Contains(got.StageOutcomes, `"ok"`+":true") || strings.Contains(got.StageOutcomes, `"name":"ok"`) {
		t.Fatalf("a succeeded stage must not be listed: %q", got.StageOutcomes)
	}
}

func TestIssue904_P2_SeedPutKeepsStageOutcomes_HitAnswersWithThem(t *testing.T) {
	e := c681Setup(t)
	var saDials atomic.Int64
	sa := c681SAServer(t, &saDials)
	if seeded := e.seed(t, seedTarget{Groups: []string{c681All}}, sa.URL); seeded == nil {
		t.Fatal("PRE: the cohort seed must Put the denial-only result (#681 G)")
	}
	entry, ok := cache.ResolvedCache().GetNoTouch(e.key(t, c681A))
	if !ok {
		t.Fatal("PRE: the seeded cell is not in the store")
	}
	want := `[{"name":"sa","ok":false,"reason":"Forbidden"}]`
	if entry.StageOutcomes != want {
		t.Fatalf("#904 RED: the seed Put kept StageOutcomes=%q, want %q", entry.StageOutcomes, want)
	}
	rec, d := e.callRec(t, c681A)
	if d != 0 {
		t.Fatalf("PRE: A's /call must hit the seeded cell (0 dials), got %d", d)
	}
	if v := rec.Header().Get(util.HeaderStageOutcomes); v != want {
		t.Fatalf("#904 RED: the hit of the seeded cell answered %q, want %q", v, want)
	}
}
