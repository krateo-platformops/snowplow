//go:build unit
// +build unit

package api

import (
	"context"
	"io"
	"log/slog"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// setup_c1c2b_test.go — #288 pm-1218 gate additions: C1 (the skip is COUNTED via
// MalformedDialSkippedTotal, not just DEBUG-only — an invisible skip storm reads
// as health) and C2b (a nameless LIST path, name=="", MUST still dial — the guard
// applies IsDNS1123Subdomain only to a non-empty name). Both drive the REAL
// createRequestOptions plan builder with REAL evalJQ interpolation from a dict.

func c1c2bLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestEmptyInterpGuard_SkipIsCounted — C1: a skipped malformed dial must bump the
// observable counter (else a spike in empty-interp segments is invisible).
func TestEmptyInterpGuard_SkipIsCounted(t *testing.T) {
	log := c1c2bLog()
	before := MalformedDialSkippedTotal()
	all := createRequestOptions(context.Background(), log,
		&templates.API{Name: "s", Path: "${ \"/api/v1/namespaces/\" + (.ns) + \"/configmaps/architecture\" }"},
		map[string]any{"ns": ""})
	if len(all) != 0 {
		t.Fatalf("C1 setup: the malformed path must be skipped; got %d call(s)", len(all))
	}
	if got := MalformedDialSkippedTotal() - before; got != 1 {
		t.Fatalf("C1: a skip must bump malformed_dial_skipped_total by 1; delta=%d", got)
	}
}

// TestEmptyInterpGuard_ListPath_StillDialed — C2b: a nameless LIST (name=="") is
// legitimate and MUST still dial; the counter must not move.
func TestEmptyInterpGuard_ListPath_StillDialed(t *testing.T) {
	log := c1c2bLog()
	const want = "/api/v1/namespaces/team-a/configmaps"
	before := MalformedDialSkippedTotal()
	all := createRequestOptions(context.Background(), log,
		&templates.API{Name: "list", Path: "${ \"/api/v1/namespaces/\" + (.ns) + \"/configmaps\" }"},
		map[string]any{"ns": "team-a"})
	if len(all) != 1 || all[0].Path != want {
		t.Fatalf("C2b: nameless LIST path must still dial (name== empty is legit); got %d %+v", len(all), all)
	}
	if got := MalformedDialSkippedTotal() - before; got != 0 {
		t.Fatalf("C2b: LIST path must NOT bump the skip counter; delta=%d", got)
	}
}
