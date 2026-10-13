//go:build !jqinterest_exhaustive

package jqinterest

// The default (per-PR CI) budget (#663). The corpus-wide tests run the
// committed stratified subset (testdata/corpus_subset.json, see
// corpus_subset_test.go), and the arm fuzz runs armTrials trials per arm.
// TestSoundnessArms_RedUnderTheirMutation runs at the SAME budget, so every
// named mutation is shown to be detected at it.
//
// The exhaustive sweep (whole corpus, 2000 trials per arm) runs with
// -tags jqinterest_exhaustive (.github/workflows/jqinterest-exhaustive.yaml).
const (
	exhaustive = false
	armTrials  = 500
)
