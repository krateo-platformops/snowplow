//go:build jqinterest_exhaustive

package jqinterest

// The exhaustive sweep (#663): the whole portal corpus and 2000 trials per
// arm. Run by .github/workflows/jqinterest-exhaustive.yaml (nightly and on
// demand); too slow for the per-PR package timeout under -race.
const (
	exhaustive = true
	armTrials  = 2000
)
