//go:build race

package cache

// raceEnabled lets the S12 measurement label its number: the race runtime
// inflates allocations, so the reportable figure is the !race run.
const raceEnabled = true
