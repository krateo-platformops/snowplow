//go:build !integration

package api

import (
	"os"
	"testing"
)

// TestMain for builds without the integration tag (whose TestMain lives in
// resolve_test.go and ends with the same check): fail the run when the E6
// chokepoint saw a violation (#674, envelope_e6_chokepoint_test.go).
func TestMain(m *testing.M) {
	os.Exit(e6ChokepointExit(m.Run()))
}
