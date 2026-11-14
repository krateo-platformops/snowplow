//go:build !(js && wasm)

package main

import (
	"os"
	"strings"
	"testing"
)

// TestExportEnv_SetsEachAndNamesTheOneItCannot: startup fails with the reason instead of running
// with a variable it could not set.
func TestExportEnv_SetsEachAndNamesTheOneItCannot(t *testing.T) {
	t.Setenv("SNOWPLOW_TEST_EXPORT_A", "")
	if err := exportEnv([][2]string{{"SNOWPLOW_TEST_EXPORT_A", "1"}}); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("SNOWPLOW_TEST_EXPORT_A"); got != "1" {
		t.Fatalf("SNOWPLOW_TEST_EXPORT_A = %q, want 1", got)
	}
	// A NUL byte is refused by the OS (syscall.Setenv: EINVAL).
	err := exportEnv([][2]string{{"SNOWPLOW_TEST_EXPORT_A", "2"}, {"SNOWPLOW_TEST_EXPORT_B", "a\x00b"}})
	if err == nil || !strings.Contains(err.Error(), "SNOWPLOW_TEST_EXPORT_B") {
		t.Fatalf("err = %v, want one naming SNOWPLOW_TEST_EXPORT_B", err)
	}
}
