package dispatchers

// shadow_wildcard_digest_probe_seam_test.go — #455 (reviewer X5): the seam's
// restore must put the requester-profile seam back AND reset the probe, so a
// drive never leaks counters or a fake profile into later arms.

import (
	"reflect"
	"testing"
)

func TestDriveWildcardDigestProbeForTest_RestoreResetsProbeAndProfileSeam(t *testing.T) {
	before := reflect.ValueOf(shadowProfileFor).Pointer()
	restore := DriveWildcardDigestProbeForTest(1, 1, 1)
	if c, o, e := ShadowWildcardDigestCounts(); c != 1 || o != 2 || e != 1 {
		t.Fatalf("drive: collision=%d observed=%d evicted=%d, want 1/2/1", c, o, e)
	}
	if reflect.ValueOf(shadowProfileFor).Pointer() == before {
		t.Fatalf("drive must override the requester-profile seam (precondition)")
	}
	restore()
	if c, o, e := ShadowWildcardDigestCounts(); c != 0 || o != 0 || e != 0 {
		t.Fatalf("restore must reset the probe: collision=%d observed=%d evicted=%d, want 0/0/0", c, o, e)
	}
	wildcardProbe.mu.Lock()
	n := len(wildcardProbe.entries)
	wildcardProbe.mu.Unlock()
	if n != 0 {
		t.Fatalf("restore must clear the probe store; %d entries left", n)
	}
	if reflect.ValueOf(shadowProfileFor).Pointer() != before {
		t.Fatalf("restore must put the requester-profile seam back")
	}
}
