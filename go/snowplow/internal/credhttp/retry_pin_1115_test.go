package credhttp

// retry_pin_1115_test.go — #1122 review (LOW): firstBackoff and retryAfter
// transcribe plumbing's unexported RetryClient internals (backoff,
// parseRetryAfter). This guard fails when go.mod requires any plumbing other
// than the one they were transcribed from, so a plumbing bump (#1135)
// re-audits them against its http/util/retry.go and updates
// retryPolicyTranscribedFrom (the TestUserConfigMirror_PlumbingVersionPin
// precedent). The parts plumbing exports are read from it, not copied: the
// base and max backoff come from util.NewRetryClient, and are pinned here too.

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/http/util"
)

func TestBudget1115_RetryPolicyPinnedToPlumbing(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var mod []byte
	for range 10 {
		if b, rerr := os.ReadFile(filepath.Join(dir, "go.mod")); rerr == nil {
			mod = b
			break
		}
		dir = filepath.Dir(dir)
	}
	m := regexp.MustCompile(`(?m)^\s*github\.com/krateo-platformops/plumbing (\S+)`).FindSubmatch(mod)
	if m == nil {
		t.Fatal("go.mod does not require github.com/krateo-platformops/plumbing")
	}
	if got := string(m[1]); got != retryPolicyTranscribedFrom {
		t.Fatalf("go.mod requires plumbing %s, but firstBackoff/retryAfter transcribe %s: re-audit them against plumbing's http/util/retry.go (backoff, parseRetryAfter) and update retryPolicyTranscribedFrom", got, retryPolicyTranscribedFrom)
	}
	rc := util.NewRetryClient(nil)
	if rc.BaseBackoff != 500*time.Millisecond || rc.MaxBackoff != 10*time.Second {
		t.Fatalf("plumbing RetryClient defaults base=%v max=%v, transcribed for 500ms / 10s", rc.BaseBackoff, rc.MaxBackoff)
	}
	for range 50 {
		if d := firstBackoff(rc.BaseBackoff, rc.MaxBackoff); d < 250*time.Millisecond || d >= 500*time.Millisecond {
			t.Fatalf("firstBackoff = %v, want plumbing's jitter [base/2, base)", d)
		}
	}
}
