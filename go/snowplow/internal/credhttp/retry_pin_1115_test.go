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
	// The jitter is pinned by its spread as well as its bounds: a firstBackoff that returned a
	// constant inside [base/2, base) (d/2, say) would pass the bounds alone. Over 200 draws
	// uniform on [250 ms, 500 ms), the chance that none falls below 300 ms is (200/250)^200 =
	// 0.8^200 ≈ 4.1e-20, and the same for none above 450 ms; this test flakes with probability
	// at most their sum, ≈ 8.3e-20.
	const draws = 200
	lo, hi := time.Duration(1<<62), time.Duration(0)
	for range draws {
		d := firstBackoff(rc.BaseBackoff, rc.MaxBackoff)
		if d < 250*time.Millisecond || d >= 500*time.Millisecond {
			t.Fatalf("firstBackoff = %v, want plumbing's jitter [base/2, base)", d)
		}
		lo, hi = min(lo, d), max(hi, d)
	}
	if lo >= 300*time.Millisecond || hi <= 450*time.Millisecond {
		t.Fatalf("firstBackoff over %d draws spans [%v, %v], want below 300ms and above 450ms: plumbing's jitter is uniform over [base/2, base)", draws, lo, hi)
	}
}
