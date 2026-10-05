// issue453_identity_log_redaction_test.go — #453: no refresher, seed or
// dispatch log line carries a representative or requester in clear.
//
// Pre-#453 a learned identity (#262) and a promoted recent hitter (#444) were
// redacted, but a RECORDED representative (the cell's first writer, a base
// cohort user) was logged in clear by the refresher ("user" on
// stage_error / external / uaf / stored). The same was true of the seed's
// cohort label for every base cohort, the per-call line's requester and the
// dispatch key diagnostic. Every one now renders the internal/redact label.
//
// ARMS (all RED on origin/main f1f18fcc: the plaintext token is in the line):
//   TestIssue453_RefresherRecordedRepresentativeNeverLogged — the real
//       resolveAndPopulateL1 over the #423 ps harness, recorded representative.
//   TestIssue453_SeedLabelsNeverCarryBaseIdentity — cohortLogLabel and
//       seedIdentityLabelFromCtx for a user cohort and a group cohort.
//   TestIssue453_PerCallAndKeyDiagNeverNameRequester — dispatcher.call.complete
//       and dispatch.cache_key.computed.
// The structural guard over every slog site is internal/redact's
// TestS453_NoLogSiteEmitsAnIdentityInClear.

package dispatchers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

func TestIssue453_RefresherRecordedRepresentativeNeverLogged(t *testing.T) {
	a := psArm{target: psConfigmapsGVR}
	e := i444Setup(t, k423PortalReaders(a)...)
	if isLearnedIdentity(psCarol) {
		t.Fatalf("PRE: carol must be a base (non-learned) representative, else the #262 redactor masks the defect")
	}
	e.seam(t)

	var logBuf syncBuffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(logger))
	if err := resolveAndPopulateL1(ctx, e.stored, e.saEP, e.saRC); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	out := logBuf.String()
	stored := ""
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "re-resolved + stored") {
			stored = line
		}
	}
	if stored == "" || !strings.Contains(stored, `"user":"`) {
		t.Fatalf("NON-VACUITY: the stored line with its user attr must be captured, else the arm cannot fail; log=%s", out)
	}
	if strings.Contains(stored, `"user":""`) {
		t.Fatalf("NON-VACUITY: the representative label must be non-empty: %s", stored)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, psCarol) {
			t.Fatalf("#453 PRIVACY: a recorded representative's username reached a refresher log line: %s", line)
		}
	}
}

func TestIssue453_SeedLabelsNeverCarryBaseIdentity(t *testing.T) {
	const user = "bob.zq453@tenant.example"
	const other = "eve.zq453@tenant.example"
	const grp = "grp-zq453-private"
	cases := []struct {
		name  string
		label func() string
		tok   string
	}{
		{"cohort user", func() string { return fmt.Sprint(cohortLogLabel(seedTarget{Username: user, Groups: []string{grp}})) }, user},
		{"cohort user groups", func() string { return fmt.Sprint(cohortLogLabel(seedTarget{Username: user, Groups: []string{grp}})) }, grp},
		{"cohort group", func() string { return fmt.Sprint(cohortLogLabel(seedTarget{Groups: []string{grp}})) }, grp},
		{"ctx user", func() string {
			return seedIdentityLabelFromCtx(xcontext.BuildContext(context.Background(),
				xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: []string{grp}})))
		}, user},
		{"ctx group", func() string {
			return seedIdentityLabelFromCtx(xcontext.BuildContext(context.Background(),
				xcontext.WithUserInfo(jwtutil.UserInfo{Groups: []string{grp}})))
		}, grp},
	}
	for _, c := range cases {
		got := c.label()
		if got == "" || got == "anonymous" {
			t.Errorf("%s: NON-VACUITY: a real identity must render a non-empty, non-anonymous label, got %q", c.name, got)
		}
		if strings.Contains(got, c.tok) {
			t.Errorf("#453 PRIVACY %s: label %q carries %q in clear", c.name, got, c.tok)
		}
		if again := c.label(); again != got {
			t.Errorf("%s: the label must be stable for the process lifetime (%q vs %q)", c.name, got, again)
		}
	}
	if fmt.Sprint(cohortLogLabel(seedTarget{Username: user})) == fmt.Sprint(cohortLogLabel(seedTarget{Username: other})) {
		t.Error("two users must render distinct labels (correlation within a pod)")
	}
}

func TestIssue453_PerCallAndKeyDiagNeverNameRequester(t *testing.T) {
	const user = "dora.zq453@tenant.example"
	const grp = "grp-zq453-dora"
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: []string{grp, "system:authenticated"}}))

	var buf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	req := httptest.NewRequest("GET", "/call", nil).WithContext(ctx)
	_, emit := beginPerCall(req, "restactions")
	emit()
	emitDispatchCacheKeyDiag(slog.Default(), "s453", ctx, "key-453", nil,
		"restactions", "templates.krateo.io", "v1", "restactions", "ns", "n", 0, 0, nil)

	out := buf.String()
	for _, msg := range []string{"dispatcher.call.complete", "dispatch.cache_key.computed"} {
		if !strings.Contains(out, msg) {
			t.Fatalf("NON-VACUITY: %s was not captured; log=%s", msg, out)
		}
	}
	for _, line := range strings.Split(out, "\n") {
		for _, tok := range []string{user, grp} {
			if strings.Contains(line, tok) {
				t.Errorf("#453 PRIVACY: %q reached a log line: %s", tok, line)
			}
		}
	}
	if !strings.Contains(out, "system:authenticated") && !strings.Contains(out, `"groups":["group:`) {
		t.Fatalf("NON-VACUITY: the key diagnostic must still carry the (redacted) group set; log=%s", out)
	}
}
