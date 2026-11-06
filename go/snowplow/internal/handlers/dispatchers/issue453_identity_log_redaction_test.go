// issue453_identity_log_redaction_test.go — #453, #1192: no refresher or
// dispatch log line carries a representative or requester, in clear or as a
// label.
//
// Pre-#453 a learned identity (#262) and a promoted recent hitter (#444) were
// redacted, but a RECORDED representative (the cell's first writer, a base
// cohort user) was logged in clear by the refresher ("user" on
// stage_error / external / uaf / stored). The same was true of the per-call
// line's requester and the dispatch key diagnostic. #453 rendered each as an
// internal/redact label; #1192 took them off the line.
//
// ARMS:
//   TestIssue453_RefresherRecordedRepresentativeNeverLogged — the real
//       resolveAndPopulateL1 over the #423 ps harness, recorded representative.
//   TestIssue453_PerCallAndKeyDiagNeverNameRequester — dispatcher.call.complete
//       and dispatch.cache_key.computed.
// The structural guard over every slog site is internal/redact's
// TestS453_NoLogSiteEmitsAnIdentityInClear.

package dispatchers

import (
	"context"
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
	if stored == "" {
		t.Fatalf("NON-VACUITY: the stored line must be captured, else the arm cannot fail; log=%s", out)
	}
	if strings.Contains(stored, `"user"`) {
		t.Fatalf("#1192: a refresher line carries a user attribute: %s", stored)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, psCarol) {
			t.Fatalf("#453 PRIVACY: a recorded representative's username reached a refresher log line: %s", line)
		}
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
	for _, key := range []string{`"user"`, `"username"`, `"groups"`} {
		if strings.Contains(out, key) {
			t.Errorf("#1192: a line carries the identity attribute %s; log=%s", key, out)
		}
	}
}
