// issue1192_refresher_identity_logs_test.go — #1192: no line of a background
// refresh carries the representative's name or groups, at any level, in clear or
// as a label.
//
// THE LOGGER. In production the refresher's logger is main's: cacheCtx carries
// the logger buildLogHandler built (main.go, xcontext.WithLogger). That function
// lives in package main, which a test here cannot import, so this arm builds the
// same base handler with the same redact.ScrubLogAttr and takes it off the
// context the same way; what it leaves out is the trace decorator, which adds
// two ids. main_identity_logs_1192_test.go proves buildLogHandler installs the
// scrub. The driver is the real resolveAndPopulateL1 over the #423 ps harness.
//
// The request half is main_identity_logs_1192_test.go.
package dispatchers

import (
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

const i1192Probe = "i1192.probe"

// i1192Refresh runs one real refresh of e's cell under the production logger
// shape; touch runs on the resolve's context, after the real resolve, and is how
// an arm makes the refresh end in one branch or another.
func i1192Refresh(t *testing.T, e *i444Env, touch func(context.Context)) []string {
	t.Helper()
	restore := setResolveOnceForTest(func(ctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		ctx = cache.WithBackgroundResolve(ctx)
		raw, err := resolveRestActionForRefresh(ctx, objects.Result{GVR: h1RAGVR, Unstructured: e.cr.DeepCopy()}, in, psAuthnNS)
		log := xcontext.Logger(ctx)
		log.Debug(i1192Probe)
		log.Info(i1192Probe)
		log.Warn(i1192Probe)
		log.Error(i1192Probe)
		if touch != nil {
			touch(ctx)
		}
		return raw, err
	})
	t.Cleanup(restore)

	var capture syncBuffer
	logger := slog.New(slog.NewJSONHandler(&capture, &slog.HandlerOptions{Level: slog.LevelDebug, ReplaceAttr: redact.ScrubLogAttr}))
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(logger))
	if err := resolveAndPopulateL1(ctx, e.stored, e.saEP, e.saRC); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	return strings.Split(strings.TrimSpace(capture.String()), "\n")
}

// i1192Assert is the arm: no line holds the representative's name or group or an
// identity key, the branch's own line is among them, and the probe proves the
// capture saw every level.
func i1192Assert(t *testing.T, e *i444Env, lines []string, wantMsg string) {
	t.Helper()
	tokens := append([]string{e.stored.RepresentativeUsername}, psGroups(e.a)...)
	if len(tokens) < 2 || tokens[0] == "" {
		t.Fatalf("PRE: the arm needs a representative name and a group, got %q", tokens)
	}
	// Matched as a whole word, not a substring: the harness's binding UID
	// "uid-rb-portal-target" embeds the group name and is not an identity.
	var matchers []*regexp.Regexp
	for _, tok := range tokens {
		matchers = append(matchers, regexp.MustCompile(`(^|[^-\w])`+regexp.QuoteMeta(tok)+`([^-\w]|$)`))
	}
	levels := map[string]bool{}
	branch := false
	for _, line := range lines {
		for i, m := range matchers {
			if m.MatchString(line) {
				t.Errorf("#1192: the representative %q reached a line: %s", tokens[i], line)
			}
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("a line is not one JSON object (%v): %s", err, line)
		}
		for _, k := range []string{"user", "username", "groups", "identity", "cohort"} {
			if _, on := rec[k]; on {
				t.Errorf("#1192: identity attribute %q on a refresher line: %s", k, line)
			}
		}
		msg, _ := rec["msg"].(string)
		if msg == i1192Probe {
			levels[rec["level"].(string)] = true
		}
		branch = branch || strings.Contains(msg, wantMsg)
	}
	// POSITIVE CONTROL.
	for _, l := range []string{"DEBUG", "INFO", "WARN", "ERROR"} {
		if !levels[l] {
			t.Errorf("CONTROL: no %s line reached the capture; lines=%v", l, lines)
		}
	}
	if !branch {
		t.Errorf("CONTROL: the refresh never reached %q, so the attribute this arm is about was not on trial; lines=%v", wantMsg, lines)
	}
}

func TestIssue1192_Refresher_StoredLine(t *testing.T) {
	e := i444Setup(t, k423PortalReaders(psArm{target: psConfigmapsGVR})...)
	i1192Assert(t, e, i1192Refresh(t, e, nil), "re-resolved + stored")
}

// The line 057 counted (121 rows in 24 h). The stage error carries the text an
// apiserver writes, so the sample on the line is on trial as well as the attribute.
func TestIssue1192_Refresher_StageErrorLine(t *testing.T) {
	e := i444Setup(t, k423PortalReaders(psArm{target: psConfigmapsGVR})...)
	lines := i1192Refresh(t, e, func(ctx context.Context) {
		sink := cache.StageErrorSinkFromContext(ctx)
		if sink == nil {
			t.Error("PRE: the refresher installs a stage-error sink")
			return
		}
		sink.Bump("denied", `configmaps is forbidden: User "`+e.stored.RepresentativeUsername+`" cannot list resource "configmaps"; `+
			`groups "`+psGroups(e.a)[0]+`" is forbidden`)
	})
	i1192Assert(t, e, lines, "stage error during refresh")
	if out := strings.Join(lines, "\n"); !strings.Contains(out, "cannot list resource") {
		t.Errorf("CONTROL: the stage error's text must reach the line, so its identity was scrubbed and not absent; lines=%v", lines)
	}
}

func TestIssue1192_Refresher_ExternalLine(t *testing.T) {
	e := i444Setup(t, k423PortalReaders(psArm{target: psConfigmapsGVR})...)
	lines := i1192Refresh(t, e, func(ctx context.Context) {
		sink := cache.ExternalTouchedSinkFromContext(ctx)
		if sink == nil {
			t.Error("PRE: the refresher installs an external-touched sink")
			return
		}
		sink.Bump()
	})
	i1192Assert(t, e, lines, "touched an external endpoint")
}
