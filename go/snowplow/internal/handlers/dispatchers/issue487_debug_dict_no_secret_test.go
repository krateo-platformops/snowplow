package dispatchers

// issue487_debug_dict_no_secret_test.go — #487 (SECURITY): at debug level the
// "resolved api" line (restactions.go) and the "base dict" line
// (api/resolve.go) dumped the whole resolve dict, so a stage that read a
// Secret wrote its data to stdout and then to otel_logs. Adopted from
// reviewer-424's #481 probe: the #443 inline-Secret harness with a debug JSON
// logger on ctx. alice's draft GETs a core Secret.
//
// RED on main 673bf9c8: the "resolved api" line carries the Secret's data.

import (
	"bytes"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
)

func TestIssue487_DebugDictLinesCarryNoStageBody(t *testing.T) {
	f := in443SecretSetup(t)
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := xcontext.BuildContext(in443Ctx(f, psAlice), xcontext.WithLogger(logger))
	obj := in443RA("draft-487", "", map[string]any{"name": "secret", "continueOnError": true, "path": in443SecretPath})
	rec := serveInline(t, f, ctx, obj)
	if rec.Code != http.StatusOK || !psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("SETUP: alice must be served the Secret (else the arm cannot fail); code=%d", rec.Code)
	}
	resolved := ""
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.Contains(line, `"msg":"resolved api"`) {
			resolved = line
		}
		if psHasSentinel([]byte(line)) {
			i := strings.Index(line, `"msg":"`)
			if i < 0 {
				i = 0
			}
			// Print the msg only: the rest of the line is the leaked body.
			t.Errorf("#487 a debug line carries the Secret's data: %s…", line[i:i+min(48, len(line)-i)])
		}
	}
	if resolved == "" {
		t.Fatalf("NON-VACUITY: the debug \"resolved api\" line was not captured; %d bytes logged", buf.Len())
	}
	if !strings.Contains(resolved, `"secret":{`) || !strings.Contains(resolved, `"sha256":`) {
		t.Fatalf("NON-VACUITY: the line must summarise the stage by id with a digest; got %.200s", resolved)
	}
}
