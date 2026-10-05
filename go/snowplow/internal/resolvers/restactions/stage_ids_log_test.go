package restactions

// stage_ids_log_test.go — #487/#490: the "resolved api" debug line's stage
// summary. It renders spec stage ids mapped to their key labels and nothing
// else, and at WARN the whole line costs zero allocations (the lazy
// pointer-shaped LogValuers are never resolved).

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

func TestS490_StageIDsLogRendersIdsAndLabelsOnly(t *testing.T) {
	in := &templates.RESTAction{}
	in.Spec.API = []*templates.API{{Name: "secret", Path: "/api/v1/namespaces/x/secrets/zq490-path"}, nil, {Name: "list"}}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("m", slog.Any("stages", stageIDsLog{api: &in.Spec.API}))
	out := buf.String()
	for _, want := range []string{`"secret":"` + redact.KeyLabel("secret") + `"`, `"list":"` + redact.KeyLabel("list") + `"`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in %s", want, out)
		}
	}
	if strings.Contains(out, "zq490-path") {
		t.Errorf("only the stage id may be rendered, not the stage spec: %s", out)
	}
}

func TestS490_ResolvedAPILineZeroAllocAtWarn(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx := context.Background()
	in := &templates.RESTAction{}
	in.Spec.API = []*templates.API{{Name: "secret"}}
	dict := map[string]any{"secret": map[string]any{"data": map[string]any{"k": "v"}}}
	n := testing.AllocsPerRun(200, func() {
		log.LogAttrs(ctx, slog.LevelDebug, "resolved api",
			redact.DictAttr("dict", dict),
			slog.Any("stages", stageIDsLog{api: &in.Spec.API}),
		)
	})
	if n != 0 {
		t.Fatalf("the resolved api line at WARN: %v allocs/op, want 0", n)
	}
}
