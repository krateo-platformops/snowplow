package dispatchers

// learned_identity_redactor_test.go — #262 redactor unit arm: every attribute
// shape a resolve-path site can hand slog (string, []string, error, group,
// []byte — which the JSON handler base64-encodes — and a WithAttrs prefix)
// leaves the class's tokens only as its sha256 label.

import (
	"bytes"
	"encoding/base64"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestS262_RedactorScrubsEveryAttrShape(t *testing.T) {
	const cn = "erin.zq9@tenant.example"
	const grp = "grp-zq9-private"
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	log := learnedRedactingLogger(base, cn, []string{grp}).With("who", cn)
	log.Info("probe "+cn,
		"user", cn,
		"groups", []string{grp, "system:authenticated"},
		"err", errors.New("denied for "+cn),
		"raw", []byte("subject="+cn),
		slog.Group("g", "inner", grp),
	)
	out := buf.String()
	for _, tok := range []string{cn, grp, base64.StdEncoding.EncodeToString([]byte("subject=" + cn))} {
		if strings.Contains(out, tok) {
			t.Fatalf("redactor leaked %q: %s", tok, out)
		}
	}
	if !strings.Contains(out, "learned:") || !strings.Contains(out, "system:authenticated") {
		t.Fatalf("NON-VACUITY: the label must replace the tokens and constants must survive: %s", out)
	}
}
