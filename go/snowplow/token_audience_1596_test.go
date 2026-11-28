package main

import (
	"bytes"
	"expvar"
	"log/slog"
	"strings"
	"testing"
)

// #1596 — the audience check is a security check that can turn off silently,
// so an empty TOKEN_AUDIENCE is a WARN at startup (never an INFO), says it is
// OFF and why, prints no value, and zeroes the gauge.
func TestTokenAudienceOptions1596_AnEmptyNameIsALoudWarn(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	if opts := tokenAudienceOptions(log, ""); len(opts) != 0 {
		t.Fatalf("an empty name must check nothing; got %d options", len(opts))
	}
	out := buf.String()
	if !strings.Contains(out, `"level":"WARN"`) || !strings.Contains(out, "audience check is OFF") || !strings.Contains(out, "TOKEN_AUDIENCE") {
		t.Fatalf("want a WARN saying the check is OFF and why; got %s", out)
	}
	if strings.Contains(out, `"level":"INFO"`) {
		t.Fatalf("the disabled check must not be an INFO: %s", out)
	}
	if got := expvar.Get("snowplow_token_audience_checked").(*expvar.Int).Value(); got != 0 {
		t.Fatalf("gauge = %d, want 0", got)
	}

	buf.Reset()
	if opts := tokenAudienceOptions(log, "snowplow.krateo-system"); len(opts) != 1 {
		t.Fatalf("a name must give one option; got %d", len(opts))
	}
	out = buf.String()
	if strings.Contains(out, "WARN") || strings.Contains(out, "snowplow.krateo-system") {
		t.Fatalf("an enabled check must not warn, and the name is never logged: %s", out)
	}
	if got := expvar.Get("snowplow_token_audience_checked").(*expvar.Int).Value(); got != 1 {
		t.Fatalf("gauge = %d, want 1", got)
	}
}
