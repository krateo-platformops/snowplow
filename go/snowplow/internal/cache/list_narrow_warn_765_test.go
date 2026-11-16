// list_narrow_warn_765_test.go — #765 follow-up: the gate ALSO requires zero
// deps.list_narrow.would_change WARN lines in ClickHouse, because a dying pod's
// last unexported counter interval is lost. That only works if EVERY
// would_change verdict writes exactly one such line, at the moment of the
// verdict, with digests and never a body. This arm pins it.
package cache

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// lockedLog765 is a goroutine-safe slog sink (the #392 rule for captures).
type lockedLog765 struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedLog765) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedLog765) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func TestListNarrow765_W1_EveryWouldChangeWritesOneWarnLine(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	resetRefresherForTest()
	t.Cleanup(resetRefresherForTest)
	sink := &lockedLog765{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(sink, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	const bodyMarker = "body-marker-w1"
	prior := &ResolvedEntry{RawJSON: []byte(`{"v":"` + bodyMarker + `-a"}`)}
	write := func(key string, rebuilt string) {
		ctx := WithNarrowAuditAvoidOnlyForTest(context.Background(), key)
		NoteNarrowAuditWrite(ctx, key, prior, true, []byte(rebuilt), true)
	}
	before := ListNarrowAuditSnapshot()[AuditVerdictChanged]
	write("L1_w1_changed_1", `{"v":"`+bodyMarker+`-b"}`)
	write("L1_w1_identical", string(prior.RawJSON))
	write("L1_w1_changed_2", `{"v":"`+bodyMarker+`-c"}`)
	changed := ListNarrowAuditSnapshot()[AuditVerdictChanged] - before

	out := sink.String()
	lines := strings.Count(out, `"msg":"deps.list_narrow.would_change"`)
	if changed != 2 || uint64(lines) != changed {
		t.Fatalf("W1 RED: %d would_change verdicts but %d deps.list_narrow.would_change WARN lines; the gate's "+
			"ClickHouse check needs exactly one line per verdict:\n%s", changed, lines, out)
	}
	for _, want := range []string{"L1_w1_changed_1", "L1_w1_changed_2", "prior_sha256", "rebuilt_sha256"} {
		if !strings.Contains(out, want) {
			t.Fatalf("W1: the WARN lines must carry %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "L1_w1_identical") {
		t.Fatalf("W1: an identical verdict wrote a would_change line:\n%s", out)
	}
	if strings.Contains(out, bodyMarker) {
		t.Fatalf("W1 RED: a cell body leaked into the would_change log line (digests only):\n%s", out)
	}
}
