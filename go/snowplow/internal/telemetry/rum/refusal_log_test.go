package rum

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestS1015_RefusalLogIsSampled — the refusal WARN line (F4) is sampled
// (#1020 delta D5): on a frozen clock, 50 refusals write exactly the
// documented burst of 20 lines, so a refusal flood cannot become a log flood.
// The counters stay the unsampled totals.
//
// MUTATION (RED, shown in the PR, the reviewer's M22): the line written for
// every refusal (50 lines).
func TestS1015_RefusalLogIsSampled(t *testing.T) {
	const docBurst, refusals = 20, 50
	var buf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	in := NewIngest(NewForwarder("http://127.0.0.1:1/v1/traces"), nil)
	frozen := time.Unix(1_761_800_000, 0)
	in.now = func() time.Time { return frozen }

	before := Read()
	for i := 0; i < refusals; i++ {
		req := httptest.NewRequest(http.MethodPost, Path, strings.NewReader("x"))
		req.Header.Set("Content-Type", "application/x-protobuf") // refused 415 before any body read
		req = req.WithContext(xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: fmt.Sprintf("flood-%d", i)})))
		rec := httptest.NewRecorder()
		in.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnsupportedMediaType {
			t.Fatalf("refusal %d: status %d, want 415", i, rec.Code)
		}
	}
	if d := Read().Requests["dropped_content_type"] - before.Requests["dropped_content_type"]; d != refusals {
		t.Fatalf("dropped_content_type delta %d, want %d: the counter is the unsampled total", d, refusals)
	}
	if n := strings.Count(buf.String(), `"msg":"rum.ingest.refused"`); n != docBurst {
		t.Fatalf("%d refusals on a frozen clock wrote %d refusal lines, want the burst of %d", refusals, n, docBurst)
	}
}
