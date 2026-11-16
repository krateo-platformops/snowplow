package rum

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

// batchBody is an OTLP/JSON body of n valid krateo.widget.fetch spans.
func batchBody(t *testing.T, n int) []byte {
	t.Helper()
	spans := make([]map[string]any, n)
	for i := range spans {
		spans[i] = map[string]any{
			"traceId":           "4bf92f3577b34da6a3ce929d0e0e4736",
			"spanId":            fmt.Sprintf("00f067aa0ba9%04x", i+1),
			"name":              SpanWidgetFetch,
			"kind":              3,
			"startTimeUnixNano": "1761800000000000000",
			"endTimeUnixNano":   "1761800000250000000",
		}
	}
	b, err := json.Marshal(map[string]any{"resourceSpans": []map[string]any{{
		"resource": map[string]any{"attributes": []map[string]any{
			{"key": "service.name", "value": map[string]any{"stringValue": ServiceName}},
		}},
		"scopeSpans": []map[string]any{{"spans": spans}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestS1015_GlobalSpanRateCap — the global spans/s cap into the collector
// (#1020 review F6), on a FROZEN clock so no token refills between batches and
// the arm cannot depend on how fast this machine serves a request: across
// distinct users (no per-user limit applies), the documented burst of 2048
// spans admits exactly 4 batches of 512; the 5th is refused 429 with
// Retry-After, counted forward_rate_limited and not forwarded.
//
// The documented numbers are literals here, so a change to the constants
// cannot loosen the arm with them.
//
// MUTATIONS (RED, shown in the PR): the global cap removed (the 5th batch is
// forwarded); the burst lowered to 2 batches (the 3rd is refused).
func TestS1015_GlobalSpanRateCap(t *testing.T) {
	const docBurst, docBatch = 2048, 512
	in := NewIngest(NewForwarder("http://127.0.0.1:1/v1/traces"), nil)
	frozen := time.Unix(1_761_800_000, 0)
	in.now = func() time.Time { return frozen }
	body := batchBody(t, docBatch)

	post := func(i int) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, Path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: fmt.Sprintf("global-%d", i)})))
		rec := httptest.NewRecorder()
		in.ServeHTTP(rec, req)
		return rec
	}

	before := Read()
	admitted := docBurst / docBatch
	for i := 0; i < admitted; i++ {
		if rec := post(i); rec.Code != http.StatusOK {
			t.Fatalf("batch %d of %d spans: status %d within the documented burst of %d", i, docBatch, rec.Code, docBurst)
		}
	}
	rec := post(admitted)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("batch %d (past the burst, clock frozen): status %d, want 429", admitted, rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("global-cap 429 without Retry-After")
	}
	if d := Read().Spans["forward_rate_limited"] - before.Spans["forward_rate_limited"]; d != docBatch {
		t.Fatalf("forward_rate_limited delta %d, want %d (one refused batch)", d, docBatch)
	}
}
