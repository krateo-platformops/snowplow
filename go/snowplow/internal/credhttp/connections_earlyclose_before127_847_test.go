//go:build !go1.27

package credhttp

// connections_earlyclose_before127_847_test.go — #847: what a body closed before
// its end costs BEFORE Go 1.27. The connection is thrown away, and the next
// request opens a new one. connections_earlyclose_go127_847_test.go is the same
// sequence from Go 1.27 on, so the bump changes which arm runs and neither has
// to be rewritten; the difference between the two is what the paired run reads.

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// The third request is the control: it reuses, so it was the early close that
// lost the first connection, not the server.
func TestConnections847_EarlyClosedBodyOnHTTP1IsANewConnection(t *testing.T) {
	s := newWireServer(t, wirePlain, nil)
	cli := s.Client()
	before := connCells()
	sendUnread(t, context.Background(), cli, s.URL)
	get(t, EgressStage, cli, s.URL)
	get(t, EgressStage, cli, s.URL)
	s.assertWire(t, 2, 3)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 2, "stage/http1/handovers": 3})
}

// #847's first row: a retryable answer has its body closed unread, so the retry
// to the same host pays a new connect inside the page budget.
func TestConnections847_RetryAfterAnUnreadBodyIsANewConnection(t *testing.T) {
	seedReserve(t, 0)
	var n atomic.Int64
	s := newWireServer(t, wirePlain, func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("upstream is restarting"))
		}
	})
	cli := s.Client()
	before := connCells()
	if resp := send(t, budgetLeft(9*time.Second), EgressStage, cli, http.MethodGet, s.URL); resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want the retry's 200", resp.StatusCode)
	}
	s.assertWire(t, 2, 2)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 2, "stage/http1/handovers": 2})
}
