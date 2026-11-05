//go:build go1.27

package credhttp

// connections_earlyclose_go127_847_test.go — #847: what a body closed before its
// end costs from Go 1.27 on, the same sequences as
// connections_earlyclose_before127_847_test.go. The transport drains the body in
// the background and keeps the connection.
//
// The drain starts after Close returns, so a request sent right behind Close can
// beat it and open a connection anyway: the arm waits for the transport's own
// PutIdleConn report, never for a time. It compiles only on a Go 1.27 toolchain,
// which CI does not run until the bump.

import (
	"context"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnections847_EarlyClosedBodyOnHTTP1IsDrainedAndReused(t *testing.T) {
	s := newWireServer(t, wirePlain, nil)
	cli := s.Client()
	idle := make(chan error, 1)
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		PutIdleConn: func(err error) { idle <- err },
	})
	before := connCells()
	sendUnread(t, ctx, cli, s.URL)
	select {
	case err := <-idle:
		if err != nil {
			t.Fatalf("the transport did not keep the drained connection: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the transport never put the connection back: the body was not drained")
	}
	get(t, EgressStage, cli, s.URL)
	get(t, EgressStage, cli, s.URL)
	s.assertWire(t, 1, 3)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/new": 1, "stage/http1/reused": 2})
}

// #847's first row: the retry after an unread body reuses the connection. This
// one uses the path's own wait: the backoff before the retry, unseamed, so what
// it reads is what a customer's retry gets. The attempt's context ends right
// after Close (sendBudgeted's x.end()), and that must not stop the drain.
func TestConnections847_RetryAfterAnUnreadBodyReusesTheConnection(t *testing.T) {
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
	s.assertWire(t, 1, 2)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/new": 1, "stage/http1/reused": 1})
}

// The drain's bound: a body longer than 256 KiB is not drained, so the
// connection is lost as before. A share below 100% on Go 1.27 is not by itself
// a defect.
func TestConnections847_BodyOverTheDrainLimitIsANewConnection(t *testing.T) {
	body := strings.Repeat("x", 256<<10+1)
	s := newWireServer(t, wirePlain, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	})
	cli := s.Client()
	before := connCells()
	sendUnread(t, context.Background(), cli, s.URL)
	get(t, EgressStage, cli, s.URL)
	s.assertWire(t, 2, 2)
	assertConnDelta(t, before, map[string]uint64{"stage/http1/new": 2})
}
