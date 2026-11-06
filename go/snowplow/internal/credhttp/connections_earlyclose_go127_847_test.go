//go:build go1.27

package credhttp

// connections_earlyclose_go127_847_test.go — #847: what a body closed before its
// end costs from Go 1.27 on, the same sequences as
// connections_earlyclose_before127_847_test.go. The transport drains the body in
// the background and keeps the connection.
//
// The drain starts after Close returns, so a request sent right behind Close can
// beat it and open a connection anyway: these arms wait for the transport to
// decide (watchPool), never for a time. They compile only on a Go 1.27 toolchain,
// which CI does not run until the bump.

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnections847_EarlyClosedBodyOnHTTP1IsDrainedAndReused(t *testing.T) {
	untilWire(t, 1, 3, "stage/http1", func(t *testing.T) *wireServer {
		s := newWireServer(t, wirePlain, nil)
		cli := s.Client()
		ctx, kept := watchPool(context.Background())
		sendUnread(t, ctx, cli, s.URL)
		decided(t, s, kept)
		get(t, EgressStage, cli, s.URL)
		get(t, EgressStage, cli, s.URL)
		return s
	})
}

// #847's first row: the retry after an unread body reuses the connection. This
// one uses the path's own wait: the backoff before the retry, unseamed, so what
// it reads is what a customer's retry gets. The attempt's context ends right
// after Close (sendBudgeted's x.end()), and that must not stop the drain.
func TestConnections847_RetryAfterAnUnreadBodyReusesTheConnection(t *testing.T) {
	seedReserve(t, 0)
	untilWire(t, 1, 2, "stage/http1", func(t *testing.T) *wireServer {
		var n atomic.Int64
		s := newWireServer(t, wirePlain, func(w http.ResponseWriter, _ *http.Request) {
			if n.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("upstream is restarting"))
			}
		})
		if resp := send(t, budgetLeft(9*time.Second), EgressStage, s.Client(), http.MethodGet, s.URL); resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d, want the retry's 200", resp.StatusCode)
		}
		return s
	})
}

// The drain's bound, from both sides. 256 KiB is drained and one byte more is not,
// whether the length is declared or the body is chunked, so a share below 100% on
// Go 1.27 is not by itself a defect. Whether the transport kept the connection is
// read from its own report and from the server seeing it close, so the next
// request can never be the reason a connection was lost.
func TestConnections847_DrainStopsAtItsLimit(t *testing.T) {
	const limit = 256 << 10
	for _, tc := range []struct {
		name    string
		size    int
		chunked bool
		kept    bool
	}{
		{"exactly the limit, declared", limit, false, true},
		{"one byte over, declared", limit + 1, false, false},
		{"exactly the limit, chunked", limit, true, true},
		{"one byte over, chunked", limit + 1, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Repeat("x", tc.size)
			big := func(t *testing.T) *wireServer {
				return newWireServer(t, wirePlain, func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/big" {
						_, _ = w.Write([]byte("ok"))
						return
					}
					if tc.chunked {
						w.(http.Flusher).Flush()
					} else {
						w.Header().Set("Content-Length", strconv.Itoa(tc.size))
					}
					_, _ = w.Write([]byte(body))
				})
			}
			if tc.kept {
				untilWire(t, 1, 2, "stage/http1", func(t *testing.T) *wireServer {
					s := big(t)
					cli := s.Client()
					ctx, kept := watchPool(context.Background())
					sendUnread(t, ctx, cli, s.URL+"/big")
					decided(t, s, kept)
					get(t, EgressStage, cli, s.URL)
					return s
				})
				return
			}
			s := big(t)
			cli := s.Client()
			ctx, kept := watchPool(context.Background())
			before := connCells()
			sendUnread(t, ctx, cli, s.URL+"/big")
			if decided(t, s, kept) {
				t.Fatal("the transport kept a connection it had to drain past the limit to keep")
			}
			get(t, EgressStage, cli, s.URL)
			s.assertWire(t, 2, 2)
			assertConnDelta(t, before, map[string]uint64{"stage/http1/opened": 2, "stage/http1/handovers": 2})
		})
	}
}
