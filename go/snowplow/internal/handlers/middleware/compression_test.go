// compression_test.go — Track 2 (transport gzip) falsifiers.
//
// Falsifier arms (docs/seed-tail-restactions-budget-and-16mb-serve-trace-2026-07-04.md
// Track 2; PM conditions T2-1..4):
//
//   - T2-1 BYTE-IDENTITY: an Accept-Encoding: gzip request over a canned
//     handler returns a gzip-encoded body whose DECOMPRESSED bytes are
//     byte-for-byte the handler's uncompressed output. Transport-only, zero
//     content-shape change.
//
//   - Accept-Encoding gate (T2-3): a request WITHOUT Accept-Encoding: gzip
//     gets no Content-Encoding header and a body byte-identical to the raw
//     handler output.
//
//   - T2-2 SSE arm (GREEN): a text/event-stream handler under the production
//     wrapper is served UNCOMPRESSED (no Content-Encoding: gzip) AND its first
//     event is flushed to the client BEFORE the handler completes (incremental
//     delivery). The exclusion's observable effect is the absent
//     Content-Encoding header.
//
//   - T2-2 MUTATION (RED): with the SSE exclusion dropped (CompressAll), the
//     SAME text/event-stream handler IS compressed — the response carries
//     Content-Encoding: gzip. The production wrapper's Content-Encoding is
//     empty; the mutation's is "gzip". THIS is the discriminating assert.
//
//     WHY NOT "buffered vs delivered": empirically verified (zz_diag probe,
//     2026-07-04) that gzhttp preserves INCREMENTAL FLUSH even when
//     compressing — its Flush() calls the gzip writer's Flush() (a sync-flush
//     emitting a stored block for sub-minSize payloads) then the underlying
//     http.Flusher, so a tiny first SSE event reaches the client before the
//     handler completes on BOTH the excluded and compressed paths. A
//     buffering-based RED arm is therefore IMPOSSIBLE with this gzhttp
//     version. The exclusion is still REQUIRED: an EventSource stream must not
//     carry Content-Encoding: gzip (browsers + intermediary proxies treat a
//     compressed text/event-stream inconsistently; X-Accel-Buffering: no and a
//     content-encoding are contradictory hints), and compressing tiny per-event
//     frames is CPU with zero wire benefit. The load-bearing, observable
//     difference is the Content-Encoding header — that is what the RED arm
//     pins. If a future gzhttp DOES buffer SSE, the incremental-delivery
//     assertion in the GREEN arm additionally catches it.
//
//   - C-1 IDLE-CONNECT (arch-required): an SSE handler shaped exactly like
//     /refreshes (Content-Type: text/event-stream, WriteHeader, then a
//     `: connected` comment preamble, Flush, then BLOCK writing nothing) under
//     the production Gzip() wrapper must commit response headers promptly at
//     connect (client.Do returns < 100ms) with NO event ever written. RED
//     proof: the same handler WITHOUT the preamble does NOT commit headers
//     promptly — gzhttp's WriteHeader only saves the status and a zero-byte
//     Flush is a no-op, so headers are withheld until the first body write
//     (a heartbeat, up to 20s in prod). The comment byte is what forces
//     gzhttp onto startPlain so headers commit. This is why the /refreshes
//     handler emits `: connected` before its initial flush at both sites.
//
//   - GZIP-ONLY NEGOTIATION (#838): a table of the Accept-Encoding values real
//     clients send (Chrome's "gzip, deflate, br, zstd", zstd-only, k6, Go)
//     against the expected Content-Encoding. gzip is the only encoding ever
//     chosen, and SSE is never encoded. RED: drop gzhttp.EnableZstd(false)
//     and the chrome and zstd-only rows get zstd.
package middleware

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/gzhttp"
)

// bigJSONBody is a >1 KiB JSON-ish payload so it clears gzhttp's 1 KiB
// DefaultMinSize (below which gzhttp declines to compress regardless of
// Accept-Encoding). The estate-graph payload Track 2 targets is ~16 MB, so a
// real /call response always clears minSize; this fixture mirrors that.
var bigJSONBody = []byte(`{"items":[` + strings.Repeat(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"default"}},`, 64) + `{"tail":true}]}`)

// cannedJSONHandler writes bigJSONBody as application/json.
func cannedJSONHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bigJSONBody)
	})
}

// TestGzip_ByteIdentity_DecompressedEqualsOriginal is the T2-1 arm. It drives
// the REAL middleware.Gzip wrapper (the exact wire construction main.go uses)
// with an Accept-Encoding: gzip request and asserts the response is gzip and
// its decompressed body equals the handler's raw output byte-for-byte.
func TestGzip_ByteIdentity_DecompressedEqualsOriginal(t *testing.T) {
	srv := httptest.NewServer(Gzip(cannedJSONHandler()))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if got := resp.Header.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip (payload %d bytes should clear minSize)", got, len(bigJSONBody))
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	gr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	decompressed, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	if !bytes.Equal(decompressed, bigJSONBody) {
		t.Fatalf("decompressed body != original:\n got %d bytes\nwant %d bytes", len(decompressed), len(bigJSONBody))
	}
}

// TestGzip_NoAcceptEncoding_ByteIdenticalUncompressed is the Accept-Encoding
// gate arm (T2-3). Without Accept-Encoding: gzip the response must carry no
// Content-Encoding and the body must be byte-identical to the raw handler
// output — a curl diagnostic sees exactly the pre-Track-2 bytes.
func TestGzip_NoAcceptEncoding_ByteIdenticalUncompressed(t *testing.T) {
	srv := httptest.NewServer(Gzip(cannedJSONHandler()))
	defer srv.Close()

	// Go's http.Transport transparently adds Accept-Encoding: gzip and
	// decodes unless we opt out. DisableCompression + no explicit header =
	// a client that does NOT advertise gzip.
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	// Explicitly ensure no gzip advertisement.
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()

	if ce := resp.Header.Get("Content-Encoding"); ce != "" {
		t.Fatalf("Content-Encoding = %q, want empty (no gzip advertised)", ce)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(body, bigJSONBody) {
		t.Fatalf("uncompressed body != original: got %d bytes, want %d", len(body), len(bigJSONBody))
	}
}

// firstEventSignal is closed by the SSE handler AFTER it writes+flushes its
// first event, and holdOpen blocks the handler from completing until the test
// releases it. This lets the test assert the client observed the first event
// while the handler is still running — i.e. genuine incremental delivery, not
// a full-buffer-at-close artifact.
type sseProbe struct {
	wroteFirst chan struct{}
	holdOpen   chan struct{}
}

// sseHandler writes ONE tiny event (well under gzhttp's 1 KiB minSize),
// flushes, signals wroteFirst, then blocks on holdOpen before writing a
// second event and returning. content-type is text/event-stream.
func (p *sseProbe) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)

		fl, ok := w.(http.Flusher)
		if !ok {
			// The wrapped writer must still expose Flush for SSE to work.
			// If it doesn't, the SSE contract is already broken.
			p.wroteFirst <- struct{}{}
			return
		}
		_, _ = io.WriteString(w, "event: refresh\ndata: k1\n\n")
		fl.Flush()
		p.wroteFirst <- struct{}{}

		<-p.holdOpen
		_, _ = io.WriteString(w, "event: refresh\ndata: k2\n\n")
		fl.Flush()
	})
}

// readFirstEvent reads from the streaming response body until it sees the
// first SSE event terminator (\n\n), returning true if it arrived within the
// deadline. It reads in a goroutine so a buffered (never-delivered) first
// event manifests as a timeout rather than a hang.
func readFirstEvent(t *testing.T, body io.Reader, within time.Duration) bool {
	t.Helper()
	got := make(chan bool, 1)
	go func() {
		buf := make([]byte, 0, 256)
		tmp := make([]byte, 64)
		for {
			n, err := body.Read(tmp)
			if n > 0 {
				buf = append(buf, tmp[:n]...)
				if bytes.Contains(buf, []byte("data: k1")) {
					got <- true
					return
				}
			}
			if err != nil {
				got <- false
				return
			}
		}
	}()
	select {
	case ok := <-got:
		return ok
	case <-time.After(within):
		return false
	}
}

// runSSEArm drives an SSE handler under the supplied wrapper against a real
// httptest server with a gzip-advertising client. It returns the response's
// Content-Encoding (the discriminating signal: "" for the excluded/production
// path, "gzip" for the compressed/mutation path) and whether the first tiny
// event was delivered incrementally (before the handler completed).
func runSSEArm(t *testing.T, wrap func(http.Handler) http.Handler) (contentEncoding string, deliveredIncrementally bool) {
	t.Helper()
	probe := &sseProbe{
		wroteFirst: make(chan struct{}, 1),
		holdOpen:   make(chan struct{}),
	}
	srv := httptest.NewServer(wrap(probe.handler()))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept-Encoding", "gzip")
	// Do NOT let the transport auto-gunzip: we observe the raw stream so the
	// Content-Encoding header survives and (on both paths) the literal
	// first-event marker is present in the raw bytes — gzip stores a
	// sub-minSize payload as an uncompressed block, so "data: k1" appears
	// verbatim inside the gzip frame too (verified 2026-07-04).
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	// Release the handler only AFTER the read window so the second write
	// cannot be what unblocks a would-be-buffered first event.
	defer close(probe.holdOpen)

	// Wait until the handler has written+flushed its first event so the read
	// below is testing transport delivery, not a handler that never wrote.
	select {
	case <-probe.wroteFirst:
	case <-time.After(2 * time.Second):
		t.Fatalf("handler never wrote first event")
	}

	delivered := readFirstEvent(t, resp.Body, 2*time.Second)
	return resp.Header.Get("Content-Encoding"), delivered
}

// TestGzip_SSE_Excluded_UncompressedAndIncremental is the T2-2 GREEN arm.
// Under the production wrapper (text/event-stream excluded) the SSE response
// carries NO Content-Encoding: gzip AND the first tiny event is delivered
// incrementally (before the handler completes).
func TestGzip_SSE_Excluded_UncompressedAndIncremental(t *testing.T) {
	ce, delivered := runSSEArm(t, func(h http.Handler) http.Handler { return Gzip(h) })
	if ce != "" {
		t.Fatalf("Content-Encoding = %q, want empty — SSE must NOT be compressed under the production wrapper", ce)
	}
	if !delivered {
		t.Fatalf("SSE first event was NOT delivered incrementally under the production wrapper — flush passthrough broken")
	}
}

// TestGzip_SSE_RED_CompressedWithoutException is the T2-2 MUTATION arm. It
// drops the SSE exclusion (CompressAllContentTypeFilter compresses
// text/event-stream too); the response then carries Content-Encoding: gzip.
// The production wrapper's Content-Encoding is empty (GREEN arm above) — this
// RED arm's is "gzip". THAT divergence is what the exclusion controls and is
// the discriminating assert. (Incremental delivery holds on BOTH paths with
// this gzhttp version — see the file header — so it is not the discriminator.)
func TestGzip_SSE_RED_CompressedWithoutException(t *testing.T) {
	compressAll := func(h http.Handler) http.Handler {
		wrapper, err := gzhttp.NewWrapper(
			gzhttp.ContentTypeFilter(gzhttp.CompressAllContentTypeFilter),
		)
		if err != nil {
			t.Fatalf("build compress-all wrapper: %v", err)
		}
		return wrapper(h)
	}
	ce, _ := runSSEArm(t, compressAll)
	if ce != "gzip" {
		t.Fatalf("MUTATION expected Content-Encoding: gzip on SSE without the exclusion, got %q — the exclusion may no longer be the mechanism that keeps SSE uncompressed (gzhttp semantics changed?)", ce)
	}
}

// idleConnectHandler mimics /refreshes' connect sequence: set the SSE
// content-type, WriteHeader(200), OPTIONALLY write the `: connected` comment
// preamble, Flush, then BLOCK writing nothing until released. withPreamble
// selects the production behavior (preamble present) vs the RED variant
// (preamble absent). It never writes an event, so the ONLY thing that can
// commit response headers early is the preamble byte.
func idleConnectHandler(withPreamble bool, release <-chan struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		if withPreamble {
			_, _ = io.WriteString(w, ": connected\n\n")
		}
		if fl != nil {
			fl.Flush()
		}
		<-release // idle: no event ever written
	})
}

// connectHeadersArrive drives an idleConnectHandler under wrap with a
// gzip-advertising client whose request context times out after `within`. It
// returns whether client.Do returned (response headers received) before the
// timeout. Headers-received is the exact EventSource "open" signal: a client
// stuck without headers sits in CONNECTING. The handler is released after Do
// resolves so the second (never-written) event cannot be what unblocks it.
func connectHeadersArrive(t *testing.T, wrap func(http.Handler) http.Handler, withPreamble bool, within time.Duration) bool {
	t.Helper()
	release := make(chan struct{})

	srv := httptest.NewServer(wrap(idleConnectHandler(withPreamble, release)))
	// Teardown order matters: the handler blocks on <-release, so the client
	// connection stays active and srv.Close would wait on it. Release the
	// handler FIRST, then close the server. Deferred LIFO: srv.Close runs
	// before close(release), so do it explicitly here instead.
	defer func() {
		close(release)
		srv.CloseClientConnections() // drop the lingering conn to the (now unblocked) handler
		srv.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept-Encoding", "gzip")
	// A dedicated transport we can close so the lingering server-side
	// connection to the blocked handler is torn down promptly at teardown.
	tr := &http.Transport{DisableCompression: true}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}

	resp, err := client.Do(req)
	if err != nil {
		// Context deadline exceeded before headers → headers did NOT arrive.
		return false
	}
	// Cancel the request context so the client abandons the still-open body,
	// letting the server connection close without waiting on the handler.
	cancel()
	resp.Body.Close()
	return true
}

// TestGzip_SSE_C1_IdleConnect_HeadersCommitAtConnect is the C-1 GREEN arm.
// Under the production Gzip() wrapper, an idle SSE handler that emits the
// `: connected` preamble before its first flush commits response headers
// promptly (< 100ms) even though no event is ever written.
func TestGzip_SSE_C1_IdleConnect_HeadersCommitAtConnect(t *testing.T) {
	arrived := connectHeadersArrive(t, func(h http.Handler) http.Handler { return Gzip(h) }, true, 100*time.Millisecond)
	if !arrived {
		t.Fatalf("C-1: SSE response headers did NOT commit within 100ms under the production wrapper with the `: connected` preamble — EventSource would sit in CONNECTING")
	}
}

// TestGzip_SSE_C1_RED_NoPreambleHeadersWithheld is the C-1 MUTATION arm. With
// the preamble REMOVED, an idle SSE handler under the production Gzip() wrapper
// does NOT commit headers within the window: gzhttp's WriteHeader only saves
// the status and a zero-byte Flush is a no-op, so headers are withheld until
// the first body write (which, for an idle stream, never comes inside the
// window). This RED proves the preamble is the mechanism committing headers at
// connect — the reason /refreshes writes `: connected` before its flush.
func TestGzip_SSE_C1_RED_NoPreambleHeadersWithheld(t *testing.T) {
	arrived := connectHeadersArrive(t, func(h http.Handler) http.Handler { return Gzip(h) }, false, 500*time.Millisecond)
	if arrived {
		t.Fatalf("C-1 MUTATION expected headers WITHHELD without the preamble, but they arrived — gzhttp may no longer buffer headers on a zero-byte flush (semantics changed?); the preamble's necessity must be re-evaluated")
	}
}

// sseBigHandler writes a text/event-stream body larger than gzhttp's 1 KiB
// minSize, so the only thing that keeps it uncompressed is the SSE exclusion,
// not the size floor.
func sseBigHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", sseContentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: " + string(bigJSONBody) + "\n\n"))
	})
}

// TestGzip_AcceptEncodingNegotiation_GzipOnly pins the Content-Encoding the
// production wrapper picks for the Accept-Encoding values real clients send
// (#838). gzhttp in klauspost/compress >= 1.18.7 enables zstd by default and
// prefers it at equal qvalues. Without EnableZstd(false), Chrome's
// "gzip, deflate, br, zstd" gets zstd and a zstd-only client gets zstd instead
// of identity. Both rows go RED if that option is removed. The other tests in
// this file send only "gzip" and cannot see the change.
func TestGzip_AcceptEncodingNegotiation_GzipOnly(t *testing.T) {
	cases := []struct {
		name           string
		handler        http.Handler
		acceptEncoding string
		want           string // expected Content-Encoding; "" = identity
	}{
		{"chrome", cannedJSONHandler(), "gzip, deflate, br, zstd", "gzip"},
		{"zstd-only", cannedJSONHandler(), "zstd", ""},
		{"k6-http", cannedJSONHandler(), "gzip, deflate", "gzip"},
		{"gzip", cannedJSONHandler(), "gzip", "gzip"},
		{"sse-chrome", sseBigHandler(), "gzip, deflate, br, zstd", ""},
		{"sse-zstd-only", sseBigHandler(), "zstd", ""},
		{"sse-gzip", sseBigHandler(), "gzip", ""},
	}
	// DisableCompression: the transport must neither add Accept-Encoding nor
	// decode the body behind the test's back.
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(Gzip(tc.handler))
			defer srv.Close()

			req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Accept-Encoding", tc.acceptEncoding)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			if got := resp.Header.Get("Content-Encoding"); got != tc.want {
				t.Fatalf("Accept-Encoding %q: Content-Encoding = %q, want %q", tc.acceptEncoding, got, tc.want)
			}
			if tc.want == "gzip" {
				gr, err := gzip.NewReader(bytes.NewReader(body))
				if err != nil {
					t.Fatalf("gzip reader: %v", err)
				}
				if body, err = io.ReadAll(gr); err != nil {
					t.Fatalf("gunzip: %v", err)
				}
			}
			if !bytes.Contains(body, bigJSONBody) {
				t.Fatalf("decoded body does not carry the handler's payload (%d bytes)", len(body))
			}
		})
	}
}
