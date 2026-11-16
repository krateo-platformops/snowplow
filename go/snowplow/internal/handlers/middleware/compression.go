// compression.go — Track 2 (transport gzip, docs/seed-tail-restactions-budget-and-16mb-serve-trace-2026-07-04.md).
//
// Gzip wraps the snowplow mux with klauspost gzhttp's transport-only gzip
// compression. A warm /call HIT serves stored bytes verbatim (µs), but the
// estate-graph JSON (~16 MB) and 50K datagrid payloads previously went
// uncompressed on the wire; gzip gives ~10-20x on resource-graph JSON.
//
// TRANSPORT-ONLY (T2-1): compression is a wire-encoding, applied AFTER the
// handler has produced its bytes. The decompressed body is byte-for-byte the
// handler's output — no content-shape change. Every /call, /list, /rbac,
// /debug/vars response is unchanged once decoded.
//
// ACCEPT-ENCODING-GATED (T2-3): gzhttp only compresses when the client sends
// `Accept-Encoding: gzip` (gzhttp.acceptsGzip). A curl diagnostic or a
// non-gzip client receives the identical uncompressed bytes with no
// Content-Encoding header — the off-path is byte-identical to pre-Track-2.
//
// SSE EXCLUSION (T2-2, HARD): the GET /refreshes live-refresh stream emits
// `text/event-stream`. A buffering compressor breaks incremental flush — a
// small first event (< gzhttp's 1 KiB minSize) would sit in the compressor's
// buffer instead of reaching the browser EventSource immediately. gzhttp's
// DefaultContentTypeFilter treats text/event-stream as compressible, so the
// exclusion is REQUIRED, not incidental: ExceptContentTypes forces the
// gzhttp writer onto its plain (ignore) path for that content-type, so the
// handler's per-event Flush() passes straight through to the underlying
// http.Flusher. This is the ONE exclusion list; it is applied generically by
// content-type, not per-route, so any future SSE endpoint inherits it by
// setting Content-Type: text/event-stream.
//
// GZIP ONLY, NO ZSTD (#838): starting with klauspost/compress 1.18.7, gzhttp
// enables zstd by default and prefers it whenever the client accepts both at
// the same qvalue. Every browser sends "gzip, deflate, br, zstd", so the
// default would move every portal /call response from gzip to zstd, and a
// zstd-only client from identity to zstd. EnableZstd(false) keeps the wire
// encoding as it was before the bump. Adopting zstd is a separate decision
// that needs its own bench comparison; it does not ride a dependency bump.
package middleware

import (
	"net/http"

	"github.com/klauspost/compress/gzhttp"
)

// sseContentType is the media type the /refreshes SSE stream sets. gzhttp
// matches ExceptContentTypes on the media-type prefix (params like charset
// are ignored), so the bare type is the correct exclusion key.
const sseContentType = "text/event-stream"

// Gzip returns an http.Handler that wraps h with Accept-Encoding-gated gzip
// compression, excluding text/event-stream so the SSE stream keeps its
// incremental-flush semantics. zstd is disabled, so gzip is the only encoding
// ever negotiated (see the GZIP ONLY note above). The gzhttp wrapper is constructed once at wire
// time; NewWrapper only errors on invalid options (a static, tested option
// set here), so a construction failure is a programmer error and panics
// rather than silently shipping an uncompressed server.
func Gzip(h http.Handler) http.Handler {
	wrapper, err := gzhttp.NewWrapper(
		gzhttp.ExceptContentTypes([]string{sseContentType}),
		gzhttp.EnableZstd(false),
	)
	if err != nil {
		panic("middleware.Gzip: gzhttp.NewWrapper: " + err.Error())
	}
	return wrapper(h)
}
