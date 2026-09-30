// callread.go — #186. The POST /call/read body-decode middleware and its wire
// envelope. It lifts a request's `extras` off the URL query (where a large
// value is charged against the gateway's ~16 KiB request-header budget and
// 431s before Snowplow is ever invoked) and onto the request BODY, stashing
// the decoded map on the context so the context-first util.ParseExtras returns
// it. Mounted ONLY on POST /call/read; every other route is untouched.
package middleware

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
)

// CallReadMaxBodyBytes bounds the POST /call/read request body. It is a fixed
// DoS ceiling (like http.Server.MaxHeaderBytes), NOT a tunable knob — matching
// the existing hardcoded 1 MiB caps at handlers/jq.go (MaxBodySize) and the
// /call body read (call.go). 1 MiB is ~65× the ~15 KiB URL budget the body
// channel replaces, so a legitimate extras body is far under it.
const CallReadMaxBodyBytes = 1 * 1024 * 1024

// callReadBody is the POST /call/read wire envelope: {"extras": { ... }}. The
// wrapper (vs a bare extras object) is forward-compatible — a future read-body
// field can be added without ambiguity — and keeps the decoded `extras` value
// byte-for-byte what `?extras=<same json>` would yield, so the two channels
// derive the identical cache key (key parity by construction).
type callReadBody struct {
	Extras map[string]any `json:"extras"`
}

// BodyExtrasDecode decodes the {"extras":{...}} body of a POST /call/read
// request and stashes the extras map on the context via util.WithExtras, so
// the context-first util.ParseExtras (extras.go) returns it at all three call
// sites unchanged. A body larger than CallReadMaxBodyBytes is truncated by the
// LimitReader and then fails JSON decode → 400 (never silently forwarded); a
// malformed body is likewise a 400. The body is consumed here; the downstream
// resolve handlers read extras from the CONTEXT, not the body, and the
// read-only Call fallthrough (handlers.CallRead) never reads a body — so
// consuming it here is safe.
//
// The stash happens even when the decoded extras is nil (an empty or
// extras-less body): that pins the read route to the body channel and stops a
// stray `?extras=` in the URL from being read on POST /call/read.
func BodyExtrasDecode(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body callReadBody
		if r.Body != nil {
			dec := json.NewDecoder(io.LimitReader(r.Body, CallReadMaxBodyBytes))
			if err := dec.Decode(&body); err != nil {
				response.BadRequest(w, fmt.Errorf("invalid POST /call/read body (expected {\"extras\":{...}}): %w", err))
				return
			}
		}
		r = r.WithContext(util.WithExtras(r.Context(), body.Extras))
		next.ServeHTTP(w, r)
	})
}
