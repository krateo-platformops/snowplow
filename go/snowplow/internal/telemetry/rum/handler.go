package rum

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"strconv"
	"sync"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"
)

const (
	// Path is the ingest URL path, under the same base URL the portal uses
	// for /call. Pattern is its ServeMux pattern.
	Path    = "/telemetry/v1/traces"
	Pattern = "POST " + Path

	// MaxBodyBytes caps one request body (413 above it). The browser's
	// BatchSpanProcessor sends at most 512 spans per export by default; a
	// krateo.* span with its three attributes is well under 1 KiB of OTLP/JSON.
	MaxBodyBytes = 512 << 10
	// MaxSpansPerRequest caps the spans in one request (413 above it): the
	// browser exporter's default maxExportBatchSize.
	MaxSpansPerRequest = 512

	// PerUserRate and PerUserBurst bound each authenticated user's requests:
	// 1/s sustained, bursts of 10. The browser exporter flushes about every
	// 5 s per tab, so several tabs fit with room to spare; past it, 429 with
	// Retry-After.
	PerUserRate  = 1
	PerUserBurst = 10
)

// Ingest serves POST /telemetry/v1/traces behind the auth middleware.
type Ingest struct {
	fwd     *Forwarder
	limiter *userLimiter
	now     func() time.Time
}

// NewIngest returns the ingest handler forwarding through fwd.
func NewIngest(fwd *Forwarder) *Ingest {
	return &Ingest{fwd: fwd, limiter: newUserLimiter(PerUserRate, PerUserBurst), now: time.Now}
}

func (in *Ingest) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ui, err := xcontext.UserInfo(r.Context())
	if err != nil || ui.Username == "" {
		// The auth middleware always places an identity; without one the
		// route is mis-wired, and an unattributable request is refused.
		requestsDroppedAuth.Add(1)
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		requestsDroppedType.Add(1)
		http.Error(w, "content type must be application/json (OTLP/JSON)", http.StatusUnsupportedMediaType)
		return
	}
	if ok, wait := in.limiter.allow(ui.Username, in.now()); !ok {
		requestsDroppedRate.Add(1)
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	if r.ContentLength > MaxBodyBytes {
		requestsDroppedSize.Add(1)
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			requestsDroppedSize.Add(1)
			http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
			return
		}
		requestsDroppedFormat.Add(1)
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}
	req, err := decode(body)
	if err != nil {
		requestsDroppedFormat.Add(1)
		http.Error(w, "body is not OTLP/JSON", http.StatusBadRequest)
		return
	}
	if n := countSpans(req); n > MaxSpansPerRequest {
		requestsDroppedSize.Add(1)
		http.Error(w, fmt.Sprintf("more than %d spans", MaxSpansPerRequest), http.StatusRequestEntityTooLarge)
		return
	}

	requestsAccepted.Add(1)
	f := filter(req)
	spansDroppedService.Add(uint64(f.droppedService))
	spansDroppedName.Add(uint64(f.droppedName))
	spansDroppedInvalid.Add(uint64(f.droppedInvalid))
	attributesDropped.Add(uint64(f.droppedAttrs))
	spansAccepted.Add(uint64(len(f.spans)))

	rejected := f.rejected()
	if len(f.spans) > 0 {
		out, err := proto.Marshal(envelope(f.spans))
		if err != nil || !in.fwd.enqueue(batch{body: out, spans: len(f.spans)}) {
			spansForwardOverflow.Add(uint64(len(f.spans)))
			rejected += len(f.spans)
		}
	}
	writeExportResponse(w, rejected)
}

func countSpans(req *exportRequest) int {
	n := 0
	for _, rs := range req.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			n += len(ss.Spans)
		}
	}
	return n
}

// writeExportResponse answers with an OTLP/JSON ExportTraceServiceResponse.
// Rejected spans are reported as a partial success with a count only: no
// value from the request is echoed.
func writeExportResponse(w http.ResponseWriter, rejected int) {
	w.Header().Set("Content-Type", "application/json")
	if rejected == 0 {
		_, _ = w.Write([]byte(`{}`))
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"partialSuccess": map[string]any{
			"rejectedSpans": strconv.Itoa(rejected),
			"errorMessage":  "spans outside the krateo-frontend allowlist, or the forward queue was full",
		},
	})
}

// CountAuthFailures wraps the auth middleware chain and counts its 401s as
// requests dropped for auth: the middleware answers before Ingest runs.
func CountAuthFailures(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		if sw.code == http.StatusUnauthorized {
			requestsDroppedAuth.Add(1)
		}
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(code int) {
	if s.code == 0 {
		s.code = code
	}
	s.ResponseWriter.WriteHeader(code)
}

// userLimiter is a token bucket per authenticated username.
type userLimiter struct {
	mu    sync.Mutex
	rate  rate.Limit
	burst int
	users map[string]*rate.Limiter
}

// maxIdleTracked is the population above which idle users' buckets (full
// again, so forgetting them changes nothing) are pruned on the next insert.
const maxIdleTracked = 4096

func newUserLimiter(perSecond float64, burst int) *userLimiter {
	return &userLimiter{rate: rate.Limit(perSecond), burst: burst, users: map[string]*rate.Limiter{}}
}

// allow takes one token from user's bucket; when none is available it
// reports how long until one is.
func (l *userLimiter) allow(user string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.users[user]
	if !ok {
		if len(l.users) >= maxIdleTracked {
			for u, b := range l.users {
				if b.TokensAt(now) >= float64(l.burst) {
					delete(l.users, u)
				}
			}
		}
		lim = rate.NewLimiter(l.rate, l.burst)
		l.users[user] = lim
	}
	res := lim.ReserveN(now, 1)
	if d := res.DelayFrom(now); d > 0 {
		res.CancelAt(now)
		return false, d
	}
	return true, 0
}
