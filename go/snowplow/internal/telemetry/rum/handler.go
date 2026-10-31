package rum

import (
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"mime"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"

	"github.com/krateo-platformops/snowplow/internal/redact"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"
)

const (
	// Path is the ingest URL path, under the same base URL the portal uses
	// for /call. Pattern is its ServeMux pattern.
	Path    = "/telemetry/v1/traces"
	Pattern = "POST " + Path

	// MaxBodyBytes caps one request body (413 above it), with or without a
	// Content-Length: the body is always read through http.MaxBytesReader.
	MaxBodyBytes = 512 << 10

	// PerUserRate and PerUserBurst bound each authenticated user's requests:
	// 1/s sustained, bursts of 10. The browser exporter flushes about every
	// 5 s per tab, so several tabs fit with room to spare; past it, 429 with
	// Retry-After.
	PerUserRate  = 1
	PerUserBurst = 10

	// GlobalSpansPerSecond and GlobalSpanBurst bound the spans ALL users
	// together may push into the collector (#1020 review F6): past them a
	// batch is not forwarded and the request gets 429 with Retry-After. The
	// burst is a few full batches; the rate is far above the portal's real
	// RUM volume and far below what one collector agent absorbs.
	GlobalSpansPerSecond = 2000
	GlobalSpanBurst      = 4 * MaxSpansPerRequest

	// refusalLogsPerSecond / refusalLogBurst bound the WARN lines that make
	// refusals attributable (F4): one line per refused request, sampled by
	// this limiter so a flood cannot become a log flood.
	refusalLogsPerSecond = 1
	refusalLogBurst      = 20
)

// Ingest serves POST /telemetry/v1/traces behind the auth middleware.
type Ingest struct {
	fwd *Forwarder
	// routes is snowplow's own routing table (RegisteredRoutes over the mux):
	// a krateo.widget.fetch http.route must be one of them.
	routes  RouteSet
	limiter *userLimiter
	// decodes bounds the requests reading and decoding a body at once, across
	// all users (F1): with the structural caps this bounds the memory the
	// ingest can hold. GOMAXPROCS slots: decoding is CPU-bound, so more in
	// flight would not finish sooner.
	decodes    chan struct{}
	global     *rate.Limiter
	refusalLog *rate.Limiter
	now        func() time.Time

	// inFlight holds the users with a request inside the ingest (#1020 delta
	// D2): one at a time per user, so a single user's slow bodies cannot hold
	// every decode slot. An entry lives exactly as long as its request.
	mu       sync.Mutex
	inFlight map[string]struct{}
}

// enter admits user's request unless that user already has one in flight.
func (in *Ingest) enter(user string) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	if _, busy := in.inFlight[user]; busy {
		return false
	}
	in.inFlight[user] = struct{}{}
	return true
}

func (in *Ingest) leave(user string) {
	in.mu.Lock()
	defer in.mu.Unlock()
	delete(in.inFlight, user)
}

// NewIngest returns the ingest handler forwarding through fwd.
func NewIngest(fwd *Forwarder, routes RouteSet) *Ingest {
	return &Ingest{
		fwd:        fwd,
		routes:     routes,
		limiter:    newUserLimiter(PerUserRate, PerUserBurst, maxTracked),
		decodes:    make(chan struct{}, runtime.GOMAXPROCS(0)),
		global:     rate.NewLimiter(GlobalSpansPerSecond, GlobalSpanBurst),
		refusalLog: rate.NewLimiter(refusalLogsPerSecond, refusalLogBurst),
		now:        time.Now,
		inFlight:   map[string]struct{}{},
	}
}

func (in *Ingest) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ui, err := xcontext.UserInfo(r.Context())
	if err != nil || ui.Username == "" {
		// The auth middleware always places an identity; without one the
		// route is mis-wired, and an unattributable request is refused.
		// CountAuthFailures, around the whole chain, counts this 401.
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	user := ui.Username
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		requestsDroppedType.Add(1)
		in.refuse(w, user, http.StatusUnsupportedMediaType, "content_type", "content type must be application/json (OTLP/JSON)", 0)
		return
	}
	if ok, wait := in.limiter.allow(user, in.now()); !ok {
		requestsDroppedRate.Add(1)
		in.refuse(w, user, http.StatusTooManyRequests, "rate", "rate limited", wait)
		return
	}
	if r.ContentLength > MaxBodyBytes {
		requestsDroppedSize.Add(1)
		in.refuse(w, user, http.StatusRequestEntityTooLarge, "size", "body too large", 0)
		return
	}
	// One request in flight per user (D2), checked BEFORE a decode slot is
	// taken: a user still sending one body cannot take a second slot.
	if !in.enter(user) {
		requestsDroppedBusy.Add(1)
		in.refuse(w, user, http.StatusTooManyRequests, "busy_user", "a request from this user is already in flight", time.Second)
		return
	}
	defer in.leave(user)
	select {
	case in.decodes <- struct{}{}:
	default:
		requestsDroppedBusy.Add(1)
		in.refuse(w, user, http.StatusTooManyRequests, "busy", "ingest busy", time.Second)
		return
	}
	req, err := decodeCapped(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	<-in.decodes
	if err != nil {
		var tooBig *http.MaxBytesError
		var tooMany capError
		switch {
		case errors.As(err, &tooBig), errors.As(err, &tooMany):
			requestsDroppedSize.Add(1)
			in.refuse(w, user, http.StatusRequestEntityTooLarge, "size", "body over a size or count cap", 0)
		default:
			requestsDroppedFormat.Add(1)
			in.refuse(w, user, http.StatusBadRequest, "malformed", "body is not OTLP/JSON", 0)
		}
		return
	}

	requestsAccepted.Add(1)
	f := filter(req, in.routes)
	spansDroppedService.Add(uint64(f.droppedService))
	spansDroppedName.Add(uint64(f.droppedName))
	spansDroppedInvalid.Add(uint64(f.droppedInvalid))
	attributesDropped.Add(uint64(f.droppedAttrs))
	widgetRoutesUnregistered.Add(uint64(f.unregisteredRoutes))
	spansAccepted.Add(uint64(len(f.spans)))

	rejected := f.rejected()
	if n := len(f.spans); n > 0 {
		now := in.now()
		if res := in.global.ReserveN(now, n); !res.OK() || res.DelayFrom(now) > 0 {
			wait := time.Second
			if res.OK() {
				wait = res.DelayFrom(now)
				res.CancelAt(now)
			}
			spansForwardRateLimited.Add(uint64(n))
			in.refuse(w, user, http.StatusTooManyRequests, "global_rate", "ingest over its global span rate", wait)
			return
		}
		out, err := proto.Marshal(envelope(f.spans))
		if err != nil || !in.fwd.enqueue(batch{body: out, spans: n}) {
			spansForwardOverflow.Add(uint64(n))
			rejected += n
		}
	}
	writeExportResponse(w, rejected)
}

// refuse answers a refusal and makes it attributable (F4). Following
// snowplow's identity-logging rule (internal/redact, #453: no username is ever
// logged in clear; a site that must say which identity it acted for renders it
// with redact.User, an HMAC label stable for the pod's lifetime), it writes a
// sampled WARN line carrying the user's label, the status and the reason, and
// nothing from the request body. The counters (by reason) are the unsampled
// totals; the line says who.
func (in *Ingest) refuse(w http.ResponseWriter, user string, status int, reason, msg string, retryAfter time.Duration) {
	if in.refusalLog.AllowN(in.now(), 1) {
		slog.Warn("rum.ingest.refused",
			slog.String("subsystem", "rum"),
			slog.String("user", redact.User(user)),
			slog.Int("status", status),
			slog.String("reason", reason))
	}
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retryAfter.Seconds()))))
	}
	http.Error(w, msg, status)
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

// CountAuthFailures wraps the auth middleware chain and counts every 401 it
// answers as a request dropped for auth. It is the only place dropped_auth is
// counted, so a 401 is counted once whichever layer wrote it.
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

// maxTracked bounds the per-user limiter table. Idle users (bucket full again,
// so forgetting them changes nothing) are pruned first; if the table is still
// full, an arbitrary entry is evicted, which at worst hands that user a fresh
// burst. The table never holds more than maxTracked entries.
const maxTracked = 4096

// userLimiter is a token bucket per authenticated username.
type userLimiter struct {
	mu    sync.Mutex
	rate  rate.Limit
	burst int
	max   int
	users map[string]*rate.Limiter
}

func newUserLimiter(perSecond float64, burst, max int) *userLimiter {
	return &userLimiter{rate: rate.Limit(perSecond), burst: burst, max: max, users: map[string]*rate.Limiter{}}
}

// allow takes one token from user's bucket; when none is available it
// reports how long until one is.
func (l *userLimiter) allow(user string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.users[user]
	if !ok {
		if len(l.users) >= l.max {
			for u, b := range l.users {
				if b.TokensAt(now) >= float64(l.burst) {
					delete(l.users, u)
				}
			}
			for u := range l.users {
				if len(l.users) < l.max {
					break
				}
				delete(l.users, u)
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

// size is the number of users tracked.
func (l *userLimiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.users)
}
