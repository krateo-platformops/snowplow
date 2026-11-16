package main

// main_log_duplicate_keys_916_test.go — #916: a request's WARN line carries
// traceId (the plumbing shortid) once and trace_id (the W3C id) once, through
// the pod's own composition: newServerHandler (span) → the request chain → the
// route → the default or request logger → buildLogHandler.
//
// WHERE THE SECOND COPY CAME FROM. plumbing's xcontext.WithLogger stamps
// "traceId" on whatever logger it is given (plumbing context/context.go:91),
// including a logger that already carries one. Two compositions handed it one:
//   - plumbing's use.Logger (server/use/logger.go:39) stamps traceId and the
//     user group, then calls WithLogger: the 057 line's
//     "time,level,msg,traceId,user,traceId" (1.12.56). #1199 replaced
//     use.Logger, so that one is gone from the pod, and the arm below still
//     drives it, because the decorator is what must hold the line to one copy.
//   - a site that carries the context's logger into a new context:
//     cluster_list.go's async populate and resolve_populate.go's learned
//     representative re-wrap xcontext.Logger(ctx) through WithLogger.
//
// THE KEYS ARE READ FROM THE BYTES. encoding/json keeps the last copy of a
// repeated key and says nothing (jsonkeys' control arm), so a decoded map
// cannot see the defect.

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"github.com/krateo-platformops/snowplow/internal/testsupport/jsonkeys"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	traceID916   = "s916TrcId"
	rewrapMsg916 = "s916.rewrapped.warn"
)

// sink916 is the log stream; every goroutine may write it (#392).
type sink916 struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *sink916) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

// line returns the one line that carries msg.
func (s *sink916) line(t *testing.T, msg string) string {
	t.Helper()
	s.mu.Lock()
	raw := s.b.String()
	s.mu.Unlock()
	var found []string
	for _, l := range strings.Split(strings.TrimSpace(raw), "\n") {
		if strings.Contains(l, msg) {
			found = append(found, l)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want one %q line for one request, got %d: %s", msg, len(found), raw)
	}
	return found[0]
}

// rewrap916 does what cluster_list.go's async populate and resolve_populate.go
// do: carry the context's logger into a context built from it. Then it logs a
// WARN with that context, as a site does.
func rewrap916(_ http.ResponseWriter, r *http.Request) {
	ctx := xcontext.BuildContext(r.Context(), xcontext.WithLogger(xcontext.Logger(r.Context())))
	xcontext.Logger(ctx).WarnContext(ctx, rewrapMsg916, slog.String("k", "v"))
}

// served916 is the pod's composition over two routes: the dispatcher's real
// "handler not found" WARN, and a route that re-wraps its logger.
func served916(t *testing.T, pretty bool, chainOf func(*slog.Logger) use.Chain) (http.Handler, *sink916) {
	t.Helper()
	sink := &sink916{}
	log := slog.New(buildLogHandler(pretty, slog.LevelWarn, sink))
	prev := slog.Default()
	slog.SetDefault(log)
	t.Cleanup(func() { slog.SetDefault(prev) })

	chain := chainOf(log)
	mux := http.NewServeMux()
	mux.Handle("GET /call", chain.Append(handlers.Dispatcher(dispatchers.All())).Then(http.HandlerFunc(ok200)))
	mux.Handle("GET /s916", chain.Then(http.HandlerFunc(rewrap916)))
	t.Cleanup(cache.ResetRouteScopeRegistryForTest)
	return newServerHandler(mux), sink
}

// keyCount916 counts a top-level key on a JSON line (from its bytes) or a text
// line, and fails on any key repeated anywhere in a JSON line.
func keyCount916(t *testing.T, pretty bool, line, key string) int {
	t.Helper()
	if pretty {
		return len(regexp.MustCompile(`(^| )`+regexp.QuoteMeta(key)+`=`).FindAllString(line, -1))
	}
	got, err := jsonkeys.Read([]byte(line))
	if err != nil {
		t.Fatalf("a log line is not one JSON object: %v: %s", err, line)
	}
	if len(got.Repeated) > 0 {
		t.Errorf("#916 RED: keys %v repeat in one object: %s", got.Repeated, line)
	}
	return got.Count(key)
}

func valueOf916(pretty bool, line, key string) string {
	if pretty {
		m := regexp.MustCompile(`(?:^| )` + regexp.QuoteMeta(key) + `=(\S+)`).FindStringSubmatch(line)
		if m == nil {
			return ""
		}
		return m[1]
	}
	m := regexp.MustCompile(`"` + regexp.QuoteMeta(key) + `":"([^"]*)"`).FindStringSubmatch(line)
	if m == nil {
		return ""
	}
	return m[1]
}

// TestS916_ARequestsWarnCarriesEachIdOnce: through the pod's chain, a request's
// WARN line, from a site that uses the request logger and from one that
// re-wraps it, carries traceId once (the request's shortid) and trace_id once
// (its Server span's), on both handlers.
func TestS916_ARequestsWarnCarriesEachIdOnce(t *testing.T) {
	chains := []struct {
		name  string
		of    func(*slog.Logger) use.Chain
		token bool
	}{
		{name: "pod chain", of: newRequestChain},
		// The 1.12.56 chain: use.Logger stamps traceId with the user group, then
		// WithLogger stamps it again. The token is what makes use.Logger stamp.
		{name: "plumbing use.Logger chain", token: true, of: func(log *slog.Logger) use.Chain {
			return use.NewChain(use.TraceId(), use.Logger(log), audit.Middleware())
		}},
	}
	routes := []struct {
		name, path, span, msg string
	}{
		{name: "request logger", path: "/call?apiVersion=none.example.io/v1&resource=things&name=x", span: "GET /call", msg: "handler not found"},
		{name: "re-wrapped logger", path: "/s916", span: "GET /s916", msg: rewrapMsg916},
	}
	for _, c := range chains {
		for _, rt := range routes {
			for _, pretty := range []bool{false, true} {
				name := c.name + "/" + rt.name + "/json"
				if pretty {
					name = c.name + "/" + rt.name + "/pretty"
				}
				t.Run(name, func(t *testing.T) {
					exp := tracer1184(t, sdktrace.WithSampler(sdktrace.AlwaysSample()))
					h, sink := served916(t, pretty, c.of)
					req := httptest.NewRequest(http.MethodGet, rt.path, nil)
					req.Header.Set(xcontext.LabelKrateoTraceId, traceID916)
					if c.token {
						req.Header.Set("Authorization", bearer1192())
					}
					h.ServeHTTP(httptest.NewRecorder(), req)

					span := serverSpan(t, exp, rt.span)
					line := sink.line(t, rt.msg)
					if !strings.Contains(line, "WARN") {
						t.Fatalf("PRECONDITION: %q is not a WARN line: %s", rt.msg, line)
					}
					if n := keyCount916(t, pretty, line, "traceId"); n != 1 {
						t.Errorf("#916 RED: the line carries traceId %d times, want once: %s", n, line)
					}
					if n := keyCount916(t, pretty, line, "trace_id"); n != 1 {
						t.Errorf("the line carries trace_id %d times, want once: %s", n, line)
					}
					if got := valueOf916(pretty, line, "traceId"); got != traceID916 {
						t.Errorf("traceId = %q, want the request's %q: %s", got, traceID916, line)
					}
					if got, want := valueOf916(pretty, line, "trace_id"), span.SpanContext.TraceID().String(); got != want {
						t.Errorf("trace_id = %q, want the Server span's %s: %s", got, want, line)
					}
					if c.token && keyCount916(t, pretty, line, "user") != 1 && keyCount916(t, pretty, line, "user.name") != 1 {
						t.Errorf("CONTROL: use.Logger's user group is not on the line, so this is not the 1.12.56 composition: %s", line)
					}
				})
			}
		}
	}
}

// TestS916_ABackgroundLineCarriesTraceIdOnce: a line with no request and no
// span, from a context whose logger was carried into a new context (the
// refresher's learned-representative re-wrap), carries traceId once and no
// trace_id.
func TestS916_ABackgroundLineCarriesTraceIdOnce(t *testing.T) {
	sink := &sink916{}
	log := slog.New(buildLogHandler(false, slog.LevelWarn, sink))
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(log))
	ctx = xcontext.BuildContext(ctx, xcontext.WithLogger(xcontext.Logger(ctx)))
	xcontext.Logger(ctx).WarnContext(ctx, "s916.background")
	line := sink.line(t, "s916.background")
	if n := keyCount916(t, false, line, "traceId"); n != 1 {
		t.Errorf("#916 RED: the line carries traceId %d times, want once: %s", n, line)
	}
	if n := keyCount916(t, false, line, "trace_id"); n != 0 {
		t.Errorf("a line with no span carries trace_id: %s", line)
	}
}
