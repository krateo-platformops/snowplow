package audit

// utf8_1251_test.go — #1251 re-review (BLOCKING): a record with a string that
// is not valid UTF-8 makes the OTLP log exporter refuse the WHOLE batch it is
// in ("string field contains invalid UTF-8"), so one request could lose other
// people's records. Emit makes every string valid UTF-8 in one place.
//
//   X  through the REAL otlploghttp exporter and a BatchProcessor (as
//      internal/logging builds them) into a test collector: one bad record
//      among good ones of other users, and EVERY record arrives, the bad one's
//      values carrying U+FFFD;
//   C  a valid User-Agent of 1023 x "a" + "é" is cut on the character
//      boundary, never through "é";
//   R  a raw 0xFF in the User-Agent and in the query, sent over a raw TCP
//      connection to a real server, reaches the record as U+FFFD.

import (
	"bufio"
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/log/logtest"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

// collector1251 is an OTLP/HTTP logs endpoint that decodes what it receives.
type collector1251 struct {
	mu       sync.Mutex
	requests int
	records  []map[string]string // attribute key -> string value (slices joined by ",")
	bodies   []string
}

func (c *collector1251) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var rd io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		rd = gz
	}
	raw, _ := io.ReadAll(rd)
	var req collogs.ExportLogsServiceRequest
	if err := proto.Unmarshal(raw, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests++
	for _, rl := range req.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				m := map[string]string{}
				for _, kv := range lr.GetAttributes() {
					v := kv.GetValue()
					if arr := v.GetArrayValue(); arr != nil {
						var parts []string
						for _, e := range arr.GetValues() {
							parts = append(parts, e.GetStringValue())
						}
						m[kv.GetKey()] = strings.Join(parts, ",")
						continue
					}
					m[kv.GetKey()] = v.GetStringValue()
				}
				c.records = append(c.records, m)
				c.bodies = append(c.bodies, lr.GetBody().GetStringValue())
			}
		}
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	out, _ := proto.Marshal(&collogs.ExportLogsServiceResponse{})
	_, _ = w.Write(out)
}

func userCtx1251(name string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: name, Groups: []string{"devs"}}))
}

// edgeCtx1251 is ctx as audit.Middleware leaves it for req.
func edgeCtx1251(ctx context.Context, req *http.Request) context.Context {
	return withReceivedRequest(ctx, req)
}

func TestUTF8_1251_X_OneBadRecordDoesNotLoseTheBatch(t *testing.T) {
	col := &collector1251{}
	srv := httptest.NewServer(col)
	t.Cleanup(srv.Close)

	ctx := context.Background()
	exp, err := otlploghttp.New(ctx, otlploghttp.WithEndpointURL(srv.URL+"/v1/logs"))
	if err != nil {
		t.Fatal(err)
	}
	// The processor internal/logging builds: one batch, flushed on shutdown.
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)))
	e := New(lp.Logger("test-1251"))

	good := func(user string) {
		req := httptest.NewRequest(http.MethodPost, "/call/dry-run?name=cm-ok", nil)
		req.Header.Set("User-Agent", "portal/1.0")
		e.Emit(edgeCtx1251(userCtx1251(user), req), Event{Action: "call", Verb: http.MethodPost, Resource: "configmaps", Name: "cm-ok", Outcome: "success"})
	}
	// The bad record: every way a request can carry bytes that are not UTF-8.
	bad := httptest.NewRequest(http.MethodPost, "/call/dry-run?name=x", nil)
	bad.URL.RawQuery = "name=cm-\xff" // a raw byte in the query
	bad.Header.Set("User-Agent", "probe-\xff-ua")
	bad.Header.Set("X-Forwarded-For", "203.0.113.7")

	good("alice-1251")
	e.Emit(edgeCtx1251(userCtx1251("bob-1251"), bad), Event{
		Action: "call", Verb: http.MethodPost, Resource: "configmaps",
		Name:    "cm-\xff",                // name=cm-%FF, decoded (already possible on main)
		Message: "upstream said \xfe\xff", // the record body
		Outcome: "failure", Code: http.StatusNotFound,
	})
	good("carol-1251")

	if err := lp.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown (flush) failed: %v", err)
	}

	col.mu.Lock()
	defer col.mu.Unlock()
	if len(col.records) != 3 {
		t.Fatalf("the collector received %d records in %d requests, want all 3: one record that is not valid UTF-8 cost the batch", len(col.records), col.requests)
	}
	users := map[string]map[string]string{}
	for i, r := range col.records {
		users[r["enduser.id"]] = r
		if !utf8.ValidString(col.bodies[i]) {
			t.Errorf("body %q is not valid UTF-8", col.bodies[i])
		}
	}
	b := users["bob-1251"]
	if b == nil {
		t.Fatalf("bob's record did not arrive: %v", col.records)
	}
	for k, want := range map[string]string{
		"k8s.resource.name": "cm-�",
		AttrUserAgent:       "probe-�-ua",
		AttrRequestURI:      "/call/dry-run?name=cm-�",
	} {
		if b[k] != want {
			t.Errorf("bob's %s = %q, want %q", k, b[k], want)
		}
	}
	for _, u := range []string{"alice-1251", "carol-1251"} {
		if users[u] == nil {
			t.Errorf("%s's record, in the same batch as bob's, did not arrive", u)
		}
	}
}

func TestUTF8_1251_C_UserAgentCutOnACharacterBoundary(t *testing.T) {
	ua := strings.Repeat("a", 1023) + "é" // valid UTF-8, 1025 bytes; byte 1024 is inside "é"
	if !utf8.ValidString(ua) || len(ua) != 1025 {
		t.Fatalf("precondition: %d bytes, valid %t", len(ua), utf8.ValidString(ua))
	}
	req := httptest.NewRequest(http.MethodGet, "/export?name=x", nil)
	req.Header.Set("User-Agent", ua)
	got := serveAndEmit(t, req, nil)[AttrUserAgent].AsString()
	if !utf8.ValidString(got) {
		t.Fatalf("the cut split a character: %q", got[len(got)-20:])
	}
	if want := strings.Repeat("a", 1023) + userAgentTruncateSuffix; got != want {
		t.Fatalf("user_agent.original ends %q, want 1023 a's then %q", got[len(got)-20:], userAgentTruncateSuffix)
	}
	// A run of invalid bytes past the bound: replaced, then cut, still bounded.
	got = truncateUserAgent(strings.Repeat("\xff", 2000))
	if !utf8.ValidString(got) || len(got) > maxUserAgentLength+len(userAgentTruncateSuffix) {
		t.Fatalf("invalid bytes past the bound: %d bytes, valid %t", len(got), utf8.ValidString(got))
	}
}

func TestUTF8_1251_R_RawBytesOverTCPReachTheRecordAsReplacement(t *testing.T) {
	rec := logtest.NewRecorder()
	e := New(rec.Logger("test-1251"))
	srv := httptest.NewServer(Middleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := xcontext.BuildContext(r.Context(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice"}))
		e.Emit(ctx, Event{Action: "export", Verb: r.Method, Resource: "restactions", Name: r.URL.Query().Get("name"), Outcome: "success"})
		w.WriteHeader(http.StatusOK)
	})))
	t.Cleanup(srv.Close)

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Bytes no client library would send: 0xFF in the request line's query and
	// in a header value.
	raw := "GET /export?name=x\xff HTTP/1.1\r\nHost: snowplow\r\nUser-Agent: probe-\xff-ua\r\nConnection: close\r\n\r\n"
	if _, err := io.WriteString(conn, raw); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("precondition: the server answered %d, so the raw request never reached the emit", resp.StatusCode)
	}

	got := attrsOf957(t, rec)
	for k, want := range map[string]string{
		AttrUserAgent:       "probe-�-ua",
		AttrRequestURI:      "/export?name=x�",
		"k8s.resource.name": "x�",
	} {
		if v := got[k].AsString(); v != want {
			t.Errorf("%s = %q, want %q", k, v, want)
		}
	}
	for k, v := range got {
		if v.Type() == attribute.STRING && !utf8.ValidString(v.AsString()) {
			t.Errorf("%s = %q is not valid UTF-8", k, v.AsString())
		}
	}
}
