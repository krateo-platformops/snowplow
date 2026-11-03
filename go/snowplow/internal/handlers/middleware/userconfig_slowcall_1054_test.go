package middleware_test

// userconfig_slowcall_1054_test.go — #1054: the time UserConfig spends
// authenticating is the request's auth phase. A key source that is slow to
// answer (JWKS unreachable, a cold fetch) makes a slow request whose record
// names auth, and the next handler's own time is not charged to it.

import (
	"bytes"
	"crypto/rsa"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/slowcall"
)

type slowKeys struct{ d time.Duration }

func (k slowKeys) PublicKey(kid string) (*rsa.PublicKey, error) {
	time.Sleep(k.d)
	return testKeys.PublicKey(kid)
}

type s1054Buf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *s1054Buf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func TestS1054_UserConfigTimeIsTheAuthPhase(t *testing.T) {
	const bound = 60 * time.Millisecond
	buf := &s1054Buf{}
	mon := slowcall.NewMonitorForTest(bound, slog.New(slog.NewJSONHandler(buf, nil)))
	var reached bool
	h := mon.Middleware(middleware.UserConfig(slowKeys{d: 2 * bound}, testAuthnNS)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		reached = true
	})))
	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	req.Pattern = "GET /call"
	req.Header.Set("Authorization", "Bearer "+helperMakeToken(t, testUsername, time.Hour))
	h.ServeHTTP(httptest.NewRecorder(), req)
	_ = reached // the clientconfig lookup may refuse here; the auth phase is timed either way

	var rec map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.b.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["msg"] == "slowcall.request" {
			rec = m
		}
	}
	if rec == nil {
		t.Fatalf("PRECONDITION: no slow-call record:\n%s", buf.b.String())
	}
	if rec["phase"] != "auth" || rec["auth_n"] != float64(1) {
		t.Fatalf("AUTH RED: phase=%v auth_n=%v, want auth and 1", rec["phase"], rec["auth_n"])
	}
	if ms, _ := rec["auth_ms"].(float64); int64(ms) < 2*bound.Milliseconds() {
		t.Fatalf("AUTH RED: auth_ms=%v, want ≥ %d (the key source's delay)", rec["auth_ms"], 2*bound.Milliseconds())
	}
	if strings.Contains(buf.b.String(), testUsername) {
		t.Fatalf("the requester reached the record")
	}
}
