package dispatchers

// work_class_slowcall_1054_test.go — #1054 (re-review B): the slow-call record
// learns its work class only at beginServeWorkClass. A trusted self-loopback
// carrying a background unit's class must be recorded under that class (so it
// is never counted as a customer's slow call nor takes the customer's
// snapshot); a plain request is a customer.

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"

	"github.com/krateo-platformops/snowplow/internal/slowcall"
	"github.com/krateo-platformops/snowplow/internal/workclass"
)

func TestS1054_LoopbackWorkClassReachesTheSlowCallRecord(t *testing.T) {
	t.Setenv("URL_SELF", "")
	const bound = 20 * time.Millisecond
	for _, c := range []struct {
		name, header, want string
	}{
		{"background loopback", workclass.Refresh.String(), "refresh"},
		{"customer", "", "customer"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			mon := slowcall.NewMonitorForTest(bound, slog.New(slog.NewJSONHandler(&buf, nil)))
			h := mon.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r = r.WithContext(xcontext.BuildContext(r.Context(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: "s1054-loopback"})))
				if c.header != "" {
					r.Header.Set(workclass.Header, c.header)
				}
				_, end := beginServeWorkClass(r)
				defer end()
				time.Sleep(2 * bound)
			}))
			req := httptest.NewRequest(http.MethodGet, "/call", nil)
			req.Pattern = "GET /call"
			h.ServeHTTP(httptest.NewRecorder(), req)
			var rec map[string]any
			for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
				var m map[string]any
				if json.Unmarshal([]byte(line), &m) == nil && m["msg"] == "slowcall.request" {
					rec = m
				}
			}
			if rec == nil {
				t.Fatalf("PRECONDITION: no slow-call record")
			}
			if rec["class"] != c.want {
				t.Fatalf("CLASS RED: class=%v, want %q (beginServeWorkClass did not tell the record)", rec["class"], c.want)
			}
		})
	}
}
