package credhttp

// do_726_test.go — #726: Do (request.Do with a client that never follows a
// redirect). A token-auth endpoint whose server answers 302/307 to a FOREIGN
// host: no request reaches the foreign host, and the refused 3xx comes back as
// a Failure with 502 Bad Gateway (#729's convention) whose fixed
// RedirectRefusedError text names the 3xx (never the redirect's body or
// Location). The non-redirect paths keep request.Do's envelopes.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
)

func TestIssue726_DoNeverFollowsARedirectWithTheCallersToken(t *testing.T) {
	for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect, http.StatusMovedPermanently, http.StatusPermanentRedirect} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var foreign atomic.Int64
			var foreignAuth atomic.Value
			other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				foreign.Add(1)
				foreignAuth.Store(r.Header.Get("Authorization"))
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"kind":"ConfigMap","metadata":{"name":"attacker"}}`))
			}))
			defer other.Close()
			var originAuth atomic.Value
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				originAuth.Store(r.Header.Get("Authorization"))
				w.Header().Set("Location", other.URL+"/steal?token=x")
				w.WriteHeader(code)
				_, _ = w.Write([]byte(`<a href="` + other.URL + `/steal?token=x">moved</a>`))
			}))
			defer origin.Close()

			handled := false
			st := Do(context.Background(), request.RequestOptions{
				RequestInfo: request.RequestInfo{Path: "/api/v1/namespaces/default/configmaps/cm-x", Verb: ptr.To(http.MethodGet)},
				Endpoint:    &endpoints.Endpoint{ServerURL: origin.URL, Token: "callers-own-token"},
				ResponseHandler: func(io.ReadCloser) error {
					handled = true
					return nil
				},
			})
			if a, _ := originAuth.Load().(string); a != "Bearer callers-own-token" {
				t.Fatalf("PRECONDITION: the origin must have received the caller's bearer (got %q)", a)
			}
			if n := foreign.Load(); n != 0 {
				a, _ := foreignAuth.Load().(string)
				t.Fatalf("#726 RED: %d request(s) reached the redirect's foreign host (Authorization %q)", n, a)
			}
			if handled {
				t.Fatalf("#726 RED: a redirected body was fed to the response handler")
			}
			if st.Status != response.StatusFailure || st.Code != http.StatusBadGateway ||
				!strings.Contains(st.Message, (&RedirectRefusedError{StatusCode: code}).Error()) {
				t.Fatalf("#726: the refused %d must be a Failure, 502 (the #729 convention), naming the %d; got status=%q code=%d message=%q",
					code, code, st.Status, st.Code, st.Message)
			}
			if strings.Contains(st.Message, other.URL) || strings.Contains(st.Message, "steal") {
				t.Fatalf("#726: the surfaced message leaked the redirect target: %q", st.Message)
			}
		})
	}
}

// Parity with request.Do on the non-redirect paths.
func TestIssue726_DoKeepsRequestDoEnvelopes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/forbidden":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","message":"nope","reason":"Forbidden","code":403}`))
		case "/html":
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(`<html/>`))
		}
	}))
	defer srv.Close()
	ep := &endpoints.Endpoint{ServerURL: srv.URL}
	for _, tc := range []struct {
		path string
		code int
	}{{"/ok", http.StatusOK}, {"/forbidden", http.StatusForbidden}, {"/html", http.StatusNotAcceptable}} {
		var body string
		mine := Do(context.Background(), request.RequestOptions{
			RequestInfo: request.RequestInfo{Path: tc.path},
			Endpoint:    ep,
			ResponseHandler: func(rc io.ReadCloser) error {
				b, _ := io.ReadAll(rc)
				body = string(b)
				return nil
			},
		})
		theirs := request.Do(context.Background(), request.RequestOptions{
			RequestInfo:     request.RequestInfo{Path: tc.path},
			Endpoint:        ep,
			ResponseHandler: func(io.ReadCloser) error { return nil },
		})
		if mine.Code != tc.code || mine.Code != theirs.Code || mine.Status != theirs.Status || mine.Message != theirs.Message {
			t.Fatalf("%s: Do = {%s %d %q}, request.Do = {%s %d %q}, want code %d and identical envelopes",
				tc.path, mine.Status, mine.Code, mine.Message, theirs.Status, theirs.Code, theirs.Message, tc.code)
		}
		if tc.path == "/ok" && body != `{"ok":true}` {
			t.Fatalf("the 2xx body was not handed to the response handler: %q", body)
		}
	}
}
