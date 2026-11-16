//go:build js && wasm

// Package jsfetch is test support for the js/wasm build's fetch mode
// (scripts/check_js_wasm.sh runs a test binary under Node with a renamed argv0,
// so net/http sends through Node's fetch as it does through a browser's).
//
// fetch never reaches Go's in-process network, where a Go test server
// listens, so a test that wants a real round trip through fetch needs a server
// fetch can reach: Start runs one in Node itself, written in JavaScript. Spy
// records the options fetch is called with, which is the only place the
// browser-side request settings (credentials, redirect) can be observed.
package jsfetch

import (
	"net/http"
	"strings"
	"syscall/js"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/dialguard"
)

// waitFor bounds every wait on a JavaScript promise.
const waitFor = 10 * time.Second

// serverSource is a Node HTTP server on 127.0.0.1, port chosen by the OS:
//
//	/redirect  302 to /target
//	anything   200 {"kind":"Status","status":"Success"}
//
// It counts the requests to each path and keeps the headers of the last.
const serverSource = `(function (http) {
  const state = { hits: {}, last: {} };
  const server = http.createServer((req, res) => {
    const path = req.url.split('?')[0];
    state.hits[path] = (state.hits[path] || 0) + 1;
    state.last[path] = req.headers;
    if (path === '/redirect') {
      res.writeHead(302, { Location: '/target' });
      res.end();
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end('{"kind":"Status","status":"Success"}');
  });
  return new Promise((resolve, reject) => {
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => resolve({ server, state, url: 'http://127.0.0.1:' + server.address().port }));
  });
})`

// Server is a running Node HTTP server.
type Server struct {
	URL string
	v   js.Value
}

// Start runs a Node HTTP server for the test and closes it at cleanup.
func Start(t testing.TB) *Server {
	t.Helper()
	start := js.Global().Call("eval", serverSource)
	v := await(t, start.Invoke(js.Global().Call("require", "http")))
	s := &Server{URL: v.Get("url").String(), v: v}
	t.Cleanup(func() { v.Get("server").Call("close") })
	return s
}

// Hits is how many requests reached path.
func (s *Server) Hits(path string) int {
	n := s.v.Get("state").Get("hits").Get(path)
	if n.IsUndefined() {
		return 0
	}
	return n.Int()
}

// Header is header name (lower case, as Node keeps it) of the last request to
// path, or "" when it had none.
func (s *Server) Header(path, name string) string {
	h := s.v.Get("state").Get("last").Get(path)
	if h.IsUndefined() || h.Get(name).IsUndefined() {
		return ""
	}
	return h.Get(name).String()
}

// Call is one call to fetch, with the options it was given.
type Call struct {
	URL, Credentials, Redirect string
}

// spySource replaces globalThis.fetch with one that records each call's URL
// and its credentials and redirect options ("" when absent), then calls the
// real fetch.
const spySource = `(function () {
  const real = globalThis.fetch;
  const calls = [];
  globalThis.fetch = function (url, opts) {
    calls.push({
      url: String(url),
      credentials: (opts && opts.credentials) || '',
      redirect: (opts && opts.redirect) || '',
    });
    return real.call(this, url, opts);
  };
  return { calls, restore () { globalThis.fetch = real; } };
})()`

// FetchSpy records every call to fetch until the test ends.
type FetchSpy struct{ v js.Value }

// Spy installs a FetchSpy and removes it at cleanup.
func Spy(t testing.TB) *FetchSpy {
	t.Helper()
	v := js.Global().Call("eval", spySource)
	t.Cleanup(func() { v.Call("restore") })
	return &FetchSpy{v: v}
}

// CallsTo is every recorded call whose URL starts with prefix.
func (s *FetchSpy) CallsTo(prefix string) []Call {
	var out []Call
	calls := s.v.Get("calls")
	for i := 0; i < calls.Length(); i++ {
		c := calls.Index(i)
		if u := c.Get("url").String(); strings.HasPrefix(u, prefix) {
			out = append(out, Call{URL: u, Credentials: c.Get("credentials").String(), Redirect: c.Get("redirect").String()})
		}
	}
	return out
}

// NetHTTPUsesFetch is net/http's own choice, observed: a Transport with no
// dial function (dialguard.Transport on js/wasm, the one every snowplow
// request leaves through) sends through fetch where net/http uses it, and its
// error then names fetch(); elsewhere it dials Go's in-process network and
// fails as a dial does. Port 1 on loopback listens on neither.
func NetHTTPUsesFetch(t testing.TB) bool {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:1/", nil)
	if err != nil {
		t.Fatal(err)
	}
	tr := dialguard.Transport()
	if tr.DialContext != nil || tr.Dial != nil || tr.DialTLSContext != nil || tr.DialTLS != nil {
		t.Fatal("jsfetch: dialguard.Transport has a dial function on js/wasm, so it never uses fetch and the probe cannot tell net/http's mode")
	}
	resp, err := tr.RoundTrip(req)
	if err == nil {
		_ = resp.Body.Close() // the probe fails on the next line; the close cannot change that
		t.Fatal("jsfetch: a request to 127.0.0.1:1 succeeded, so the probe cannot tell net/http's mode")
	}
	switch msg := err.Error(); {
	case strings.Contains(msg, "fetch()"):
		return true
	case strings.Contains(msg, "dial"):
		return false
	default:
		t.Fatalf("jsfetch: the probe's error %q is neither fetch's nor a dial's", msg)
		return false
	}
}

// FetchOnly skips the test where net/http does not use fetch.
// scripts/check_js_wasm.sh runs the fetch-mode tests with a renamed argv0.
func FetchOnly(t testing.TB) {
	t.Helper()
	if !NetHTTPUsesFetch(t) {
		t.Skip("net/http does not use fetch here; scripts/check_js_wasm.sh runs this in fetch mode")
	}
}

// await blocks until p settles. Go's wasm runtime returns to the JavaScript
// event loop while every goroutine is blocked, so the promise's work runs
// meanwhile.
func await(t testing.TB, p js.Value) js.Value {
	t.Helper()
	done := make(chan js.Value, 1)
	failed := make(chan string, 1)
	onOK := js.FuncOf(func(_ js.Value, args []js.Value) any { done <- args[0]; return nil })
	onErr := js.FuncOf(func(_ js.Value, args []js.Value) any { failed <- args[0].Call("toString").String(); return nil })
	defer onOK.Release()
	defer onErr.Release()
	p.Call("then", onOK, onErr)
	select {
	case v := <-done:
		return v
	case msg := <-failed:
		t.Fatalf("jsfetch: %s", msg)
	case <-time.After(waitFor):
		t.Fatalf("jsfetch: the promise did not settle within %s", waitFor)
	}
	return js.Undefined()
}
