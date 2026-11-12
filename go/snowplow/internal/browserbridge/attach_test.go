//go:build js && wasm

package browserbridge

import (
	"io"
	"net/http"
	"runtime"
	"strings"
	"syscall/js"
	"testing"
	"time"
)

// attachForTest installs a fake snowplowBridgeAttach, as bridge.js does, runs
// Attach against addr and returns what Attach handed over.
func attachForTest(t *testing.T, addr string) js.Value {
	t.Helper()
	got := make(chan js.Value, 1)
	host := js.FuncOf(func(_ js.Value, args []js.Value) any {
		got <- args[0]
		return nil
	})
	js.Global().Set(HostAttach, host)
	t.Cleanup(func() {
		js.Global().Delete(HostAttach)
		host.Release()
	})
	Attach(addr)
	select {
	case b := <-got:
		return b
	case <-time.After(5 * time.Second):
		t.Fatal("Attach did not call snowplowBridgeAttach within 5s of the server accepting")
		return js.Undefined()
	}
}

// jsRecordingSink is a BridgeSink in JavaScript whose calls land in a
// recordingSink.
func jsRecordingSink(t *testing.T, rec *recordingSink) js.Value {
	t.Helper()
	fns := map[string]js.Func{
		"head": js.FuncOf(func(_ js.Value, a []js.Value) any {
			var hs [][2]string
			for i := 0; i < a[2].Length(); i++ {
				hs = append(hs, [2]string{a[2].Index(i).Index(0).String(), a[2].Index(i).Index(1).String()})
			}
			rec.Head(a[0].Int(), a[1].String(), hs)
			return nil
		}),
		"chunk": js.FuncOf(func(_ js.Value, a []js.Value) any {
			b := make([]byte, a[0].Length())
			js.CopyBytesToGo(b, a[0])
			rec.Chunk(b)
			return nil
		}),
		"end":   js.FuncOf(func(js.Value, []js.Value) any { rec.End(); return nil }),
		"error": js.FuncOf(func(_ js.Value, a []js.Value) any { rec.Error(a[0].String()); return nil }),
	}
	obj := js.Global().Get("Object").New()
	for name, f := range fns {
		obj.Set(name, f)
	}
	t.Cleanup(func() {
		for _, f := range fns {
			f.Release()
		}
	})
	return obj
}

// serveGoroutines counts the goroutines bridge.serve started that are still
// running.
func serveGoroutines() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), "browserbridge.(*bridge).serve.func1(")
		}
		buf = make([]byte, 2*len(buf))
	}
}

// B2b (N6): bridge.js's cancel(id) must end the request: the handler's
// context ends, so a /refreshes handler returns, and the goroutine serve
// started exits. A no-op cancel would leave both running for as long as the
// worker lives, one more for every navigation.
func TestAttach_CancelEndsTheHandlersContextAndTheRequestsGoroutine(t *testing.T) {
	returned := make(chan struct{})
	addr := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(returned)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, ": connected\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	bridge := attachForTest(t, addr)
	rec := newSink()
	req := js.ValueOf(map[string]any{
		"method":  "GET",
		"url":     "https://snowplow.sandbox.invalid/refreshes",
		"headers": []any{[]any{"accept", "text/event-stream"}},
		"body":    nil,
	})
	const id = 7
	bridge.Call("serve", id, req, jsRecordingSink(t, rec))

	if e := rec.next(t); e.kind != "head" || e.status != 200 {
		t.Fatalf("first call = %+v, want head 200", e)
	}
	readBody(t, rec, ": connected\n\n")
	if n := serveGoroutines(); n != 1 {
		t.Fatalf("PRE: %d serve goroutines while the stream is open, want 1", n)
	}

	bridge.Call("cancel", id)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler did not return within 5s of cancel(id): its request context never ended")
	}
	deadline := time.Now().Add(5 * time.Second)
	for serveGoroutines() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the request's goroutine was still running 5s after cancel(id)")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, e := range rec.all()[2:] {
		t.Errorf("after cancel the sink got %+v", e)
	}

	// A cancel for a request that has finished is a no-op.
	bridge.Call("cancel", id)
}
