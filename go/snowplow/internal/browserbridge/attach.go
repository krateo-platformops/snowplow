//go:build js && wasm

package browserbridge

import (
	"context"
	"sync"
	"syscall/js"
)

// HostAttach is the global function bridge.js defines before it runs the
// program. Attach calls it once, with the bridge's functions, when the
// server accepts connections; bridge.js's start() resolves then.
const HostAttach = "snowplowBridgeAttach"

// Attach waits, in its own goroutine, until the server on addr accepts, then
// hands bridge.js an object with two functions:
//
//	serve(id, {method, url, headers, body}, sink)  start serving one request
//	cancel(id)                                     stop it
//
// serve returns at once: a function JS calls must not block, or the
// browser's event loop, which every fetch needs, stops with it. The request
// runs in a goroutine and reports to sink (head, chunk, end, error). bridge.js
// numbers the requests, so a cancel for a request that has finished is a
// no-op rather than a call into a released function.
func Attach(addr string) {
	host := js.Global().Get(HostAttach)
	if host.Type() != js.TypeFunction {
		panic("browserbridge: " + HostAttach + " is not defined; this program runs only under snowplow-wasm's bridge.js")
	}
	b := &bridge{addr: addr, live: map[int]context.CancelFunc{}}
	go func() {
		// The server is wired by main in this same process; there is
		// nothing to give up on, so this waits for as long as it takes.
		_ = WaitAccepting(context.Background(), addr)
		host.Invoke(js.ValueOf(map[string]any{
			"serve":  js.FuncOf(b.serve),
			"cancel": js.FuncOf(b.cancel),
		}))
	}()
}

type bridge struct {
	addr string
	mu   sync.Mutex
	live map[int]context.CancelFunc
}

func (b *bridge) serve(_ js.Value, args []js.Value) any {
	id, req, sink := args[0].Int(), readRequest(args[1]), jsSink{args[2]}
	ctx, cancel := context.WithCancel(context.Background())
	b.mu.Lock()
	b.live[id] = cancel
	b.mu.Unlock()
	go func() {
		defer b.finish(id)
		Forward(ctx, b.addr, req, sink)
	}()
	return nil
}

func (b *bridge) cancel(_ js.Value, args []js.Value) any {
	b.finish(args[0].Int())
	return nil
}

// finish cancels request id's context and forgets it.
func (b *bridge) finish(id int) {
	b.mu.Lock()
	cancel, ok := b.live[id]
	delete(b.live, id)
	b.mu.Unlock()
	if ok {
		cancel()
	}
}

// readRequest copies a BridgeRequest ({method, url, headers: [[name, value],
// ...], body: Uint8Array | null}) out of JS.
func readRequest(v js.Value) Request {
	req := Request{Method: v.Get("method").String(), URL: v.Get("url").String()}
	hs := v.Get("headers")
	for i := 0; i < hs.Length(); i++ {
		kv := hs.Index(i)
		req.Header = append(req.Header, [2]string{kv.Index(0).String(), kv.Index(1).String()})
	}
	if body := v.Get("body"); body.Truthy() {
		req.Body = make([]byte, body.Length())
		js.CopyBytesToGo(req.Body, body)
	}
	return req
}

// jsSink calls a BridgeSink. bridge.js hands over a sink whose methods do not
// throw, because an exception thrown into Go would stop the whole program.
type jsSink struct{ v js.Value }

func (s jsSink) Head(status int, statusText string, header [][2]string) {
	hs := make([]any, len(header))
	for i, kv := range header {
		hs[i] = []any{kv[0], kv[1]}
	}
	s.v.Call("head", status, statusText, hs)
}

// Chunk copies b into a new Uint8Array: Forward reuses its buffer.
func (s jsSink) Chunk(b []byte) {
	u := js.Global().Get("Uint8Array").New(len(b))
	js.CopyBytesToJS(u, b)
	s.v.Call("chunk", u)
}

func (s jsSink) End() { s.v.Call("end") }

func (s jsSink) Error(message string) { s.v.Call("error", message) }
