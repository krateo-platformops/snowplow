//go:build js && wasm

package browserbridge

import (
	"io"
	"net"
	"net/http"
	"syscall/js"
	"testing"
	"time"
)

// TestAttach_ATargetRegistersBeforeTheMainServerAccepts: snowplow calls the cluster while it starts,
// before its own server accepts, so a target must reach bridge.js on its own (d94c2ffc). Its
// requests are relayed: the fake apiserver's 401 reaches the caller as a head.
func TestAttach_ATargetRegistersBeforeTheMainServerAccepts(t *testing.T) {
	const status = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}`
	target := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, status)
	}))
	// The main server's port: free, and nothing listens on it until the target has registered.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	main := ln.Addr().String()
	_ = ln.Close()
	if conn, err := loopback.Dial("tcp", main); err == nil {
		_ = conn.Close()
		t.Fatalf("%s accepts after its listener closed: the test cannot hold the main server back", main)
	}

	type registered struct {
		name   string
		bridge js.Value
	}
	got := make(chan registered, 1)
	mainAttached := make(chan struct{}, 1)
	attach := js.FuncOf(func(js.Value, []js.Value) any { mainAttached <- struct{}{}; return nil })
	defer attach.Release()
	onTarget := js.FuncOf(func(_ js.Value, args []js.Value) any { got <- registered{args[0].String(), args[1]}; return nil })
	defer onTarget.Release()
	js.Global().Set(HostAttach, attach)
	js.Global().Set(HostTarget, onTarget)
	defer js.Global().Delete(HostAttach)
	defer js.Global().Delete(HostTarget)

	Attach(main, Target{Name: "apiserver", Addr: target})

	var r registered
	select {
	case r = <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("the target was not handed to bridge.js while snowplow's server did not accept")
	}
	if r.name != "apiserver" {
		t.Fatalf("target name %q", r.name)
	}
	select {
	case <-mainAttached:
		t.Fatal("the main bridge attached although its server never accepted")
	case <-time.After(200 * time.Millisecond):
	}
	// Once the main server accepts, the main bridge attaches too.
	mainLn, err := net.Listen("tcp", main)
	if err != nil {
		t.Fatal(err)
	}
	defer mainLn.Close()
	go func() {
		for {
			c, err := mainLn.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	select {
	case <-mainAttached:
	case <-time.After(10 * time.Second):
		t.Fatal("the main bridge did not attach once its server accepted")
	}

	heads := make(chan int, 1)
	errs := make(chan string, 1)
	fns := []js.Func{
		js.FuncOf(func(_ js.Value, a []js.Value) any { heads <- a[0].Int(); return nil }),
		js.FuncOf(func(js.Value, []js.Value) any { return nil }),
		js.FuncOf(func(js.Value, []js.Value) any { return nil }),
		js.FuncOf(func(_ js.Value, a []js.Value) any { errs <- a[0].String(); return nil }),
	}
	for _, f := range fns {
		defer f.Release()
	}
	sink := js.ValueOf(map[string]any{"head": fns[0], "chunk": fns[1], "end": fns[2], "error": fns[3]})
	req := js.ValueOf(map[string]any{"method": "GET", "url": "https://kubernetes.sandbox.invalid/api/v1/namespaces", "headers": []any{}, "body": nil})
	r.bridge.Call("serve", 1, req, sink)
	select {
	case code := <-heads:
		if code != http.StatusUnauthorized {
			t.Fatalf("head %d, want the relayed 401", code)
		}
	case msg := <-errs:
		t.Fatalf("the target's 401 became an error (%q): it went through Forward, not Relay", msg)
	case <-time.After(10 * time.Second):
		t.Fatal("no answer from the target")
	}
}
