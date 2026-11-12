//go:build js && wasm

package fakeapiserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"syscall/js"
)

// HostConfig is the global bridge.js defines before it runs the program:
//
//	{snapshot: <snapshot.json text>, pages: [<objects/<n>.json text>, ...],
//	 saToken: <the ServiceAccount token it also puts in the token file>,
//	 visitorToken: <the synthesised clientconfig's token>, port: <number>}
//
// bridge.js has validated the snapshot (snapshot-rules.js) and generated both tokens; they exist
// only in this worker's memory.
const HostConfig = "snowplowFakeApiserver"

// Listen starts the fake apiserver from HostConfig on the in-process loopback network and returns
// its address, for browserbridge to relay snowplow's requests to. Request-log lines go to the
// worker's console as console.debug('fake-env', line).
func Listen() (string, error) {
	cfg := js.Global().Get(HostConfig)
	if cfg.Type() != js.TypeObject {
		return "", errors.New(HostConfig + " is not defined; bridge.js defines it before it runs the program")
	}
	str := func(k string) (string, error) {
		v := cfg.Get(k)
		if v.Type() != js.TypeString || v.String() == "" {
			return "", fmt.Errorf("%s.%s must be a non-empty string", HostConfig, k)
		}
		return v.String(), nil
	}
	snap, err := str("snapshot")
	if err != nil {
		return "", err
	}
	sa, err := str("saToken")
	if err != nil {
		return "", err
	}
	visitor, err := str("visitorToken")
	if err != nil {
		return "", err
	}
	pv := cfg.Get("pages")
	if pv.Type() != js.TypeObject || pv.Length() == 0 {
		return "", errors.New(HostConfig + ".pages must be a non-empty array")
	}
	pages := make([][]byte, pv.Length())
	for i := range pages {
		if pv.Index(i).Type() != js.TypeString {
			return "", fmt.Errorf("%s.pages[%d] must be a string", HostConfig, i)
		}
		pages[i] = []byte(pv.Index(i).String())
	}
	port := cfg.Get("port")
	if port.Type() != js.TypeNumber || port.Int() <= 0 || port.Int() > 65535 {
		return "", errors.New(HostConfig + ".port must be a TCP port number")
	}
	console := js.Global().Get("console")
	jsonParse := js.Global().Get("JSON").Get("parse")
	s, err := New(Options{
		Snapshot: []byte(snap), Pages: pages, SAToken: sa, VisitorToken: visitor,
		Log: func(line map[string]any) {
			if b, err := json.Marshal(line); err == nil {
				console.Call("debug", "fake-env", jsonParse.Invoke(string(b)))
			}
		},
	})
	if err != nil {
		return "", err
	}
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port.Int()))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	go func() { _ = http.Serve(ln, s) }()
	return addr, nil
}
