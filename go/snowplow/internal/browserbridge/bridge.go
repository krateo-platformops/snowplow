//go:build js && wasm

// Package browserbridge serves a web page's requests from snowplow's own
// http.Server when snowplow runs in a browser worker: the js/wasm build, used
// by the Agentiko sandbox. It exists only in that build; a server build
// compiles none of it (main_browser_build_test.go).
//
// The page side is the sandbox's boot/ loader (agentiko-sandbox
// boot/PROTOCOL.md). Its worker imports snowplow-wasm's bridge.js, which runs
// this program and calls the two functions Attach registers. Each request is
// written, as the page sent it, to the server over Go's in-process loopback
// network, so it passes through the server and its whole middleware chain
// exactly as a request from the network does. The response goes back as a
// head, a chunk for every read of the body, and an end or an error. A handler
// that flushes (the /refreshes stream) is read as it writes, so each frame
// reaches the page when it is written, not when the handler returns.
//
// WHAT DOES NOT APPLY IN THE BROWSER BUILD:
//
//   - The page never sees a 401. Every 401 path in the portal leaves the
//     sandbox, and the sandbox's token is the build's own, so a 401 means the
//     token and the JWKS snowplow verifies it with do not match: a build or
//     configuration defect. Forward reports it as an error on that request,
//     with the reason snowplow gave, and sends no head.
//   - Egress, the destination policy and TLS client certificates: see the
//     credhttp package doc. Every outbound request leaves through the
//     browser's fetch.
//   - Shutdown: no signal reaches a worker. The program runs until the worker
//     is terminated.
package browserbridge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Request is one request as the page sent it.
type Request struct {
	Method string
	// URL is absolute (https://snowplow.sandbox.invalid/call?...). Its path
	// and query are sent as they are; its host is sent as the Host header.
	URL    string
	Header [][2]string
	Body   []byte
}

// Sink receives one response: Head once, then Chunk for each body read, then
// exactly one of End or Error. Error without a Head means the request failed
// before it was answered (the page's fetch rejects); after a Head, the body
// failed.
type Sink interface {
	Head(status int, statusText string, header [][2]string)
	Chunk(b []byte)
	End()
	Error(message string)
}

// loopback dials snowplow's own server on Go's in-process network. It has no
// Control hook, so it is not the destination-policy dialer (dialguard): the
// in-process network refuses every dial that carries one (ENOTSUP), and the
// policy is about egress, which this is not. The address is always the
// server's own (Attach).
var loopback = &net.Dialer{}

// chunkBytes bounds one body read. A flushed write the size of an SSE frame
// arrives as one read; a large JSON answer arrives in several.
const chunkBytes = 32 << 10

// unauthorizedBodyBytes bounds how much of a 401's body Forward reads to find
// the reason snowplow gave.
const unauthorizedBodyBytes = 64 << 10

// Forward sends req to the server listening on addr, over the in-process
// network, and streams the response to sink. When ctx is cancelled it closes
// the connection, so the handler's request context ends and a long-lived
// handler returns, and it reports nothing more to sink.
func Forward(ctx context.Context, addr string, req Request, sink Sink) {
	hreq, err := newRequest(ctx, addr, req)
	if err != nil {
		sink.Error(err.Error())
		return
	}
	conn, err := loopback.DialContext(ctx, "tcp", addr)
	if err != nil {
		if ctx.Err() == nil {
			sink.Error("snowplow's in-process server did not accept the connection: " + err.Error())
		}
		return
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	if err := hreq.Write(conn); err != nil {
		if ctx.Err() == nil {
			sink.Error("writing the request to snowplow failed: " + err.Error())
		}
		return
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), hreq)
	if err != nil {
		if ctx.Err() == nil {
			sink.Error("reading snowplow's response failed: " + err.Error())
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		sink.Error(unauthorizedMessage(hreq, resp))
		return
	}

	sink.Head(resp.StatusCode, statusText(resp), headerList(resp.Header))
	buf := make([]byte, chunkBytes)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 && ctx.Err() == nil {
			sink.Chunk(buf[:n])
		}
		if rerr == nil {
			continue
		}
		switch {
		case ctx.Err() != nil:
		case errors.Is(rerr, io.EOF):
			sink.End()
		default:
			sink.Error("snowplow's response body failed: " + rerr.Error())
		}
		return
	}
}

// newRequest builds the request written to the server: the page's method,
// path, query, headers and body, with the page URL's host as Host.
func newRequest(ctx context.Context, addr string, req Request) (*http.Request, error) {
	u, err := url.Parse(req.URL)
	if err != nil || !u.IsAbs() {
		return nil, fmt.Errorf("the bridge was handed %q, which is not an absolute URL", req.URL)
	}
	var body io.Reader
	if len(req.Body) > 0 {
		body = bytes.NewReader(req.Body)
	}
	hreq, err := http.NewRequestWithContext(ctx, req.Method, "http://"+addr+u.RequestURI(), body)
	if err != nil {
		return nil, err
	}
	hreq.Host = u.Host
	for _, kv := range req.Header {
		hreq.Header.Add(kv[0], kv[1])
	}
	// Request.Write adds Go's own User-Agent unless the header is present; an
	// empty one is written as nothing, so the server sees the page's headers
	// and no others.
	if _, ok := hreq.Header["User-Agent"]; !ok {
		hreq.Header["User-Agent"] = []string{""}
	}
	return hreq, nil
}

// unauthorizedMessage is the error a 401 becomes: the route and the reason
// code snowplow gave (details.causes[].reason of its Status), never the
// query, which can carry identity, nor the message, which can name the user.
func unauthorizedMessage(hreq *http.Request, resp *http.Response) string {
	var st struct {
		Details struct {
			Causes []struct {
				Reason string `json:"reason"`
			} `json:"causes"`
		} `json:"details"`
	}
	reason := "no reason given"
	if b, err := io.ReadAll(io.LimitReader(resp.Body, unauthorizedBodyBytes)); err == nil && json.Unmarshal(b, &st) == nil {
		var rs []string
		for _, c := range st.Details.Causes {
			if c.Reason != "" {
				rs = append(rs, c.Reason)
			}
		}
		if len(rs) > 0 {
			reason = strings.Join(rs, ", ")
		}
	}
	return fmt.Sprintf("snowplow answered 401 (%s) to %s %s. The sandbox's token and the JWKS snowplow verifies it with do not match: "+
		"this is a defect in how the sandbox was built or configured, not a sign-in problem, so it is reported as an error and never as a 401.",
		reason, hreq.Method, hreq.URL.Path)
}

// statusText is the reason phrase of the status line ("OK" of "200 OK").
func statusText(resp *http.Response) string {
	return strings.TrimPrefix(resp.Status, strconv.Itoa(resp.StatusCode)+" ")
}

// headerList is h as name/value pairs, one per value, in a stable order.
func headerList(h http.Header) [][2]string {
	out := make([][2]string, 0, len(h))
	for name, values := range h {
		for _, v := range values {
			out = append(out, [2]string{name, v})
		}
	}
	return out
}

// acceptPollInterval is how often WaitAccepting tries the server. The server
// starts listening once main has wired it, in about a second, so this costs a
// few dozen refused dials on the in-process network.
const acceptPollInterval = 10 * time.Millisecond

// WaitAccepting returns once the server on addr accepts a connection, or with
// ctx's error. The server gives no other sign that it listens: main logs that
// it is ready before ListenAndServe has bound the address.
func WaitAccepting(ctx context.Context, addr string) error {
	for {
		conn, err := loopback.DialContext(ctx, "tcp", addr)
		if err == nil {
			return conn.Close()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(acceptPollInterval):
		}
	}
}

// Addr is the loopback address of a server listening on port.
func Addr(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}
