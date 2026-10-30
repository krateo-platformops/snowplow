package rum

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// ForwardQueue is how many accepted batches may wait for the collector. A
// batch that finds the queue full is dropped and counted (forward_overflow):
// the request that carried it has already been answered, so a slow or absent
// collector never holds a portal request.
const ForwardQueue = 64

// ForwardTimeout bounds one POST to the collector.
const ForwardTimeout = 5 * time.Second

type batch struct {
	body  []byte // OTLP protobuf ExportTraceServiceRequest
	spans int
}

// Forwarder posts accepted batches to the collector's OTLP/HTTP traces
// endpoint from ONE background worker, in protobuf (the encoding every
// collector OTLP/HTTP receiver accepts). It is deliberately NOT
// tracing.WrapTransport'd: forwarding the browser's spans must not mint
// snowplow Client spans of its own.
type Forwarder struct {
	url    string
	client *http.Client
	queue  chan batch
}

// NewForwarder returns a forwarder for the collector at url (the full
// .../v1/traces URL). Start runs its worker. Its client comes from credhttp
// like every client the module builds (#723/#724): it refuses redirects, so a
// collector address can never bounce the browser's spans elsewhere.
func NewForwarder(url string) *Forwarder {
	return &Forwarder{
		url:    url,
		client: credhttp.Client(&http.Client{Timeout: ForwardTimeout}),
		queue:  make(chan batch, ForwardQueue),
	}
}

// enqueue hands b to the worker without ever blocking; false = queue full.
func (f *Forwarder) enqueue(b batch) bool {
	select {
	case f.queue <- b:
		return true
	default:
		return false
	}
}

// Start runs the worker until ctx is done. Batches still queued then are
// counted as forward_failed: they were accepted and never delivered.
func (f *Forwarder) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				for {
					select {
					case b := <-f.queue:
						spansForwardFailed.Add(uint64(b.spans))
					default:
						return
					}
				}
			case b := <-f.queue:
				f.post(ctx, b)
			}
		}
	}()
}

func (f *Forwarder) post(ctx context.Context, b batch) {
	ctx, cancel := context.WithTimeout(ctx, ForwardTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url, bytes.NewReader(b.body))
	if err != nil {
		spansForwardFailed.Add(uint64(b.spans))
		return
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := f.client.Do(req)
	if err != nil {
		spansForwardFailed.Add(uint64(b.spans))
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		spansForwardFailed.Add(uint64(b.spans))
		return
	}
	spansForwarded.Add(uint64(b.spans))
}
