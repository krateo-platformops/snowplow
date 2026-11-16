// refused_read_metrics.go — #1116: snowplow_refused_read_total{class}, one
// count per read item a dial guard refused (undialled_write.go
// failUndialledWrite), by the #871 work class of the resolve that refused it.
//
// WHY BY CLASS. A refused read is a stage failure: the body around it is not
// stored. A keep-warm or seed resolve that refuses reads routinely (a
// templated path with an extras key it does not have) builds no cell any real
// request reads, which is its own finding; a customer resolve that refuses
// reads is a misconfigured page. The class tells the two apart on 057 and in
// the bench. Every cell is present, so a 0 is a reading.
//
// Ungated on the cache mode (listed in the CFG-1 nonCacheInitPublishers
// exception): the refusal happens on every RESTAction resolve.
package api

import (
	"context"
	"expvar"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/workclass"
)

// refusedReadNone is the cell of a resolve inside no labelled unit (a test,
// a caller that labels nothing).
const refusedReadNone = "none"

var refusedRead = func() map[string]*atomic.Uint64 {
	m := map[string]*atomic.Uint64{refusedReadNone: {}}
	for _, c := range workclass.Classes {
		m[c.String()] = &atomic.Uint64{}
	}
	return m
}()

// recordRefusedRead counts one refused read item under ctx's work class.
func recordRefusedRead(ctx context.Context) {
	name := workclass.Of(ctx).String()
	if name == "" {
		name = refusedReadNone
	}
	if c, ok := refusedRead[name]; ok {
		c.Add(1)
	}
}

// RefusedReadSnapshot returns the {class} counts.
func RefusedReadSnapshot() map[string]uint64 {
	out := make(map[string]uint64, len(refusedRead))
	for k, v := range refusedRead {
		out[k] = v.Load()
	}
	return out
}

var refusedReadExpvarOnce sync.Once

func init() { registerRefusedReadExpvar() }

func registerRefusedReadExpvar() {
	refusedReadExpvarOnce.Do(func() {
		expvar.Publish("snowplow_refused_read_total", expvar.Func(func() any {
			return RefusedReadSnapshot()
		}))
	})
}
