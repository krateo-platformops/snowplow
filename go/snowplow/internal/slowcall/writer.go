package slowcall

// writer.go — the response path's time (#1054 F4): the network write and
// the compression in front of it.
//
// Two writers, both inside the request's one allocation (recordCtx):
//   - netWriter, installed by Middleware OUTSIDE the compressor: every byte
//     that reaches the connection. Its time is PhaseWrite (a slow reader, a
//     full socket buffer).
//   - bodyWriter, installed by Body INSIDE the compressor: every byte the
//     handler writes. Its time minus the network writes made during it is
//     compression. The compressor also flushes and closes after the handler
//     returns; that tail, minus its network writes, is compression too.
// So PhaseCompress is gzip's own time, PhaseWrite the network's, and neither
// counts the other.

import (
	"net/http"
	"time"
)

type netWriter struct {
	http.ResponseWriter
	r *Record
}

func (w *netWriter) timed(t0 time.Time) {
	d := int64(time.Since(t0))
	a := &w.r.ph[PhaseWrite]
	a.ns.Add(d)
	a.n.Add(1)
	storeMax(&a.max, d)
}

func (w *netWriter) Write(p []byte) (int, error) {
	t0 := time.Now()
	n, err := w.ResponseWriter.Write(p)
	w.timed(t0)
	return n, err
}

func (w *netWriter) WriteHeader(code int) {
	t0 := time.Now()
	w.ResponseWriter.WriteHeader(code)
	w.timed(t0)
}

func (w *netWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		t0 := time.Now()
		f.Flush()
		w.timed(t0)
	}
}

// Unwrap lets http.ResponseController reach the connection's writer.
func (w *netWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type bodyWriter struct {
	http.ResponseWriter
	r *Record
}

func (w *bodyWriter) timed(t0 time.Time, net0 int64) {
	d := int64(time.Since(t0)) - (w.r.ph[PhaseWrite].ns.Load() - net0)
	if d < 0 {
		d = 0
	}
	a := &w.r.ph[PhaseCompress]
	a.ns.Add(d)
	a.n.Add(1)
	storeMax(&a.max, d)
}

func (w *bodyWriter) Write(p []byte) (int, error) {
	net0, t0 := w.r.ph[PhaseWrite].ns.Load(), time.Now()
	n, err := w.ResponseWriter.Write(p)
	w.timed(t0, net0)
	return n, err
}

func (w *bodyWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		net0, t0 := w.r.ph[PhaseWrite].ns.Load(), time.Now()
		f.Flush()
		w.timed(t0, net0)
	}
}

func (w *bodyWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Body times the handler's writes into the response compressor. It must run
// INSIDE the compressor (main: Middleware → Gzip → Body → mux). On a request
// without a record it is a pass-through.
func Body(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc, _ := r.Context().Value(recordCtxKey{}).(*recordCtx)
		if rc == nil {
			next.ServeHTTP(w, r)
			return
		}
		rec := &rc.rec
		rc.inner = bodyWriter{ResponseWriter: w, r: rec}
		rec.bodyRan = true
		next.ServeHTTP(&rc.inner, r)
		rec.handlerEnd = time.Now()
		rec.netAtEnd = rec.ph[PhaseWrite].ns.Load()
	})
}

// compressTail adds the compressor's work after the handler returned (its
// final flush and close), net of the network writes in it.
func (r *Record) compressTail() {
	if !r.bodyRan {
		return
	}
	d := int64(time.Since(r.handlerEnd)) - (r.ph[PhaseWrite].ns.Load() - r.netAtEnd)
	if d <= 0 {
		return
	}
	a := &r.ph[PhaseCompress]
	a.ns.Add(d)
	a.n.Add(1)
	storeMax(&a.max, d)
}
