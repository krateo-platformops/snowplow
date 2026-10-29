package handlers

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/metrics"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

// #970 — peak memory of a 50,000-row x 10-column export, end to end through
// the handler: the buffered /call envelope, the decoded rows, the
// flattened cells and the built document. The PDF row is set against the
// CSV row of the same export, so the PDF's own cost reads directly.
//
//	go test ./internal/handlers -run '^$' -bench Export970_50Kx10 -benchtime 3x
//
// peak-heap-MiB is the highest live+unswept heap seen during one export
// above the heap before it (runtime/metrics, sampled every 100µs).

func envelope50Kx10() []byte {
	var b strings.Builder
	b.WriteString(`{"items":[`)
	for r := range 50000 {
		if r > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('{')
		for c := range 10 {
			if c > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `"c%d":"row-%05d-col-%d-value"`, c, r, c)
		}
		b.WriteByte('}')
	}
	b.WriteString(`]}`)
	return []byte(b.String())
}

// peakHeap runs f and returns the peak heap-object bytes above the
// starting heap.
func peakHeap(f func()) uint64 {
	runtime.GC()
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	base := sample[0].Value.Uint64()
	var peak atomic.Uint64
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		tick := time.NewTicker(100 * time.Microsecond)
		defer tick.Stop()
		for {
			metrics.Read(s)
			if v := s[0].Value.Uint64(); v > peak.Load() {
				peak.Store(v)
			}
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()
	f()
	close(stop)
	<-done
	if p := peak.Load(); p > base {
		return p - base
	}
	return 0
}

func benchmarkExport50Kx10(b *testing.B, format string) {
	body := envelope50Kx10()
	call := http.HandlerFunc(func(wri http.ResponseWriter, _ *http.Request) {
		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		_, _ = wri.Write(body)
	})
	h := Export(call)
	var worst uint64
	var size int
	b.ResetTimer()
	for range b.N {
		req := httptest.NewRequest(http.MethodGet, "/export?resource=restactions&name=bench&format="+format+"&page=1&perPage=50000", nil)
		req = req.WithContext(xcontext.BuildContext(req.Context(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: "bench"})))
		var rec *httptest.ResponseRecorder
		p := peakHeap(func() {
			rec = httptest.NewRecorder()
			h.ServeHTTP(rec, req)
		})
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
		worst = max(worst, p)
		size = rec.Body.Len()
	}
	b.ReportMetric(float64(worst)/(1<<20), "peak-heap-MiB")
	b.ReportMetric(float64(size)/(1<<20), "MiB-out")
	b.ReportMetric(float64(len(body))/(1<<20), "MiB-call-envelope")
}

func BenchmarkExport970_50Kx10_PDF(b *testing.B) { benchmarkExport50Kx10(b, "pdf") }
func BenchmarkExport970_50Kx10_CSV(b *testing.B) { benchmarkExport50Kx10(b, "csv") }
