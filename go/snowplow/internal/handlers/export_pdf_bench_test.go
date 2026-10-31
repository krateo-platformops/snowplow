package handlers

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

// #970 — peak memory of a ~50,000-row x 10-column export, end to end
// through the handler: the buffered /call envelopes, the decoded rows, the
// cells and the built document. pdf is set against csv of the same export.
//
//	go test ./internal/handlers -run '^$' -bench Export970_Peak -benchtime 2x
//
// Two shapes:
//   - window:   one /call window of 50,000 rows (page=1&perPage=50000);
//   - fullList: the scheduled-export shape, no page/perPage, 49,999 rows
//     over 100 internal 500-row re-dispatches (one short of the bound).
//
// Two cell sizes: short (~22 characters) and 200 characters.
//
// The response goes to a writer that counts and discards the bytes, as a
// network connection would; an httptest.ResponseRecorder would add a copy
// of the body to the measurement.
//
// Two peaks, above the heap before the export (runtime/metrics, sampled
// every 100µs): peak-heap-MiB is the highest heap of live and not yet
// collected objects (what the process holds); peak-live-MiB is the highest
// heap the collector found live at the end of a cycle (what the export
// retains). The first is up to ~2x the second at GOGC=100.

// cellValue is row r, column c, padded to width characters (0: unpadded).
func cellValue(r, c, width int) string {
	v := fmt.Sprintf("row-%05d-col-%d-value", r, c)
	if width > len(v) {
		v += strings.Repeat("x", width-len(v))
	}
	return v
}

// envelopeRows is a /call envelope holding rows [from, to).
func envelopeRows(from, to, width int) []byte {
	var b strings.Builder
	b.WriteString(`{"items":[`)
	for r := from; r < to; r++ {
		if r > from {
			b.WriteByte(',')
		}
		b.WriteByte('{')
		for c := range 10 {
			if c > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `"c%d":%q`, c, cellValue(r, c, width))
		}
		b.WriteByte('}')
	}
	b.WriteString(`]}`)
	return []byte(b.String())
}

// peakHeap runs f and returns the peak heap-object bytes and the peak
// live-heap bytes, each above its value before f.
func peakHeap(f func()) (heap, live uint64) {
	runtime.GC()
	names := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}, {Name: "/gc/heap/live:bytes"}}
	metrics.Read(names)
	base := [2]uint64{names[0].Value.Uint64(), names[1].Value.Uint64()}
	var peak [2]atomic.Uint64
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}, {Name: "/gc/heap/live:bytes"}}
		tick := time.NewTicker(100 * time.Microsecond)
		defer tick.Stop()
		for {
			metrics.Read(s)
			for i := range s {
				if v := s[i].Value.Uint64(); v > peak[i].Load() {
					peak[i].Store(v)
				}
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
	above := func(i int) uint64 {
		if p := peak[i].Load(); p > base[i] {
			return p - base[i]
		}
		return 0
	}
	return above(0), above(1)
}

// discardWriter is a ResponseWriter that counts the body and keeps none of it.
type discardWriter struct {
	header http.Header
	code   int
	n      int
}

func (w *discardWriter) Header() http.Header { return w.header }
func (w *discardWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
}
func (w *discardWriter) Write(p []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	w.n += len(p)
	return len(p), nil
}

func BenchmarkExport970_Peak(b *testing.B) {
	for _, width := range []int{0, 200} {
		cells := "short"
		if width > 0 {
			cells = fmt.Sprintf("cell%d", width)
		}
		window := envelopeRows(0, 50000, width)
		pages := make([][]byte, 0, exportMaxPages)
		for p := 0; p < exportMaxPages; p++ {
			pages = append(pages, envelopeRows(p*exportPageSize, min((p+1)*exportPageSize, 49999), width))
		}
		shapes := []struct {
			name, query string
			call        http.Handler
		}{
			{"window50000", "&page=1&perPage=50000", http.HandlerFunc(func(wri http.ResponseWriter, _ *http.Request) {
				wri.WriteHeader(http.StatusOK)
				_, _ = wri.Write(window)
			})},
			{"fullList49999", "", http.HandlerFunc(func(wri http.ResponseWriter, req *http.Request) {
				page, _ := strconv.Atoi(req.URL.Query().Get("page"))
				wri.WriteHeader(http.StatusOK)
				_, _ = wri.Write(pages[page-1])
			})},
		}
		for _, shape := range shapes {
			for _, format := range []string{"pdf", "csv"} {
				b.Run(format+"/"+shape.name+"/"+cells, func(b *testing.B) {
					h := Export(shape.call)
					var worst, worstLive uint64
					var size int
					for range b.N {
						req := httptest.NewRequest(http.MethodGet, "/export?resource=restactions&name=bench&format="+format+shape.query, nil)
						req = req.WithContext(xcontext.BuildContext(req.Context(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: "bench"})))
						w := &discardWriter{header: http.Header{}}
						p, live := peakHeap(func() { h.ServeHTTP(w, req) })
						if w.code != http.StatusOK {
							b.Fatalf("status %d", w.code)
						}
						worst, worstLive = max(worst, p), max(worstLive, live)
						size = w.n
					}
					b.ReportMetric(float64(worst)/(1<<20), "peak-heap-MiB")
					b.ReportMetric(float64(worstLive)/(1<<20), "peak-live-MiB")
					b.ReportMetric(float64(size)/(1<<20), "MiB-out")
				})
			}
		}
	}
}
