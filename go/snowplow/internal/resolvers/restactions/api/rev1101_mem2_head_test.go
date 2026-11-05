//go:build unit

package api

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

func TestRev1101_MemoryPerClientUnconnected(t *testing.T) {
	p := newRevPKI(t)
	orig := stageEndpointClients
	stageEndpointClients = credhttp.NewEndpointClientsForTest(time.Hour, time.Now)
	defer func() { stageEndpointClients = orig }()
	held := credhttp.WithHeldCredential(context.Background())
	const N = 400
	eps := make([]endpoints.Endpoint, N)
	for i := range eps {
		eps[i], _ = certEP(t, p, "https://127.0.0.1:1", fmt.Sprintf("u%d", i))
	}
	w, _ := certEP(t, p, "https://127.0.0.1:1", "warm")
	_, _ = httpClientForEndpoint(held, &w, &httpcall.RequestInfo{})
	measure := func() (uint64, int) {
		for i := 0; i < 3; i++ {
			runtime.GC()
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return ms.HeapInuse, runtime.NumGoroutine()
	}
	h0, g0 := measure()
	for i := range eps {
		if _, err := httpClientForEndpoint(held, &eps[i], &httpcall.RequestInfo{}); err != nil {
			t.Fatal(err)
		}
	}
	h1, g1 := measure()
	t.Logf("UNCONNECTED: N=%d heap_inuse_delta=%d B (%.1f KiB/client) goroutines_delta=%d entries=%d",
		N, h1-h0, float64(h1-h0)/N/1024, g1-g0, stageEndpointClients.Len())
	runtime.KeepAlive(eps)
}
