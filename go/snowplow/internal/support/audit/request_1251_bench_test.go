package audit

// request_1251_bench_test.go — #1251 review item 5: what audit.Middleware costs
// a request that never emits a record (a GET /call read: the common route).
//
//	go test -tags unit -run '^$' -bench Middleware1251 -benchmem ./internal/support/audit/

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func BenchmarkMiddleware1251_NonEmittingRoute(b *testing.B) {
	h := Middleware()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	req := httptest.NewRequest(http.MethodGet,
		"/call?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=krateo-system&name=compositions-list", nil)
	req.RemoteAddr = "10.12.0.31:52344"
	req.Header.Set("X-Forwarded-For", "203.0.113.7, 198.51.100.2")
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36")
	w := httptest.NewRecorder()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.ServeHTTP(w, req)
	}
}
