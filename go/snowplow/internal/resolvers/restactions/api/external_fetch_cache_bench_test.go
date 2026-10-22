//go:build unit
// +build unit

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
)

// BenchmarkExternalYAMLFetchConvert — one read of an external YAML document,
// fetched over the real wire and converted, before and after the
// content-addressed conversion memo. Every variant performs the fetch.
//
//	before:     httpFetchAllowingNonJSON: full body + YAMLToJSON per read
//	after-200:  the layer, upstream sends the full body every time (no
//	            validator): full body + sha256, conversion reused
//	after-304:  the layer, upstream sends an ETag: a conditional GET per read,
//	            answered 304, the bytes last seen for it reused
//
// Run with a fixed -benchtime Nx (e.g. 300x): plumbing's client opens a new
// connection per request, and a time-based run exhausts local ports.
func BenchmarkExternalYAMLFetchConvert(b *testing.B) {
	b.Setenv("CACHE_ENABLED", "true")
	doc := benchHelmIndex(400)
	b.Logf("document: %d bytes of YAML", len(doc))

	serve := func(etag string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if etag != "" {
				w.Header().Set("ETag", etag)
				if r.Header.Get("If-None-Match") == etag {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
			w.Header().Set("Content-Type", "text/yaml")
			_, _ = w.Write([]byte(doc))
		}))
	}
	opts := func(url string) httpcall.RequestOptions {
		ep := endpoints.Endpoint{ServerURL: url}
		return httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: "/index.yaml", Verb: ptr.To(http.MethodGet)}, Endpoint: &ep}
	}
	run := func(b *testing.B, fetch func() ([]byte, error)) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			body, err := fetch()
			if err != nil || len(body) == 0 {
				b.Fatalf("fetch: %v (%d bytes)", err, len(body))
			}
		}
	}
	layer := func(b *testing.B, etag string) {
		srv := serve(etag)
		defer srv.Close()
		c := newExternalFetchCache()
		c.budget = func(int64) int64 { return 1 << 30 }
		o := opts(srv.URL)
		run(b, func() ([]byte, error) {
			_, body, _, err := c.do(context.Background(), o)
			return body, err
		})
	}

	b.Run("before", func(b *testing.B) {
		srv := serve("")
		defer srv.Close()
		o := opts(srv.URL)
		run(b, func() ([]byte, error) {
			_, body, _, err := httpFetchAllowingNonJSON(context.Background(), o)
			return body, err
		})
	})
	b.Run("after-200", func(b *testing.B) { layer(b, "") })
	b.Run("after-304", func(b *testing.B) { layer(b, `"v1"`) })
}

// benchHelmIndex builds a Helm repository index with n charts of 5 versions.
func benchHelmIndex(n int) string {
	var sb strings.Builder
	sb.WriteString("apiVersion: v1\nentries:\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, "  chart-%d:\n", i)
		for v := 0; v < 5; v++ {
			fmt.Fprintf(&sb, "    - name: chart-%d\n      version: 1.%d.0\n      appVersion: \"2.%d.0\"\n", i, v, v)
			fmt.Fprintf(&sb, "      description: A chart used to measure the external fetch-and-convert path\n")
			fmt.Fprintf(&sb, "      digest: %064d\n      created: \"2026-10-01T00:00:00Z\"\n", i*10+v)
			fmt.Fprintf(&sb, "      urls:\n        - https://charts.example.test/chart-%d-1.%d.0.tgz\n", i, v)
			fmt.Fprintf(&sb, "      keywords: [krateo, bench, yaml]\n")
		}
	}
	sb.WriteString("generated: \"2026-10-22T00:00:00Z\"\n")
	return sb.String()
}
