package cache

// content_digest_1324_test.go — #1324: the digest is deterministic, names the
// bytes, is memoised once per entry, and its cost on the /call path.

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestIssue1324_ContentDigestIsDeterministicAndNamesTheBytes(t *testing.T) {
	a, b := []byte(`{"v":1}`), []byte(`{"v":2}`)
	if ContentDigest(a) != ContentDigest(append([]byte(nil), a...)) {
		t.Fatal("RED: the digest of equal bytes differs (it must survive restarts and replicas)")
	}
	if ContentDigest(a) == ContentDigest(b) {
		t.Fatal("RED: different bytes share a digest")
	}
	// A golden, computed independently (printf '%s' '{"v":1}' | shasum -a 256 |
	// cut -c1-32): the algorithm is part of the wire contract (the doc pins it too).
	if got, want := ContentDigest([]byte(`{"v":1}`)), "v1.afbf9d0f3560b0fd7795e81c42a0a79e"; got != want {
		t.Fatalf("RED: ContentDigest changed (%s, golden %s) — a change of algorithm must bump the v1. prefix", got, want)
	}
}

func TestIssue1324_EntryDigestIsMemoisedOnceAndRaceFree(t *testing.T) {
	e := &ResolvedEntry{RawJSON: []byte(`{"cell":1}`)}
	want := ContentDigest(e.RawJSON)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := e.ContentDigest(); got != want {
				t.Errorf("RED: entry digest %s, want %s", got, want)
			}
		}()
	}
	wg.Wait()
	if p := e.digest.Load(); p == nil || *p != want {
		t.Fatal("RED: the digest was not memoised on the entry")
	}
	var nilEntry *ResolvedEntry
	if nilEntry.ContentDigest() != "" {
		t.Fatal("RED: a nil entry has a digest")
	}
}

// BenchmarkIssue1324_ContentDigest is the per-entry (memo miss) cost: paid once
// per stored entry on its first armed serve, and per widgetContent shell hit
// on the gated bytes.
func BenchmarkIssue1324_ContentDigest(b *testing.B) {
	for _, n := range []int{1 << 10, 16 << 10, 256 << 10, 4 << 20} {
		body := bytes.Repeat([]byte("x"), n)
		b.Run(fmt.Sprintf("%dKiB", n>>10), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = ContentDigest(body)
			}
		})
	}
}

// BenchmarkIssue1324_EntryDigestMemoised is the cost on every later armed hit
// of the same entry (restactions / widgets hits): one atomic load.
func BenchmarkIssue1324_EntryDigestMemoised(b *testing.B) {
	e := &ResolvedEntry{RawJSON: bytes.Repeat([]byte("x"), 256<<10)}
	_ = e.ContentDigest()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = e.ContentDigest()
	}
}
