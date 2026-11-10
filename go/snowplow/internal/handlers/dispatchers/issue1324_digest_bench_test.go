package dispatchers

// issue1324_digest_bench_test.go — #1324: the digest's cost where it is paid.
//
//   - widgetContent shell hit: the gate it already runs, then one SHA-256 of
//     the gated bytes (no memo: the bytes are per requester). Compare
//     BenchmarkIssue1324_ShellHit/gate with /gate+digest.
//   - restactions / widgets hit: the entry's memoised digest, one atomic load
//     after the entry's first armed serve (cache BenchmarkIssue1324_*).
//   - arm: BehindKeys over an armed set of 40 widgets keys and 10 widgetContent
//     keys, every one held current (the common case: nothing behind).
//
// Run: go test -run '^$' -bench Issue1324 ./internal/handlers/dispatchers/

import (
	"context"
	"fmt"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

func b1324Shell(n int) []byte {
	items := make([]map[string]any, n)
	for i := range items {
		items[i] = map[string]any{
			"id":      fmt.Sprintf("item-%d", i),
			"path":    "/call?resource=compositions&apiVersion=composition.krateo.io/v1&namespace=fireworks-app&name=comp-x",
			"verb":    "GET",
			"allowed": true,
		}
	}
	return fakeEnvelope(items)
}

func b1324Ctx() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "admin", Groups: []string{"system:authenticated"}}))
}

func BenchmarkIssue1324_ShellHit(b *testing.B) {
	cache.CompleteRBACSnapshotForTest(b) // the in-process RBAC path, as in production
	raw := b1324Shell(20)
	ctx := b1324Ctx()
	b.Run("gate", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, ok := gateWidgetEnvelope(ctx, raw); !ok {
				b.Fatal("gate failed")
			}
		}
	})
	b.Run("gate+digest", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			gated, ok := gateWidgetEnvelope(ctx, raw)
			if !ok {
				b.Fatal("gate failed")
			}
			_ = cache.ContentDigest(gated)
		}
	})
}

func BenchmarkIssue1324_ArmBehindKeys_40Widgets10Shells(b *testing.B) {
	b.Setenv("CACHE_ENABLED", "true")
	b.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	b.Cleanup(cache.ResetResolvedCacheForTest)
	cache.CompleteRBACSnapshotForTest(b) // the store admits writes; the gate evaluates in-process
	store := cache.ResolvedCache()
	ctx := b1324Ctx()
	held := map[string]HeldDigests{}
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("w-%d", i)
		body := []byte(fmt.Sprintf(`{"widget":%d,"rows":[%s]}`, i, b1324Rows(50)))
		store.PutResolved(fixtureStoreVerdict, k, &cache.ResolvedEntry{RawJSON: body, Inputs: &cache.ResolvedKeyInputs{CacheEntryClass: "widgets"}})
		held[k] = HeldDigests{Class: "widgets", Digests: []string{cache.ContentDigest(body)}}
	}
	shell := b1324Shell(20)
	gated, _ := gateWidgetEnvelope(ctx, shell)
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("wc-%d", i)
		store.PutResolved(fixtureStoreVerdict, k, &cache.ResolvedEntry{RawJSON: shell, Inputs: &cache.ResolvedKeyInputs{CacheEntryClass: cache.CacheEntryClassWidgetContent}})
		held[k] = HeldDigests{Class: cache.CacheEntryClassWidgetContent, Digests: []string{cache.ContentDigest(gated)}}
	}
	if got := BehindKeys(ctx, held); len(got) != 0 {
		b.Fatalf("PRE: %d keys behind, want none", len(got))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = BehindKeys(ctx, held)
	}
}

func b1324Rows(n int) string {
	s := ""
	for i := 0; i < n; i++ {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf(`{"name":"comp-%d","status":"Ready"}`, i)
	}
	return s
}
