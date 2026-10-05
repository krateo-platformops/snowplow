package redact

// lazy_attrs_test.go — #490 review (reviewer-424). The #487 helpers ran
// json.Marshal and the HMAC before slog checked the level: 144 ms/op,
// 95 MB/op and 1.5M allocs/op on a 50K-item dict through a WARN logger, on
// every RESTAction resolve. They are now lazy pointer-shaped LogValuers. These
// arms pin the debug-off cost: zero allocations, at parity with main's
// slog.Any.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
)

func bigDict(n int) map[string]any {
	d := make(map[string]any, n)
	for i := 0; i < n; i++ {
		d[fmt.Sprintf("k%06d", i)] = map[string]any{"name": fmt.Sprintf("item-%d", i), "n": i}
	}
	return d
}

func warnLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func TestS490_LazyAttrsZeroAllocAtWarn(t *testing.T) {
	log := warnLogger()
	ctx := context.Background()
	dict := bigDict(50_000)
	var v any = dict

	for name, f := range map[string]func(){
		"LogAttrs DictAttr":  func() { log.LogAttrs(ctx, slog.LevelDebug, "resolved api", DictAttr("dict", dict)) },
		"LogAttrs ValueAttr": func() { log.LogAttrs(ctx, slog.LevelDebug, "v", ValueAttr("value", &v)) },
		"build DictAttr":     func() { a := DictAttr("dict", dict); _ = a },
		"build ValueAttr":    func() { a := ValueAttr("value", &v); _ = a },
	} {
		if n := testing.AllocsPerRun(200, f); n != 0 {
			t.Errorf("%s at WARN: %v allocs/op, want 0 (the summary must run only inside LogValue)", name, n)
		}
	}

	// Parity through the variadic Debug path with main's slog.Any form: the
	// only allocation is the Attr's boxing into ...any, the same as before.
	mainForm := testing.AllocsPerRun(200, func() { log.Debug("resolved api", slog.Any("dict", dict)) })
	for name, f := range map[string]func(){
		"Debug DictAttr":  func() { log.Debug("resolved api", DictAttr("dict", dict)) },
		"Debug ValueAttr": func() { log.Debug("v", ValueAttr("value", &v)) },
	} {
		if n := testing.AllocsPerRun(200, f); n > mainForm {
			t.Errorf("%s at WARN: %v allocs/op, main's slog.Any form %v", name, n, mainForm)
		}
	}
}

// The C1-style comparison: debug-off cost of the three shapes on a 50K-item
// dict. main = the pre-#487 slog.Any dump; pre490 = #490's eager summary
// (the regression); lazy = this fix.
func BenchmarkS490_DebugOff_Main(b *testing.B) {
	log, ctx, dict := warnLogger(), context.Background(), bigDict(50_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Debug("resolved api", slog.Any("dict", dict))
	}
	_ = ctx
}

func BenchmarkS490_DebugOff_EagerPre490(b *testing.B) {
	log, dict := warnLogger(), bigDict(50_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Debug("resolved api", slog.Attr{Key: "dict", Value: dictRef(dict).LogValue()})
	}
}

func BenchmarkS490_DebugOff_Lazy(b *testing.B) {
	log, ctx, dict := warnLogger(), context.Background(), bigDict(50_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.LogAttrs(ctx, slog.LevelDebug, "resolved api", DictAttr("dict", dict))
	}
}

func BenchmarkS490_DebugOff_LazyVariadic(b *testing.B) {
	log, dict := warnLogger(), bigDict(50_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Debug("resolved api", DictAttr("dict", dict))
	}
}
